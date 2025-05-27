// Import necessary libraries
require("dotenv").config();
const axios = require("axios");
const WebSocket = require("ws"); // Still needed for liquidations
const { Telegraf } = require("telegraf");
const { Decimal } = require("decimal.js");

// --- Configuration (Loaded from .env file or defaults) --- //
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const PARES_MONITORADOS = (process.env.PARES_MONITORADOS || "BTCUSDT,ETHUSDT").split(",");
const INTERVALO_VERIFICACAO_MS = parseInt(process.env.INTERVALO_VERIFICACAO_MS || "300000", 10); // 5 minutes default
const PERIODO_DADOS_LSR = process.env.PERIODO_DADOS_LSR || "5m";
const LIMITE_LSR_LONG = new Decimal(process.env.LIMITE_LSR_LONG || "2.0"); // From alertasv3
const TEMPO_MAX_LIQUIDACAO_MIN = parseInt(process.env.TEMPO_MAX_LIQUIDACAO_MIN || "10", 10);

// --- EMA 3m Config (Using REST API Kline Data) --- //
const EMA_3M_TIMEFRAME = "3m";
const EMA_3M_PERIOD_1 = 55;
const EMA_3M_PERIOD_2 = 233;
const KLINE_LIMIT_EMA_3M = 300; // Number of 3m klines to fetch via REST

// Binance API URLs
const BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com";
const BINANCE_FUTURES_WS_BASE_URL = "wss://fstream.binance.com";

// --- Basic Validation --- //
if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error("ERRO: Variáveis de ambiente TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID devem ser definidas!");
    process.exit(1);
}

// --- Initialize Telegraf Bot --- //
const bot = new Telegraf(TELEGRAM_BOT_TOKEN);

// --- State Variables --- //
let dadosAnteriores = {}; // Stores previous OI, Volume 24h, LSR
let contadoresAlertas = {}; // Stores alert counts per pair
let ultimasLiquidacoes = {}; // Stores last liquidation event per pair (from WS)
let webSockets = {}; // Stores active WebSocket instances { forceOrder: {} }
// No need for ultimosKlines state variable anymore

// --- Helper Functions --- //

function formatDecimal(value, places = 4) {
    if (value === null || value === undefined || !(value instanceof Decimal)) return "N/A";
    try { return value.toDecimalPlaces(places).toString(); } catch (e) { return "N/A"; }
}

// --- WebSocket Connection Function (Only for Liquidations) --- //

function conectarWebSocketLiquidacao(par) {
    const wsUrl = `${BINANCE_FUTURES_WS_BASE_URL}/ws/${par.toLowerCase()}@forceOrder`;
    const ws = new WebSocket(wsUrl);
    const wsKey = `forceOrder_${par}`;

    ws.on("open", () => {
        console.log(`[WS LIQ ${par}] Conectado.`);
        webSockets[wsKey] = ws;
    });

    ws.on("message", (data) => {
        try {
            const message = JSON.parse(data);
            if (message.e === "forceOrder") {
                ultimasLiquidacoes[par] = message.o; // Store the order details
                // console.log(`[WS LIQ ${par}] Liquidação recebida:`, message.o);
            }
        } catch (error) {
            console.error(`[WS LIQ ${par}] Erro ao processar mensagem:`, error);
        }
    });

    ws.on("error", (error) => {
        console.error(`[WS LIQ ${par}] Erro no WebSocket:`, error);
    });

    ws.on("close", () => {
        console.log(`[WS LIQ ${par}] Desconectado. Tentando reconectar em 5s...`);
        delete webSockets[wsKey];
        setTimeout(() => conectarWebSocketLiquidacao(par), 5000);
    });
}

// --- Calculation Functions --- //

function calculateEMA(closes, period) {
    if (!closes || closes.length < period) return null;
    const multiplier = new Decimal(2).dividedBy(period + 1);
    let sma = new Decimal(0);
    // Calculate initial SMA
    for (let i = 0; i < period; i++) {
        sma = sma.plus(closes[i]);
    }
    let ema = sma.dividedBy(period);
    // Calculate EMA for the rest
    for (let i = period; i < closes.length; i++) {
        ema = (closes[i].minus(ema)).times(multiplier).plus(ema);
    }
    return ema;
}

// Function from alertasv3.txt to detect abnormal volume
function detectarVolumeAnormal(volumes) {
    if (!volumes || volumes.length < 21) return false; // Need 20 previous volumes + current
    const recentVolumes = volumes.slice(-21, -1); // Get the 20 volumes before the last one
    if (recentVolumes.length === 0) return false;

    const media = recentVolumes.reduce((sum, v) => sum.plus(v), new Decimal(0)).dividedBy(recentVolumes.length);
    // Calculate standard deviation
    const variance = recentVolumes.map(v => v.minus(media).pow(2)).reduce((sum, sq) => sum.plus(sq), new Decimal(0)).dividedBy(recentVolumes.length);
    const desvio = variance.sqrt();

    const ultimoVolume = volumes[volumes.length - 1];
    const limiteAnormal = media.plus(desvio.times(2)); // Mean + 2 * StdDev

    // console.log(`[Vol Anormal] Ultimo: ${formatDecimal(ultimoVolume, 2)}, Media: ${formatDecimal(media, 2)}, Desvio: ${formatDecimal(desvio, 2)}, Limite: ${formatDecimal(limiteAnormal, 2)}`);

    return ultimoVolume.greaterThan(limiteAnormal);
}

// --- Data Fetching and Processing --- //

async function getKlinesRest(symbol, interval, limit) {
    try {
        const response = await axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/klines`, {
            params: { symbol, interval, limit }
        });
        // Return closes and quote volumes as Decimal arrays
        const closes = response.data.map(kline => new Decimal(kline[4]));
        const volumes = response.data.map(kline => new Decimal(kline[7])); // Quote Asset Volume
        return { closes, volumes };
    } catch (error) {
        console.error(`[REST KLINE ${symbol}] Erro ao buscar klines ${interval}:`, error.response ? error.response.data : error.message);
        return null;
    }
}

async function obterDadosRest(par) {
    try {
        // Fetch Ticker (for 24h vol), Open Interest, LSR, AND 3m Klines via REST
        const [tickerRes, oiRes, lsrRes, klines3mRes] = await Promise.all([
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/ticker/24hr`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/openInterest`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/futures/data/globalLongShortAccountRatio`, { params: { symbol: par, period: PERIODO_DADOS_LSR } }),
            getKlinesRest(par, EMA_3M_TIMEFRAME, KLINE_LIMIT_EMA_3M) // Fetch 3m klines here
        ]);

        if (!klines3mRes) { // Handle error from getKlinesRest
             console.error(`[REST ${par}] Falha ao obter Klines 3m.`);
             return null;
        }

        const tickerInfo = tickerRes.data;
        const oiInfo = oiRes.data;
        const lsrInfo = lsrRes.data;

        const volume24h = new Decimal(tickerInfo.quoteVolume);
        const openInterest = new Decimal(oiInfo.openInterest);
        let lsrAtual = null;
        if (lsrInfo && lsrInfo.length > 0) {
            lsrAtual = new Decimal(lsrInfo[lsrInfo.length - 1].longShortRatio);
        } else {
            console.warn(`[REST ${par}] Não foi possível obter LSR.`);
        }

        return {
            volume_24h: volume24h,
            open_interest: openInterest,
            lsr: lsrAtual,
            klines_3m: klines3mRes // Include fetched klines { closes, volumes }
        };

    } catch (error) {
        console.error(`[REST ${par}] Falha ao obter dados (Ticker/OI/LSR/Klines):`, error.response ? error.response.data : error.message);
        return null;
    }
}

// --- Alerting Function --- //

async function enviarAlertaTelegram(mensagem) {
    try {
        await bot.telegram.sendMessage(TELEGRAM_CHAT_ID, mensagem, { parse_mode: "Markdown" });
        console.log(`Alerta enviado: ${mensagem.substring(0, 60)}...`);
    } catch (error) {
        console.error(`Erro ao enviar alerta para o Telegram: ${error}`);
    }
}

// --- Main Verification Loop --- //

async function verificarPares() {
    console.log(`\n[${new Date().toISOString()}] Iniciando verificação dos pares...`);

    for (const par of PARES_MONITORADOS) {
        console.log(`--- Verificando ${par} ---`);

        // 1. Get REST Data (OI, LSR, Vol24h, Klines 3m)
        const dadosRest = await obterDadosRest(par);
        if (!dadosRest || !dadosRest.klines_3m) {
            console.warn(`[${par}] Pulando verificação devido a erro nos dados REST.`);
            continue;
        }

        // 2. Calculate EMAs and Volume Anomaly from REST Kline Data
        const klineData = dadosRest.klines_3m;
        if (klineData.closes.length < EMA_3M_PERIOD_2) { // Check if enough klines were fetched
            console.log(`[${par}] Dados de kline insuficientes via REST (${klineData.closes.length}/${EMA_3M_PERIOD_2}). Pulando cálculo de EMAs.`);
            continue;
        }

        const preco_3m_atual = klineData.closes[klineData.closes.length - 1];
        const ema_55_3m = calculateEMA(klineData.closes, EMA_3M_PERIOD_1);
        const ema_233_3m = calculateEMA(klineData.closes, EMA_3M_PERIOD_2);
        const volumeAnormal = detectarVolumeAnormal(klineData.volumes);

        if (preco_3m_atual === null || ema_55_3m === null || ema_233_3m === null) {
             console.warn(`[${par}] Não foi possível calcular EMAs ou obter preço atual a partir dos dados REST.`);
             continue;
        }

        // 3. Get Previous Data
        const anterior = dadosAnteriores[par];
        if (!anterior) {
            console.log(`[${par}] Armazenando dados iniciais (OI, Vol24h, LSR).`);
            dadosAnteriores[par] = { // Store only necessary previous data
                open_interest: dadosRest.open_interest,
                volume_24h: dadosRest.volume_24h,
                lsr: dadosRest.lsr
            };
            continue;
        }

        // 4. Check Conditions for LONG Alert (Based on alertasv3.txt)
        const lsrValido = dadosRest.lsr !== null && anterior.lsr !== null;

        const oiSubindo = dadosRest.open_interest.greaterThan(anterior.open_interest);
        const volume24hSubindo = dadosRest.volume_24h.greaterThan(anterior.volume_24h);
        const lsrCaindo = lsrValido && dadosRest.lsr.lessThan(anterior.lsr);
        const lsrAbaixoLimite = lsrValido && dadosRest.lsr.lessThan(LIMITE_LSR_LONG);
        const precoAcimaEMAs = preco_3m_atual.greaterThan(ema_55_3m) && preco_3m_atual.greaterThan(ema_233_3m);

        const filtrosLongAtendidos = oiSubindo && volume24hSubindo && lsrCaindo && lsrAbaixoLimite && precoAcimaEMAs;

        // --- DEBUG LOGS --- //
        console.log(`[DEBUG ${par}] Preço 3m (REST): ${formatDecimal(preco_3m_atual)}`);
        console.log(`[DEBUG ${par}] EMA55: ${formatDecimal(ema_55_3m)}, EMA233: ${formatDecimal(ema_233_3m)}`);
        console.log(`[DEBUG ${par}] Preço > EMAs: ${precoAcimaEMAs}`);
        console.log(`[DEBUG ${par}] OI: ${formatDecimal(anterior.open_interest, 0)} -> ${formatDecimal(dadosRest.open_interest, 0)} (Subindo: ${oiSubindo})`);
        console.log(`[DEBUG ${par}] Vol 24h: ${formatDecimal(anterior.volume_24h, 0)} -> ${formatDecimal(dadosRest.volume_24h, 0)} (Subindo: ${volume24hSubindo})`);
        if (lsrValido) {
            console.log(`[DEBUG ${par}] LSR: ${formatDecimal(anterior.lsr)} -> ${formatDecimal(dadosRest.lsr)} (Caindo: ${lsrCaindo})`);
            console.log(`[DEBUG ${par}] LSR < ${LIMITE_LSR_LONG}: ${lsrAbaixoLimite}`);
        } else {
            console.log(`[DEBUG ${par}] LSR: Inválido`);
        }
        console.log(`[DEBUG ${par}] Volume Anormal (3m REST): ${volumeAnormal}`);
        console.log(`[DEBUG ${par}] Filtros LONG Atendidos: ${filtrosLongAtendidos}`);
        // --- DEBUG LOGS END --- //

        // 5. Send Alert if Conditions Met
        if (filtrosLongAtendidos) {
            if (!contadoresAlertas[par]) contadoresAlertas[par] = 0;
            contadoresAlertas[par]++;
            const numeroAlerta = contadoresAlertas[par];
            const alertaEmoji = volumeAnormal ? "💥" : "🟢"; // Use 💥 if volume is abnormal

            // Check for recent liquidation (from WebSocket)
            let liquidacaoRecenteInfo = "Nenhuma Recente";
            const ultimaLiquidacao = ultimasLiquidacoes[par];
            if (ultimaLiquidacao) {
                const agora = Date.now();
                const tempoLimiteMs = TEMPO_MAX_LIQUIDACAO_MIN * 60 * 1000;
                if (agora - ultimaLiquidacao.T <= tempoLimiteMs) {
                    const lado = ultimaLiquidacao.S; // Side (SELL or BUY)
                    const tipoLiquidado = lado === "BUY" ? "Short" : "Long";
                    const precoLiquidacao = formatDecimal(new Decimal(ultimaLiquidacao.ap), 2);
                    const quantidade = formatDecimal(new Decimal(ultimaLiquidacao.q), 4);
                    liquidacaoRecenteInfo = ` ${tipoLiquidado} @ ${precoLiquidacao} (Qtd: ${quantidade})`;
                    delete ultimasLiquidacoes[par]; // Consume the liquidation info after reporting
                }
            }

            const mensagem = 
`${alertaEmoji} *Analisar Long #${numeroAlerta}* 💁🏼‍♀️

*Par:* \`${par}\`
*Preço:* $${formatDecimal(preco_3m_atual)}
*Liquidação Recente:* ${liquidacaoRecenteInfo}
*Condições Atendidas:*
  ✅ OI Subindo (${formatDecimal(anterior.open_interest, 0)} -> ${formatDecimal(dadosRest.open_interest, 0)})
  ✅ Volume 24h Subindo (${formatDecimal(anterior.volume_24h, 0)} -> ${formatDecimal(dadosRest.volume_24h, 0)})
  ✅ LSR (${PERIODO_DADOS_LSR}) Caindo (${formatDecimal(anterior.lsr)} -> ${formatDecimal(dadosRest.lsr)})
  ✅ LSR < ${LIMITE_LSR_LONG.toString()} (Atual: ${formatDecimal(dadosRest.lsr)})
  ✅ Preço (${EMA_3M_TIMEFRAME}) > EMA ${EMA_3M_PERIOD_1} & EMA ${EMA_3M_PERIOD_2}${volumeAnormal ? "\n  💥 Volume Anormal Detectado!" : ""}`;

            await enviarAlertaTelegram(mensagem);
        }

        // 6. Update Previous Data for next cycle
        dadosAnteriores[par] = { // Store only necessary previous data
             open_interest: dadosRest.open_interest,
             volume_24h: dadosRest.volume_24h,
             lsr: dadosRest.lsr
        };

        // Small delay between pairs
        await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`[${new Date().toISOString()}] Verificação concluída.`);
}

// --- Main Execution Logic --- //

async function main() {
    console.log("--- Iniciando Monitoramento Integrado (Node.js - Klines via REST) ---");
    console.log(`Pares: ${PARES_MONITORADOS.join(", ")}`);
    console.log(`Intervalo: ${INTERVALO_VERIFICACAO_MS / 1000}s`);
    console.log(`LSR Long Limit: ${LIMITE_LSR_LONG}`);
    console.log(`EMAs: ${EMA_3M_TIMEFRAME} ${EMA_3M_PERIOD_1}/${EMA_3M_PERIOD_2} (via REST)`);
    console.log(`Alerta Volume Anormal: Ativo (via REST)`);
    console.log(`Contador de Alertas: Ativo`);
    console.log(`Liquidação via WebSocket: Ativa (Recente < ${TEMPO_MAX_LIQUIDACAO_MIN} min)`);
    console.log("-----------------------------------------------------------------------");

    // Connect ONLY Liquidation WebSockets for each pair
    PARES_MONITORADOS.forEach(par => {
        conectarWebSocketLiquidacao(par);
    });

    // Wait a bit for WS connections to establish
    console.log("Aguardando conexões WebSocket de Liquidação...");
    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds

    // Start the main verification loop
    console.log("Iniciando ciclo de verificação periódica...");
    verificarPares(); // Run first check immediately
    setInterval(verificarPares, INTERVALO_VERIFICACAO_MS); // Run periodically

    console.log("Monitoramento ativo. Pressione Ctrl+C para parar.");
}

// Graceful shutdown
function shutdown() {
    console.log("\nParando bot e fechando WebSockets...");
    Object.values(webSockets).forEach(ws => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            try { ws.close(); } catch (e) { /* ignore */ }
        }
    });
    console.log("WebSockets fechados.");
    process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start the main function
main().catch(error => {
    console.error("Erro fatal na execução principal:", error);
    process.exit(1);
});

