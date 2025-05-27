// Import necessary libraries
require("dotenv").config(); // Carrega as variaves que vc quiser armazenar no .env 
const axios = require("axios");
const WebSocket = require("ws"); // <<< Importa a biblioteca WebSocket
const { Telegraf } = require("telegraf");
const { Decimal } = require("decimal.js"); // Precisa para calculos

// --- CONFIGURATION (Loaded from .env file) Se for usar api tira o //--- //
// const BINANCE_API_KEY = process.env.BINANCE_API_KEY; // Não necessário para endpoints públicos/websocket
// const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET; // Não necessário
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// --- CONFIGURATION (pode por no codigo ou no .env(ex btc eth que deixei abaixo os pares todos estao no .env)) --- //
const PARES_MONITORADOS = (process.env.PARES_MONITORADOS || "BTCUSDT,ETHUSDT").split(",");
const INTERVALO_VERIFICACAO_MS = parseInt(process.env.INTERVALO_VERIFICACAO_MS || "300000", 10);
const PERIODO_DADOS_LSR = process.env.PERIODO_DADOS_LSR || "5m"; // Periodo LSR/OI
// const LIMITE_ORDENS_LIQUIDACAO = parseInt(process.env.LIMITE_ORDENS_LIQUIDACAO || "10", 10); // <<< REMOVIDO: Não aplicável ao WebSocket
const TEMPO_MAX_LIQUIDACAO_MIN = parseInt(process.env.TEMPO_MAX_LIQUIDACAO_MIN || "10", 10); // Tempo máximo em minutos para considerar uma liquidação como recente

// --- EMA 3m Config --- //
const EMA_3M_TIMEFRAME = "3m";
const EMA_3M_PERIOD_1 = 55;
const EMA_3M_PERIOD_2 = 233;
const KLINE_LIMIT_EMA_3M = 300;

// --- Premium Alert Config --- //
const PREMIUM_LSR_LIMIT = new Decimal(process.env.PREMIUM_LSR_LIMIT || "1.8");
const PREMIUM_TIMEFRAME = "15m";
const PREMIUM_EMA_PERIOD = 34;
const PREMIUM_RSI_PERIOD = 14;
const PREMIUM_RSI_MA_PERIOD = 14;
const PREMIUM_RSI_THRESHOLD = 45;
const KLINE_LIMIT_PREMIUM = 200;

// Binance API URLs
const BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com";
const BINANCE_FUTURES_WS_BASE_URL = "wss://fstream.binance.com"; // URL Base do WebSocket

// --- Basic Validation --- //
if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error("ERRO: Variáveis de ambiente TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID devem ser definidas!");
    process.exit(1);
}

// --- Initialize Telegraf Bot --- //
const bot = new Telegraf(TELEGRAM_BOT_TOKEN);

// --- State Variables --- //
let dadosAnteriores = {};
let contadoresAlertas = {};
let ultimasLiquidacoes = {}; // Armazena a última liquidação recebida por par via WebSocket
let webSockets = {}; // Armazena as instâncias de WebSocket por par

// --- Helper Functions --- //

function formatDecimal(value, places = 4) {
    if (value === null || value === undefined) return "N/A";
    try { return new Decimal(value).toDecimalPlaces(places).toString(); } catch (e) { return "N/A"; }
}

async function getKlinesFull(symbol, interval, limit) {
    try {
        const response = await axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/klines`, {
            params: { symbol, interval, limit }
        });
        return response.data;
    } catch (error) {
        console.error(`Erro ao buscar klines ${interval} para ${symbol}:`, error.response ? error.response.data : error.message);
        return null;
    }
}

// <<< REMOVIDA: Função getRecentLiquidations (substituída por WebSocket) >>>

function calculateEMA(closes, period) {
    if (!closes || closes.length < period) return null;
    const multiplier = new Decimal(2).dividedBy(period + 1);
    let sma = new Decimal(0);
    for (let i = 0; i < period; i++) sma = sma.plus(closes[i]);
    let ema = sma.dividedBy(period);
    for (let i = period; i < closes.length; i++) {
        ema = (closes[i].minus(ema)).times(multiplier).plus(ema);
    }
    return ema;
}

function calculateRSIValues(closes, period) {
    if (!closes || closes.length <= period) return null;
    const rsiValues = [];
    let gains = new Decimal(0);
    let losses = new Decimal(0);
    for (let i = 1; i <= period; i++) {
        const diff = closes[i].minus(closes[i - 1]);
        if (diff.greaterThan(0)) gains = gains.plus(diff);
        else losses = losses.plus(diff.abs());
    }
    let avgGain = gains.dividedBy(period);
    let avgLoss = losses.dividedBy(period);
    const firstRS = avgLoss.equals(0) ? new Decimal(100) : avgGain.dividedBy(avgLoss);
    const firstRSI = new Decimal(100).minus(new Decimal(100).dividedBy(new Decimal(1).plus(firstRS)));
    rsiValues.push(firstRSI);
    for (let i = period + 1; i < closes.length; i++) {
        const diff = closes[i].minus(closes[i - 1]);
        let currentGain = diff.greaterThan(0) ? diff : new Decimal(0);
        let currentLoss = diff.lessThanOrEqualTo(0) ? diff.abs() : new Decimal(0);
        avgGain = (avgGain.times(period - 1).plus(currentGain)).dividedBy(period);
        avgLoss = (avgLoss.times(period - 1).plus(currentLoss)).dividedBy(period);
        const rs = avgLoss.equals(0) ? new Decimal(100) : avgGain.dividedBy(avgLoss);
        const rsi = new Decimal(100).minus(new Decimal(100).dividedBy(new Decimal(1).plus(rs)));
        rsiValues.push(rsi);
    }
    return rsiValues;
}

function calculateMA(values, period) {
    if (!values || values.length < period) return null;
    const maValues = [];
    for (let i = period - 1; i < values.length; i++) {
        let sum = new Decimal(0);
        for (let j = i - period + 1; j <= i; j++) {
            sum = sum.plus(values[j]);
        }
        maValues.push(sum.dividedBy(period));
    }
    const nulls = Array(period - 1).fill(null);
    return [...nulls, ...maValues];
}

// Conectar ao WebSocket de Liquidação 
function conectarWebSocketLiquidacao(par) {
    const wsUrl = `${BINANCE_FUTURES_WS_BASE_URL}/ws/${par.toLowerCase()}@forceOrder`;
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log(`[WS ${par}] Conectado ao stream de liquidação.`);
    });

    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            if (message.e === 'forceOrder') {
                // Armazena a última liquidação recebida para este par
                ultimasLiquidacoes[par] = message.o; // 'o' contém os detalhes da ordem
                // console.log(`[WS ${par}] Liquidação recebida:`, message.o); // Log opcional
            }
        } catch (error) {
            console.error(`[WS ${par}] Erro ao processar mensagem:`, error);
        }
    });

    ws.on('error', (error) => {
        console.error(`[WS ${par}] Erro no WebSocket:`, error);
        // Implementar lógica de reconexão se necessário
    });

    ws.on('close', () => {
        console.log(`[WS ${par}] Desconectado do stream de liquidação.`);
        // Implementar lógica de reconexão se necessário
        delete webSockets[par]; // Remove da lista ativa
        // Tentar reconectar após um tempo
        setTimeout(() => conectarWebSocketLiquidacao(par), 5000); // Tenta reconectar após 5s
    });

    webSockets[par] = ws; // Armazena a instância
}

async function obterDadosBinance(par) {
    try {
        // Busca de Klines para EMAs e RSI (mantido)
        const [klinesEMA3m, klinesPremium15m] = await Promise.all([
            getKlinesFull(par, EMA_3M_TIMEFRAME, KLINE_LIMIT_EMA_3M),
            getKlinesFull(par, PREMIUM_TIMEFRAME, KLINE_LIMIT_PREMIUM)
        ]).catch(error => {
             console.error(`Erro em Promise.all (klines) para ${par}:`, error);
             throw error;
        });

        // Cálculos de EMAs e RSI 
        let ema55_3m = null, ema233_3m = null, preco_3m_atual = null;
        if (klinesEMA3m) {
            const closesEMA3m = klinesEMA3m.map(k => new Decimal(k[4]));
            ema55_3m = calculateEMA(closesEMA3m, EMA_3M_PERIOD_1);
            ema233_3m = calculateEMA(closesEMA3m, EMA_3M_PERIOD_2);
            if (closesEMA3m.length > 0) preco_3m_atual = closesEMA3m[closesEMA3m.length - 1];
        }
        let ema34_15m = null, rsi14_15m = null, rsiMa14_15m = null, preco_15m_atual = null;
        if (klinesPremium15m) {
            const closesPremium15m = klinesPremium15m.map(k => new Decimal(k[4]));
            ema34_15m = calculateEMA(closesPremium15m, PREMIUM_EMA_PERIOD);
            const rsiValues = calculateRSIValues(closesPremium15m, PREMIUM_RSI_PERIOD);
            if (rsiValues) {
                rsi14_15m = rsiValues[rsiValues.length - 1];
                const rsiMaValues = calculateMA(rsiValues, PREMIUM_RSI_MA_PERIOD);
                rsiMa14_15m = rsiMaValues ? rsiMaValues[rsiMaValues.length - 1] : null;
            }
            if (closesPremium15m.length > 0) preco_15m_atual = closesPremium15m[closesPremium15m.length - 1];
        }

        // Busca de Ticker, Open Interest, LSR (sem liquidação aqui)
        const [tickerRes, oiRes, lsrRes] = await Promise.all([
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/ticker/24hr`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/openInterest`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/futures/data/globalLongShortAccountRatio`, { params: { symbol: par, period: PERIODO_DADOS_LSR } })
        ]).catch(error => {
            console.error(`Erro em Promise.all (ticker/oi/lsr) para ${par}:`, error.response ? error.response.data : error.message);
            throw error;
        });

        // Processamento dos dados básicos 
        const tickerInfo = tickerRes.data;
        const oiInfo = oiRes.data;
        const lsrInfo = lsrRes.data;
        const precoTicker = new Decimal(tickerInfo.lastPrice);
        const volume24h = new Decimal(tickerInfo.quoteVolume);
        const openInterest = new Decimal(oiInfo.openInterest);
        let lsrAtual = null;
        if (lsrInfo && lsrInfo.length > 0) {
            lsrAtual = new Decimal(lsrInfo[lsrInfo.length - 1].longShortRatio);
        } else {
            console.warn(`Não foi possível obter LSR para ${par}`);
        }

        // <<< Processamento da Última Liquidação Armazenada >>>
        let liquidacaoRecenteInfo = "Nenhuma Recente";
        const ultimaLiquidacao = ultimasLiquidacoes[par]; // Pega a última armazenada pelo WebSocket
        if (ultimaLiquidacao) {
            const agora = Date.now();
            const tempoLimiteMs = TEMPO_MAX_LIQUIDACAO_MIN * 60 * 1000;

            // Verifica se a liquidação armazenada é recente
            if (agora - ultimaLiquidacao.T <= tempoLimiteMs) { // 'T' é o timestamp da ordem
                const lado = ultimaLiquidacao.S; // 'S' é o Side (SELL ou BUY)
                const tipoLiquidado = lado === 'BUY' ? 'Short' : 'Long'; // BUY liquida Short, SELL liquida Long
                const precoLiquidacao = formatDecimal(ultimaLiquidacao.ap, 2); // 'ap' é o Average Price
                const quantidade = formatDecimal(ultimaLiquidacao.q, 4); // 'q' é a Original Quantity
                liquidacaoRecenteInfo = ` ${tipoLiquidado} @ ${precoLiquidacao} (Qtd: ${quantidade})`;
                // console.log(`[LIQ ${par}] Usando liquidação recente armazenada: ${tipoLiquidado} @ ${precoLiquidacao}`); // Log opcional
            } else {
                 // console.log(`[LIQ ${par}] Liquidação armazenada é antiga.`); // Log opcional
                 // Limpa a liquidação antiga para não ser usada novamente
                 delete ultimasLiquidacoes[par];
            }
        }
        // <<< FIM: Processamento da Última Liquidação Armazenada >>>

        // Retorna todos os dados agregados
        return {
            preco_ticker: precoTicker,
            volume_24h: volume24h,
            open_interest: openInterest,
            lsr: lsrAtual,
            preco_3m: preco_3m_atual,
            ema_55_3m: ema55_3m,
            ema_233_3m: ema233_3m,
            preco_15m: preco_15m_atual,
            ema_34_15m: ema34_15m,
            rsi_14_15m: rsi14_15m,
            rsi_ma_14_15m: rsiMa14_15m,
            liquidacao_recente: liquidacaoRecenteInfo //Informações agora pelo WS
        };

    } catch (error) {
        console.error(`Falha final ao obter dados agregados para ${par}.`);
        return null;
    }
}

async function enviarAlertaTelegram(mensagem) {
    try {
        await bot.telegram.sendMessage(TELEGRAM_CHAT_ID, mensagem, { parse_mode: "Markdown" });
        console.log(`Alerta enviado para o Telegram: ${mensagem.substring(0, 50)}...`);
    } catch (error) {
        console.error(`Erro ao enviar alerta para o Telegram: ${error}`);
    }
}

async function verificarPares() {
    console.log(`[${new Date().toISOString()}] Iniciando verificação dos pares...`);

    for (const par of PARES_MONITORADOS) {
        console.log(`Verificando par: ${par}`);
        const dadosAtuais = await obterDadosBinance(par);

        if (dadosAtuais === null) {
            console.warn(`Pulando ${par} devido a erro na obtenção de dados.`);
            continue;
        }

        const anterior = dadosAnteriores[par];

        if (anterior) {
            // --- Condições Base e Premium  --- //
            const oiSubindo = dadosAtuais.open_interest.greaterThan(anterior.open_interest);
            const volume24hSubindo = dadosAtuais.volume_24h.greaterThan(anterior.volume_24h);
            const lsrValido = dadosAtuais.lsr !== null;
            const lsrAbaixoLimitePremium = lsrValido && dadosAtuais.lsr.lessThan(PREMIUM_LSR_LIMIT);
            const ema3mValido = dadosAtuais.preco_3m !== null && dadosAtuais.ema_55_3m !== null && dadosAtuais.ema_233_3m !== null;
            const precoAcimaEMAs3m = ema3mValido &&
                                     dadosAtuais.preco_3m.greaterThan(dadosAtuais.ema_55_3m) &&
                                     dadosAtuais.preco_3m.greaterThan(dadosAtuais.ema_233_3m);
            const baseConditionsMet = oiSubindo && volume24hSubindo && lsrValido && lsrAbaixoLimitePremium && ema3mValido && precoAcimaEMAs3m;

            const ema15mValido = dadosAtuais.preco_15m !== null && dadosAtuais.ema_34_15m !== null;
            const precoAcimaEMA15m = ema15mValido && dadosAtuais.preco_15m.greaterThan(dadosAtuais.ema_34_15m);
            const rsi15mValido = dadosAtuais.rsi_14_15m !== null && dadosAtuais.rsi_ma_14_15m !== null;
            const rsiAcimaLimitePremium = rsi15mValido &&
                                         dadosAtuais.rsi_14_15m.greaterThan(PREMIUM_RSI_THRESHOLD) &&
                                         dadosAtuais.rsi_14_15m.greaterThan(dadosAtuais.rsi_ma_14_15m);
            const premiumConditionsMet = ema15mValido && precoAcimaEMA15m && rsi15mValido && rsiAcimaLimitePremium;

            // --- DEBUG LOGS  --- 
            console.log(`[DEBUG ${par}] Base - OI Subindo: ${oiSubindo}`);
            console.log(`[DEBUG ${par}] Base - Vol 24h Subindo: ${volume24hSubindo}`);
            if(lsrValido) console.log(`[DEBUG ${par}] Base - LSR < ${PREMIUM_LSR_LIMIT}: ${lsrAbaixoLimitePremium} (Atual: ${formatDecimal(dadosAtuais.lsr)})`);
            if(ema3mValido) console.log(`[DEBUG ${par}] Base - Preço 3m > EMAs 3m: ${precoAcimaEMAs3m}`);
            if(ema15mValido) console.log(`[DEBUG ${par}] Premium - Preço 15m > EMA 34 15m: ${precoAcimaEMA15m}`);
            if(rsi15mValido) {
                 console.log(`[DEBUG ${par}] Premium - RSI 15m > ${PREMIUM_RSI_THRESHOLD}: ${dadosAtuais.rsi_14_15m.greaterThan(PREMIUM_RSI_THRESHOLD)} (Atual: ${formatDecimal(dadosAtuais.rsi_14_15m, 2)})`);
                 console.log(`[DEBUG ${par}] Premium - RSI 15m > MA(${PREMIUM_RSI_MA_PERIOD}): ${dadosAtuais.rsi_14_15m.greaterThan(dadosAtuais.rsi_ma_14_15m)} (MA: ${formatDecimal(dadosAtuais.rsi_ma_14_15m, 2)})`);
            }
            console.log(`[DEBUG ${par}] Liquidação Recente (WS): ${dadosAtuais.liquidacao_recente}`); // Log atualizado
            // --- DEBUG LOGS END --- 

            // --- Envio de Alertas --- //
            if (baseConditionsMet) {
                if (!contadoresAlertas[par]) contadoresAlertas[par] = 0;
                contadoresAlertas[par]++;
                const numeroAlerta = contadoresAlertas[par];

                const mensagemBase =
`🟢 *Avaliar Long #${numeroAlerta}* 🟢

*Par:* \`${par}\`
*Preço:* $${formatDecimal(dadosAtuais.preco_ticker)}
*Liquidação Recente:* ${dadosAtuais.liquidacao_recente}
*Condições Base Atendidas:*
  ✅ OI (${PERIODO_DADOS_LSR}) Subindo
  ✅ Volume 24h Subindo
  ✅ LSR (${PERIODO_DADOS_LSR}) < ${PREMIUM_LSR_LIMIT.toString()} (Atual: ${formatDecimal(dadosAtuais.lsr)})
  ✅ Preço (${EMA_3M_TIMEFRAME}) > EMA ${EMA_3M_PERIOD_1} & ${EMA_3M_PERIOD_2}`;

                await enviarAlertaTelegram(mensagemBase);
                console.log(`Alerta BASE #${numeroAlerta} atingido para ${par}.`);

                if (premiumConditionsMet) {
                    const mensagemPremium =
`🚀 *ALERTA PREMIUM - LONG #${numeroAlerta}* 🚀

*Par:* \`${par}\`
*Preço:* $${formatDecimal(dadosAtuais.preco_ticker)}
*Liquidação Recente:* ${dadosAtuais.liquidacao_recente}
*Condições Base Atendidas:*
  ✅ OI (${PERIODO_DADOS_LSR}) Subindo
  ✅ Volume 24h Subindo
  ✅ LSR (${PERIODO_DADOS_LSR}) < ${PREMIUM_LSR_LIMIT.toString()} (Atual: ${formatDecimal(dadosAtuais.lsr)})
  ✅ Preço (${EMA_3M_TIMEFRAME}) > EMA ${EMA_3M_PERIOD_1} & ${EMA_3M_PERIOD_2}
*Condições Premium Adicionais:*
  ✅ Preço (${PREMIUM_TIMEFRAME}) > EMA ${PREMIUM_EMA_PERIOD}
  ✅ RSI (${PREMIUM_TIMEFRAME}) > ${PREMIUM_RSI_THRESHOLD} (Atual: ${formatDecimal(dadosAtuais.rsi_14_15m, 2)})
  ✅ RSI (${PREMIUM_TIMEFRAME}) > MA(${PREMIUM_RSI_MA_PERIOD}) (${formatDecimal(dadosAtuais.rsi_ma_14_15m, 2)})`;

                    await enviarAlertaTelegram(mensagemPremium);
                    console.log(`Alerta PREMIUM #${numeroAlerta} ATINGIDO para ${par}!`);
                }
            } else {
                console.log(`Alertas NÃO atingidos para ${par}.`);
            }
        } else {
             console.log(`Armazenando dados iniciais para ${par}.`);
        }

        dadosAnteriores[par] = dadosAtuais;
        await new Promise(resolve => setTimeout(resolve, 500)); // Pausa entre pares
    }

    console.log(`[${new Date().toISOString()}] Verificação concluída.`);
}

// --- Main Execution Logic --- //

async function main() {
    console.log("Iniciando monitoramento (Node.js com Alerta Base, Premium, Contador e Liquidação via WebSocket)..." );
    console.log(`Pares monitorados: ${PARES_MONITORADOS.join(", ")}`);
    console.log(`Intervalo de verificação: ${INTERVALO_VERIFICACAO_MS / 1000} segundos`);
    console.log(`Config Base: LSR < ${PREMIUM_LSR_LIMIT}, EMAs ${EMA_3M_TIMEFRAME}=${EMA_3M_PERIOD_1}/${EMA_3M_PERIOD_2}`);
    console.log(`Config Premium: TF=${PREMIUM_TIMEFRAME}, EMA=${PREMIUM_EMA_PERIOD}, RSI > ${PREMIUM_RSI_THRESHOLD} e MA(${PREMIUM_RSI_MA_PERIOD})`);
    console.log(`Config Liquidação: Via WebSocket <symbol>@forceOrder, considera recentes até ${TEMPO_MAX_LIQUIDACAO_MIN} min atrás.`);

    // <<<  Conecta aos WebSockets de liquidação para cada par >>>
    PARES_MONITORADOS.forEach(par => {
        conectarWebSocketLiquidacao(par);
    });

    // Primeira verificação imediata
    await verificarPares();

    // Verificações subsequentes em intervalo
    setInterval(verificarPares, INTERVALO_VERIFICACAO_MS);

    console.log("Monitoramento ativo. Pressione Ctrl+C para parar.");
}

// Inicia a execução principal
main().catch(error => {
    console.error("Erro fatal na execução principal:", error);
    process.exit(1);
});

// Graceful shutdown
function shutdown() {
    console.log("Parando bot e fechando WebSockets...");
    Object.values(webSockets).forEach(ws => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.close();
        }
    });
    process.exit(0);
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

