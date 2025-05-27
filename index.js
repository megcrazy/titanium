// Import necessary libraries
require("dotenv").config();
const axios = require("axios");
const { Telegraf } = require("telegraf");
const { Decimal } = require("decimal.js");

const BINANCE_API_KEY = process.env.BINANCE_API_KEY;
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET; 
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const PARES_MONITORADOS = (process.env.PARES_MONITORADOS || "BTCUSDT,ETHUSDT").split(",");
const INTERVALO_VERIFICACAO_MS = parseInt(process.env.INTERVALO_VERIFICACAO_MS || "300000", 10);
const LIMITE_LSR = new Decimal(process.env.LIMITE_LSR || "2.0");
const PERIODO_DADOS = process.env.PERIODO_DADOS || "5m";

const EMA_TIMEFRAME = "3m";
const EMA_PERIOD_1 = 55;
const EMA_PERIOD_2 = 233;
const KLINE_LIMIT_EMA = 300;

const BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com";

if (!BINANCE_API_KEY || !TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error("ERRO: Variáveis de ambiente BINANCE_API_KEY, TELEGRAM_BOT_TOKEN, e TELEGRAM_CHAT_ID devem ser definidas no arquivo .env!");
    process.exit(1);
}

const bot = new Telegraf(TELEGRAM_BOT_TOKEN);
let dadosAnteriores = {}; 

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

function detectarVolumeAnormal(volumes) {
    if (volumes.length < 20) return false;
    const recent = volumes.slice(-21, -1);
    const media = recent.reduce((sum, v) => sum.plus(v), new Decimal(0)).dividedBy(recent.length);
    const desvio = new Decimal(Math.sqrt(recent.map(v => v.minus(media).pow(2).toNumber()).reduce((a, b) => a + b, 0) / recent.length));
    const ultimo = volumes[volumes.length - 1];
    return ultimo.greaterThan(media.plus(desvio.times(2)));
}

async function obterDadosBinance(par) {
    try {
        const klinesEMA = await getKlinesFull(par, EMA_TIMEFRAME, KLINE_LIMIT_EMA);

        let ema55_3m = null;
        let ema233_3m = null;
        let preco_3m_atual = null;
        let volumeSeries = [];
        if (klinesEMA) {
            const closesEMA = klinesEMA.map(kline => new Decimal(kline[4]));
            volumeSeries = klinesEMA.map(kline => new Decimal(kline[7]));
            ema55_3m = calculateEMA(closesEMA, EMA_PERIOD_1);
            ema233_3m = calculateEMA(closesEMA, EMA_PERIOD_2);
            if (closesEMA.length > 0) preco_3m_atual = closesEMA[closesEMA.length - 1];
        } else {
            console.warn(`Não foi possível obter klines ${EMA_TIMEFRAME} para EMAs de ${par}`);
        }

        const [tickerRes, oiRes, lsrRes] = await Promise.all([
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/ticker/24hr`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/fapi/v1/openInterest`, { params: { symbol: par } }),
            axios.get(`${BINANCE_FUTURES_BASE_URL}/futures/data/globalLongShortAccountRatio`, { params: { symbol: par, period: PERIODO_DADOS } })
        ]);

        const precoTicker = new Decimal(tickerRes.data.lastPrice);
        const volume24h = new Decimal(tickerRes.data.quoteVolume); 
        const openInterest = new Decimal(oiRes.data.openInterest);

        const lsrList = lsrRes.data;
        const lsrAtual = (lsrList && lsrList.length > 0) ? new Decimal(lsrList[lsrList.length - 1].longShortRatio) : null;

        const volumeAnormal = detectarVolumeAnormal(volumeSeries);

        return {
            preco_ticker: precoTicker,
            volume_24h: volume24h,
            open_interest: openInterest,
            lsr: lsrAtual,
            preco_3m: preco_3m_atual,
            ema_55_3m: ema55_3m,
            ema_233_3m: ema233_3m,
            volume_anormal: volumeAnormal
        };

    } catch (error) {
        console.error(`Falha ao obter dados para ${par}:`, error);
        return null;
    }
}

async function enviarAlertaTelegram(mensagem) {
    try {
        await bot.telegram.sendMessage(TELEGRAM_CHAT_ID, mensagem, { parse_mode: "Markdown" });
        console.log(`Alerta enviado: ${mensagem.substring(0, 50)}...`);
    } catch (error) {
        console.error(`Erro ao enviar alerta: ${error}`);
    }
}

async function verificarPares() {
    console.log(`[${new Date().toISOString()}] Iniciando verificação dos pares...`);
    for (const par of PARES_MONITORADOS) {
        const dadosAtuais = await obterDadosBinance(par);
        if (!dadosAtuais) continue;

        const anterior = dadosAnteriores[par];
        if (!anterior) {
            dadosAnteriores[par] = dadosAtuais;
            console.log(`Dados iniciais salvos para ${par}.`);
            continue;
        }

        const oiSubindo = dadosAtuais.open_interest.greaterThan(anterior.open_interest);
        const volume24hSubindo = dadosAtuais.volume_24h.greaterThan(anterior.volume_24h);
        const lsrValido = dadosAtuais.lsr !== null && anterior.lsr !== null;
        const lsrCaindo = lsrValido && dadosAtuais.lsr.lessThan(anterior.lsr);
        const lsrAbaixoLimite = lsrValido && dadosAtuais.lsr.lessThan(LIMITE_LSR);
        const emaValido = dadosAtuais.preco_3m && dadosAtuais.ema_55_3m && dadosAtuais.ema_233_3m;
        const precoAcimaEMAs = emaValido && dadosAtuais.preco_3m.greaterThan(dadosAtuais.ema_55_3m) && dadosAtuais.preco_3m.greaterThan(dadosAtuais.ema_233_3m);

        const filtrosAtendidos = oiSubindo && volume24hSubindo && lsrCaindo && lsrAbaixoLimite && precoAcimaEMAs;
        const alertaEmoji = dadosAtuais.volume_anormal ? "💥" : "🟢";

        if (filtrosAtendidos) {
            const mensagem = 
`${alertaEmoji} *Analisar Long* 💁🏼‍♀️

*Par:* \`${par}\`
*Preço:* $${formatDecimal(dadosAtuais.preco_ticker)}
*Condições Atendidas:*
  ✅ OI Subindo (${formatDecimal(anterior.open_interest, 0)} -> ${formatDecimal(dadosAtuais.open_interest, 0)})
  ✅ Volume 24h Subindo (${formatDecimal(anterior.volume_24h, 0)} -> ${formatDecimal(dadosAtuais.volume_24h, 0)})
  ✅ LSR (${PERIODO_DADOS}) Caindo (${formatDecimal(anterior.lsr)} -> ${formatDecimal(dadosAtuais.lsr)})
  ✅ LSR < ${LIMITE_LSR.toString()} (Atual: ${formatDecimal(dadosAtuais.lsr)})
  ✅ Preço (${EMA_TIMEFRAME}) > EMA ${EMA_PERIOD_1} & EMA ${EMA_PERIOD_2}${dadosAtuais.volume_anormal ? "\n  💥 Volume Anormal Detectado!" : ""}`;
            await enviarAlertaTelegram(mensagem);
        } else {
            console.log(`Filtro NÃO atingido para ${par}.`);
        }
        dadosAnteriores[par] = dadosAtuais;
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    console.log(`[${new Date().toISOString()}] Verificação concluída.`);
}

async function main() {
    console.log("Iniciando monitoramento...");
    await verificarPares();
    setInterval(verificarPares, INTERVALO_VERIFICACAO_MS);
}

process.once("SIGINT", () => { console.log("Parando bot..."); process.exit(0); });
process.once("SIGTERM", () => { console.log("Parando bot..."); process.exit(0); });
main().catch(error => {
    console.error("Erro fatal:", error);
    process.exit(1);
});
