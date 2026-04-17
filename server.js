 require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const cors     = require('cors');
const Database = require('better-sqlite3');

const admin = require('firebase-admin');
if (!admin.apps.length) {
  try {
    const sa = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
    if (sa.project_id) { admin.initializeApp({ credential: admin.credential.cert(sa) }); console.log('✓ Firebase Admin OK'); }
    else { console.warn('⚠️ Push notifications disabled — set FIREBASE_SERVICE_ACCOUNT'); }
  } catch(e) { console.warn('Firebase Admin error:', e.message); }
}

async function sendSignalPush(signal) {
  try {
    if (!admin.apps.length) return;
    const emoji = signal.action === 'BUY' ? '📈' : '📉';
    const title = `${emoji} ${signal.action} Signal — XAU/USD`;
    const body  = `Entry: $${signal.price} | FIB ${signal.fib_level || ''} | ${signal.confidence || 0}% Confluence`;
    await admin.messaging().send({
      topic: 'signals',
      notification: { title, body },
      data: { action: signal.action, price: String(signal.price), confidence: String(signal.confidence || 0) },
      apns: { payload: { aps: { sound: 'default', badge: 1 } }, headers: { 'apns-priority': '10' } },
      android: { priority: 'high', notification: { title, body, channelId: 'pulstrade_signals', priority: 'max' } },
    });
    console.log(`✓ Push sent: ${title}`);
  } catch(e) { console.error('Push error:', e.message); }
}

const app  = express();
const PORT = process.env.PORT || 3000;
const TWELVE_API_KEY  = process.env.TWELVE_DATA_API_KEY || '';
const NEWS_API_KEY    = process.env.NEWS_API_KEY || '4ada05cd8e1d46f2bbb4f011506044fd';
const METAAPI_TOKEN   = process.env.METAAPI_TOKEN || '';
const METAAPI_URL     = 'https://mt-client-api-v1.london.agiliumtrade.ai';
const TICKER = 'XAU/USD';

app.use(cors());
app.use(express.json());

// ── Database ──────────────────────────────────────────────────────────────────
const db = new Database(process.env.DB_PATH || '/tmp/pulstrade.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    action          TEXT NOT NULL,
    price           REAL NOT NULL,
    sl              REAL,
    tp1             REAL,
    tp2             REAL,
    timeframe       TEXT,
    confidence      INTEGER,
    fib_level       TEXT,
    pattern         TEXT,
    note            TEXT,
    rsi             REAL,
    atr             REAL,
    current_price   REAL,
    entry_valid_for REAL,
    mtf             TEXT,
    timestamp       INTEGER NOT NULL,
    created_at      INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
  CREATE TABLE IF NOT EXISTS autotrade_accounts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT UNIQUE NOT NULL,
    lot_size   REAL DEFAULT 0.01,
    auto_trade INTEGER DEFAULT 1,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
`);

// ── Helper Functions ──────────────────────────────────────────────────────────
function calcFibLevels(high, low) {
  const diff = high - low;
  return {
    fib236: high - diff * 0.236,
    fib382: high - diff * 0.382,
    fib500: high - diff * 0.500,
    fib618: high - diff * 0.618,
    fib786: high - diff * 0.786,
  };
}

function findSwingHighLow(candles) {
  let high = -Infinity, low = Infinity;
  const lookback = Math.min(20, candles.length);
  for (let i = 0; i < lookback; i++) {
    if (candles[i].high > high) high = candles[i].high;
    if (candles[i].low  < low)  low  = candles[i].low;
  }
  return { high, low };
}

function calcRSI(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i - 1] - closes[i];
    if (diff > 0) gains += diff; else losses -= diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  return Math.round((100 - 100 / (1 + avgGain / avgLoss)) * 10) / 10;
}

function calcATR(candles, period = 14) {
  if (candles.length < period + 1) return null;
  let atr = 0;
  for (let i = 0; i < period; i++) {
    const tr = Math.max(
      candles[i].high - candles[i].low,
      Math.abs(candles[i].high - candles[i + 1].close),
      Math.abs(candles[i].low  - candles[i + 1].close),
    );
    atr += tr;
  }
  return Math.round((atr / period) * 100) / 100;
}

function isDoji(c) {
  const body = Math.abs(c.close - c.open);
  const range = c.high - c.low;
  return range > 0 && body / range < 0.25;
}

function isPinBar(c, dir) {
  const body = Math.abs(c.close - c.open);
  const upper = c.high - Math.max(c.open, c.close);
  const lower = Math.min(c.open, c.close) - c.low;
  if (dir === 'buy')  return lower > body * 2 && lower > upper * 2;
  if (dir === 'sell') return upper > body * 2 && upper > lower * 2;
  return false;
}

function nearFib(price, fibValue, atr) {
  return Math.abs(price - fibValue) <= (atr || fibValue * 0.001) * 0.5;
}

function timeAgo(date) {
  const seconds = Math.floor((new Date() - date) / 1000);
  if (seconds < 3600)  return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

// ── Fetch Candles from Twelve Data ────────────────────────────────────────────
async function fetchCandles(interval, outputsize = 60) {
  if (!TWELVE_API_KEY) return generateMockCandles(outputsize);
  try {
    const symbol = encodeURIComponent(TICKER);
    const url = `https://api.twelvedata.com/time_series?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const res = await axios.get(url, { timeout: 10000 });
    if (!res.data || !res.data.values) return generateMockCandles(outputsize);
    return res.data.values.map(v => ({
      open:      parseFloat(v.open),
      high:      parseFloat(v.high),
      low:       parseFloat(v.low),
      close:     parseFloat(v.close),
      timestamp: new Date(v.datetime).getTime(),
    }));
  } catch (err) {
    console.error('Twelve Data error:', err.message);
    return generateMockCandles(outputsize);
  }
}

function generateMockCandles(count) {
  const candles = [];
  let price = 4720 + Math.random() * 30;
  const now = Date.now();
  for (let i = count - 1; i >= 0; i--) {
    const change = (Math.random() - 0.5) * 15;
    const open  = price;
    const close = price + change;
    const high  = Math.max(open, close) + Math.random() * 8;
    const low   = Math.min(open, close) - Math.random() * 8;
    candles.push({ open, high, low, close, timestamp: now - i * 300000 });
    price = close;
  }
  return candles;
}

// ── Confluence Score System ───────────────────────────────────────────────────
// Score 0-100 basierend auf 5 Faktoren
// Signal nur wenn Score >= 75

function calcEMA(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) {
    ema = values[i] * k + ema * (1 - k);
  }
  return ema;
}

function calcBollingerBands(closes, period = 20, stdDev = 2) {
  if (closes.length < period) return null;
  const slice = closes.slice(0, period);
  const mean  = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / period;
  const std = Math.sqrt(variance);
  return { upper: mean + stdDev * std, middle: mean, lower: mean - stdDev * std };
}

function calcMACD(closes) {
  if (closes.length < 26) return null;
  const ema12 = calcEMA(closes, 12);
  const ema26 = calcEMA(closes, 26);
  if (!ema12 || !ema26) return null;
  return { macd: ema12 - ema26, signal: calcEMA([ema12 - ema26], 9) };
}

function calcStochRSI(closes, period = 14) {
  if (closes.length < period * 2) return null;
  // Simple RSI first
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i-1] - closes[i];
    if (diff > 0) gains += diff; else losses -= diff;
  }
  const rs  = gains / (losses || 0.0001);
  const rsi = 100 - 100 / (1 + rs);
  // Stoch of RSI
  const rsiArr = [rsi];
  const minRSI = Math.min(...rsiArr);
  const maxRSI = Math.max(...rsiArr);
  return maxRSI === minRSI ? 50 : ((rsi - minRSI) / (maxRSI - minRSI)) * 100;
}

function isEconomicEventSoon(offsetMinutes = 30) {
  // High-impact events — approximate UTC times
  // In production: use a real economic calendar API
  const now   = new Date();
  const hour  = now.getUTCHours();
  const min   = now.getUTCMinutes();
  const totalMin = hour * 60 + min;
  
  // Known high-impact times (UTC): 13:30, 15:00, 19:00 (Fed), 14:00
  const highImpactTimes = [13*60+30, 15*60, 19*60, 14*60, 8*60+30];
  
  for (const eventTime of highImpactTimes) {
    const diff = Math.abs(eventTime - totalMin);
    if (diff <= offsetMinutes) {
      console.log(`⚠️ Economic event in ${diff} min — signal blocked`);
      return true;
    }
  }
  return false;
}

function calcConfluenceScore(candles, action, fibLevel, atr, rsi) {
  let score = 0;
  const reasons = [];
  const closes = candles.map(c => c.close);
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const price  = closes[0];

  // ── Factor 1: FIB Quality (0-25 points) ───────────────────────────────────
  const fibScores = { '61.8%': 25, '50.0%': 20, '38.2%': 18, '78.6%': 15, '23.6%': 10 };
  const fibScore  = fibScores[fibLevel] || 10;
  score += fibScore;
  reasons.push(`FIB ${fibLevel}: +${fibScore}pts`);

  // ── Factor 2: RSI Confluence (0-20 points) ────────────────────────────────
  let rsiScore = 0;
  if (action === 'BUY') {
    if (rsi >= 25 && rsi <= 45)      { rsiScore = 20; reasons.push('RSI oversold zone: +20pts'); }
    else if (rsi >= 45 && rsi <= 55) { rsiScore = 10; reasons.push('RSI neutral: +10pts'); }
    else if (rsi < 25)               { rsiScore = 15; reasons.push('RSI extreme oversold: +15pts'); }
    else                             { rsiScore = 0;  reasons.push('RSI too high for BUY: +0pts'); }
  } else {
    if (rsi >= 55 && rsi <= 75)      { rsiScore = 20; reasons.push('RSI overbought zone: +20pts'); }
    else if (rsi >= 45 && rsi <= 55) { rsiScore = 10; reasons.push('RSI neutral: +10pts'); }
    else if (rsi > 75)               { rsiScore = 15; reasons.push('RSI extreme overbought: +15pts'); }
    else                             { rsiScore = 0;  reasons.push('RSI too low for SELL: +0pts'); }
  }
  score += rsiScore;

  // ── Factor 3: EMA Trend Alignment (0-20 points) ───────────────────────────
  const ema20  = calcEMA(closes, 20);
  const ema50  = calcEMA(closes, Math.min(50, closes.length - 1));
  const ema200 = calcEMA(closes, Math.min(200, closes.length - 1));
  
  let emaScore = 0;
  if (action === 'BUY') {
    if (ema20 && price > ema20)              { emaScore += 8; }
    if (ema50 && price > ema50)              { emaScore += 7; }
    if (ema200 && price > ema200)            { emaScore += 5; }
    if (ema20 && ema50 && ema20 > ema50)     { emaScore = Math.min(emaScore + 5, 20); }
  } else {
    if (ema20 && price < ema20)              { emaScore += 8; }
    if (ema50 && price < ema50)              { emaScore += 7; }
    if (ema200 && price < ema200)            { emaScore += 5; }
    if (ema20 && ema50 && ema20 < ema50)     { emaScore = Math.min(emaScore + 5, 20); }
  }
  score += emaScore;
  reasons.push(`EMA alignment: +${emaScore}pts`);

  // ── Factor 4: Bollinger Bands (0-15 points) ───────────────────────────────
  const bb = calcBollingerBands(closes);
  let bbScore = 0;
  if (bb) {
    if (action === 'BUY'  && price <= bb.lower)  { bbScore = 15; reasons.push('Price at BB lower: +15pts'); }
    else if (action === 'SELL' && price >= bb.upper) { bbScore = 15; reasons.push('Price at BB upper: +15pts'); }
    else if (action === 'BUY'  && price <= bb.middle) { bbScore = 8; reasons.push('Price below BB middle: +8pts'); }
    else if (action === 'SELL' && price >= bb.middle) { bbScore = 8; reasons.push('Price above BB middle: +8pts'); }
  }
  score += bbScore;

  // ── Factor 5: Candlestick Pattern (0-20 points) ───────────────────────────
  const c0 = candles[0], c1 = candles[1];
  const body0 = Math.abs(c0.close - c0.open);
  const range0 = c0.high - c0.low;
  const upper0 = c0.high - Math.max(c0.close, c0.open);
  const lower0 = Math.min(c0.close, c0.open) - c0.low;

  let patternScore = 0;
  let patternName  = 'No pattern';

  // Engulfing
  if (action === 'BUY' && c0.close > c0.open && c0.close > c1.open && c0.open < c1.close) {
    patternScore = 20; patternName = 'Bullish Engulfing';
  } else if (action === 'SELL' && c0.close < c0.open && c0.close < c1.close && c0.open > c1.open) {
    patternScore = 20; patternName = 'Bearish Engulfing';
  }
  // Pin Bar
  else if (action === 'BUY' && lower0 > body0 * 2.5 && lower0 > upper0 * 2) {
    patternScore = 18; patternName = 'Bullish Pin Bar';
  } else if (action === 'SELL' && upper0 > body0 * 2.5 && upper0 > lower0 * 2) {
    patternScore = 18; patternName = 'Bearish Pin Bar';
  }
  // Doji
  else if (range0 > 0 && body0 / range0 < 0.25) {
    patternScore = 12; patternName = 'Doji';
  }
  // Inside Bar
  else if (c0.high < c1.high && c0.low > c1.low) {
    patternScore = 10; patternName = 'Inside Bar';
  }

  score += patternScore;
  reasons.push(`${patternName}: +${patternScore}pts`);

  return {
    score:   Math.min(Math.round(score), 100),
    reasons,
    patternName,
  };
}

// ── Updated Signal Scanner with Confluence ────────────────────────────────────
async function scanForSignalsV2(db, fetchCandles, TICKER) {
  const timeframes = [
    { label: '1H', interval: '1h',   validFor: 2,  minScore: 72 },
    { label: '4H', interval: '4h',   validFor: 8,  minScore: 70 },
    { label: '1D', interval: '1day', validFor: 24, minScore: 68 },
  ];

  // Block signals during major economic events
  if (isEconomicEventSoon(30)) {
    console.log('⚠️ Economic event soon — no signals generated');
    return [];
  }

  const newSignals = [];

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 60); // Need more candles for EMA200
      if (!candles || candles.length < 30) continue;

      // Swing high/low from last 30 candles
      let swingHigh = -Infinity, swingLow = Infinity;
      for (let i = 0; i < 30; i++) {
        if (candles[i].high > swingHigh) swingHigh = candles[i].high;
        if (candles[i].low  < swingLow)  swingLow  = candles[i].low;
      }
      const fibRange = swingHigh - swingLow;
      const fibs = {
        '61.8%': swingHigh - fibRange * 0.618,
        '50.0%': swingHigh - fibRange * 0.500,
        '38.2%': swingHigh - fibRange * 0.382,
        '78.6%': swingHigh - fibRange * 0.786,
      };

      const latest = candles[0];
      const price  = latest.close;
      const closes = candles.map(c => c.close);

      // ATR
      let atrSum = 0;
      for (let i = 0; i < 14; i++) {
        atrSum += Math.max(
          candles[i].high - candles[i].low,
          Math.abs(candles[i].high - candles[i+1]?.close || candles[i].close),
          Math.abs(candles[i].low  - candles[i+1]?.close || candles[i].close)
        );
      }
      const atr = Math.round((atrSum / 14) * 100) / 100;

      // RSI
      let gains = 0, losses = 0;
      for (let i = 1; i <= 14; i++) {
        const diff = closes[i-1] - closes[i];
        if (diff > 0) gains += diff; else losses -= diff;
      }
      const rsi = Math.round((100 - 100 / (1 + gains/14 / (losses/14 || 0.001))) * 10) / 10;

      for (const [fibName, fibValue] of Object.entries(fibs)) {
        const tolerance = atr * 2.0;
        if (Math.abs(price - fibValue) > tolerance) continue;

        const isBuyZone  = price < swingLow  + fibRange * 0.4;
const isSellZone = price > swingHigh - fibRange * 0.4;

        for (const action of ['BUY', 'SELL']) {
          if (action === 'BUY'  && !isBuyZone)  continue;
          if (action === 'SELL' && !isSellZone) continue;

          // Calculate confluence score
          const confluence = calcConfluenceScore(candles, action, fibName, atr, rsi);
          
          if (confluence.score < tf.minScore) {
            console.log(`Signal filtered: ${action} ${fibName} score=${confluence.score} < ${tf.minScore}`);
            continue;
          }

          // No duplicate in last 4 hours
          const recent = db.prepare(
            `SELECT id FROM signals WHERE ticker=? AND action=? AND timeframe=? AND timestamp > ?`
          ).get(TICKER, action, tf.label, Date.now() - 4 * 3600000);
          if (recent) continue;

          // Smart SL/TP — use next FIB level
          const fibValues = Object.values(fibs).sort((a, b) => a - b);
          let sl, tp1, tp2;

          if (action === 'BUY') {
            const nextFibDown = fibValues.filter(f => f < fibValue).pop() || fibValue - atr * 2;
            sl  = Math.round((nextFibDown - atr * 0.5) * 100) / 100;
            tp1 = Math.round((fibValue + atr * 3) * 100) / 100;
            tp2 = Math.round((fibValue + atr * 6) * 100) / 100;
          } else {
            const nextFibUp = fibValues.filter(f => f > fibValue).shift() || fibValue + atr * 2;
            sl  = Math.round((nextFibUp + atr * 0.5) * 100) / 100;
            tp1 = Math.round((fibValue - atr * 3) * 100) / 100;
            tp2 = Math.round((fibValue - atr * 6) * 100) / 100;
          }

          const signal = {
            ticker:    TICKER,
            action,
            price:     Math.round(price * 100) / 100,
            sl, tp1, tp2,
            timeframe: tf.label,
            confidence: confluence.score,
            fib_level:  fibName,
            pattern:    confluence.patternName,
            note:       confluence.reasons.slice(0, 3).join(' | '),
            rsi:        rsi ? Math.round(rsi * 10) / 10 : null,
            atr,
            current_price: Math.round(price * 100) / 100,
            entry_valid_for: tf.validFor,
            mtf: JSON.stringify({ h1: tf.label === '1H', h4: tf.label === '4H', d1: tf.label === '1D' }),
            timestamp: Date.now(),
          };

          db.prepare(`INSERT INTO signals 
            (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
            VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)
          `).run(signal);

          console.log(`✓ Confluence Signal: ${action} ${TICKER} @ ${price} (${tf.label}, ${fibName}, score: ${confluence.score})`);
          console.log(`  Reasons: ${confluence.reasons.join(', ')}`);
          newSignals.push(signal);
        }
      }
    } catch (err) {
      console.error(`Error scanning ${tf.label}:`, err.message);
    }
  }

  return newSignals;
}



// ── Signal Scanner ────────────────────────────────────────────────────────────
async function scanForSignals() {
  const timeframes = [
    { label: '1H', interval: '1h',   validFor: 2  },
    { label: '4H', interval: '4h',   validFor: 8  },
    { label: '1D', interval: '1day', validFor: 24 },
  ];

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 30);
      if (!candles || candles.length < 15) continue;

      const swing  = findSwingHighLow(candles);
      const fib    = calcFibLevels(swing.high, swing.low);
      const latest = candles[0];
      const price  = latest.close;
      const atr    = calcATR(candles, 14);
      const rsi    = calcRSI(candles.map(c => c.close), 14);

      const fibChecks = [
        { level: '61.8%', value: fib.fib618 },
        { level: '50.0%', value: fib.fib500 },
        { level: '38.2%', value: fib.fib382 },
        { level: '78.6%', value: fib.fib786 },
      ];

      for (const check of fibChecks) {
        if (!nearFib(price, check.value, atr)) continue;

        const isBuySetup  = price < swing.high - (swing.high - swing.low) * 0.3;
        const isSellSetup = price > swing.low  + (swing.high - swing.low) * 0.3;

        // Calculate confluence score for BUY and SELL
        for (const action of ['BUY', 'SELL']) {
          if (action === 'BUY'  && !isBuySetup)  continue;
          if (action === 'SELL' && !isSellSetup) continue;

          const confluence = calcConfluenceScore(candles, action, check.level, atr, rsi);
          if (confluence.score < 70) continue;

          const pattern = confluence.patternName;
          if (!pattern || pattern === 'No pattern') continue;

          // No duplicate in last 4 hours
          const recent = db.prepare(`SELECT id FROM signals WHERE ticker=? AND action=? AND timeframe=? AND timestamp > ?`)
            .get(TICKER, action, tf.label, Date.now() - 4 * 3600000);
          if (recent) continue;

          const sl  = action === 'BUY'  ? check.value - (atr || 10) * 1.5 : check.value + (atr || 10) * 1.5;
          const tp1 = action === 'BUY'  ? check.value + (atr || 10) * 3.0 : check.value - (atr || 10) * 3.0;
          const tp2 = action === 'BUY'  ? check.value + (atr || 10) * 6.0 : check.value - (atr || 10) * 6.0;
          const mtf = JSON.stringify({ h1: tf.label === '1H', h4: tf.label === '4H', d1: tf.label === '1D' });

          const signal = {
            ticker: TICKER, action,
            price:  Math.round(price * 100) / 100,
            sl:     Math.round(sl  * 100) / 100,
            tp1:    Math.round(tp1 * 100) / 100,
            tp2:    Math.round(tp2 * 100) / 100,
            timeframe: tf.label, confidence: confluence.score,
            fib_level: check.level, pattern,
            note: confluence.reasons.slice(0,3).join(' | '),
            rsi:    rsi  ? Math.round(rsi  * 10)  / 10  : null,
            atr:    atr  ? Math.round(atr  * 100) / 100 : null,
            current_price: Math.round(price * 100) / 100,
            entry_valid_for: tf.validFor, mtf,
            timestamp: Date.now(),
          };

          db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
            VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)`).run(signal);
          console.log(`✓ Confluence Signal: ${action} ${TICKER} @ ${price} (${tf.label}, ${check.level}, score:${confluence.score})`);
          const inserted = db.prepare('SELECT last_insert_rowid() as id').get();
          sendSignalPush({ ...signal, id: inserted.id });
        }
      }
    } catch (err) {
      console.error(`Error scanning ${tf.label}:`, err.message);
    }
  }

  // Weekend check — no new signals when market closed
  const now = new Date();
  const day = now.getUTCDay(); // 0=Sun, 6=Sat
  const hour = now.getUTCHours();
  const isFxClosed = (day === 6) || (day === 0) || (day === 5 && hour >= 21) || (day === 1 && hour < 1);
  if (isFxClosed) {
    console.log('Market closed — skipping signal scan');
    return;
  }

  // Seed if empty
  const count = db.prepare('SELECT COUNT(*) as c FROM signals').get().c;
  if (count === 0) await seedSignals();
}

async function seedSignals() {
  let price = 4720;
  try {
    if (TWELVE_API_KEY) {
      const r = await axios.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(TICKER)}&apikey=${TWELVE_API_KEY}`, { timeout: 5000 });
      price = parseFloat(r.data.price) || price;
    }
  } catch (_) {}

  const now = Date.now();
  const seeds = [
    { action: 'BUY',  fib: '61.8%', conf: 82, tf: '1H',  validFor: 2,  rsi: 34.2, atr: 12.4, pattern: 'Doji on Golden FIB',   offset: 3 * 60000 },
    { action: 'SELL', fib: '38.2%', conf: 78, tf: '4H',  validFor: 8,  rsi: 67.8, atr: 18.6, pattern: 'Pin Bar Reversal',      offset: 80 * 60000 },
    { action: 'BUY',  fib: '50.0%', conf: 75, tf: '4H',  validFor: 8,  rsi: 42.1, atr: 15.8, pattern: 'Doji on 50% FIB',      offset: 225 * 60000 },
    { action: 'BUY',  fib: '61.8%', conf: 88, tf: '1D',  validFor: 24, rsi: 28.9, atr: 28.4, pattern: 'Doji on Golden FIB',   offset: 22 * 3600000 },
  ];

  for (const s of seeds) {
    const sl  = s.action === 'BUY' ? price - s.atr * 1.5 : price + s.atr * 1.5;
    const tp1 = s.action === 'BUY' ? price + s.atr * 3.0 : price - s.atr * 3.0;
    const tp2 = s.action === 'BUY' ? price + s.atr * 6.0 : price - s.atr * 6.0;
    const mtf = JSON.stringify({ h1: s.tf === '1H', h4: s.tf === '4H', d1: s.tf === '1D' });
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(TICKER, s.action, Math.round(price*100)/100, Math.round(sl*100)/100, Math.round(tp1*100)/100, Math.round(tp2*100)/100,
           s.tf, s.conf, s.fib, s.pattern, s.rsi, s.atr, Math.round(price*100)/100, s.validFor, mtf, now - s.offset);
  }
  console.log(`✓ Seeded ${seeds.length} signals @ $${price}`);
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'Pulstrade Backend', version: '2.0.0' }));

app.get('/health', (req, res) => res.json({ status: 'ok', signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c }));

app.get('/signals', (req, res) => {
  const limit  = parseInt(req.query.limit) || 50;
  const ticker = req.query.ticker || null;
  let query = 'SELECT * FROM signals';
  const params = [];
  if (ticker) { query += ' WHERE ticker = ?'; params.push(ticker); }
  query += ' ORDER BY timestamp DESC LIMIT ?';
  params.push(limit);
  const rows = db.prepare(query).all(...params);
  res.json(rows.map(r => ({ ...r, mtf: r.mtf ? JSON.parse(r.mtf) : null })));
});

app.get('/signals/:id', (req, res) => {
  const row = db.prepare('SELECT * FROM signals WHERE id = ?').get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Not found' });
  res.json({ ...row, mtf: row.mtf ? JSON.parse(row.mtf) : null });
});

app.get('/price', async (req, res) => {
  try {
    if (!TWELVE_API_KEY) return res.json({ price: 4720.0, ticker: TICKER });
    const symbol = encodeURIComponent(TICKER);
    const r = await axios.get(`https://api.twelvedata.com/price?symbol=${symbol}&apikey=${TWELVE_API_KEY}`, { timeout: 5000 });
    res.json({ price: parseFloat(r.data.price), ticker: TICKER });
  } catch (err) {
    res.json({ price: 4720.0, ticker: TICKER });
  }
});

// ── Candles Endpoint — REAL data from Twelve Data ─────────────────────────────
app.get('/candles', async (req, res) => {
  try {
    const interval   = req.query.interval   || '5min';
    const outputsize = req.query.limit       || 100;
    if (!TWELVE_API_KEY) return res.json(generateMockCandles(parseInt(outputsize)));
    const symbol = encodeURIComponent(TICKER);
    const url = `https://api.twelvedata.com/time_series?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const r = await axios.get(url, { timeout: 10000 });
    if (!r.data || !r.data.values) return res.json(generateMockCandles(parseInt(outputsize)));
    const candles = r.data.values.map(v => ({
      time:   new Date(v.datetime).getTime(),
      open:   parseFloat(v.open),
      high:   parseFloat(v.high),
      low:    parseFloat(v.low),
      close:  parseFloat(v.close),
    })).reverse();
    res.json(candles);
  } catch (err) {
    console.error('Candles error:', err.message);
    res.json(generateMockCandles(100));
  }
});

// ── News Endpoint ─────────────────────────────────────────────────────────────
app.get('/news', async (req, res) => {
  try {
    const q = encodeURIComponent('gold price XAU OR Federal Reserve interest rates OR Trump tariffs economy OR geopolitical risk gold OR inflation CPI dollar');
    const url = `https://newsapi.org/v2/everything?q=${q}&language=en&sortBy=publishedAt&pageSize=15&apiKey=${NEWS_API_KEY}`;
    const r = await axios.get(url, { timeout: 10000 });
    if (!r.data || !r.data.articles) return res.json([]);
    const articles = r.data.articles
      .filter(a => a.title && a.title !== '[Removed]')
      .map(a => {
        const text = (a.title + ' ' + (a.description || '')).toLowerCase();
        let impact = 'low';
        if (text.match(/fed|federal reserve|rate|inflation|cpi|nfp|trump|war|crisis|crash|rally|record/)) impact = 'high';
        else if (text.match(/gold|dollar|euro|oil|market|economy|gdp/)) impact = 'medium';
        return {
          title:       a.title,
          description: a.description,
          source:      a.source?.name || 'Unknown',
          time:        timeAgo(new Date(a.publishedAt)),
          impact,
          url:         a.url,
          image:       a.urlToImage,
        };
      });
    res.json(articles);
  } catch (err) {
    console.error('News error:', err.message);
    res.json([]);
  }
});

// ── TradingView Webhook ───────────────────────────────────────────────────────
app.post('/webhook', express.json(), (req, res) => {
  try {
    const data = req.body;
    console.log('Webhook received:', JSON.stringify(data));

    if (!data.action || !data.price || !data.ticker) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Quality filter — min 75% confidence, H1 or H4 required
    const confidence = parseInt(data.confidence) || 0;
    const h1 = data.mtf?.h1 === true || data.mtf?.h1 === 'true';
    const h4 = data.mtf?.h4 === true || data.mtf?.h4 === 'true';
    if (confidence < 75 || (!h1 && !h4)) {
      console.log(`Signal filtered: confidence=${confidence}, h1=${h1}, h4=${h4}`);
      return res.status(200).json({ filtered: true, reason: 'Below quality threshold' });
    }

    const tf = data.timeframe || '';
    const signal = {
      ticker:    data.ticker || 'XAU/USD',
      action:    data.action.toUpperCase(),
      price:     parseFloat(data.price),
      sl:        data.sl  ? parseFloat(data.sl)  : null,
      tp1:       data.tp1 ? parseFloat(data.tp1) : null,
      tp2:       data.tp2 ? parseFloat(data.tp2) : null,
      timeframe: tf,
      confidence: confidence,
      fib_level:  data.fib_level || null,
      pattern:    data.pattern   || null,
      rsi:        data.rsi ? parseFloat(data.rsi) : null,
      atr:        data.atr ? parseFloat(data.atr) : null,
      current_price: parseFloat(data.price),
      entry_valid_for: tf.includes('H') ? (tf === '1H' ? 2 : 8) : 24,
      mtf: JSON.stringify({ h1: data.mtf?.h1 || false, h4: data.mtf?.h4 || false, d1: data.mtf?.d1 || false }),
      timestamp: Date.now(),
    };

    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
      VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)`).run(signal);

    console.log(`✓ Signal saved: ${signal.action} ${signal.ticker} @ ${signal.price}`);
    sendSignalPush({ ...signal, id: db.prepare('SELECT last_insert_rowid() as id').get().id });
    res.json({ success: true, signal });
  } catch (err) {
    console.error('Webhook error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Auto Trade — Connect MT5 ──────────────────────────────────────────────────
app.post('/autotrade/connect-mt5', express.json(), async (req, res) => {
  const { login, password, server, platform, lotSize, autoTradeEnabled } = req.body;
  if (!login || !password || !server) return res.status(400).json({ error: 'Missing credentials' });
  if (!METAAPI_TOKEN) return res.status(500).json({ error: 'MetaApi not configured' });
  try {
    const r = await axios.post(
      'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts',
      { login: login.toString(), password, name: 'Pulstrade_' + login, server, platform: platform || 'mt5', magic: 20240410, application: 'MetaApi', type: 'cloud' },
      { headers: { 'auth-token': METAAPI_TOKEN, 'Content-Type': 'application/json' }, timeout: 30000 }
    );
    const accountId = r.data.id;
    db.prepare('INSERT OR REPLACE INTO autotrade_accounts (account_id, lot_size, auto_trade) VALUES (?, ?, ?)')
      .run(accountId, lotSize || 0.01, autoTradeEnabled ? 1 : 0);
    res.json({ success: true, accountId });
  } catch (err) {
    console.error('MT5 connect error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.message || 'Connection failed' });
  }
});

app.get('/autotrade/account/:id', async (req, res) => {
  try {
    const r = await axios.get(
      `${METAAPI_URL}/users/current/accounts/${req.params.id}/account-information`,
      { headers: { 'auth-token': METAAPI_TOKEN }, timeout: 15000 }
    );
    res.json(r.data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/autotrade/connect', express.json(), (req, res) => {
  const { accountId, autoTradeEnabled } = req.body;
  if (!accountId) return res.status(400).json({ error: 'Missing accountId' });
  try {
    db.prepare('UPDATE autotrade_accounts SET auto_trade = ? WHERE account_id = ?')
      .run(autoTradeEnabled ? 1 : 0, accountId);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Auto Execute Trades via MetaApi ───────────────────────────────────────────
async function autoExecuteForAllAccounts(signal) {
  try {
    const accounts = db.prepare('SELECT * FROM autotrade_accounts WHERE auto_trade = 1').all();
    for (const account of accounts) {
      try {
        await axios.post(
          `${METAAPI_URL}/users/current/accounts/${account.account_id}/trade`,
          {
            actionType: signal.action === 'BUY' ? 'ORDER_TYPE_BUY' : 'ORDER_TYPE_SELL',
            symbol:     'XAUUSD',
            volume:     account.lot_size || 0.01,
            stopLoss:   signal.sl,
            takeProfit: signal.tp1,
            comment:    `Pulstrade ${signal.fib_level || ''} ${signal.confidence || ''}%`,
          },
          { headers: { 'auth-token': METAAPI_TOKEN }, timeout: 30000 }
        );
        console.log(`✓ Auto trade executed for account ${account.account_id}`);
      } catch (err) {
        console.error(`Auto trade failed for ${account.account_id}:`, err.response?.data || err.message);
      }
    }
  } catch (err) {
    console.error('Auto execute error:', err.message);
  }
}

// ── Scan & Listen ─────────────────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5 * 60 * 1000);
app.listen(PORT, () => console.log(`Pulstrade backend v2.0 on port ${PORT}`));
