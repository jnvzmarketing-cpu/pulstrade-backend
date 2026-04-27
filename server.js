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
    else { console.warn('⚠️ Push disabled'); }
  } catch(e) { console.warn('Firebase error:', e.message); }
}

function sendSignalPush(signal) {
  // Fire-and-forget — never blocks scanner
  if (!admin.apps.length) return;
  const emoji = signal.action === 'BUY' ? '📈' : '📉';
  const title = `${emoji} ${signal.action} Signal — XAU/USD`;
  const body  = `${signal.strategy || 'FIB'} · Entry: $${signal.price} · ${signal.confidence || 0}% Confidence`;
  admin.messaging().send({
    topic: 'signals',
    notification: { title, body },
    data: { action: signal.action, price: String(signal.price), confidence: String(signal.confidence || 0) },
    apns: { payload: { aps: { sound: 'default', badge: 1 } }, headers: { 'apns-priority': '10' } },
    android: { priority: 'high', notification: { title, body, channelId: 'pulstrade_signals', priority: 'max' } },
  }).then(() => console.log(`✓ Push: ${title}`)).catch(e => console.error('Push error:', e.message));
}

const app  = express();
const PORT = process.env.PORT || 3000;
const TWELVE_API_KEY = process.env.TWELVE_DATA_API_KEY || '';
const NEWS_API_KEY   = process.env.NEWS_API_KEY || '4ada05cd8e1d46f2bbb4f011506044fd';
const METAAPI_TOKEN  = process.env.METAAPI_TOKEN || '';
const METAAPI_URL    = 'https://mt-client-api-v1.london.agiliumtrade.ai';
const TICKER = 'XAU/USD';

app.use(cors());
app.use(express.json());

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
    strategy        TEXT DEFAULT 'FIB',
    rsi             REAL,
    atr             REAL,
    current_price   REAL,
    entry_valid_for REAL,
    mtf             TEXT,
    timestamp       INTEGER NOT NULL,
    created_at      INTEGER DEFAULT (strftime('%s','now') * 1000),
    outcome         TEXT DEFAULT 'open',
    exit_price      REAL,
    closed_at       INTEGER,
    pnl_r           REAL
  );
  CREATE TABLE IF NOT EXISTS autotrade_accounts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT UNIQUE NOT NULL,
    lot_size   REAL DEFAULT 0.01,
    auto_trade INTEGER DEFAULT 1,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
`);

// ── Migrations ────────────────────────────────────────────
try {
  const cols = db.prepare("PRAGMA table_info(signals)").all().map(c => c.name);
  if (!cols.includes('outcome'))    db.exec("ALTER TABLE signals ADD COLUMN outcome TEXT DEFAULT 'open'");
  if (!cols.includes('exit_price')) db.exec("ALTER TABLE signals ADD COLUMN exit_price REAL");
  if (!cols.includes('closed_at'))  db.exec("ALTER TABLE signals ADD COLUMN closed_at INTEGER");
  if (!cols.includes('pnl_r'))      db.exec("ALTER TABLE signals ADD COLUMN pnl_r REAL");
  if (!cols.includes('strategy'))   db.exec("ALTER TABLE signals ADD COLUMN strategy TEXT DEFAULT 'FIB'");
  console.log('✓ Migrations OK');
} catch(e) { console.error('Migration error:', e.message); }

// ── Cleanup ────────────────────────────────────────────────
try {
  db.prepare("DELETE FROM signals WHERE timeframe = '1D'").run();
  db.prepare("DELETE FROM signals WHERE timestamp < ?").run(Date.now() - 30*24*3600000);
} catch(e) {}

// ── PRICE CACHE ───────────────────────────────────────────
let cachedPrice = { price: null, timestamp: 0 };
async function fetchLivePrice() {
  if (!TWELVE_API_KEY) return null;
  try {
    const r = await axios.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(TICKER)}&apikey=${TWELVE_API_KEY}`, { timeout: 5000 });
    const p = parseFloat(r.data?.price);
    if (!isNaN(p) && p > 0) { cachedPrice = { price: p, timestamp: Date.now() }; return p; }
    return null;
  } catch(e) { return null; }
}
fetchLivePrice();
setInterval(fetchLivePrice, 30000);

// ── CANDLE CACHE ──────────────────────────────────────────
const candleCache = {};
const CANDLE_CACHE_TTL = 5 * 60 * 1000;

async function fetchCandles(interval, outputsize=100) {
  const cacheKey = `${interval}_${outputsize}`;
  const cached = candleCache[cacheKey];
  if (cached && (Date.now() - cached.timestamp) < CANDLE_CACHE_TTL) return cached.data;
  if (!TWELVE_API_KEY) return generateMockCandles(outputsize);
  try {
    const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(TICKER)}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const res = await axios.get(url, { timeout: 10000 });
    if (!res.data?.values) return cached?.data || generateMockCandles(outputsize);
    const candles = res.data.values.map(v => ({
      open: parseFloat(v.open), high: parseFloat(v.high),
      low:  parseFloat(v.low),  close: parseFloat(v.close),
      timestamp: new Date(v.datetime).getTime(),
    }));
    candleCache[cacheKey] = { data: candles, timestamp: Date.now() };
    return candles;
  } catch(e) { return cached?.data || generateMockCandles(outputsize); }
}

function generateMockCandles(count) {
  const candles = [];
  const basePrice = cachedPrice.price || 4700;
  let price = basePrice + Math.random()*30;
  const now = Date.now();
  for (let i=count-1; i>=0; i--) {
    const change = (Math.random()-0.5)*15;
    const open=price, close=price+change;
    candles.push({ open, high:Math.max(open,close)+Math.random()*8, low:Math.min(open,close)-Math.random()*8, close, timestamp:now-i*300000 });
    price = close;
  }
  return candles;
}

// ── Helpers ──────────────────────────────────────────────
function timeAgo(date) {
  const s = Math.floor((new Date() - date) / 1000);
  if (s < 3600)  return `${Math.floor(s/60)}m ago`;
  if (s < 86400) return `${Math.floor(s/3600)}h ago`;
  return `${Math.floor(s/86400)}d ago`;
}

function calcEMA(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce((a,b) => a+b, 0) / period;
  for (let i = period; i < values.length; i++) ema = values[i] * k + ema * (1-k);
  return ema;
}

function calcRSI(closes) {
  let gains=0, losses=0;
  for (let i=1; i<=14; i++) {
    const diff = closes[i-1]-closes[i];
    if (diff>0) gains+=diff; else losses-=diff;
  }
  return Math.round((100-100/(1+gains/14/(losses/14||0.001)))*10)/10;
}

function calcATR(candles) {
  let atrSum=0;
  for (let i=0; i<14; i++) {
    atrSum += Math.max(
      candles[i].high - candles[i].low,
      Math.abs(candles[i].high - (candles[i+1]?.close||candles[i].close)),
      Math.abs(candles[i].low  - (candles[i+1]?.close||candles[i].close))
    );
  }
  return Math.round((atrSum/14)*100)/100;
}

function calcBollingerBands(closes, period=20, std=2) {
  if (closes.length < period) return null;
  const slice = closes.slice(0, period);
  const mean = slice.reduce((a,b) => a+b, 0) / period;
  const variance = slice.reduce((a,b) => a + Math.pow(b-mean,2), 0) / period;
  const s = Math.sqrt(variance);
  return { upper: mean+std*s, middle: mean, lower: mean-std*s };
}

function isEconomicEventSoon(offsetMin=10) {
  // DISABLED in v4.9.2 — was blocking too much
  // Re-enable later when we have real economic calendar API
  return false;
}

function isMarketClosed() {
  const now = new Date();
  const day  = now.getUTCDay();
  const hour = now.getUTCHours();
  return day === 6 || day === 0 || (day === 5 && hour >= 21) || (day === 1 && hour < 1);
}

function detectPattern(candles, action) {
  const c0=candles[0], c1=candles[1];
  const body0  = Math.abs(c0.close-c0.open);
  const range0 = c0.high-c0.low;
  const upper0 = c0.high - Math.max(c0.close,c0.open);
  const lower0 = Math.min(c0.close,c0.open) - c0.low;

  if (action==='BUY' && c0.close>c0.open && c0.close>c1.open && c0.open<c1.close) return { score: 20, name: 'Bullish Engulfing' };
  if (action==='SELL' && c0.close<c0.open && c0.close<c1.close && c0.open>c1.open) return { score: 20, name: 'Bearish Engulfing' };
  if (action==='BUY' && lower0>body0*2.5 && lower0>upper0*2) return { score: 18, name: 'Bullish Pin Bar' };
  if (action==='SELL' && upper0>body0*2.5 && upper0>lower0*2) return { score: 18, name: 'Bearish Pin Bar' };
  if (range0>0 && body0/range0<0.25) return { score: 12, name: 'Doji' };
  if (c0.high<c1.high && c0.low>c1.low) return { score: 10, name: 'Inside Bar' };
  return { score: 0, name: 'No pattern' };
}

// ════════════════════════════════════════════════════════════════════════
// STRATEGY 1: FIB PULLBACK (Trend markets) — REQUIRES 3+ CONFIRMATIONS
// ════════════════════════════════════════════════════════════════════════
function scanFibPullback(candles, tf) {
  const closes = candles.map(c => c.close);
  const price = closes[0];
  const rsi = calcRSI(closes);
  const atr = calcATR(candles);

  const ema50 = calcEMA(closes, 50);
  const ema200 = calcEMA(closes, 200);
  const uptrend = ema50 && ema200 && ema50 > ema200;
  const downtrend = ema50 && ema200 && ema50 < ema200;

  // Only run in trending markets
  if (!uptrend && !downtrend) return [];

  let swingHigh=-Infinity, swingLow=Infinity;
  for (let i=0; i<30; i++) {
    if (candles[i].high > swingHigh) swingHigh = candles[i].high;
    if (candles[i].low  < swingLow)  swingLow = candles[i].low;
  }
  const fibRange = swingHigh - swingLow;

  const fibs = {
    '61.8%': swingHigh - fibRange*0.618,
    '50.0%': swingHigh - fibRange*0.500,
    '38.2%': swingHigh - fibRange*0.382,
    '78.6%': swingHigh - fibRange*0.786,
  };

  const signals = [];

  // Calculate Bollinger Bands once
  const bb = calcBollingerBands(closes);

  for (const [fibName, fibValue] of Object.entries(fibs)) {
    // CONFIRMATION 1: Price near FIB level (TIGHTER — was 2.5, now 1.5)
    const tolerance = atr * 1.5;
    if (Math.abs(price - fibValue) > tolerance) continue;

    const action = uptrend ? 'BUY' : 'SELL';

    // CONFIRMATION 2: Price in correct zone (TIGHTER — was 70%, now 50%)
    const isBuyZone  = price < swingLow  + fibRange * 0.50;
    const isSellZone = price > swingHigh - fibRange * 0.50;
    if (action==='BUY'  && !isBuyZone)  continue;
    if (action==='SELL' && !isSellZone) continue;

    // Now count VALID CONFIRMATIONS — need 3+
    const confirmations = [];
    let score = 0;

    // CONF 1: FIB Level itself (always counts since we passed the check)
    const fibScores = { '61.8%':25, '50.0%':20, '38.2%':18, '78.6%':15 };
    const fibPts = fibScores[fibName] || 10;
    score += fibPts;
    confirmations.push(`FIB ${fibName}: +${fibPts}pts`);

    // CONF 2: Strong RSI position (NOT neutral)
    let rsiConfirmed = false;
    if (action==='BUY' && rsi >= 25 && rsi <= 45) {
      score += 20;
      confirmations.push(`RSI oversold ${rsi}: +20pts`);
      rsiConfirmed = true;
    } else if (action==='SELL' && rsi >= 55 && rsi <= 75) {
      score += 20;
      confirmations.push(`RSI overbought ${rsi}: +20pts`);
      rsiConfirmed = true;
    } else if (action==='BUY' && rsi < 25) {
      score += 25;
      confirmations.push(`RSI extreme oversold ${rsi}: +25pts`);
      rsiConfirmed = true;
    } else if (action==='SELL' && rsi > 75) {
      score += 25;
      confirmations.push(`RSI extreme overbought ${rsi}: +25pts`);
      rsiConfirmed = true;
    }

    // CONF 3: Trend confirmation via EMA50
    let emaConfirmed = false;
    if (action==='BUY' && price > ema50) {
      score += 15;
      confirmations.push('Above EMA50: +15pts');
      emaConfirmed = true;
    } else if (action==='SELL' && price < ema50) {
      score += 15;
      confirmations.push('Below EMA50: +15pts');
      emaConfirmed = true;
    }

    // CONF 4: Bullish/Bearish Pattern (Pin Bar, Engulfing, etc.)
    const pattern = detectPattern(candles, action);
    let patternConfirmed = false;
    if (pattern.score >= 18) { // Only strong patterns count
      score += pattern.score;
      confirmations.push(`${pattern.name}: +${pattern.score}pts`);
      patternConfirmed = true;
    } else if (pattern.score > 0) {
      score += pattern.score; // weak pattern adds points but doesn't count as confirmation
    }

    // CONF 5: Bollinger Band position
    let bbConfirmed = false;
    if (bb) {
      if (action==='BUY' && price <= bb.lower * 1.005) {
        score += 15;
        confirmations.push('At BB Lower: +15pts');
        bbConfirmed = true;
      } else if (action==='SELL' && price >= bb.upper * 0.995) {
        score += 15;
        confirmations.push('At BB Upper: +15pts');
        bbConfirmed = true;
      }
    }

    // CONF 6: Strong candle (>60% body of range — momentum confirmation)
    const c0 = candles[0];
    const candleBody = Math.abs(c0.close - c0.open);
    const candleRange = c0.high - c0.low;
    let momentumConfirmed = false;
    if (candleRange > 0 && candleBody / candleRange > 0.6) {
      const isBullishCandle = c0.close > c0.open;
      const isBearishCandle = c0.close < c0.open;
      if ((action==='BUY' && isBullishCandle) || (action==='SELL' && isBearishCandle)) {
        score += 10;
        confirmations.push('Strong momentum candle: +10pts');
        momentumConfirmed = true;
      }
    }

    // ── REQUIRE 3+ CONFIRMATIONS ──
    // FIB level is implicit (already filtered). Count rsi/ema/pattern/bb/momentum
    const validConfirmations = [rsiConfirmed, emaConfirmed, patternConfirmed, bbConfirmed, momentumConfirmed].filter(Boolean).length;
    
    if (validConfirmations < 3) {
      console.log(`🚫 [${tf.label}] ${action} ${fibName} blocked — only ${validConfirmations}/5 confirmations (need 3+)`);
      continue;
    }

    // SL/TP
    let sl, tp1, tp2;
    if (action==='BUY') {
      sl  = Math.round((Math.min(fibValue, price) - atr*1.5)*100)/100;
      tp1 = Math.round((price + atr*3.0)*100)/100;
      tp2 = Math.round((price + atr*5.0)*100)/100;
    } else {
      sl  = Math.round((Math.max(fibValue, price) + atr*1.5)*100)/100;
      tp1 = Math.round((price - atr*3.0)*100)/100;
      tp2 = Math.round((price - atr*5.0)*100)/100;
    }

    if (action==='BUY' && sl >= price) continue;
    if (action==='SELL' && sl <= price) continue;
    const rr = Math.abs(tp1-price) / Math.abs(price-sl);
    if (rr < 2.0) continue;

    signals.push({
      action, price: Math.round(price*100)/100,
      sl, tp1, tp2,
      confidence: Math.min(Math.round(score), 100),
      fib_level: fibName,
      pattern: pattern.name,
      strategy: 'FIB',
      note: `${validConfirmations}/5 confirmations | ${confirmations.slice(0,3).join(' | ')}`,
      rsi, atr,
    });
    console.log(`✅ [${tf.label}] ${action} ${fibName} — ${validConfirmations}/5 confirmations (${confirmations.length} reasons)`);
  }

  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// STRATEGY 2: RANGE BOUNCE (Sideways markets)
// ════════════════════════════════════════════════════════════════════════
function scanRangeBounce(candles, tf) {
  const closes = candles.map(c => c.close);
  const price = closes[0];
  const rsi = calcRSI(closes);
  const atr = calcATR(candles);

  const ema50 = calcEMA(closes, 50);
  const ema200 = calcEMA(closes, 200);
  
  // Range = EMA50 close to EMA200 (no strong trend)
  if (!ema50 || !ema200) return [];
  const emaDiff = Math.abs(ema50 - ema200) / ema200;
  if (emaDiff > 0.03) return []; // > 3% difference = too trendy, skip (was 2%)

  // Find range high/low
  let rangeHigh=-Infinity, rangeLow=Infinity;
  for (let i=0; i<50; i++) {
    if (!candles[i]) break;
    if (candles[i].high > rangeHigh) rangeHigh = candles[i].high;
    if (candles[i].low  < rangeLow)  rangeLow = candles[i].low;
  }
  const rangeSize = rangeHigh - rangeLow;
  if (rangeSize < atr * 2) return []; // range too small (was 3)

  const bb = calcBollingerBands(closes);
  if (!bb) return [];

  const signals = [];

  // BUY near range low OR BB lower
  const nearLow = price < rangeLow + rangeSize * 0.25;
  const atBBLower = price <= bb.lower * 1.005;

  if (nearLow && atBBLower && rsi < 55) {
    let score = 30; // base
    const reasons = ['Range Low Bounce: +30pts'];
    
    if (rsi < 30) { score += 20; reasons.push('RSI extreme oversold: +20pts'); }
    else if (rsi < 40) { score += 15; reasons.push('RSI oversold: +15pts'); }

    const pattern = detectPattern(candles, 'BUY');
    score += pattern.score;
    if (pattern.score > 0) reasons.push(`${pattern.name}: +${pattern.score}pts`);

    const sl = Math.round((rangeLow - atr*1.5)*100)/100;
    const tp1 = Math.round((rangeHigh - rangeSize*0.3)*100)/100;
    const tp2 = Math.round((rangeHigh - rangeSize*0.15)*100)/100;
    const rr = Math.abs(tp1-price) / Math.abs(price-sl);

    if (rr >= 1.5 && score >= 50) {
      signals.push({
        action: 'BUY', price: Math.round(price*100)/100,
        sl, tp1, tp2,
        confidence: Math.min(Math.round(score), 100),
        fib_level: null,
        pattern: pattern.name,
        strategy: 'Range Bounce',
        note: reasons.slice(0,3).join(' | '),
        rsi, atr,
      });
    }
  }

  // SELL near range high OR BB upper
  const nearHigh = price > rangeHigh - rangeSize * 0.25;
  const atBBUpper = price >= bb.upper * 0.995;

  if (nearHigh && atBBUpper && rsi > 45) {
    let score = 30;
    const reasons = ['Range High Rejection: +30pts'];

    if (rsi > 70) { score += 20; reasons.push('RSI extreme overbought: +20pts'); }
    else if (rsi > 60) { score += 15; reasons.push('RSI overbought: +15pts'); }

    const pattern = detectPattern(candles, 'SELL');
    score += pattern.score;
    if (pattern.score > 0) reasons.push(`${pattern.name}: +${pattern.score}pts`);

    const sl = Math.round((rangeHigh + atr*1.5)*100)/100;
    const tp1 = Math.round((rangeLow + rangeSize*0.3)*100)/100;
    const tp2 = Math.round((rangeLow + rangeSize*0.15)*100)/100;
    const rr = Math.abs(tp1-price) / Math.abs(price-sl);

    if (rr >= 1.5 && score >= 50) {
      signals.push({
        action: 'SELL', price: Math.round(price*100)/100,
        sl, tp1, tp2,
        confidence: Math.min(Math.round(score), 100),
        fib_level: null,
        pattern: pattern.name,
        strategy: 'Range Bounce',
        note: reasons.slice(0,3).join(' | '),
        rsi, atr,
      });
    }
  }

  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// STRATEGY 3: BREAKOUT (Momentum)
// ════════════════════════════════════════════════════════════════════════
function scanBreakout(candles, tf) {
  const closes = candles.map(c => c.close);
  const price = closes[0];
  const rsi = calcRSI(closes);
  const atr = calcATR(candles);

  // Find consolidation high/low from candles 2-20
  let consHigh=-Infinity, consLow=Infinity;
  for (let i=2; i<22; i++) {
    if (!candles[i]) break;
    if (candles[i].high > consHigh) consHigh = candles[i].high;
    if (candles[i].low  < consLow)  consLow = candles[i].low;
  }
  const consRange = consHigh - consLow;
  if (consRange < atr * 1.5) return []; // consolidation too tight (was 2)

  const signals = [];

  // BULLISH BREAKOUT — price broke above consHigh
  if (price > consHigh && candles[0].close > candles[0].open) {
    const breakoutSize = price - consHigh;
    if (breakoutSize > atr * 0.3 && breakoutSize < atr * 3) {
      let score = 35;
      const reasons = ['Bullish Breakout: +35pts'];

      // Volume check via candle body
      const body = Math.abs(candles[0].close - candles[0].open);
      const range = candles[0].high - candles[0].low;
      if (range > 0 && body/range > 0.7) { score += 15; reasons.push('Strong breakout candle: +15pts'); }

      if (rsi > 50 && rsi < 70) { score += 15; reasons.push('RSI bullish: +15pts'); }

      const pattern = detectPattern(candles, 'BUY');
      if (pattern.score > 0) { score += pattern.score; reasons.push(`${pattern.name}: +${pattern.score}pts`); }

      const sl = Math.round((consHigh - atr*0.5)*100)/100;
      const tp1 = Math.round((price + consRange*0.75)*100)/100;
      const tp2 = Math.round((price + consRange*1.5)*100)/100;
      const rr = Math.abs(tp1-price) / Math.abs(price-sl);

      if (rr >= 1.5 && score >= 50) {
        signals.push({
          action: 'BUY', price: Math.round(price*100)/100,
          sl, tp1, tp2,
          confidence: Math.min(Math.round(score), 100),
          fib_level: null,
          pattern: pattern.name,
          strategy: 'Breakout',
          note: reasons.slice(0,3).join(' | '),
          rsi, atr,
        });
      }
    }
  }

  // BEARISH BREAKDOWN
  if (price < consLow && candles[0].close < candles[0].open) {
    const breakdownSize = consLow - price;
    if (breakdownSize > atr * 0.3 && breakdownSize < atr * 3) {
      let score = 35;
      const reasons = ['Bearish Breakdown: +35pts'];

      const body = Math.abs(candles[0].close - candles[0].open);
      const range = candles[0].high - candles[0].low;
      if (range > 0 && body/range > 0.7) { score += 15; reasons.push('Strong breakdown candle: +15pts'); }

      if (rsi < 50 && rsi > 30) { score += 15; reasons.push('RSI bearish: +15pts'); }

      const pattern = detectPattern(candles, 'SELL');
      if (pattern.score > 0) { score += pattern.score; reasons.push(`${pattern.name}: +${pattern.score}pts`); }

      const sl = Math.round((consLow + atr*0.5)*100)/100;
      const tp1 = Math.round((price - consRange*0.75)*100)/100;
      const tp2 = Math.round((price - consRange*1.5)*100)/100;
      const rr = Math.abs(tp1-price) / Math.abs(price-sl);

      if (rr >= 1.5 && score >= 50) {
        signals.push({
          action: 'SELL', price: Math.round(price*100)/100,
          sl, tp1, tp2,
          confidence: Math.min(Math.round(score), 100),
          fib_level: null,
          pattern: pattern.name,
          strategy: 'Breakout',
          note: reasons.slice(0,3).join(' | '),
          rsi, atr,
        });
      }
    }
  }

  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// STRATEGY 4: EMA PULLBACK (Very common — trend + mini-correction)
// ════════════════════════════════════════════════════════════════════════
function scanEmaPullback(candles, tf) {
  const closes = candles.map(c => c.close);
  const price = closes[0];
  const rsi = calcRSI(closes);
  const atr = calcATR(candles);

  const ema20 = calcEMA(closes, 20);
  const ema50 = calcEMA(closes, 50);
  const ema200 = calcEMA(closes, 200);
  if (!ema20 || !ema50 || !ema200) return [];

  const uptrend = ema50 > ema200;
  const downtrend = ema50 < ema200;
  if (!uptrend && !downtrend) return [];

  const signals = [];

  // BUY: Uptrend + price pulled back to EMA20 or EMA50
  if (uptrend) {
    // Looser check — price below EMA within 3 ATR range
    const nearEma20 = Math.abs(price - ema20) < atr * 2.5 && price < ema20;
    const nearEma50 = Math.abs(price - ema50) < atr * 3.0 && price < ema50;
    
    if (nearEma20 || nearEma50) {
      let score = 40;
      const reasons = [nearEma20 ? 'Pullback to EMA20: +40pts' : 'Pullback to EMA50: +40pts'];

      if (rsi >= 30 && rsi <= 55) { score += 20; reasons.push('RSI oversold: +20pts'); }
      else if (rsi < 30) { score += 15; reasons.push('RSI very oversold: +15pts'); }

      const pattern = detectPattern(candles, 'BUY');
      if (pattern.score > 0) { score += pattern.score; reasons.push(`${pattern.name}: +${pattern.score}pts`); }

      // Trend strength bonus
      if (price > ema200) { score += 10; reasons.push('Above EMA200: +10pts'); }

      // SL always below price for BUY — simple and safe
      const sl = Math.round((price - atr * 1.5) * 100) / 100;
      const tp1 = Math.round((price + atr * 2.5)*100)/100;
      const tp2 = Math.round((price + atr * 4.5)*100)/100;
      const rr = Math.abs(tp1-price) / Math.abs(price-sl);

      if (rr >= 1.0 && score >= 30) {
        signals.push({
          action: 'BUY', price: Math.round(price*100)/100,
          sl, tp1, tp2,
          confidence: Math.min(Math.round(score), 100),
          fib_level: null,
          pattern: pattern.name,
          strategy: 'EMA Pullback',
          note: reasons.slice(0,3).join(' | '),
          rsi, atr,
        });
      }
    }
  }

  // SELL: Downtrend + price pulled up to EMA20 or EMA50
  if (downtrend) {
    // Looser check — price above EMA within 3 ATR range
    const nearEma20 = Math.abs(price - ema20) < atr * 2.5 && price > ema20;
    const nearEma50 = Math.abs(price - ema50) < atr * 3.0 && price > ema50;
    
    if (nearEma20 || nearEma50) {
      let score = 40;
      const reasons = [nearEma20 ? 'Rally to EMA20: +40pts' : 'Rally to EMA50: +40pts'];

      if (rsi >= 45 && rsi <= 70) { score += 20; reasons.push('RSI overbought: +20pts'); }
      else if (rsi > 70) { score += 15; reasons.push('RSI very overbought: +15pts'); }

      const pattern = detectPattern(candles, 'SELL');
      if (pattern.score > 0) { score += pattern.score; reasons.push(`${pattern.name}: +${pattern.score}pts`); }

      if (price < ema200) { score += 10; reasons.push('Below EMA200: +10pts'); }

      // SL always above price for SELL — simple and safe
      const sl = Math.round((price + atr * 1.5) * 100) / 100;
      const tp1 = Math.round((price - atr * 2.5)*100)/100;
      const tp2 = Math.round((price - atr * 4.5)*100)/100;
      const rr = Math.abs(tp1-price) / Math.abs(price-sl);

      if (rr >= 1.0 && score >= 30) {
        signals.push({
          action: 'SELL', price: Math.round(price*100)/100,
          sl, tp1, tp2,
          confidence: Math.min(Math.round(score), 100),
          fib_level: null,
          pattern: pattern.name,
          strategy: 'EMA Pullback',
          note: reasons.slice(0,3).join(' | '),
          rsi, atr,
        });
      }
    }
  }

  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// SETUP 2: AGGRESSIVE BREAKOUT + RETEST (David's strategy)
// State-Machine implementation matching pulstrade_setup2_v4_1.pine
// ════════════════════════════════════════════════════════════════════════

// State per timeframe — persisted in memory between scans
const setup2State = {
  '5m': null,
  '15m': null,
};

function freshState() {
  return {
    aggBreakLevel: null,
    aggCandleLow:  null,
    aggCandleHigh: null,
    aggRangeSize:  null,
    aggBar:        null,    // candle timestamp when aggressive candle fired
    aggIsBull:     null,
    retestDone:    false,
    retestBar:     null,
  };
}

// SMA helper (Pine SMA equivalent)
function calcSMA(values, period) {
  if (values.length < period) return null;
  const slice = values.slice(0, period);
  return slice.reduce((a,b) => a+b, 0) / period;
}

// CVD calculation (cumulative volume delta over candles, oldest first)
function calcCVD(candles) {
  // candles[0] is newest; reverse for chronological order
  const chrono = [...candles].reverse();
  const cvdSeries = [];
  let cum = 0;
  for (const c of chrono) {
    const vol = c.volume || 1; // Twelve Data Forex = tick volume = always 1+, fallback 1
    const delta = c.close >= c.open ? vol : -vol;
    cum += delta;
    cvdSeries.push(cum);
  }
  // Most recent CVD value
  const cvdNow = cvdSeries[cvdSeries.length - 1];
  // SMA of last 20 CVD values
  const last20 = cvdSeries.slice(-20);
  const cvdMA = last20.length >= 5 ? last20.reduce((a,b) => a+b, 0) / last20.length : cvdNow;
  return { cvdNow, cvdMA, cvdBullish: cvdNow > cvdMA, cvdBearish: cvdNow < cvdMA };
}

function scanSetup2BreakoutRetest(candles, tf) {
  const signals = [];
  if (!candles || candles.length < 220) return signals;
  
  const closes = candles.map(c => c.close);
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const opens  = candles.map(c => c.open);
  
  // Current candle (newest)
  const c0 = candles[0];
  const c1 = candles[1];
  const price = c0.close;
  const open0 = c0.open;
  
  const atr = calcATR(candles);
  const ema200 = calcEMA(closes, 200);
  const sma200 = calcSMA(closes, 200);
  if (!ema200 || !sma200) return signals;
  
  // ── Get or initialize state for this timeframe ──
  if (!setup2State[tf.label]) setup2State[tf.label] = freshState();
  const state = setup2State[tf.label];
  
  // ── 1. ACCUMULATION RANGE CHECK ──
  // Last 20 bars (excluding current)
  const rangeWindow = candles.slice(1, 21);
  if (rangeWindow.length < 20) return signals;
  const rangeHigh = Math.max(...rangeWindow.map(c => c.high));
  const rangeLow  = Math.min(...rangeWindow.map(c => c.low));
  const rangeSize = rangeHigh - rangeLow;
  const isAccumulation = rangeSize <= atr * 2.0;
  
  // ── 2. AGGRESSIVE CANDLE DETECTION ──
  const body0 = Math.abs(c0.close - c0.open);
  const isAggressive = body0 >= atr * 1.5;
  
  // Bullish breakout: prev bar below MAs, current bar broke above + bullish body
  const bullishBreak = isAccumulation && isAggressive
    && c1.close < ema200 && c1.close < sma200
    && c0.close > ema200 && c0.close > sma200
    && c0.close > c0.open;
  
  // Bearish breakdown: prev bar above MAs, current bar broke below + bearish body
  const bearishBreak = isAccumulation && isAggressive
    && c1.close > ema200 && c1.close > sma200
    && c0.close < ema200 && c0.close < sma200
    && c0.close < c0.open;
  
  // ── If new aggressive candle detected, RESET state ──
  if (bullishBreak || bearishBreak) {
    state.aggBreakLevel = bullishBreak ? rangeHigh : rangeLow;
    state.aggCandleLow  = c0.low;
    state.aggCandleHigh = c0.high;
    state.aggRangeSize  = rangeSize;
    state.aggBar        = c0.timestamp;
    state.aggIsBull     = bullishBreak;
    state.retestDone    = false;
    state.retestBar     = null;
    console.log(`💥 [${tf.label}] Setup 2: Aggressive ${bullishBreak ? 'BULL' : 'BEAR'} candle detected @ ${c0.close.toFixed(2)}`);
    return signals; // Setup just started, no signal yet
  }
  
  // ── 3. RETEST DETECTION (only if we have an aggressive candle pending) ──
  if (state.aggBar && !state.retestDone) {
    // Find how many bars since aggressive candle
    const barsSinceAgg = candles.findIndex(c => c.timestamp <= state.aggBar);
    
    if (barsSinceAgg < 0 || barsSinceAgg > 15) {
      // Setup expired — reset
      console.log(`⏰ [${tf.label}] Setup 2: Aggressive candle expired (no retest in 15 bars)`);
      setup2State[tf.label] = freshState();
      return signals;
    }
    
    const retestTolerance = atr * 0.3;
    const breakLevel = state.aggBreakLevel;
    
    if (state.aggIsBull) {
      // Bull retest: price must come down to break level
      if (c0.low <= breakLevel + retestTolerance && c0.low >= breakLevel - retestTolerance) {
        state.retestDone = true;
        state.retestBar = c0.timestamp;
        console.log(`🔄 [${tf.label}] Setup 2: BULL retest hit @ ${c0.low.toFixed(2)} (level: ${breakLevel.toFixed(2)})`);
      }
    } else {
      // Bear retest: price must come up to break level
      if (c0.high >= breakLevel - retestTolerance && c0.high <= breakLevel + retestTolerance) {
        state.retestDone = true;
        state.retestBar = c0.timestamp;
        console.log(`🔄 [${tf.label}] Setup 2: BEAR retest hit @ ${c0.high.toFixed(2)} (level: ${breakLevel.toFixed(2)})`);
      }
    }
  }
  
  // ── 4. TRIGGER CANDLE (only if retest is done) ──
  if (state.aggBar && state.retestDone && state.retestBar) {
    const barsSinceRetest = candles.findIndex(c => c.timestamp <= state.retestBar);
    
    if (barsSinceRetest < 0 || barsSinceRetest > 5) {
      // Setup expired — reset
      console.log(`⏰ [${tf.label}] Setup 2: Retest expired (no trigger in 5 bars)`);
      setup2State[tf.label] = freshState();
      return signals;
    }
    
    // CVD check
    const cvd = calcCVD(candles);
    
    if (state.aggIsBull) {
      // Long trigger: bullish close + (optional) CVD bullish
      const bullishClose = c0.close > c0.open;
      const cvdOK = !state.cvdRequired || cvd.cvdBullish; // We'll honor CVD for now but allow override
      
      if (bullishClose && cvd.cvdBullish) {
        const entry = c0.close;
        const sl = state.aggCandleLow - atr * 0.1;
        const tp1 = state.aggBreakLevel + (state.aggRangeSize * 1.5);
        const tp2 = state.aggBreakLevel + (state.aggRangeSize * 2.5);
        
        // Validate SL placement
        if (sl < entry && tp1 > entry) {
          const rr = (tp1 - entry) / (entry - sl);
          if (rr >= 1.0) {
            signals.push({
              action: 'BUY',
              price: Math.round(entry * 100) / 100,
              sl: Math.round(sl * 100) / 100,
              tp1: Math.round(tp1 * 100) / 100,
              tp2: Math.round(tp2 * 100) / 100,
              confidence: 80,
              fib_level: null,
              pattern: 'Aggressive Bull Break + Retest',
              strategy: 'Setup 2 Breakout+Retest',
              note: `Range: ${state.aggRangeSize.toFixed(2)} | TP = Break + Range×1.5 | RR ${rr.toFixed(2)}`,
              rsi: calcRSI(closes),
              atr,
            });
            console.log(`✅ [${tf.label}] Setup 2 BUY @ ${entry.toFixed(2)} | SL ${sl.toFixed(2)} | TP1 ${tp1.toFixed(2)}`);
          }
        }
        // Reset state — setup fired
        setup2State[tf.label] = freshState();
        return signals;
      }
    } else {
      // Short trigger: bearish close + CVD bearish
      const bearishClose = c0.close < c0.open;
      
      if (bearishClose && cvd.cvdBearish) {
        const entry = c0.close;
        const sl = state.aggCandleHigh + atr * 0.1;
        const tp1 = state.aggBreakLevel - (state.aggRangeSize * 1.5);
        const tp2 = state.aggBreakLevel - (state.aggRangeSize * 2.5);
        
        if (sl > entry && tp1 < entry) {
          const rr = (entry - tp1) / (sl - entry);
          if (rr >= 1.0) {
            signals.push({
              action: 'SELL',
              price: Math.round(entry * 100) / 100,
              sl: Math.round(sl * 100) / 100,
              tp1: Math.round(tp1 * 100) / 100,
              tp2: Math.round(tp2 * 100) / 100,
              confidence: 80,
              fib_level: null,
              pattern: 'Aggressive Bear Break + Retest',
              strategy: 'Setup 2 Breakout+Retest',
              note: `Range: ${state.aggRangeSize.toFixed(2)} | TP = Break - Range×1.5 | RR ${rr.toFixed(2)}`,
              rsi: calcRSI(closes),
              atr,
            });
            console.log(`✅ [${tf.label}] Setup 2 SELL @ ${entry.toFixed(2)} | SL ${sl.toFixed(2)} | TP1 ${tp1.toFixed(2)}`);
          }
        }
        setup2State[tf.label] = freshState();
        return signals;
      }
    }
  }
  
  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// SETUP 3: DOUBLE TOP / DOUBLE BOTTOM REJECTION (David's strategy)
// State-Machine: First pivot → Second pivot in tolerance → EMA20 break = Signal
// ════════════════════════════════════════════════════════════════════════

// Constants
const S3_PIVOT_LEFT = 5;
const S3_PIVOT_RIGHT = 3;
const S3_TOP_TOLERANCE_ATR = 0.5;
const S3_MIN_BARS_BETWEEN = 5;
const S3_MAX_BARS_BETWEEN = 30;
const S3_MAX_BARS_TO_TRIGGER = 10;
const S3_WICK_RATIO = 0.4;
const S3_TP_MULTIPLIER = 1.5;
const S3_SL_BUFFER_ATR = 0.1;
const S3_EMA_TRIGGER_LEN = 20;

// State per timeframe
const setup3State = {
  '5m': null,
  '15m': null,
};

function freshSetup3State() {
  return {
    firstTop:        null,   // { price, barTimestamp, lowSinceTop }
    firstBottom:     null,   // { price, barTimestamp, highSinceBottom }
    secondTop:       null,
    secondBottom:    null,
    triggerArmedBarTimestamp: null,
    lastSignalBarTimestamp:   null,
  };
}

// Pivot detection: looks for confirmed pivot at index (length - right - 1)
// candles array is NEWEST FIRST (index 0 = current bar)
function detectPivotHighFromCandles(candles, left, right) {
  // Convert to chronological order (oldest first) for cleaner indexing
  const chrono = [...candles].reverse();
  const pivotIdx = chrono.length - right - 1;
  if (pivotIdx < left) return null;
  
  const candidate = chrono[pivotIdx];
  for (let i = pivotIdx - left; i < pivotIdx; i++) {
    if (chrono[i].high >= candidate.high) return null;
  }
  for (let i = pivotIdx + 1; i <= pivotIdx + right; i++) {
    if (chrono[i].high >= candidate.high) return null;
  }
  
  return {
    price: candidate.high,
    barTimestamp: candidate.timestamp,
    upperWick: candidate.high - Math.max(candidate.open, candidate.close),
    range: candidate.high - candidate.low,
  };
}

function detectPivotLowFromCandles(candles, left, right) {
  const chrono = [...candles].reverse();
  const pivotIdx = chrono.length - right - 1;
  if (pivotIdx < left) return null;
  
  const candidate = chrono[pivotIdx];
  for (let i = pivotIdx - left; i < pivotIdx; i++) {
    if (chrono[i].low <= candidate.low) return null;
  }
  for (let i = pivotIdx + 1; i <= pivotIdx + right; i++) {
    if (chrono[i].low <= candidate.low) return null;
  }
  
  return {
    price: candidate.low,
    barTimestamp: candidate.timestamp,
    lowerWick: Math.min(candidate.open, candidate.close) - candidate.low,
    range: candidate.high - candidate.low,
  };
}

// Calculate bar distance between two timestamps in candle array
function barsBetween(candles, ts1, ts2) {
  const idx1 = candles.findIndex(c => c.timestamp === ts1);
  const idx2 = candles.findIndex(c => c.timestamp === ts2);
  if (idx1 < 0 || idx2 < 0) return -1;
  return Math.abs(idx1 - idx2);
}

function scanSetup3DoubleTop(candles, tf) {
  const signals = [];
  if (!candles || candles.length < 250) return signals;
  
  const closes = candles.map(c => c.close);
  const c0 = candles[0]; // newest
  const c1 = candles[1]; // previous
  if (!c0 || !c1) return signals;
  
  const atr = calcATR(candles);
  const ema20 = calcEMA(closes, S3_EMA_TRIGGER_LEN);
  if (!ema20 || !atr) return signals;
  
  // Get/init state
  if (!setup3State[tf.label]) setup3State[tf.label] = freshSetup3State();
  const state = setup3State[tf.label];
  
  // ─── Detect pivots ───
  const ph = detectPivotHighFromCandles(candles, S3_PIVOT_LEFT, S3_PIVOT_RIGHT);
  const pl = detectPivotLowFromCandles(candles, S3_PIVOT_LEFT, S3_PIVOT_RIGHT);
  
  const phHasWick = ph && ph.range > 0 && (ph.upperWick / ph.range) >= S3_WICK_RATIO;
  const plHasWick = pl && pl.range > 0 && (pl.lowerWick / pl.range) >= S3_WICK_RATIO;
  
  // ═══════════════ DOUBLE TOP / SHORT SETUP ═══════════════
  
  if (ph && phHasWick) {
    if (!state.firstTop) {
      state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
      console.log(`📍 [${tf.label}] Setup 3: First TOP @ ${ph.price.toFixed(2)}`);
    } else {
      const barsSince = barsBetween(candles, state.firstTop.barTimestamp, ph.barTimestamp);
      const priceDiff = Math.abs(ph.price - state.firstTop.price);
      const tolerance = atr * S3_TOP_TOLERANCE_ATR;
      
      if (barsSince >= S3_MIN_BARS_BETWEEN && barsSince <= S3_MAX_BARS_BETWEEN && priceDiff <= tolerance) {
        // Valid second top → arm trigger
        state.secondTop = { price: ph.price, barTimestamp: ph.barTimestamp };
        state.triggerArmedBarTimestamp = c0.timestamp;
        console.log(`🎯 [${tf.label}] Setup 3: DOUBLE TOP confirmed @ ${ph.price.toFixed(2)} (1st: ${state.firstTop.price.toFixed(2)}) — armed`);
      } else if (barsSince > S3_MAX_BARS_BETWEEN) {
        // Reset, this becomes new first
        state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
        state.secondTop = null;
        state.triggerArmedBarTimestamp = null;
      } else {
        // Within window but doesn't match → update first
        state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
      }
    }
  }
  
  // Track lowest point since first top
  if (state.firstTop && c0.low < state.firstTop.lowSinceTop) {
    state.firstTop.lowSinceTop = c0.low;
  }
  
  // Check for SHORT trigger
  if (state.secondTop && state.triggerArmedBarTimestamp) {
    const barsSinceArm = barsBetween(candles, state.triggerArmedBarTimestamp, c0.timestamp);
    
    if (barsSinceArm > S3_MAX_BARS_TO_TRIGGER) {
      console.log(`⏰ [${tf.label}] Setup 3: SHORT trigger expired (${barsSinceArm} bars without EMA break)`);
      state.secondTop = null;
      state.triggerArmedBarTimestamp = null;
      // Keep firstTop in case a new pattern develops
    } else if (barsSinceArm >= 1 && c0.close < ema20 && c1.close >= ema20) {
      // EMA20 break to downside on this bar
      const topHigh = Math.max(state.firstTop.price, state.secondTop.price);
      const middleLow = state.firstTop.lowSinceTop;
      const measuredMove = topHigh - middleLow;
      
      if (measuredMove > 0) {
        const entry = c0.close;
        const sl = topHigh + (atr * S3_SL_BUFFER_ATR);
        const tp1 = entry - (measuredMove * S3_TP_MULTIPLIER);
        const tp2 = entry - (measuredMove * 2.5);
        
        // Validate SL > entry > tp1 (correct SELL geometry)
        if (sl > entry && tp1 < entry) {
          const rr = (entry - tp1) / (sl - entry);
          if (rr >= 1.0) {
            signals.push({
              action: 'SELL',
              price: Math.round(entry * 100) / 100,
              sl: Math.round(sl * 100) / 100,
              tp1: Math.round(tp1 * 100) / 100,
              tp2: Math.round(tp2 * 100) / 100,
              confidence: 80,
              fib_level: null,
              pattern: 'Double Top + EMA20 Break',
              strategy: 'Setup 3 Double Top',
              note: `Tops: ${state.firstTop.price.toFixed(2)} & ${state.secondTop.price.toFixed(2)} | Move: ${measuredMove.toFixed(2)} | RR ${rr.toFixed(2)}`,
              rsi: calcRSI(closes),
              atr,
            });
            console.log(`✅ [${tf.label}] Setup 3 SELL @ ${entry.toFixed(2)} | SL ${sl.toFixed(2)} | TP1 ${tp1.toFixed(2)}`);
          }
        }
        
        // Reset state — signal fired
        state.firstTop = null;
        state.secondTop = null;
        state.triggerArmedBarTimestamp = null;
        state.lastSignalBarTimestamp = c0.timestamp;
        return signals;
      }
    }
  }
  
  // ═══════════════ DOUBLE BOTTOM / LONG SETUP ═══════════════
  
  if (pl && plHasWick) {
    if (!state.firstBottom) {
      state.firstBottom = { price: pl.price, barTimestamp: pl.barTimestamp, highSinceBottom: pl.price };
      console.log(`📍 [${tf.label}] Setup 3: First BOTTOM @ ${pl.price.toFixed(2)}`);
    } else {
      const barsSince = barsBetween(candles, state.firstBottom.barTimestamp, pl.barTimestamp);
      const priceDiff = Math.abs(pl.price - state.firstBottom.price);
      const tolerance = atr * S3_TOP_TOLERANCE_ATR;
      
      if (barsSince >= S3_MIN_BARS_BETWEEN && barsSince <= S3_MAX_BARS_BETWEEN && priceDiff <= tolerance) {
        state.secondBottom = { price: pl.price, barTimestamp: pl.barTimestamp };
        state.triggerArmedBarTimestamp = c0.timestamp;
        console.log(`🎯 [${tf.label}] Setup 3: DOUBLE BOTTOM confirmed @ ${pl.price.toFixed(2)} (1st: ${state.firstBottom.price.toFixed(2)}) — armed`);
      } else if (barsSince > S3_MAX_BARS_BETWEEN) {
        state.firstBottom = { price: pl.price, barTimestamp: pl.barTimestamp, highSinceBottom: pl.price };
        state.secondBottom = null;
        state.triggerArmedBarTimestamp = null;
      } else {
        state.firstBottom = { price: pl.price, barTimestamp: pl.barTimestamp, highSinceBottom: pl.price };
      }
    }
  }
  
  // Track highest point since first bottom
  if (state.firstBottom && c0.high > state.firstBottom.highSinceBottom) {
    state.firstBottom.highSinceBottom = c0.high;
  }
  
  // Check for LONG trigger
  if (state.secondBottom && state.triggerArmedBarTimestamp) {
    const barsSinceArm = barsBetween(candles, state.triggerArmedBarTimestamp, c0.timestamp);
    
    if (barsSinceArm > S3_MAX_BARS_TO_TRIGGER) {
      console.log(`⏰ [${tf.label}] Setup 3: LONG trigger expired`);
      state.secondBottom = null;
      state.triggerArmedBarTimestamp = null;
    } else if (barsSinceArm >= 1 && c0.close > ema20 && c1.close <= ema20) {
      const bottomLow = Math.min(state.firstBottom.price, state.secondBottom.price);
      const middleHigh = state.firstBottom.highSinceBottom;
      const measuredMove = middleHigh - bottomLow;
      
      if (measuredMove > 0) {
        const entry = c0.close;
        const sl = bottomLow - (atr * S3_SL_BUFFER_ATR);
        const tp1 = entry + (measuredMove * S3_TP_MULTIPLIER);
        const tp2 = entry + (measuredMove * 2.5);
        
        if (sl < entry && tp1 > entry) {
          const rr = (tp1 - entry) / (entry - sl);
          if (rr >= 1.0) {
            signals.push({
              action: 'BUY',
              price: Math.round(entry * 100) / 100,
              sl: Math.round(sl * 100) / 100,
              tp1: Math.round(tp1 * 100) / 100,
              tp2: Math.round(tp2 * 100) / 100,
              confidence: 80,
              fib_level: null,
              pattern: 'Double Bottom + EMA20 Break',
              strategy: 'Setup 3 Double Bottom',
              note: `Bottoms: ${state.firstBottom.price.toFixed(2)} & ${state.secondBottom.price.toFixed(2)} | Move: ${measuredMove.toFixed(2)} | RR ${rr.toFixed(2)}`,
              rsi: calcRSI(closes),
              atr,
            });
            console.log(`✅ [${tf.label}] Setup 3 BUY @ ${entry.toFixed(2)} | SL ${sl.toFixed(2)} | TP1 ${tp1.toFixed(2)}`);
          }
        }
        
        state.firstBottom = null;
        state.secondBottom = null;
        state.triggerArmedBarTimestamp = null;
        state.lastSignalBarTimestamp = c0.timestamp;
        return signals;
      }
    }
  }
  
  return signals;
}

// ════════════════════════════════════════════════════════════════════════
// MAIN SCANNER — runs all strategies on all timeframes
// ════════════════════════════════════════════════════════════════════════
async function scanForSignals() {
  console.log('🔍 Scanner starting...');
  if (isMarketClosed()) { console.log('❌ Market closed — abort'); return; }
  if (isEconomicEventSoon(10)) { console.log('❌ Economic event window — abort'); return; }
  if (!cachedPrice.price) { console.log('❌ No live price — abort'); return; }
  console.log(`✓ Scanner running @ $${cachedPrice.price}`);

  const timeframes = [
    { label: '5m',  interval: '5min',  validFor: 0.25, minScore: 55 },
    { label: '15m', interval: '15min', validFor: 0.5,  minScore: 45 },
    { label: '30m', interval: '30min', validFor: 1,    minScore: 48 },
    { label: '1H',  interval: '1h',    validFor: 2,    minScore: 50 },
    { label: '4H',  interval: '4h',    validFor: 8,    minScore: 48 },
  ];

  // ── MULTI-TIMEFRAME CONSENSUS CHECK ─────────────────────
  // Calculate trend across ALL timeframes first
  const trendVotes = { UP: 0, DOWN: 0, NEUTRAL: 0 };
  const tfTrends = {};
  
  for (const tf of timeframes) {
    try {
      const tfCandles = await fetchCandles(tf.interval, 220);
      if (!tfCandles || tfCandles.length < 50) continue;
      const tfCloses = tfCandles.map(c => c.close);
      const tfEma50 = calcEMA(tfCloses, 50);
      const tfEma200 = calcEMA(tfCloses, 200);
      
      let trend = 'NEUTRAL';
      if (tfEma50 && tfEma200) {
        if (tfEma50 > tfEma200) trend = 'UP';
        else if (tfEma50 < tfEma200) trend = 'DOWN';
      }
      tfTrends[tf.label] = trend;
      trendVotes[trend]++;
    } catch(e) { tfTrends[tf.label] = 'ERROR'; }
  }
  
  console.log(`📊 Trend Consensus: UP=${trendVotes.UP} DOWN=${trendVotes.DOWN} NEUTRAL=${trendVotes.NEUTRAL} | ${JSON.stringify(tfTrends)}`);
  
  // Determine consensus direction (at least 3 of 5 TFs must agree)
  const CONSENSUS_THRESHOLD = 3;
  let consensusDirection = null;
  if (trendVotes.UP >= CONSENSUS_THRESHOLD) consensusDirection = 'BUY';
  else if (trendVotes.DOWN >= CONSENSUS_THRESHOLD) consensusDirection = 'SELL';
  
  if (!consensusDirection) {
    console.log(`⚠️ No clear MTF consensus — skipping all signals (need ${CONSENSUS_THRESHOLD}/5 agreement)`);
    return;
  }
  
  console.log(`✅ MTF Consensus: ${consensusDirection} (${consensusDirection === 'BUY' ? trendVotes.UP : trendVotes.DOWN}/5 timeframes)`);

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 30) continue;

      const price = candles[0].close;

      // Deviation check against live price
      if (cachedPrice.price) {
        const dev = Math.abs(price - cachedPrice.price) / cachedPrice.price;
        if (dev > 0.05) { console.log(`⚠️ [${tf.label}] Stale candles`); continue; }
      }

      // Active strategies: FIB Pullback + Setup 2 (Breakout+Retest) + Setup 3 (Double Top/Bottom)
      // Setup 2 & 3 only on 5m and 15m (David's preferred TFs)
      const fibSignals    = scanFibPullback(candles, tf);
      const setup2Signals = (tf.label === '5m' || tf.label === '15m') 
                            ? scanSetup2BreakoutRetest(candles, tf) 
                            : [];
      const setup3Signals = (tf.label === '5m' || tf.label === '15m')
                            ? scanSetup3DoubleTop(candles, tf)
                            : [];
      const allSignals    = [...fibSignals, ...setup2Signals, ...setup3Signals];

      console.log(`📦 [${tf.label}] Got ${allSignals.length} candidate signals`);
      for (const sig of allSignals) {
        console.log(`  → Checking ${sig.action} ${sig.strategy} conf=${sig.confidence} fib=${sig.fib_level || 'n/a'}`);
        
        // ── MTF CONSENSUS FILTER ──
        if (sig.action !== consensusDirection) {
          console.log(`  🚫 BLOCKED: against MTF consensus (need ${consensusDirection})`);
          continue;
        }
        
        // Filter by minimum confidence
        if (sig.confidence < tf.minScore) {
          console.log(`  🚫 BLOCKED: confidence ${sig.confidence} < minScore ${tf.minScore}`);
          continue;
        }

        // Dedup check — no same action/strategy/TF in last 2h
        const recent = db.prepare(
          `SELECT id FROM signals WHERE ticker=? AND action=? AND strategy=? AND timeframe=? AND timestamp > ?`
        ).get(TICKER, sig.action, sig.strategy, tf.label, Date.now()-2*3600000);
        if (recent) {
          console.log(`  🚫 BLOCKED: duplicate (same strategy/TF in last 2h, signal #${recent.id})`);
          continue;
        }

        const record = {
          ticker: TICKER,
          ...sig,
          timeframe: tf.label,
          current_price: sig.price,
          entry_valid_for: tf.validFor,
          mtf: JSON.stringify({ h1: tf.label==='1H', h4: tf.label==='4H', d1: false }),
          timestamp: Date.now(),
        };

        try {
          db.prepare(`INSERT INTO signals
            (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,strategy,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome)
            VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@strategy,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')
          `).run(record);
          console.log(`✓ INSERTED [${sig.strategy}] ${sig.action} ${TICKER} @ ${sig.price} (${tf.label}, ${sig.confidence}%)`);
          const inserted = db.prepare('SELECT last_insert_rowid() as id').get();
          try { sendSignalPush({ ...record, id: inserted.id }); } catch(pushErr) { console.error('Push call error:', pushErr.message); }
        } catch(dbErr) {
          console.error(`❌ DB INSERT FAILED [${tf.label}] ${sig.action} ${sig.strategy}:`, dbErr.message);
          console.error('Record was:', JSON.stringify(record));
        }
      }
    } catch(err) {
      console.error(`Scanner error ${tf.label}:`, err.message);
    }
  }
}

// ── OUTCOME TRACKER ───────────────────────────────────────
async function trackSignalOutcomes() {
  try {
    const openSignals = db.prepare("SELECT * FROM signals WHERE outcome = 'open' OR outcome IS NULL").all();
    if (openSignals.length === 0) return;

    const candles = await fetchCandles('5min', 100);
    if (!candles || candles.length < 5) return;

    const sortedCandles = [...candles].sort((a, b) => a.timestamp - b.timestamp);
    let closedCount = 0;

    for (const signal of openSignals) {
      try {
        const relevantCandles = sortedCandles.filter(c => c.timestamp > signal.timestamp);
        if (relevantCandles.length === 0) continue;

        const isBuy = signal.action === 'BUY';
        const sl = signal.sl, tp1 = signal.tp1, tp2 = signal.tp2;
        if (!sl || !tp1) continue;

        let outcome = null, exitPrice = null, exitTime = null;

        for (const c of relevantCandles) {
          if (isBuy) {
            if (c.low <= sl) { outcome='sl_hit'; exitPrice=sl; exitTime=c.timestamp; break; }
            if (c.high >= (tp2 || tp1)) { outcome='tp2_hit'; exitPrice=tp2||tp1; exitTime=c.timestamp; break; }
            if (c.high >= tp1) { outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
          } else {
            if (c.high >= sl) { outcome='sl_hit'; exitPrice=sl; exitTime=c.timestamp; break; }
            if (c.low <= (tp2 || tp1)) { outcome='tp2_hit'; exitPrice=tp2||tp1; exitTime=c.timestamp; break; }
            if (c.low <= tp1) { outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
          }
        }

        if (!outcome) {
          const ageHours = (Date.now() - signal.timestamp) / 3600000;
          if (ageHours > 48) {
            outcome = 'expired';
            const last = relevantCandles[relevantCandles.length - 1];
            exitPrice = last ? last.close : signal.price;
            exitTime = last ? last.timestamp : Date.now();
          }
        }

        if (outcome) {
          const risk = Math.abs(signal.price - sl);
          const profit = isBuy ? (exitPrice - signal.price) : (signal.price - exitPrice);
          const pnlR = risk > 0 ? profit / risk : 0;
          db.prepare(`UPDATE signals SET outcome=?, exit_price=?, closed_at=?, pnl_r=? WHERE id=?`)
            .run(outcome, exitPrice, exitTime, Math.round(pnlR * 100) / 100, signal.id);
          closedCount++;
        }
      } catch(e) {}
    }
    if (closedCount > 0) console.log(`✓ Closed ${closedCount} signals`);
  } catch(e) { console.error('Tracker error:', e.message); }
}
trackSignalOutcomes();
setInterval(trackSignalOutcomes, 15 * 60 * 1000);

// ── Routes ─────────────────────────────────────────────────
app.get('/', (req,res) => res.json({ status:'Pulstrade Backend', version:'4.9.2-econ-off' }));
app.get('/health', (req,res) => res.json({
  status:'ok',
  signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c,
  open:    db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
  closed:  db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome IS NOT NULL AND outcome != 'open'").get().c,
  marketClosed: isMarketClosed(),
  priceCache: cachedPrice,
  strategies: ['FIB Pullback', 'Setup 2 Breakout+Retest', 'Setup 3 Double Top/Bottom'],
  timeframes: ['5m', '15m', '30m', '1H', '4H'],
}));

app.get('/signals', (req,res) => {
  const limit  = parseInt(req.query.limit)||50;
  const ticker = req.query.ticker||null;
  let query='SELECT * FROM signals', params=[];
  if (ticker) { query+=' WHERE ticker=?'; params.push(ticker); }
  query+=' ORDER BY timestamp DESC LIMIT ?'; params.push(limit);
  res.json(db.prepare(query).all(...params).map(r=>({...r,mtf:r.mtf?JSON.parse(r.mtf):null})));
});

app.get('/signals/:id', (req,res) => {
  const row = db.prepare('SELECT * FROM signals WHERE id=?').get(req.params.id);
  if (!row) return res.status(404).json({error:'Not found'});
  res.json({...row, mtf:row.mtf?JSON.parse(row.mtf):null});
});

app.get('/stats', (req, res) => {
  try {
    const all = db.prepare("SELECT * FROM signals WHERE outcome IS NOT NULL AND outcome != 'open'").all();
    const totalClosed = all.length;
    
    if (totalClosed === 0) {
      return res.json({
        totalSignals: db.prepare("SELECT COUNT(*) as c FROM signals").get().c,
        closedSignals: 0,
        openSignals: db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
        winRate: null, avgRR: null, profitFactor: null,
        wins: 0, losses: 0, expired: 0,
        fibPerformance: {},
        strategyPerformance: {},
      });
    }

    const wins = all.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
    const losses = all.filter(s => s.outcome === 'sl_hit');
    const expired = all.filter(s => s.outcome === 'expired');
    const decisive = wins.length + losses.length;
    const winRate = decisive > 0 ? (wins.length / decisive) * 100 : 0;
    
    const allPnL = all.map(s => s.pnl_r || 0);
    const totalPnL = allPnL.reduce((a,b) => a+b, 0);
    const avgRR = all.length > 0 ? totalPnL / all.length : 0;
    const grossWin = wins.reduce((a,s) => a + (s.pnl_r || 0), 0);
    const grossLoss = Math.abs(losses.reduce((a,s) => a + (s.pnl_r || 0), 0));
    const profitFactor = grossLoss > 0 ? grossWin / grossLoss : grossWin;

    const fibPerformance = {};
    for (const level of ['61.8%', '50.0%', '38.2%', '78.6%', '23.6%']) {
      const atLevel = all.filter(s => s.fib_level === level);
      const winsAt = atLevel.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
      const lossesAt = atLevel.filter(s => s.outcome === 'sl_hit');
      const decisiveAt = winsAt.length + lossesAt.length;
      fibPerformance[level] = {
        total: atLevel.length, wins: winsAt.length, losses: lossesAt.length,
        winRate: decisiveAt > 0 ? Math.round((winsAt.length / decisiveAt) * 1000) / 10 : null,
      };
    }

    const strategyPerformance = {};
    for (const strat of ['FIB', 'Range Bounce', 'Breakout']) {
      const ofStrat = all.filter(s => s.strategy === strat);
      const winsS = ofStrat.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
      const lossesS = ofStrat.filter(s => s.outcome === 'sl_hit');
      const decisiveS = winsS.length + lossesS.length;
      strategyPerformance[strat] = {
        total: ofStrat.length, wins: winsS.length, losses: lossesS.length,
        winRate: decisiveS > 0 ? Math.round((winsS.length / decisiveS) * 1000) / 10 : null,
        avgPnL: ofStrat.length > 0 ? Math.round((ofStrat.reduce((a,s) => a+(s.pnl_r||0),0) / ofStrat.length) * 100) / 100 : null,
      };
    }

    res.json({
      totalSignals: db.prepare("SELECT COUNT(*) as c FROM signals").get().c,
      closedSignals: totalClosed,
      openSignals: db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
      winRate: Math.round(winRate * 10) / 10,
      avgRR: Math.round(avgRR * 100) / 100,
      profitFactor: Math.round(profitFactor * 100) / 100,
      totalPnL: Math.round(totalPnL * 100) / 100,
      bestTrade: allPnL.length > 0 ? Math.round(Math.max(...allPnL) * 100) / 100 : 0,
      worstTrade: allPnL.length > 0 ? Math.round(Math.min(...allPnL) * 100) / 100 : 0,
      wins: wins.length, losses: losses.length, expired: expired.length,
      fibPerformance, strategyPerformance,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/price', (req,res) => {
  if (cachedPrice.price) {
    return res.json({ price: cachedPrice.price, ticker: TICKER, cached: true, age: Math.floor((Date.now()-cachedPrice.timestamp)/1000) });
  }
  fetchLivePrice().then(p => res.json({ price: p, ticker: TICKER, cached: false }));
});

app.get('/candles', async (req,res) => {
  try {
    const interval=req.query.interval||'5min', outputsize=req.query.limit||100;
    const data = await fetchCandles(interval, parseInt(outputsize));
    res.json(data.map(c=>({time:c.timestamp,open:c.open,high:c.high,low:c.low,close:c.close})).reverse());
  } catch(e) { res.json(generateMockCandles(100)); }
});

app.get('/news', async (req,res) => {
  try {
    const q=encodeURIComponent('gold price XAU OR Federal Reserve interest rates OR inflation CPI');
    const r=await axios.get(`https://newsapi.org/v2/everything?q=${q}&language=en&sortBy=publishedAt&pageSize=15&apiKey=${NEWS_API_KEY}`,{timeout:10000});
    if (!r.data?.articles) return res.json([]);
    res.json(r.data.articles.filter(a=>a.title&&a.title!=='[Removed]').map(a=>{
      const text=(a.title+' '+(a.description||'')).toLowerCase();
      let impact='low';
      if (text.match(/fed|federal reserve|rate|inflation|cpi|nfp|trump|war|crisis|crash|rally|record/)) impact='high';
      else if (text.match(/gold|dollar|euro|oil|market|economy|gdp/)) impact='medium';
      return {title:a.title,description:a.description,source:a.source?.name||'Unknown',time:timeAgo(new Date(a.publishedAt)),impact,url:a.url,image:a.urlToImage};
    }));
  } catch(e) { res.json([]); }
});

app.post('/webhook', express.json(), (req,res) => {
  try {
    const data=req.body;
    if (!data.action||!data.price||!data.ticker) return res.status(400).json({error:'Missing'});
    const confidence=parseInt(data.confidence)||0;
    if (confidence<75) return res.json({filtered:true});
    const tf=data.timeframe||'';
    const signal={ticker:data.ticker||'XAU/USD',action:data.action.toUpperCase(),price:parseFloat(data.price),sl:data.sl?parseFloat(data.sl):null,tp1:data.tp1?parseFloat(data.tp1):null,tp2:data.tp2?parseFloat(data.tp2):null,timeframe:tf,confidence,fib_level:data.fib_level||null,pattern:data.pattern||null,strategy:'TradingView',rsi:data.rsi?parseFloat(data.rsi):null,atr:data.atr?parseFloat(data.atr):null,current_price:parseFloat(data.price),entry_valid_for:tf.includes('H')?(tf==='1H'?2:8):24,mtf:JSON.stringify({h1:data.mtf?.h1||false,h4:data.mtf?.h4||false,d1:data.mtf?.d1||false}),timestamp:Date.now()};
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,strategy,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome) VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@strategy,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')`).run(signal);
    try { sendSignalPush({...signal,id:db.prepare('SELECT last_insert_rowid() as id').get().id}); } catch(e) {}
    res.json({success:true});
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.post('/autotrade/connect-mt5', express.json(), async (req,res) => {
  const {login,password,server,platform,lotSize,autoTradeEnabled}=req.body;
  if (!login||!password||!server) return res.status(400).json({error:'Missing'});
  if (!METAAPI_TOKEN) return res.status(500).json({error:'MetaApi not configured'});
  try {
    const r=await axios.post('https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts',{login:login.toString(),password,name:'Pulstrade_'+login,server,platform:platform||'mt5',magic:20240410,application:'MetaApi',type:'cloud'},{headers:{'auth-token':METAAPI_TOKEN,'Content-Type':'application/json'},timeout:30000});
    db.prepare('INSERT OR REPLACE INTO autotrade_accounts (account_id,lot_size,auto_trade) VALUES (?,?,?)').run(r.data.id,lotSize||0.01,autoTradeEnabled?1:0);
    res.json({success:true,accountId:r.data.id});
  } catch(e) { res.status(500).json({error:e.response?.data?.message||'Failed'}); }
});

app.post('/autotrade/connect', express.json(), (req,res) => {
  const {accountId,autoTradeEnabled}=req.body;
  if (!accountId) return res.status(400).json({error:'Missing'});
  db.prepare('UPDATE autotrade_accounts SET auto_trade=? WHERE account_id=?').run(autoTradeEnabled?1:0,accountId);
  res.json({success:true});
});

// ── TREND CONSENSUS DEBUG — show MTF trend votes ─────────
app.get('/trend-consensus', async (req, res) => {
  try {
    const timeframes = [
      { label: '5m',  interval: '5min' },
      { label: '15m', interval: '15min' },
      { label: '30m', interval: '30min' },
      { label: '1H',  interval: '1h' },
      { label: '4H',  interval: '4h' },
    ];
    const trendVotes = { UP: 0, DOWN: 0, NEUTRAL: 0 };
    const details = {};
    
    for (const tf of timeframes) {
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 50) {
        details[tf.label] = { error: 'not enough candles' };
        continue;
      }
      const closes = candles.map(c => c.close);
      const ema50 = calcEMA(closes, 50);
      const ema200 = calcEMA(closes, 200);
      let trend = 'NEUTRAL';
      if (ema50 && ema200) {
        if (ema50 > ema200) trend = 'UP';
        else if (ema50 < ema200) trend = 'DOWN';
      }
      details[tf.label] = { 
        trend, 
        price: candles[0].close.toFixed(2),
        ema50: ema50?.toFixed(2),
        ema200: ema200?.toFixed(2),
      };
      trendVotes[trend]++;
    }
    
    let consensusDirection = null;
    if (trendVotes.UP >= 3) consensusDirection = 'BUY';
    else if (trendVotes.DOWN >= 3) consensusDirection = 'SELL';
    
    res.json({
      consensus: consensusDirection || 'NO_CONSENSUS',
      votes: trendVotes,
      timeframes: details,
      message: consensusDirection 
        ? `Allowed signal direction: ${consensusDirection}` 
        : 'No signals will be generated until 3+ timeframes agree on direction',
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── STRATEGY STATE — inspect state machines ──────────────
app.get('/strategy-state', (req, res) => {
  res.json({
    setup2: setup2State,
    setup3: setup3State,
    note: 'State per timeframe — null means no active setup pending',
  });
});

// ── DEBUG SCANNER — shows why signals are filtered ─────────
app.get('/scan-debug', async (req, res) => {
  const results = { timestamp: new Date().toISOString(), livePrice: cachedPrice.price, timeframes: {} };
  const timeframes = [
    { label: '5m',  interval: '5min',  validFor: 0.25, minScore: 55 },
    { label: '15m', interval: '15min', validFor: 0.5,  minScore: 45 },
    { label: '30m', interval: '30min', validFor: 1,    minScore: 48 },
    { label: '1H',  interval: '1h',    validFor: 2,    minScore: 50 },
    { label: '4H',  interval: '4h',    validFor: 8,    minScore: 48 },
  ];

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 30) { results.timeframes[tf.label] = { error: 'not enough candles' }; continue; }
      const closes = candles.map(c => c.close);
      const price = closes[0];
      const ema50 = calcEMA(closes, 50);
      const ema200 = calcEMA(closes, 200);
      const rsi = calcRSI(closes);
      const atr = calcATR(candles);
      
      const fibSignals    = scanFibPullback(candles, tf);
      // Other strategies disabled in v4.7 — FIB only
      const rangeSignals  = [];
      const breakoutSigs  = [];
      const emaSignals    = [];
      
      results.timeframes[tf.label] = {
        price: price.toFixed(2),
        ema50: ema50 ? ema50.toFixed(2) : null,
        ema200: ema200 ? ema200.toFixed(2) : null,
        rsi, atr,
        trend: ema50 && ema200 ? (ema50 > ema200 ? 'UP' : 'DOWN') : 'NEUTRAL',
        minScore: tf.minScore,
        fib: { count: fibSignals.length, signals: fibSignals.map(s => ({action:s.action,conf:s.confidence,passes:s.confidence>=tf.minScore,fibLevel:s.fib_level})) },
        range: { count: rangeSignals.length, signals: rangeSignals.map(s => ({action:s.action,conf:s.confidence,passes:s.confidence>=tf.minScore})) },
        breakout: { count: breakoutSigs.length, signals: breakoutSigs.map(s => ({action:s.action,conf:s.confidence,passes:s.confidence>=tf.minScore})) },
        ema: { count: emaSignals.length, signals: emaSignals.map(s => ({action:s.action,conf:s.confidence,passes:s.confidence>=tf.minScore})) },
      };
    } catch(e) { results.timeframes[tf.label] = { error: e.message }; }
  }
  res.json(results);
});

// ── DB CHECK — inspect schema and test insert ─────────────
app.get('/db-check', (req, res) => {
  try {
    const columns = db.prepare("PRAGMA table_info(signals)").all();
    const count = db.prepare("SELECT COUNT(*) as c FROM signals").get().c;
    
    // Try a test insert
    let insertResult = 'not attempted';
    try {
      const testRecord = {
        ticker: 'TEST', action: 'BUY', price: 1000,
        sl: 990, tp1: 1010, tp2: 1020,
        timeframe: 'TEST', confidence: 50, fib_level: null,
        pattern: 'test', strategy: 'TEST', note: 'test',
        rsi: 50, atr: 10, current_price: 1000, entry_valid_for: 1,
        mtf: '{}', timestamp: Date.now(),
      };
      db.prepare(`INSERT INTO signals
        (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,strategy,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome)
        VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@strategy,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')
      `).run(testRecord);
      // Delete the test
      db.prepare("DELETE FROM signals WHERE ticker='TEST'").run();
      insertResult = 'SUCCESS';
    } catch(e) {
      insertResult = 'FAILED: ' + e.message;
    }
    
    res.json({
      columns: columns.map(c => ({name: c.name, type: c.type})),
      signalCount: count,
      testInsert: insertResult,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── TEST PUSH — manually send a test notification ─────────
app.get('/test-push', async (req, res) => {
  try {
    if (!admin.apps.length) {
      return res.status(500).json({ error: 'Firebase not configured. Set FIREBASE_SERVICE_ACCOUNT env variable.' });
    }
    const title = req.query.title || '📈 Test Signal';
    const body  = req.query.body  || 'This is a test notification from Pulstrade backend';
    
    const result = await admin.messaging().send({
      topic: 'signals',
      notification: { title, body },
      apns: { payload: { aps: { sound: 'default', badge: 1 } }, headers: { 'apns-priority': '10' } },
      android: { priority: 'high', notification: { title, body, channelId: 'pulstrade_signals', priority: 'max' } },
    });
    
    res.json({ success: true, messageId: result, title, body });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── FORCE SCAN — manually trigger scanner ──────────────────
// ── DB CHECK — inspect schema and test insert ─────────────
app.get('/db-check', (req, res) => {
  try {
    const columns = db.prepare("PRAGMA table_info(signals)").all();
    const count = db.prepare("SELECT COUNT(*) as c FROM signals").get().c;
    
    // Try a test insert
    let insertResult = 'not attempted';
    try {
      const testRecord = {
        ticker: 'TEST', action: 'BUY', price: 1000,
        sl: 990, tp1: 1010, tp2: 1020,
        timeframe: 'TEST', confidence: 50, fib_level: null,
        pattern: 'test', strategy: 'TEST', note: 'test',
        rsi: 50, atr: 10, current_price: 1000, entry_valid_for: 1,
        mtf: '{}', timestamp: Date.now(),
      };
      db.prepare(`INSERT INTO signals
        (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,strategy,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome)
        VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@strategy,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')
      `).run(testRecord);
      // Delete the test
      db.prepare("DELETE FROM signals WHERE ticker='TEST'").run();
      insertResult = 'SUCCESS';
    } catch(e) {
      insertResult = 'FAILED: ' + e.message;
    }
    
    res.json({
      columns: columns.map(c => ({name: c.name, type: c.type})),
      signalCount: count,
      testInsert: insertResult,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── FORCE SCAN — manually trigger scanner ──────────────────
app.get('/force-scan', async (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as c FROM signals').get().c;
  await scanForSignals();
  const after = db.prepare('SELECT COUNT(*) as c FROM signals').get().c;
  res.json({ triggered: true, signalsBefore: before, signalsAfter: after, newSignals: after - before });
});

app.get('/calendar', async (req, res) => {
  try { res.json(generateWeeklyCalendar(new Date())); }
  catch(e) { res.json([]); }
});

function generateWeeklyCalendar(now) {
  const day = now.getUTCDay();
  const monday = new Date(now);
  monday.setUTCDate(now.getUTCDate() - (day === 0 ? 6 : day - 1));
  monday.setUTCHours(0,0,0,0);
  const getDay = (offset, hour, min) => {
    const d = new Date(monday);
    d.setUTCDate(monday.getUTCDate() + offset);
    d.setUTCHours(hour, min, 0, 0);
    return d.toISOString();
  };
  const events = [
    { title: 'US CPI (MoM)', currency: 'USD', category: 'Inflation', impact: 'high', time: getDay(1, 13, 30), forecast: '0.3%', previous: '0.4%', actual: null },
    { title: 'FOMC Meeting Minutes', currency: 'USD', category: 'Central Bank', impact: 'high', time: getDay(2, 19, 0), forecast: '—', previous: '—', actual: null },
    { title: 'US Jobless Claims', currency: 'USD', category: 'Employment', impact: 'medium', time: getDay(3, 13, 30), forecast: '215K', previous: '210K', actual: null },
    { title: 'US Non-Farm Payrolls', currency: 'USD', category: 'Employment', impact: 'high', time: getDay(4, 13, 30), forecast: '185K', previous: '175K', actual: null },
    { title: 'US Core PCE', currency: 'USD', category: 'Inflation', impact: 'high', time: getDay(4, 13, 30), forecast: '0.3%', previous: '0.3%', actual: null },
  ];
  const cutoff = new Date(now.getTime() - 24 * 3600000);
  return events.filter(e => new Date(e.time) >= cutoff).sort((a, b) => new Date(a.time) - new Date(b.time)).map(e => {
    const eventTime = new Date(e.time);
    const diff = eventTime - now;
    const diffMin = Math.floor(diff / 60000);
    const diffH = Math.floor(diff / 3600000);
    let timeLabel;
    if (diff < 0 && diff > -3600000) timeLabel = 'Just released';
    else if (diff < 0) timeLabel = `${Math.abs(diffH)}h ago`;
    else if (diffMin < 60) timeLabel = `in ${diffMin}min ⚡`;
    else if (diffH < 24) timeLabel = `Today ${eventTime.getUTCHours().toString().padStart(2,'0')}:${eventTime.getUTCMinutes().toString().padStart(2,'0')} UTC`;
    else {
      const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      timeLabel = `${days[eventTime.getUTCDay()]} ${eventTime.getUTCHours().toString().padStart(2,'0')}:${eventTime.getUTCMinutes().toString().padStart(2,'0')} UTC`;
    }
    return { ...e, timeLabel, isSoon: diff > 0 && diffMin <= 30, isPast: diff < 0, timestamp: eventTime.getTime() };
  });
}

// ── Start ──────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5*60*1000);
app.listen(PORT, () => console.log(`Pulstrade Backend v4.0 — Multi-Strategy on port ${PORT}`));
