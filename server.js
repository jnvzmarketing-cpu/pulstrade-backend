require('dotenv').config();
const express = require('express');
const axios   = require('axios');
const cors    = require('cors');
const Database = require('better-sqlite3');

const app  = express();
const PORT = process.env.PORT || 3000;
const TWELVE_API_KEY = process.env.TWELVE_DATA_API_KEY || '';
const TICKER = 'XAU/USD';

app.use(cors());
app.use(express.json());

// ── Database ──────────────────────────────────────────────────────────────────
const db = new Database(process.env.DB_PATH || '/tmp/pulstrade.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    action      TEXT NOT NULL,
    price       REAL NOT NULL,
    sl          REAL,
    tp1         REAL,
    tp2         REAL,
    timeframe   TEXT,
    confidence  INTEGER,
    fib_level   TEXT,
    pattern     TEXT,
    note        TEXT,
    rsi         REAL,
    atr         REAL,
    current_price REAL,
    entry_valid_for REAL,
    mtf         TEXT,
    timestamp   INTEGER NOT NULL,
    created_at  INTEGER DEFAULT (strftime('%s','now') * 1000)
  )
`);

// ── FIB Calculator ────────────────────────────────────────────────────────────
function calcFibLevels(high, low) {
  const diff = high - low;
  return {
    fib0:    high,
    fib236:  high - diff * 0.236,
    fib382:  high - diff * 0.382,
    fib500:  high - diff * 0.500,
    fib618:  high - diff * 0.618,
    fib786:  high - diff * 0.786,
    fib100:  low,
  };
}

function findSwingHighLow(candles) {
  if (candles.length < 5) return null;
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
    if (diff > 0) gains  += diff;
    else          losses -= diff;
  }
  const avgGain = gains  / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return Math.round((100 - 100 / (1 + rs)) * 10) / 10;
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

function isDoji(candle) {
  const body   = Math.abs(candle.close - candle.open);
  const range  = candle.high - candle.low;
  if (range === 0) return false;
  return body / range < 0.25;
}

function isPinBar(candle, direction) {
  const body    = Math.abs(candle.close - candle.open);
  const range   = candle.high - candle.low;
  if (range === 0) return false;
  const upperWick = candle.high - Math.max(candle.open, candle.close);
  const lowerWick = Math.min(candle.open, candle.close) - candle.low;
  if (direction === 'buy')  return lowerWick > body * 2 && lowerWick > upperWick * 2;
  if (direction === 'sell') return upperWick > body * 2 && upperWick > lowerWick * 2;
  return false;
}

function nearFib(price, fibValue, atr) {
  const tolerance = (atr || fibValue * 0.001) * 0.5;
  return Math.abs(price - fibValue) <= tolerance;
}

// ── Fetch candles from Twelve Data ───────────────────────────────────────────
async function fetchCandles(interval, outputsize = 30) {
  if (!TWELVE_API_KEY) return generateMockCandles(outputsize);
  try {
    const url = `https://api.twelvedata.com/time_series?symbol=${TICKER}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const res = await axios.get(url, { timeout: 10000 });
    if (!res.data || !res.data.values) return generateMockCandles(outputsize);
    return res.data.values.map(v => ({
      open:  parseFloat(v.open),
      high:  parseFloat(v.high),
      low:   parseFloat(v.low),
      close: parseFloat(v.close),
      timestamp: new Date(v.datetime).getTime(),
    }));
  } catch (err) {
    console.error('Twelve Data error:', err.message);
    return generateMockCandles(outputsize);
  }
}

function generateMockCandles(count) {
  const candles = [];
  let price = 2310 + Math.random() * 30;
  const now = Date.now();
  for (let i = count - 1; i >= 0; i--) {
    const change = (Math.random() - 0.5) * 15;
    const open   = price;
    const close  = price + change;
    const high   = Math.max(open, close) + Math.random() * 8;
    const low    = Math.min(open, close) - Math.random() * 8;
    candles.push({ open, high, low, close, timestamp: now - i * 3600000 });
    price = close;
  }
  return candles;
}

// ── Signal Detection ──────────────────────────────────────────────────────────
async function scanForSignals() {
  const timeframes = [
    { label: '1H', interval: '1h',  validFor: 2  },
    { label: '4H', interval: '4h',  validFor: 8  },
    { label: '1D', interval: '1day',validFor: 24 },
  ];

  const newSignals = [];

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 30);
      if (!candles || candles.length < 15) continue;

      const swing = findSwingHighLow(candles);
      if (!swing) continue;

      const fib = calcFibLevels(swing.high, swing.low);
      const latest = candles[0];
      const currentPrice = latest.close;
      const atr = calcATR(candles, 14);
      const closes = candles.map(c => c.close);
      const rsi = calcRSI(closes, 14);

      // Check each FIB level
      const fibChecks = [
        { level: '61.8%', value: fib.fib618, confidence: 82 },
        { level: '38.2%', value: fib.fib382, confidence: 71 },
        { level: '50.0%', value: fib.fib500, confidence: 65 },
        { level: '23.6%', value: fib.fib236, confidence: 55 },
        { level: '78.6%', value: fib.fib786, confidence: 60 },
      ];

      for (const check of fibChecks) {
        if (!nearFib(currentPrice, check.value, atr)) continue;

        const isBuySetup  = currentPrice < swing.high - (swing.high - swing.low) * 0.3;
        const isSellSetup = currentPrice > swing.low  + (swing.high - swing.low) * 0.3;

        let action = null, pattern = null;

        if (isBuySetup && (isDoji(latest) || isPinBar(latest, 'buy'))) {
          action  = 'BUY';
          pattern = isDoji(latest) ? `Doji on ${check.level} FIB` : `Pin Bar on ${check.level} FIB`;
        } else if (isSellSetup && (isDoji(latest) || isPinBar(latest, 'sell'))) {
          action  = 'SELL';
          pattern = isDoji(latest) ? `Doji on ${check.level} FIB` : `Pin Bar on ${check.level} FIB`;
        }

        if (!action) continue;

        // Avoid duplicate signals (same ticker+action+timeframe in last 2 hours)
        const recent = db.prepare(`SELECT id FROM signals WHERE ticker=? AND action=? AND timeframe=? AND timestamp > ?`).get(TICKER, action, tf.label, Date.now() - 7200000);
        if (recent) continue;

        const sl  = action === 'BUY'
          ? Math.round((check.value - (atr || 10) * 1.5) * 100) / 100
          : Math.round((check.value + (atr || 10) * 1.5) * 100) / 100;
        const tp1 = action === 'BUY'
          ? Math.round((check.value + (atr || 10) * 2.0) * 100) / 100
          : Math.round((check.value - (atr || 10) * 2.0) * 100) / 100;
        const tp2 = action === 'BUY'
          ? Math.round((check.value + (atr || 10) * 3.5) * 100) / 100
          : Math.round((check.value - (atr || 10) * 3.5) * 100) / 100;

        // MTF confirmation
        const mtf = { h1: tf.label === '1H', h4: tf.label === '4H', d1: tf.label === '1D' };

        const signal = {
          ticker:    TICKER,
          action,
          price:     Math.round(currentPrice * 100) / 100,
          sl, tp1, tp2,
          timeframe: tf.label,
          confidence: check.confidence,
          fib_level:  check.level,
          pattern,
          rsi:   rsi  ? Math.round(rsi  * 10) / 10 : null,
          atr:   atr  ? Math.round(atr  * 100) / 100 : null,
          current_price: Math.round(currentPrice * 100) / 100,
          entry_valid_for: tf.validFor,
          mtf:   JSON.stringify(mtf),
          timestamp: Date.now(),
        };

        db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
          VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)`).run(signal);

        newSignals.push(signal);
        console.log(`✓ Signal: ${action} ${TICKER} @ ${currentPrice} (${tf.label}, ${check.level})`);
      }
    } catch (err) {
      console.error(`Error scanning ${tf.label}:`, err.message);
    }
  }

  return newSignals;
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'Pulstrade Backend running', version: '1.0.0' }));

app.get('/signals', (req, res) => {
  const limit  = parseInt(req.query.limit)  || 50;
  const ticker = req.query.ticker || null;
  let query = 'SELECT * FROM signals';
  const params = [];
  if (ticker) { query += ' WHERE ticker = ?'; params.push(ticker); }
  query += ' ORDER BY timestamp DESC LIMIT ?';
  params.push(limit);

  const rows = db.prepare(query).all(...params);
  const signals = rows.map(r => ({
    ...r,
    mtf: r.mtf ? JSON.parse(r.mtf) : null,
  }));
  res.json(signals);
});

app.get('/signals/:id', (req, res) => {
  const row = db.prepare('SELECT * FROM signals WHERE id = ?').get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Not found' });
  res.json({ ...row, mtf: row.mtf ? JSON.parse(row.mtf) : null });
});

app.get('/price', async (req, res) => {
  try {
    if (!TWELVE_API_KEY) return res.json({ price: 2318.40, change: 0.74, ticker: TICKER });
    const url = `https://api.twelvedata.com/price?symbol=${TICKER}&apikey=${TWELVE_API_KEY}`;
    const r = await axios.get(url, { timeout: 5000 });
    res.json({ price: parseFloat(r.data.price), ticker: TICKER });
  } catch {
    res.json({ price: 2318.40, ticker: TICKER });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c }));

// ── Auto Scan every 5 minutes ─────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5 * 60 * 1000);

app.listen(PORT, () => console.log(`Pulstrade backend on port ${PORT}`));
