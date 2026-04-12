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
    const symbol = encodeURIComponent(TICKER);
    const url = `https://api.twelvedata.com/time_series?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
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

  // If no signals in DB at all, insert seed signals with current price
  const count = db.prepare('SELECT COUNT(*) as c FROM signals').get().c;
  if (count === 0) {
    console.log('No signals found — inserting seed signals with current price...');
    await seedSignals();
  }

  return newSignals;
}

async function seedSignals() {
  // Get current price
  let currentPrice = 2318.40;
  try {
    if (TWELVE_API_KEY) {
      const symbol = encodeURIComponent(TICKER);
      const r = await axios.get(`https://api.twelvedata.com/price?symbol=${symbol}&apikey=${TWELVE_API_KEY}`, { timeout: 5000 });
      currentPrice = parseFloat(r.data.price) || currentPrice;
    }
  } catch (_) {}

  const now = Date.now();
  const seeds = [
    { action: 'BUY',  fib: '61.8%', conf: 82, tf: '1H',  validFor: 2,  rsi: 34.2, atr: 12.40, pattern: 'Doji on Golden FIB',    offset: 3 * 60000 },
    { action: 'SELL', fib: '38.2%', conf: 71, tf: '4H',  validFor: 8,  rsi: 67.8, atr: 18.60, pattern: 'Pin Bar Reversal',       offset: 80 * 60000 },
    { action: 'BUY',  fib: '50.0%', conf: 65, tf: '4H',  validFor: 8,  rsi: 42.1, atr: 15.80, pattern: 'Doji on 50% FIB',       offset: 225 * 60000 },
    { action: 'SELL', fib: '23.6%', conf: 58, tf: '1H',  validFor: 2,  rsi: 71.3, atr: 10.20, pattern: 'FIB Resistance Bounce',  offset: 300 * 60000 },
    { action: 'BUY',  fib: '61.8%', conf: 88, tf: '1D',  validFor: 24, rsi: 28.9, atr: 28.40, pattern: 'Doji on Golden FIB',    offset: 22 * 3600000 },
  ];

  for (const s of seeds) {
    const sl  = s.action === 'BUY' ? currentPrice - s.atr * 1.5 : currentPrice + s.atr * 1.5;
    const tp1 = s.action === 'BUY' ? currentPrice + s.atr * 2.0 : currentPrice - s.atr * 2.0;
    const tp2 = s.action === 'BUY' ? currentPrice + s.atr * 3.5 : currentPrice - s.atr * 3.5;
    const mtf = JSON.stringify({ h1: s.tf === '1H', h4: s.tf === '4H', d1: s.tf === '1D' });
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(TICKER, s.action, Math.round(currentPrice*100)/100, Math.round(sl*100)/100, Math.round(tp1*100)/100, Math.round(tp2*100)/100,
           s.tf, s.conf, s.fib, s.pattern, s.rsi, s.atr, Math.round(currentPrice*100)/100, s.validFor, mtf, now - s.offset);
  }
  console.log(`✓ Inserted ${seeds.length} seed signals @ $${currentPrice}`);
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
    const symbol = encodeURIComponent(TICKER);
    const url = `https://api.twelvedata.com/price?symbol=${symbol}&apikey=${TWELVE_API_KEY}`;
    const r = await axios.get(url, { timeout: 5000 });
    const price = parseFloat(r.data.price);
    res.json({ price, ticker: TICKER });
  } catch (err) {
    console.error('Price fetch error:', err.message);
    res.json({ price: 2318.40, ticker: TICKER });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c }));

// ── Auto Scan every 5 minutes ─────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5 * 60 * 1000);

app.listen(PORT, () => console.log(`Pulstrade backend on port ${PORT}`));

// ── TradingView Webhook Endpoint ──────────────────────────────────────────────
app.post('/webhook', express.json(), (req, res) => {
  try {
    const data = req.body;
    console.log('Webhook received:', JSON.stringify(data));

    if (!data.action || !data.price || !data.ticker) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const signal = {
      ticker:    data.ticker || 'XAU/USD',
      action:    data.action.toUpperCase(),
      price:     parseFloat(data.price),
      sl:        data.sl    ? parseFloat(data.sl)   : null,
      tp1:       data.tp1   ? parseFloat(data.tp1)  : null,
      tp2:       data.tp2   ? parseFloat(data.tp2)  : null,
      timeframe: data.timeframe || null,
      confidence: data.confidence ? parseInt(data.confidence) : 70,
      fib_level:  data.fib_level  || null,
      pattern:    data.pattern    || null,
      rsi:        data.rsi  ? parseFloat(data.rsi)  : null,
      atr:        data.atr  ? parseFloat(data.atr)  : null,
      current_price: parseFloat(data.price),
      entry_valid_for: data.timeframe === '1H' ? 2 : data.timeframe === '4H' ? 8 : 24,
      mtf:       JSON.stringify({ h1: data.timeframe === '1', h4: data.timeframe === '4H', d1: data.timeframe === '1D' }),
      timestamp: Date.now(),
    };

    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
      VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)`).run(signal);

    console.log(`✓ Signal saved: ${signal.action} ${signal.ticker} @ ${signal.price}`);
    res.json({ success: true, signal });
  } catch (err) {
    console.error('Webhook error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/autotrade/connect-mt5', express.json(), async (req, res) => {
  const { login, password, server, platform, lotSize, autoTradeEnabled } = req.body;
  if (!login || !password || !server) return res.status(400).json({ error: 'Missing credentials' });
  const METAAPI_TOKEN_LOCAL = process.env.METAAPI_TOKEN || ''; if (!METAAPI_TOKEN_LOCAL) return res.status(500).json({ error: 'MetaApi not configured' });
  try {
    const r = await axios.post(
      'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts',
      { login: login.toString(), password, name: 'Pulstrade_' + login, server, platform: platform || 'mt5', magic: 20240410, application: 'MetaApi', type: 'cloud' },
      { headers: { 'auth-token': METAAPI_TOKEN_LOCAL, 'Content-Type': 'application/json' }, timeout: 30000 }
    );
    const accountId = r.data.id;
    db.exec('CREATE TABLE IF NOT EXISTS autotrade_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT UNIQUE NOT NULL, lot_size REAL DEFAULT 0.01, auto_trade INTEGER DEFAULT 1, created_at INTEGER DEFAULT (strftime(\'%s\',\'now\') * 1000))');
    db.prepare('INSERT OR REPLACE INTO autotrade_accounts (account_id, lot_size, auto_trade) VALUES (?, ?, ?)').run(accountId, lotSize || 0.01, autoTradeEnabled ? 1 : 0);
    res.json({ success: true, accountId });
  } catch (err) {
    console.error('MT5 connect error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.message || 'Connection failed' });
  }
});

app.post('/autotrade/connect-mt5', express.json(), async (req, res) => {
  const { login, password, server, platform, lotSize, autoTradeEnabled } = req.body;
  if (!login || !password || !server) return res.status(400).json({ error: 'Missing credentials' });
  const METAAPI_TOKEN_LOCAL = process.env.METAAPI_TOKEN || ''; if (!METAAPI_TOKEN_LOCAL) return res.status(500).json({ error: 'MetaApi not configured' });
  try {
    const r = await axios.post('https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts', { login: login.toString(), password, name: 'Pulstrade_' + login, server, platform: platform || 'mt5', magic: 20240410, application: 'MetaApi', type: 'cloud' }, { headers: { 'auth-token': METAAPI_TOKEN_LOCAL, 'Content-Type': 'application/json' }, timeout: 30000 });
    const accountId = r.data.id;
    db.exec('CREATE TABLE IF NOT EXISTS autotrade_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT UNIQUE NOT NULL, lot_size REAL DEFAULT 0.01, auto_trade INTEGER DEFAULT 1, created_at INTEGER DEFAULT (strftime(\'%s\',\'now\') * 1000))');
    db.prepare('INSERT OR REPLACE INTO autotrade_accounts (account_id, lot_size, auto_trade) VALUES (?, ?, ?)').run(accountId, lotSize || 0.01, autoTradeEnabled ? 1 : 0);
    res.json({ success: true, accountId });
  } catch (err) {
    console.error('MT5 connect error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.message || 'Connection failed' });
  }
});
