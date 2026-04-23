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

// ── Migrations — add outcome columns if old DB ──────────────────────────
try {
  const cols = db.prepare("PRAGMA table_info(signals)").all().map(c => c.name);
  if (!cols.includes('outcome'))    db.exec("ALTER TABLE signals ADD COLUMN outcome TEXT DEFAULT 'open'");
  if (!cols.includes('exit_price')) db.exec("ALTER TABLE signals ADD COLUMN exit_price REAL");
  if (!cols.includes('closed_at'))  db.exec("ALTER TABLE signals ADD COLUMN closed_at INTEGER");
  if (!cols.includes('pnl_r'))      db.exec("ALTER TABLE signals ADD COLUMN pnl_r REAL");
  console.log('✓ Migrations applied');
} catch(e) { console.error('Migration error:', e.message); }

// ── CLEANUP on startup ──────────────────────────────────────────────────
try {
  const del1D = db.prepare("DELETE FROM signals WHERE timeframe = '1D'").run();
  console.log(`✓ Deleted ${del1D.changes} old 1D signals`);
} catch(e) { console.error('Cleanup error:', e.message); }
try {
  const staleCut = Date.now() - 30*24*3600000;
  const delStale = db.prepare("DELETE FROM signals WHERE timestamp < ?").run(staleCut);
  console.log(`✓ Deleted ${delStale.changes} signals older than 30 days`);
} catch(e) {}

// ── PRICE CACHE ─────────────────────────────────────────────────────────
let cachedPrice = { price: null, timestamp: 0 };
const PRICE_CACHE_TTL = 30 * 1000;

async function fetchLivePriceFromSource() {
  if (!TWELVE_API_KEY) return null;
  try {
    const r = await axios.get(
      `https://api.twelvedata.com/price?symbol=${encodeURIComponent(TICKER)}&apikey=${TWELVE_API_KEY}`,
      { timeout: 5000 }
    );
    const p = parseFloat(r.data?.price);
    if (!isNaN(p) && p > 0) {
      cachedPrice = { price: p, timestamp: Date.now() };
      console.log(`✓ Price updated: $${p.toFixed(2)}`);
      return p;
    }
    if (r.data?.code === 429) console.warn('⚠️ Twelve Data rate limit reached');
    return null;
  } catch(e) { return null; }
}
fetchLivePriceFromSource();
setInterval(fetchLivePriceFromSource, PRICE_CACHE_TTL);

// ── CANDLE CACHE ────────────────────────────────────────────────────────
const candleCache = {};
const CANDLE_CACHE_TTL = 5 * 60 * 1000;

async function fetchCandles(interval, outputsize=60) {
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
  const basePrice = cachedPrice.price || 3300;
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

// ── Helpers ─────────────────────────────────────────────────────────────
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

function calcBollingerBands(closes, period=20, std=2) {
  if (closes.length < period) return null;
  const slice = closes.slice(0, period);
  const mean  = slice.reduce((a,b) => a+b, 0) / period;
  const variance = slice.reduce((a,b) => a + Math.pow(b-mean,2), 0) / period;
  const s = Math.sqrt(variance);
  return { upper: mean+std*s, middle: mean, lower: mean-std*s };
}

function isEconomicEventSoon(offsetMin=30) {
  const now = new Date();
  const total = now.getUTCHours()*60 + now.getUTCMinutes();
  const events = [13*60+30, 15*60, 19*60, 14*60, 8*60+30];
  return events.some(e => Math.abs(e - total) <= offsetMin);
}

function isMarketClosed() {
  const now = new Date();
  const day  = now.getUTCDay();
  const hour = now.getUTCHours();
  return day === 6 || day === 0 || (day === 5 && hour >= 21) || (day === 1 && hour < 1);
}

// ── Confluence Score ───────────────────────────────────────────────────
function calcConfluenceScore(candles, action, fibLevel, atr, rsi) {
  let score = 0;
  const reasons = [];
  const closes = candles.map(c => c.close);
  const price  = closes[0];

  const fibScores = { '61.8%':25, '50.0%':20, '38.2%':18, '78.6%':15, '23.6%':10 };
  const fibScore  = fibScores[fibLevel] || 10;
  score += fibScore;
  reasons.push(`FIB ${fibLevel}: +${fibScore}pts`);

  let rsiScore = 0;
  if (action === 'BUY') {
    if (rsi >= 25 && rsi <= 45)      { rsiScore=20; reasons.push('RSI oversold: +20pts'); }
    else if (rsi >= 45 && rsi <= 55) { rsiScore=10; reasons.push('RSI neutral: +10pts'); }
    else if (rsi < 25)               { rsiScore=15; reasons.push('RSI extreme oversold: +15pts'); }
    else                             { rsiScore=0;  reasons.push('RSI too high for BUY: +0pts'); }
  } else {
    if (rsi >= 55 && rsi <= 75)      { rsiScore=20; reasons.push('RSI overbought: +20pts'); }
    else if (rsi >= 45 && rsi <= 55) { rsiScore=10; reasons.push('RSI neutral: +10pts'); }
    else if (rsi > 75)               { rsiScore=15; reasons.push('RSI extreme overbought: +15pts'); }
    else                             { rsiScore=0;  reasons.push('RSI too low for SELL: +0pts'); }
  }
  score += rsiScore;

  const ema20  = calcEMA(closes, Math.min(20, closes.length-1));
  const ema50  = calcEMA(closes, Math.min(50, closes.length-1));
  const ema200 = calcEMA(closes, Math.min(200, closes.length-1));

  const uptrend   = ema50 && ema200 && ema50 > ema200;
  const downtrend = ema50 && ema200 && ema50 < ema200;

  let emaScore = 0;
  if (action === 'BUY') {
    if (!uptrend) { emaScore = -10; reasons.push('Counter-trend BUY: -10pts'); }
    else {
      if (price > ema20)  emaScore += 8;
      if (price > ema50)  emaScore += 7;
      if (price > ema200) emaScore += 5;
      reasons.push(`Uptrend confirmed: +${emaScore}pts`);
    }
  } else {
    if (!downtrend) { emaScore = -10; reasons.push('Counter-trend SELL: -10pts'); }
    else {
      if (price < ema20)  emaScore += 8;
      if (price < ema50)  emaScore += 7;
      if (price < ema200) emaScore += 5;
      reasons.push(`Downtrend confirmed: +${emaScore}pts`);
    }
  }
  score += emaScore;

  const bb = calcBollingerBands(closes);
  let bbScore = 0;
  if (bb) {
    if      (action==='BUY'  && price<=bb.lower)  { bbScore=15; reasons.push('Price at BB lower: +15pts'); }
    else if (action==='SELL' && price>=bb.upper)  { bbScore=15; reasons.push('Price at BB upper: +15pts'); }
    else if (action==='BUY'  && price<=bb.middle) { bbScore=8;  reasons.push('Price below BB mid: +8pts'); }
    else if (action==='SELL' && price>=bb.middle) { bbScore=8;  reasons.push('Price above BB mid: +8pts'); }
  }
  score += bbScore;

  const c0=candles[0], c1=candles[1];
  const body0  = Math.abs(c0.close-c0.open);
  const range0 = c0.high-c0.low;
  const upper0 = c0.high - Math.max(c0.close,c0.open);
  const lower0 = Math.min(c0.close,c0.open) - c0.low;

  let patternScore=0, patternName='No pattern';
  if      (action==='BUY'  && c0.close>c0.open && c0.close>c1.open && c0.open<c1.close) { patternScore=20; patternName='Bullish Engulfing'; }
  else if (action==='SELL' && c0.close<c0.open && c0.close<c1.close && c0.open>c1.open) { patternScore=20; patternName='Bearish Engulfing'; }
  else if (action==='BUY'  && lower0>body0*2.5 && lower0>upper0*2)                      { patternScore=18; patternName='Bullish Pin Bar'; }
  else if (action==='SELL' && upper0>body0*2.5 && upper0>lower0*2)                      { patternScore=18; patternName='Bearish Pin Bar'; }
  else if (range0>0 && body0/range0<0.25)                                                { patternScore=12; patternName='Doji'; }
  else if (c0.high<c1.high && c0.low>c1.low)                                            { patternScore=10; patternName='Inside Bar'; }

  score += patternScore;
  reasons.push(`${patternName}: +${patternScore}pts`);

  return { score: Math.min(Math.round(score), 100), reasons, patternName };
}

// ── OUTCOME TRACKER — real TP/SL hit detection ──────────────────────────
async function trackSignalOutcomes() {
  try {
    // Find all open signals
    const openSignals = db.prepare("SELECT * FROM signals WHERE outcome = 'open' OR outcome IS NULL").all();
    if (openSignals.length === 0) return;

    console.log(`🔍 Tracking ${openSignals.length} open signals...`);

    // Fetch recent candles once for all checks (5m interval = granular enough)
    const candles = await fetchCandles('5min', 100);
    if (!candles || candles.length < 5) {
      console.log('⚠️ No candles available for outcome tracking');
      return;
    }

    // Sort oldest → newest for scanning
    const sortedCandles = [...candles].sort((a, b) => a.timestamp - b.timestamp);

    let closedCount = 0;

    for (const signal of openSignals) {
      try {
        // Only check candles AFTER signal timestamp
        const relevantCandles = sortedCandles.filter(c => c.timestamp > signal.timestamp);
        if (relevantCandles.length === 0) continue;

        const isBuy = signal.action === 'BUY';
        const entry = signal.price;
        const sl    = signal.sl;
        const tp1   = signal.tp1;
        const tp2   = signal.tp2;

        if (!sl || !tp1) continue;

        let outcome = null;
        let exitPrice = null;
        let exitTime = null;
        let pnlR = null;

        // Scan candles in order — first one to hit SL or TP wins
        for (const c of relevantCandles) {
          if (isBuy) {
            // BUY: SL below, TP above
            if (c.low <= sl)  { outcome='sl_hit';  exitPrice=sl;  exitTime=c.timestamp; break; }
            if (c.high >= (tp2 || tp1)) { outcome='tp2_hit'; exitPrice=tp2||tp1; exitTime=c.timestamp; break; }
            if (c.high >= tp1){ outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
          } else {
            // SELL: SL above, TP below
            if (c.high >= sl) { outcome='sl_hit';  exitPrice=sl;  exitTime=c.timestamp; break; }
            if (c.low <= (tp2 || tp1)) { outcome='tp2_hit'; exitPrice=tp2||tp1; exitTime=c.timestamp; break; }
            if (c.low <= tp1) { outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
          }
        }

        // Expired check — signal older than 48h and no hit → mark expired
        if (!outcome) {
          const ageHours = (Date.now() - signal.timestamp) / 3600000;
          if (ageHours > 48) {
            outcome = 'expired';
            // Close at current price if available
            const lastCandle = relevantCandles[relevantCandles.length - 1];
            exitPrice = lastCandle ? lastCandle.close : entry;
            exitTime = lastCandle ? lastCandle.timestamp : Date.now();
          }
        }

        if (outcome) {
          // Calculate PnL in R units (1R = risk to SL)
          const risk = Math.abs(entry - sl);
          const profit = isBuy ? (exitPrice - entry) : (entry - exitPrice);
          pnlR = risk > 0 ? profit / risk : 0;

          db.prepare(`UPDATE signals SET outcome=?, exit_price=?, closed_at=?, pnl_r=? WHERE id=?`)
            .run(outcome, exitPrice, exitTime, Math.round(pnlR * 100) / 100, signal.id);

          console.log(`✓ Signal #${signal.id} ${signal.action} closed: ${outcome} @ ${exitPrice.toFixed(2)} (${pnlR >= 0 ? '+' : ''}${pnlR.toFixed(2)}R)`);
          closedCount++;
        }
      } catch(e) {
        console.error(`Tracker error for signal #${signal.id}:`, e.message);
      }
    }

    if (closedCount > 0) console.log(`✓ Closed ${closedCount} signals this cycle`);
  } catch(e) {
    console.error('Outcome tracker error:', e.message);
  }
}

// Run outcome tracker every 15 minutes
trackSignalOutcomes();
setInterval(trackSignalOutcomes, 15 * 60 * 1000);

// ── MAIN SCANNER ──────────────────────────────────────────────────────────
async function scanForSignals() {
  if (isMarketClosed()) { console.log('Market closed'); return; }
  if (isEconomicEventSoon(30)) { console.log('⚠️ Economic event block'); return; }
  if (!cachedPrice.price) { console.log('⚠️ No live price'); return; }

  const timeframes = [
    { label:'1H', interval:'1h', validFor:2, minScore:75 },
    { label:'4H', interval:'4h', validFor:8, minScore:73 },
  ];

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 30) continue;

      const closes = candles.map(c => c.close);
      const price  = closes[0];

      // Deviation check
      if (cachedPrice.price) {
        const dev = Math.abs(price - cachedPrice.price) / cachedPrice.price;
        if (dev > 0.05) { console.log(`⚠️ [${tf.label}] Stale candles — skipping`); continue; }
      }

      const ema50  = calcEMA(closes, Math.min(50, closes.length-1));
      const ema200 = calcEMA(closes, Math.min(200, closes.length-1));
      const masterUptrend   = ema50 && ema200 && ema50 > ema200;
      const masterDowntrend = ema50 && ema200 && ema50 < ema200;

      console.log(`[${tf.label}] $${price.toFixed(2)} Trend:${masterUptrend?'UP':masterDowntrend?'DOWN':'NEUTRAL'}`);

      let swingHigh=-Infinity, swingLow=Infinity;
      for (let i=0; i<30; i++) {
        if (candles[i].high > swingHigh) swingHigh=candles[i].high;
        if (candles[i].low  < swingLow)  swingLow=candles[i].low;
      }
      const fibRange = swingHigh - swingLow;

      const fibs = {
        '61.8%': swingHigh - fibRange*0.618,
        '50.0%': swingHigh - fibRange*0.500,
        '38.2%': swingHigh - fibRange*0.382,
        '78.6%': swingHigh - fibRange*0.786,
      };

      let atrSum=0;
      for (let i=0; i<14; i++) {
        atrSum += Math.max(
          candles[i].high - candles[i].low,
          Math.abs(candles[i].high - (candles[i+1]?.close||candles[i].close)),
          Math.abs(candles[i].low  - (candles[i+1]?.close||candles[i].close))
        );
      }
      const atr = Math.round((atrSum/14)*100)/100;

      let gains=0, losses=0;
      for (let i=1; i<=14; i++) {
        const diff = closes[i-1]-closes[i];
        if (diff>0) gains+=diff; else losses-=diff;
      }
      const rsi = Math.round((100-100/(1+gains/14/(losses/14||0.001)))*10)/10;

      for (const [fibName, fibValue] of Object.entries(fibs)) {
        const tolerance = atr * 2.0;
        if (Math.abs(price - fibValue) > tolerance) continue;

        const isBuyZone  = price < swingLow  + fibRange * 0.45;
        const isSellZone = price > swingHigh - fibRange * 0.45;

        for (const action of ['BUY', 'SELL']) {
          if (action==='BUY'  && !isBuyZone)  continue;
          if (action==='SELL' && !isSellZone) continue;
          if (action==='BUY'  && masterDowntrend) continue;
          if (action==='SELL' && masterUptrend) continue;

          const confluence = calcConfluenceScore(candles, action, fibName, atr, rsi);
          if (confluence.score < tf.minScore) continue;

          const recent = db.prepare(
            `SELECT id FROM signals WHERE ticker=? AND action=? AND fib_level=? AND timeframe=? AND timestamp > ?`
          ).get(TICKER, action, fibName, tf.label, Date.now()-2*3600000);
          if (recent) continue;

          let sl, tp1, tp2;
          const fibSorted = Object.values(fibs).sort((a,b)=>a-b);
          if (action==='BUY') {
            const nextDown = fibSorted.filter(f=>f<fibValue).pop() || swingLow - atr;
            sl  = Math.round((nextDown - atr*1.5)*100)/100;
            tp1 = Math.round((fibValue + atr*3.0)*100)/100;
            tp2 = Math.round((fibValue + atr*5.0)*100)/100;
          } else {
            const nextUp = fibSorted.filter(f=>f>fibValue).shift() || swingHigh + atr;
            sl  = Math.round((nextUp   + atr*1.5)*100)/100;
            tp1 = Math.round((fibValue - atr*3.0)*100)/100;
            tp2 = Math.round((fibValue - atr*5.0)*100)/100;
          }

          const risk = Math.abs(price - sl);
          const reward = Math.abs(tp1 - price);
          const rr = risk > 0 ? reward / risk : 0;
          if (rr < 2.0) continue;

          const signal = {
            ticker: TICKER, action,
            price: Math.round(price*100)/100,
            sl, tp1, tp2,
            timeframe: tf.label,
            confidence: confluence.score,
            fib_level: fibName,
            pattern: confluence.patternName,
            note: confluence.reasons.slice(0,3).join(' | '),
            rsi: Math.round(rsi*10)/10,
            atr,
            current_price: Math.round(price*100)/100,
            entry_valid_for: tf.validFor,
            mtf: JSON.stringify({ h1:tf.label==='1H', h4:tf.label==='4H', d1:false }),
            timestamp: Date.now(),
          };

          db.prepare(`INSERT INTO signals
            (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome)
            VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')
          `).run(signal);

          console.log(`✓ ${action} ${TICKER} @ ${price} (${tf.label}, ${fibName}, ${confluence.score}%)`);
          const inserted = db.prepare('SELECT last_insert_rowid() as id').get();
          sendSignalPush({ ...signal, id: inserted.id });
        }
      }
    } catch(err) { console.error(`Scanner error ${tf.label}:`, err.message); }
  }
}

// ── Routes ────────────────────────────────────────────────────────────
app.get('/',       (req,res) => res.json({ status:'Pulstrade Backend', version:'3.3.0-tracking' }));
app.get('/health', (req,res) => res.json({
  status:'ok',
  signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c,
  open:    db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
  closed:  db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome IS NOT NULL AND outcome != 'open'").get().c,
  marketClosed: isMarketClosed(),
  priceCache: cachedPrice,
  cacheAge: cachedPrice.timestamp ? Math.floor((Date.now()-cachedPrice.timestamp)/1000) + 's' : 'none',
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

// ── /stats — REAL statistics from actual outcomes ─────────────────────
app.get('/stats', (req, res) => {
  try {
    const all = db.prepare("SELECT * FROM signals WHERE outcome IS NOT NULL AND outcome != 'open'").all();
    const totalClosed = all.length;
    
    if (totalClosed === 0) {
      return res.json({
        totalSignals: db.prepare("SELECT COUNT(*) as c FROM signals").get().c,
        closedSignals: 0,
        openSignals: db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
        winRate: null,
        avgRR: null,
        profitFactor: null,
        totalPnL: null,
        bestTrade: null,
        worstTrade: null,
        wins: 0, losses: 0, expired: 0,
        fibPerformance: {},
        message: 'Not enough closed trades yet. Stats will appear after signals close.',
      });
    }

    const wins = all.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
    const losses = all.filter(s => s.outcome === 'sl_hit');
    const expired = all.filter(s => s.outcome === 'expired');
    
    const decisiveClosed = wins.length + losses.length; // exclude expired from win rate
    const winRate = decisiveClosed > 0 ? (wins.length / decisiveClosed) * 100 : 0;
    
    const allPnL = all.map(s => s.pnl_r || 0);
    const totalPnL = allPnL.reduce((a,b) => a+b, 0);
    const avgRR = all.length > 0 ? totalPnL / all.length : 0;
    
    const grossWin = wins.reduce((a,s) => a + (s.pnl_r || 0), 0);
    const grossLoss = Math.abs(losses.reduce((a,s) => a + (s.pnl_r || 0), 0));
    const profitFactor = grossLoss > 0 ? grossWin / grossLoss : grossWin;
    
    const bestTrade = allPnL.length > 0 ? Math.max(...allPnL) : 0;
    const worstTrade = allPnL.length > 0 ? Math.min(...allPnL) : 0;
    
    // FIB performance breakdown
    const fibPerformance = {};
    for (const level of ['61.8%', '50.0%', '38.2%', '78.6%', '23.6%']) {
      const atLevel = all.filter(s => s.fib_level === level);
      const winsAtLevel = atLevel.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
      const lossesAtLevel = atLevel.filter(s => s.outcome === 'sl_hit');
      const decisive = winsAtLevel.length + lossesAtLevel.length;
      fibPerformance[level] = {
        total: atLevel.length,
        wins: winsAtLevel.length,
        losses: lossesAtLevel.length,
        winRate: decisive > 0 ? Math.round((winsAtLevel.length / decisive) * 1000) / 10 : null,
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
      bestTrade: Math.round(bestTrade * 100) / 100,
      worstTrade: Math.round(worstTrade * 100) / 100,
      wins: wins.length,
      losses: losses.length,
      expired: expired.length,
      fibPerformance,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/price', (req,res) => {
  if (cachedPrice.price) {
    return res.json({
      price: cachedPrice.price,
      ticker: TICKER,
      cached: true,
      age: Math.floor((Date.now()-cachedPrice.timestamp)/1000),
    });
  }
  fetchLivePriceFromSource().then(p => res.json({ price: p, ticker: TICKER, cached: false }));
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
    const q=encodeURIComponent('gold price XAU OR Federal Reserve interest rates OR inflation CPI dollar');
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
    if (!data.action||!data.price||!data.ticker) return res.status(400).json({error:'Missing fields'});
    const confidence=parseInt(data.confidence)||0;
    if (confidence<75) return res.json({filtered:true});
    const tf=data.timeframe||'';
    const signal={ticker:data.ticker||'XAU/USD',action:data.action.toUpperCase(),price:parseFloat(data.price),sl:data.sl?parseFloat(data.sl):null,tp1:data.tp1?parseFloat(data.tp1):null,tp2:data.tp2?parseFloat(data.tp2):null,timeframe:tf,confidence,fib_level:data.fib_level||null,pattern:data.pattern||null,rsi:data.rsi?parseFloat(data.rsi):null,atr:data.atr?parseFloat(data.atr):null,current_price:parseFloat(data.price),entry_valid_for:tf.includes('H')?(tf==='1H'?2:8):24,mtf:JSON.stringify({h1:data.mtf?.h1||false,h4:data.mtf?.h4||false,d1:data.mtf?.d1||false}),timestamp:Date.now()};
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome) VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open')`).run(signal);
    sendSignalPush({...signal,id:db.prepare('SELECT last_insert_rowid() as id').get().id});
    res.json({success:true,signal});
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.post('/autotrade/connect-mt5', express.json(), async (req,res) => {
  const {login,password,server,platform,lotSize,autoTradeEnabled}=req.body;
  if (!login||!password||!server) return res.status(400).json({error:'Missing credentials'});
  if (!METAAPI_TOKEN) return res.status(500).json({error:'MetaApi not configured'});
  try {
    const r=await axios.post('https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts',{login:login.toString(),password,name:'Pulstrade_'+login,server,platform:platform||'mt5',magic:20240410,application:'MetaApi',type:'cloud'},{headers:{'auth-token':METAAPI_TOKEN,'Content-Type':'application/json'},timeout:30000});
    db.prepare('INSERT OR REPLACE INTO autotrade_accounts (account_id,lot_size,auto_trade) VALUES (?,?,?)').run(r.data.id,lotSize||0.01,autoTradeEnabled?1:0);
    res.json({success:true,accountId:r.data.id});
  } catch(e) { res.status(500).json({error:e.response?.data?.message||'Connection failed'}); }
});

app.post('/autotrade/connect', express.json(), (req,res) => {
  const {accountId,autoTradeEnabled}=req.body;
  if (!accountId) return res.status(400).json({error:'Missing accountId'});
  db.prepare('UPDATE autotrade_accounts SET auto_trade=? WHERE account_id=?').run(autoTradeEnabled?1:0,accountId);
  res.json({success:true});
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
  const allEvents = [
    { title: 'EUR CPI Flash Estimate', currency: 'EUR', category: 'Inflation', impact: 'high', time: getDay(0, 10, 0), forecast: '2.2%', previous: '2.3%', actual: null },
    { title: 'US ISM Manufacturing', currency: 'USD', category: 'Business', impact: 'medium', time: getDay(0, 15, 0), forecast: '48.5', previous: '47.8', actual: null },
    { title: 'US CPI (MoM)', currency: 'USD', category: 'Inflation', impact: 'high', time: getDay(1, 13, 30), forecast: '0.3%', previous: '0.4%', actual: null },
    { title: 'FOMC Meeting Minutes', currency: 'USD', category: 'Central Bank', impact: 'high', time: getDay(2, 19, 0), forecast: '—', previous: '—', actual: null },
    { title: 'US Jobless Claims', currency: 'USD', category: 'Employment', impact: 'medium', time: getDay(3, 13, 30), forecast: '215K', previous: '210K', actual: null },
    { title: 'US Non-Farm Payrolls', currency: 'USD', category: 'Employment', impact: 'high', time: getDay(4, 13, 30), forecast: '185K', previous: '175K', actual: null },
    { title: 'US Core PCE Price Index', currency: 'USD', category: 'Inflation', impact: 'high', time: getDay(4, 13, 30), forecast: '0.3%', previous: '0.3%', actual: null },
  ];
  const cutoff = new Date(now.getTime() - 24 * 3600000);
  return allEvents.filter(e => new Date(e.time) >= cutoff).sort((a, b) => new Date(a.time) - new Date(b.time)).map(e => {
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

// ── Start ──────────────────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5*60*1000);
app.listen(PORT, () => console.log(`Pulstrade backend v3.3 (real tracking) on port ${PORT}`));
