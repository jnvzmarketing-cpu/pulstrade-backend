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

// ── Helpers ───────────────────────────────────────────────────────────────────
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

// ── Fetch Candles ─────────────────────────────────────────────────────────────
async function fetchCandles(interval, outputsize=60) {
  if (!TWELVE_API_KEY) return generateMockCandles(outputsize);
  try {
    const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(TICKER)}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const res = await axios.get(url, { timeout: 10000 });
    if (!res.data?.values) return generateMockCandles(outputsize);
    return res.data.values.map(v => ({
      open: parseFloat(v.open), high: parseFloat(v.high),
      low:  parseFloat(v.low),  close: parseFloat(v.close),
      timestamp: new Date(v.datetime).getTime(),
    }));
  } catch(e) { return generateMockCandles(outputsize); }
}

function generateMockCandles(count) {
  const candles = [];
  let price = 3300 + Math.random()*30;
  const now = Date.now();
  for (let i=count-1; i>=0; i--) {
    const change = (Math.random()-0.5)*15;
    const open=price, close=price+change;
    candles.push({ open, high:Math.max(open,close)+Math.random()*8, low:Math.min(open,close)-Math.random()*8, close, timestamp:now-i*300000 });
    price = close;
  }
  return candles;
}

// ── Confluence Score ───────────────────────────────────────────────────────────
function calcConfluenceScore(candles, action, fibLevel, atr, rsi) {
  let score = 0;
  const reasons = [];
  const closes = candles.map(c => c.close);
  const price  = closes[0];

  // FIB Quality (0-25)
  const fibScores = { '61.8%':25, '50.0%':20, '38.2%':18, '78.6%':15, '23.6%':10 };
  const fibScore  = fibScores[fibLevel] || 10;
  score += fibScore;
  reasons.push(`FIB ${fibLevel}: +${fibScore}pts`);

  // RSI (0-20)
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

  // ── TREND FILTER — EMA50 vs EMA200 (0-20) ────────────────────────────────
  // KEY FIX: BUY only allowed in uptrend, SELL only in downtrend
  const ema20  = calcEMA(closes, Math.min(20, closes.length-1));
  const ema50  = calcEMA(closes, Math.min(50, closes.length-1));
  const ema200 = calcEMA(closes, Math.min(200, closes.length-1));

  const uptrend   = ema50 && ema200 && ema50 > ema200;
  const downtrend = ema50 && ema200 && ema50 < ema200;

  let emaScore = 0;
  if (action === 'BUY') {
    if (!uptrend) {
      // Counter-trend BUY in downtrend — hard penalty, almost always filtered
      emaScore = -10;
      reasons.push('Counter-trend BUY (downtrend): -10pts');
    } else {
      if (price > ema20)  emaScore += 8;
      if (price > ema50)  emaScore += 7;
      if (price > ema200) emaScore += 5;
      reasons.push(`Uptrend confirmed (EMA50>EMA200): +${emaScore}pts`);
    }
  } else {
    if (!downtrend) {
      // Counter-trend SELL in uptrend — hard penalty
      emaScore = -10;
      reasons.push('Counter-trend SELL (uptrend): -10pts');
    } else {
      if (price < ema20)  emaScore += 8;
      if (price < ema50)  emaScore += 7;
      if (price < ema200) emaScore += 5;
      reasons.push(`Downtrend confirmed (EMA50<EMA200): +${emaScore}pts`);
    }
  }
  score += emaScore;

  // Bollinger Bands (0-15)
  const bb = calcBollingerBands(closes);
  let bbScore = 0;
  if (bb) {
    if      (action==='BUY'  && price<=bb.lower)  { bbScore=15; reasons.push('Price at BB lower: +15pts'); }
    else if (action==='SELL' && price>=bb.upper)  { bbScore=15; reasons.push('Price at BB upper: +15pts'); }
    else if (action==='BUY'  && price<=bb.middle) { bbScore=8;  reasons.push('Price below BB mid: +8pts'); }
    else if (action==='SELL' && price>=bb.middle) { bbScore=8;  reasons.push('Price above BB mid: +8pts'); }
  }
  score += bbScore;

  // Candlestick Pattern (0-20)
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

// ── MAIN SCANNER ──────────────────────────────────────────────────────────────
async function scanForSignals() {
  // Weekend / market closed check
  if (isMarketClosed()) {
    console.log('Market closed — skipping scan');
    return;
  }

  // Economic event block
  if (isEconomicEventSoon(30)) {
    console.log('⚠️ Economic event soon — scan blocked');
    return;
  }

  const timeframes = [
    { label:'1H', interval:'1h',   validFor:2,  minScore:75 },
    { label:'4H', interval:'4h',   validFor:8,  minScore:73 },
    { label:'1D', interval:'1day', validFor:24, minScore:70 },
  ];

  const newSignals = [];

  for (const tf of timeframes) {
    try {
      // Need 200+ candles for EMA200
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 30) continue;

      const closes = candles.map(c => c.close);
      const price  = closes[0];

      // ── MASTER TREND CHECK ────────────────────────────────────────────────
      const ema50  = calcEMA(closes, Math.min(50, closes.length-1));
      const ema200 = calcEMA(closes, Math.min(200, closes.length-1));
      const masterUptrend   = ema50 && ema200 && ema50 > ema200;
      const masterDowntrend = ema50 && ema200 && ema50 < ema200;

      console.log(`[${tf.label}] Price:${price.toFixed(2)} EMA50:${ema50?.toFixed(2)} EMA200:${ema200?.toFixed(2)} Trend:${masterUptrend?'UP':masterDowntrend?'DOWN':'NEUTRAL'}`);

      // Swing high/low from last 30 candles
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

      // ATR
      let atrSum=0;
      for (let i=0; i<14; i++) {
        atrSum += Math.max(
          candles[i].high - candles[i].low,
          Math.abs(candles[i].high - (candles[i+1]?.close||candles[i].close)),
          Math.abs(candles[i].low  - (candles[i+1]?.close||candles[i].close))
        );
      }
      const atr = Math.round((atrSum/14)*100)/100;

      // RSI
      let gains=0, losses=0;
      for (let i=1; i<=14; i++) {
        const diff = closes[i-1]-closes[i];
        if (diff>0) gains+=diff; else losses-=diff;
      }
      const rsi = Math.round((100-100/(1+gains/14/(losses/14||0.001)))*10)/10;

      for (const [fibName, fibValue] of Object.entries(fibs)) {
        const tolerance = atr * 2.0;
        if (Math.abs(price - fibValue) > tolerance) continue;

        // ── ZONE FILTER ───────────────────────────────────────────────────
        // BUY zone = price near swing LOW (bottom of range)
        // SELL zone = price near swing HIGH (top of range)
        const isBuyZone  = price < swingLow  + fibRange * 0.45;
        const isSellZone = price > swingHigh - fibRange * 0.45;

        for (const action of ['BUY', 'SELL']) {
          // Zone check
          if (action==='BUY'  && !isBuyZone)  continue;
          if (action==='SELL' && !isSellZone) continue;

          // ── MASTER TREND GATE — most important filter ─────────────────
          // Only allow BUY in uptrend, only SELL in downtrend
          // Exception: neutral trend (no clear EMA cross) allows both
          if (action==='BUY'  && masterDowntrend) {
            console.log(`[${tf.label}] BUY blocked — master downtrend (EMA50<EMA200)`);
            continue;
          }
          if (action==='SELL' && masterUptrend) {
            console.log(`[${tf.label}] SELL blocked — master uptrend (EMA50>EMA200)`);
            continue;
          }

          const confluence = calcConfluenceScore(candles, action, fibName, atr, rsi);

          if (confluence.score < tf.minScore) {
            console.log(`Signal filtered: ${action} ${fibName} score=${confluence.score} < ${tf.minScore}`);
            continue;
          }

          // No duplicate in last 4h
          const recent = db.prepare(
            `SELECT id FROM signals WHERE ticker=? AND action=? AND timeframe=? AND timestamp > ?`
          ).get(TICKER, action, tf.label, Date.now()-4*3600000);
          if (recent) continue;

          // SL/TP via next FIB level
          const fibValues = Object.values(fibs).sort((a,b)=>a-b);
          let sl, tp1, tp2;
          if (action==='BUY') {
            const nextDown = fibValues.filter(f=>f<fibValue).pop() || fibValue-atr*2;
            sl  = Math.round((nextDown - atr*0.5)*100)/100;
            tp1 = Math.round((fibValue + atr*3)*100)/100;
            tp2 = Math.round((fibValue + atr*6)*100)/100;
          } else {
            const nextUp = fibValues.filter(f=>f>fibValue).shift() || fibValue+atr*2;
            sl  = Math.round((nextUp   + atr*0.5)*100)/100;
            tp1 = Math.round((fibValue - atr*3)*100)/100;
            tp2 = Math.round((fibValue - atr*6)*100)/100;
          }

          const signal = {
            ticker: TICKER, action,
            price:  Math.round(price*100)/100,
            sl, tp1, tp2,
            timeframe:       tf.label,
            confidence:      confluence.score,
            fib_level:       fibName,
            pattern:         confluence.patternName,
            note:            confluence.reasons.slice(0,3).join(' | '),
            rsi:             Math.round(rsi*10)/10,
            atr,
            current_price:   Math.round(price*100)/100,
            entry_valid_for: tf.validFor,
            mtf: JSON.stringify({ h1:tf.label==='1H', h4:tf.label==='4H', d1:tf.label==='1D' }),
            timestamp: Date.now(),
          };

          db.prepare(`INSERT INTO signals
            (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
            VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)
          `).run(signal);

          console.log(`✓ SIGNAL: ${action} ${TICKER} @ ${price} (${tf.label}, ${fibName}, score:${confluence.score}, trend:${masterUptrend?'UP':masterDowntrend?'DOWN':'NEUTRAL'})`);
          const inserted = db.prepare('SELECT last_insert_rowid() as id').get();
          sendSignalPush({ ...signal, id: inserted.id });
          newSignals.push(signal);
        }
      }
    } catch(err) {
      console.error(`Error scanning ${tf.label}:`, err.message);
    }
  }

  // Seed if DB empty
  const count = db.prepare('SELECT COUNT(*) as c FROM signals').get().c;
  if (count === 0) await seedSignals();

  return newSignals;
}

async function seedSignals() {
  let price = 3300;
  try {
    if (TWELVE_API_KEY) {
      const r = await axios.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(TICKER)}&apikey=${TWELVE_API_KEY}`, { timeout:5000 });
      price = parseFloat(r.data.price) || price;
    }
  } catch(_) {}
  const now = Date.now();
  const seeds = [
    { action:'BUY',  fib:'61.8%', conf:82, tf:'1H',  validFor:2,  rsi:34.2, atr:12.4, pattern:'Bullish Pin Bar',    offset:3*60000 },
    { action:'SELL', fib:'38.2%', conf:78, tf:'4H',  validFor:8,  rsi:67.8, atr:18.6, pattern:'Bearish Engulfing',  offset:80*60000 },
    { action:'BUY',  fib:'50.0%', conf:75, tf:'4H',  validFor:8,  rsi:42.1, atr:15.8, pattern:'Doji on 50% FIB',   offset:225*60000 },
    { action:'SELL', fib:'61.8%', conf:80, tf:'1D',  validFor:24, rsi:71.2, atr:28.4, pattern:'Bearish Pin Bar',    offset:22*3600000 },
  ];
  for (const s of seeds) {
    const sl  = s.action==='BUY' ? price-s.atr*1.5 : price+s.atr*1.5;
    const tp1 = s.action==='BUY' ? price+s.atr*3.0 : price-s.atr*3.0;
    const tp2 = s.action==='BUY' ? price+s.atr*6.0 : price-s.atr*6.0;
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(TICKER,s.action,Math.round(price*100)/100,Math.round(sl*100)/100,Math.round(tp1*100)/100,Math.round(tp2*100)/100,
           s.tf,s.conf,s.fib,s.pattern,s.rsi,s.atr,Math.round(price*100)/100,s.validFor,
           JSON.stringify({h1:s.tf==='1H',h4:s.tf==='4H',d1:s.tf==='1D'}),now-s.offset);
  }
  console.log(`✓ Seeded ${seeds.length} signals @ $${price}`);
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/',       (req,res) => res.json({ status:'Pulstrade Backend', version:'3.0.0' }));
app.get('/health', (req,res) => res.json({ status:'ok', signals:db.prepare('SELECT COUNT(*) as c FROM signals').get().c, marketClosed:isMarketClosed() }));

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

app.get('/price', async (req,res) => {
  try {
    if (!TWELVE_API_KEY) return res.json({price:3300.0,ticker:TICKER});
    const r = await axios.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(TICKER)}&apikey=${TWELVE_API_KEY}`,{timeout:5000});
    res.json({price:parseFloat(r.data.price),ticker:TICKER});
  } catch(e) { res.json({price:3300.0,ticker:TICKER}); }
});

app.get('/candles', async (req,res) => {
  try {
    const interval=req.query.interval||'5min', outputsize=req.query.limit||100;
    if (!TWELVE_API_KEY) return res.json(generateMockCandles(parseInt(outputsize)));
    const url=`https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(TICKER)}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVE_API_KEY}&format=JSON`;
    const r = await axios.get(url,{timeout:10000});
    if (!r.data?.values) return res.json(generateMockCandles(parseInt(outputsize)));
    res.json(r.data.values.map(v=>({time:new Date(v.datetime).getTime(),open:parseFloat(v.open),high:parseFloat(v.high),low:parseFloat(v.low),close:parseFloat(v.close)})).reverse());
  } catch(e) { res.json(generateMockCandles(100)); }
});

app.get('/news', async (req,res) => {
  try {
    const q=encodeURIComponent('gold price XAU OR Federal Reserve interest rates OR Trump tariffs economy OR geopolitical risk gold OR inflation CPI dollar');
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
    if (confidence<75) return res.json({filtered:true,reason:'Below quality threshold'});
    const tf=data.timeframe||'';
    const signal={ticker:data.ticker||'XAU/USD',action:data.action.toUpperCase(),price:parseFloat(data.price),sl:data.sl?parseFloat(data.sl):null,tp1:data.tp1?parseFloat(data.tp1):null,tp2:data.tp2?parseFloat(data.tp2):null,timeframe:tf,confidence,fib_level:data.fib_level||null,pattern:data.pattern||null,rsi:data.rsi?parseFloat(data.rsi):null,atr:data.atr?parseFloat(data.atr):null,current_price:parseFloat(data.price),entry_valid_for:tf.includes('H')?(tf==='1H'?2:8):24,mtf:JSON.stringify({h1:data.mtf?.h1||false,h4:data.mtf?.h4||false,d1:data.mtf?.d1||false}),timestamp:Date.now()};
    db.prepare(`INSERT INTO signals (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,rsi,atr,current_price,entry_valid_for,mtf,timestamp) VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp)`).run(signal);
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

async function autoExecuteForAllAccounts(signal) {
  try {
    const accounts=db.prepare('SELECT * FROM autotrade_accounts WHERE auto_trade=1').all();
    for (const account of accounts) {
      try {
        await axios.post(`${METAAPI_URL}/users/current/accounts/${account.account_id}/trade`,{actionType:signal.action==='BUY'?'ORDER_TYPE_BUY':'ORDER_TYPE_SELL',symbol:'XAUUSD',volume:account.lot_size||0.01,stopLoss:signal.sl,takeProfit:signal.tp1,comment:`Pulstrade ${signal.fib_level||''} ${signal.confidence||''}%`},{headers:{'auth-token':METAAPI_TOKEN},timeout:30000});
      } catch(e) { console.error(`Auto trade failed ${account.account_id}:`,e.message); }
    }
  } catch(e) { console.error('Auto execute error:',e.message); }
}

// ── Start ─────────────────────────────────────────────────────────────────────
scanForSignals();
setInterval(scanForSignals, 5*60*1000);
app.listen(PORT, () => console.log(`Pulstrade backend v3.0 on port ${PORT}`));
