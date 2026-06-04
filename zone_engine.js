// ════════════════════════════════════════════════════════════════════
// ZONE ENGINE v1 — Sniper limit-zone signals (Telegram parity)
// 2 limit entries + 5-TP ladder + 1 SL, generated BEFORE each session
// opens, with live lifecycle tracking (fills, TP hits, SL, expiry).
//
// Integration (see PATCH_INSTRUCTIONS.md):
//   const zoneEngine = require('./zone_engine');
//   zoneEngine.init({ db, admin, fetchCandles, getPrice: () => cachedPrice.price, app });
//
// SL POLICY: SL never moves (no break-even shift) — David's rule.
// ════════════════════════════════════════════════════════════════════

let db, admin, fetchCandles, getPrice;

// ── Config ────────────────────────────────────────────────────────────
const MIN_CONFIDENCE   = 55;   // launch threshold (was 65)
const MAX_ZONES        = 3;        // per session
const ZONE_TTL_HOURS   = 24;       // hard expiry
const ENTRY2_ATR       = 0.4;      // entry2 = entry1 + 0.4*ATR deeper
const SL_BUFFER_ATR    = 0.3;      // SL beyond entry2
const MIN_DIST_ATR     = 0.5;      // level must be ≥ 0.5*ATR from price
const MAX_DIST_ATR     = 3.5;  // widened (was 2.5)      // …and ≤ 2.5*ATR (reachable this session)
const TOUCH_TOL_ATR    = 0.3;      // counts as a "touch" of a level
const SNAP_TOL         = 2.0;      // snap TPs to round numbers within ±$2

// Sessions in UTC: [label, openHour, closeHour, zoneGenHour, zoneGenMin]
const SESSIONS = [
  { key: 'tokyo',  open: 0,  close: 8,  genH: 23, genM: 45, prevDay: true  },
  { key: 'london', open: 7,  close: 16, genH: 6,  genM: 45, prevDay: false },
  { key: 'ny',     open: 12, close: 21, genH: 11, genM: 45, prevDay: false },
];

// ── Small helpers (self-contained — no imports from index.js) ─────────
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let e = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}
function atr14(candles) {
  let s = 0;
  for (let i = 0; i < 14; i++) {
    s += Math.max(
      candles[i].high - candles[i].low,
      Math.abs(candles[i].high - (candles[i + 1]?.close || candles[i].close)),
      Math.abs(candles[i].low - (candles[i + 1]?.close || candles[i].close))
    );
  }
  return s / 14;
}
function rsi14(closes) {
  let g = 0, l = 0;
  for (let i = 1; i <= 14; i++) {
    const d = closes[i - 1] - closes[i];
    if (d > 0) g += d; else l -= d;
  }
  return Math.round((100 - 100 / (1 + (g / 14) / ((l / 14) || 0.001))) * 10) / 10;
}
const round2 = (v) => Math.round(v * 100) / 100;
const nearestRound = (v, step = 10) => Math.round(v / step) * step;
function snapToRound(price, isSellTp) {
  const r = nearestRound(price);
  if (Math.abs(price - r) <= SNAP_TOL) {
    // Never snap a TP further away than computed (be conservative)
    return isSellTp ? Math.max(r, price - SNAP_TOL) && r : r;
  }
  return round2(price);
}

// Pivot detection on chronological candles (oldest-first)
function findPivots(chrono, left = 5, right = 3) {
  const highs = [], lows = [];
  for (let i = left; i < chrono.length - right; i++) {
    let isH = true, isL = true;
    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;
      if (chrono[j].high >= chrono[i].high) isH = false;
      if (chrono[j].low <= chrono[i].low) isL = false;
      if (!isH && !isL) break;
    }
    if (isH) highs.push({ price: chrono[i].high, ts: chrono[i].timestamp });
    if (isL) lows.push({ price: chrono[i].low, ts: chrono[i].timestamp });
  }
  return { highs, lows };
}

// ── Topic publishing ──────────────────────────────────────────────────
// One FCM send per confidence tier, with a session condition so users
// with session_all are reached exactly once.
function tierTopics(confidence) {
  const t = [];
  if (confidence >= 65) t.push('aggr');
  if (confidence >= 72) t.push('mode');
  if (confidence >= 80) t.push('cons');
  return t;
}

async function sendZonePush(zone, type, extra = {}) {
  if (!admin || !admin.apps.length) return;
  const sell = zone.action === 'SELL';
  const e1 = zone.entries[0].price, e2 = zone.entries[1] ? zone.entries[1].price : null;
  const zoneStr = e2 ? `${Math.min(e1, e2).toFixed(0)}–${Math.max(e1, e2).toFixed(0)}` : e1.toFixed(0);

  let title, body;
  switch (type) {
    case 'zone_ready':
      title = `🎯 ${zone.action} LIMIT Zone ${zoneStr}`;
      body = `${zone.session.toUpperCase()} · SL ${zone.sl.toFixed(0)} · ${zone.tps.length} TPs · Set your limit orders`;
      break;
    case 'entry_filled':
      title = `✅ Entry filled @ ${extra.price.toFixed(2)}`;
      body = `You're in the trade · TP1 ${zone.tps[0].price.toFixed(0)} · SL stays ${zone.sl.toFixed(0)}`;
      break;
    case 'tp_hit':
      title = `💰 TP${extra.level} hit @ ${extra.price.toFixed(2)}`;
      body = `${zone.tps.filter(t => t.status === 'hit').length}/${zone.tps.length} TPs banked · SL unchanged ${zone.sl.toFixed(0)}`;
      break;
    case 'sl_hit':
      title = `🛑 SL @ ${zone.sl.toFixed(2)}`;
      body = `Zone closed · −1R · next zones before the next session open`;
      break;
    default:
      return;
  }

  const data = {
    type, kind: 'zone',
    signal_id: String(zone.id || ''),
    action: zone.action,
    session: zone.session || '',
  };

  const apns = { payload: { aps: { sound: 'default', badge: 1 } }, headers: { 'apns-priority': '10' } };
  const android = { priority: 'high', notification: { title, body, channelId: 'pulstrade_signals', priority: 'max', color: sell ? '#EF4444' : '#10B981' } };

  for (const tier of tierTopics(zone.confidence || 65)) {
    const condition = `'sig_limit_${tier}' in topics && ('session_${zone.session}' in topics || 'session_all' in topics)`;
    admin.messaging().send({ condition, notification: { title, body }, data, apns, android })
      .then(() => console.log(`🔔 zone push [${type}] → sig_limit_${tier}/${zone.session}`))
      .catch(e => console.error(`zone push error (${tier}):`, e.message));
  }
}

// ── DB ────────────────────────────────────────────────────────────────
function migrate() {
  const cols = db.prepare('PRAGMA table_info(signals)').all().map(c => c.name);
  if (!cols.includes('kind'))    db.exec("ALTER TABLE signals ADD COLUMN kind TEXT DEFAULT 'setup'");
  if (!cols.includes('status'))  db.exec("ALTER TABLE signals ADD COLUMN status TEXT DEFAULT 'active'");
  if (!cols.includes('session')) db.exec('ALTER TABLE signals ADD COLUMN session TEXT');
  if (!cols.includes('entries')) db.exec('ALTER TABLE signals ADD COLUMN entries TEXT');
  if (!cols.includes('tps'))     db.exec('ALTER TABLE signals ADD COLUMN tps TEXT');
  if (!cols.includes('watch_from')) db.exec('ALTER TABLE signals ADD COLUMN watch_from INTEGER');
  console.log('✓ Zone Engine migrations OK');
}

function loadActiveZones() {
  return db.prepare(
    "SELECT * FROM signals WHERE kind='zone' AND status IN ('active','filled')"
  ).all().map(rowToZone);
}
function rowToZone(r) {
  return { ...r, entries: JSON.parse(r.entries || '[]'), tps: JSON.parse(r.tps || '[]') };
}
function saveZoneState(zone) {
  db.prepare('UPDATE signals SET entries=?, tps=?, status=?, outcome=?, exit_price=?, closed_at=?, pnl_r=? WHERE id=?')
    .run(JSON.stringify(zone.entries), JSON.stringify(zone.tps), zone.status,
         zone.outcome || 'open', zone.exit_price || null, zone.closed_at || null,
         zone.pnl_r ?? null, zone.id);
}

// ════════════════════════════════════════════════════════════════════
// ZONE GENERATION — runs 15 min before each session open
// ════════════════════════════════════════════════════════════════════
async function buildZonesForSession(sessionKey) {
  try {
    const price = getPrice();
    if (!price) { console.log('🚫 Zone gen: no live price'); return; }

    const m15 = await fetchCandles('15min', 220); // newest-first
    const h1  = await fetchCandles('1h', 200);
    if (!m15 || m15.length < 60 || !h1 || h1.length < 60) {
      console.log('🚫 Zone gen: not enough candles'); return;
    }

    const atr = atr14(m15);
    const rsi = rsi14(m15.map(c => c.close));
    const dailyRange = Math.max(...m15.slice(0, 96).map(c => c.high)) -
                       Math.min(...m15.slice(0, 96).map(c => c.low));

    // ── Candidate levels ──
    const chronoH1  = [...h1].reverse();
    const chronoM15 = [...m15].reverse();
    const hp = findPivots(chronoH1, 4, 3);
    const mp = findPivots(chronoM15, 5, 3);

    // Previous day high/low from H1
    const dayKey = (ts) => new Date(ts).toISOString().slice(0, 10);
    const today = dayKey(Date.now());
    const prevDayCandles = chronoH1.filter(c => dayKey(c.timestamp) !== today).slice(-24);
    const pdh = prevDayCandles.length ? Math.max(...prevDayCandles.map(c => c.high)) : null;
    const pdl = prevDayCandles.length ? Math.min(...prevDayCandles.map(c => c.low)) : null;

    // Asia range (00–08 UTC today) for london/ny zones
    const asiaCandles = chronoM15.filter(c => {
      const d = new Date(c.timestamp);
      return dayKey(c.timestamp) === today && d.getUTCHours() < 8;
    });
    const asiaHigh = asiaCandles.length ? Math.max(...asiaCandles.map(c => c.high)) : null;
    const asiaLow  = asiaCandles.length ? Math.min(...asiaCandles.map(c => c.low)) : null;

    const candidates = new Map(); // levelPrice(rounded2) -> {price, sources:Set}
    const addLevel = (p, source) => {
      if (!p || !isFinite(p)) return;
      // merge with existing level within 0.3*ATR
      for (const [, lv] of candidates) {
        if (Math.abs(lv.price - p) <= atr * TOUCH_TOL_ATR) { lv.sources.add(source); return; }
      }
      candidates.set(round2(p), { price: p, sources: new Set([source]) });
    };

    hp.highs.slice(-12).forEach(p => addLevel(p.price, 'h1_swing'));
    hp.lows.slice(-12).forEach(p => addLevel(p.price, 'h1_swing'));
    mp.highs.slice(-15).forEach(p => addLevel(p.price, 'm15_pivot'));
    mp.lows.slice(-15).forEach(p => addLevel(p.price, 'm15_pivot'));
    addLevel(pdh, 'prev_day'); addLevel(pdl, 'prev_day');
    if (sessionKey !== 'tokyo') { addLevel(asiaHigh, 'asia_range'); addLevel(asiaLow, 'asia_range'); }
    // Round numbers within ±1.5 × daily range
    const span = Math.max(dailyRange * 1.5, atr * 4);
    for (let r = nearestRound(price - span); r <= price + span; r += 10) addLevel(r, 'round');

    // ── Scoring ──
    const recent48h = m15.filter(c => c.timestamp > Date.now() - 48 * 3600000);
    const scored = [];
    for (const [, lv] of candidates) {
      const dist = Math.abs(lv.price - price);
      if (dist < atr * MIN_DIST_ATR || dist > atr * MAX_DIST_ATR) continue;

      const action = lv.price > price ? 'SELL' : 'BUY';
      let score = 0;
      const reasons = [];

      // Touches in last 48h
      const touches = recent48h.filter(c =>
        action === 'SELL'
          ? Math.abs(c.high - lv.price) <= atr * TOUCH_TOL_ATR
          : Math.abs(c.low - lv.price) <= atr * TOUCH_TOL_ATR
      ).length;
      if (touches >= 2) { score += 25; reasons.push(`${touches} touches`); }
      else if (touches === 1) { score += 10; reasons.push('1 touch'); }

      // MTF confluence: H1 swing agreeing with an M15 pivot/level
      if (lv.sources.has('h1_swing') && (lv.sources.has('m15_pivot') || lv.sources.has('round'))) {
        score += 20; reasons.push('MTF confluence');
      } else if (lv.sources.has('h1_swing') || lv.sources.has('prev_day') || lv.sources.has('asia_range')) {
        score += 12; reasons.push([...lv.sources][0]);
      }

      // Round level in zone
      if (lv.sources.has('round') || Math.abs(lv.price - nearestRound(lv.price)) <= SNAP_TOL) {
        score += 15; reasons.push('round level');
      }

      // RSI context
      if ((action === 'SELL' && rsi > 60) || (action === 'BUY' && rsi < 40)) {
        score += 15; reasons.push(`RSI ${rsi}`);
      }

      // Reachable-but-not-immediate distance band sweet spot
      if (dist >= atr * 0.8 && dist <= atr * 2.0) { score += 10; reasons.push('clean distance'); }

      console.log(`  🔬 [${sessionKey}] ${action} ${lv.price.toFixed(1)} → ${score}pts (${reasons.join(', ') || 'no factors'}) dist ${(dist/atr).toFixed(1)}ATR`);
      if (score < MIN_CONFIDENCE) continue;
      scored.push({ level: lv.price, action, score: Math.min(score, 95), reasons, dist });
    }

    scored.sort((a, b) => b.score - a.score);

    // Max 1 zone per direction, max MAX_ZONES total, levels not overlapping
    const picked = [];
    for (const s of scored) {
      if (picked.length >= MAX_ZONES) break;
      if (picked.some(p => Math.abs(p.level - s.level) < atr * 1.2)) continue;
      if (picked.filter(p => p.action === s.action).length >= 2) continue;
      picked.push(s);
    }

    if (!picked.length) { console.log(`🎯 Zone gen [${sessionKey}]: no qualifying levels`); return; }

    for (const z of picked) await insertZone(z, sessionKey, atr, m15, hp);
  } catch (e) {
    console.error('Zone gen error:', e.message);
  }
}

async function insertZone(z, sessionKey, atr, m15, h1Pivots) {
  const sell = z.action === 'SELL';
  const dir = sell ? 1 : -1;

  const entry1 = round2(z.level);
  const entry2 = round2(z.level + dir * ENTRY2_ATR * atr);
  let sl = entry2 + dir * SL_BUFFER_ATR * atr;
  // SL snapped OUTWARD to the next round number if close (never tighter)
  const slRound = nearestRound(sl);
  if (sell && slRound > sl && slRound - sl <= SNAP_TOL) sl = slRound;
  if (!sell && slRound < sl && sl - slRound <= SNAP_TOL) sl = slRound;
  sl = round2(sl);

  const avgEntry = (entry1 + entry2) / 2;
  const risk = Math.abs(avgEntry - sl);

  // TP ladder
  const chronoM15 = [...m15].reverse();
  const mp = findPivots(chronoM15, 5, 3);
  const swings = sell ? mp.lows : mp.highs;
  const nextSwing = swings
    .map(s => s.price)
    .filter(p => sell ? p < entry1 - atr * 0.6 : p > entry1 + atr * 0.6)
    .sort((a, b) => sell ? b - a : a - b)[0];
  const htfLevels = (sell ? h1Pivots.lows : h1Pivots.highs)
    .map(s => s.price)
    .filter(p => sell ? p < entry1 - atr * 1.5 : p > entry1 + atr * 1.5)
    .sort((a, b) => sell ? b - a : a - b)[0];

  let raw = [
    entry1 - dir * 0.5 * atr,                 // TP1 quick bank
    entry1 - dir * 1.0 * atr,                 // TP2
    nextSwing ?? (entry1 - dir * 1.6 * atr),  // TP3 swing
    avgEntry - dir * 1.5 * risk,              // TP4 measured move 1.5R
    htfLevels ?? (entry1 - dir * 3.0 * atr),  // TP5 HTF level
  ].map(p => snapToRound(p, sell));

  // sort by distance, dedupe (< 0.3*ATR apart)
  raw = raw
    .filter(p => sell ? p < entry1 : p > entry1)
    .sort((a, b) => sell ? b - a : a - b)
    .filter((p, i, arr) => i === 0 || Math.abs(p - arr[i - 1]) >= atr * TOUCH_TOL_ATR);

  if (raw.length < 3) {
    console.log(`🚫 Zone [${sessionKey}] ${z.action} ${entry1} skipped — only ${raw.length} valid TPs`);
    return;
  }

  const tps = raw.slice(0, 5).map((p, i) => ({ level: i + 1, price: round2(p), status: 'open', hit_at: null }));
  const entries = [
    { price: entry1, kind: 'limit', status: 'pending', filled_at: null },
    { price: entry2, kind: 'limit', status: 'pending', filled_at: null },
  ];

  const record = {
    ticker: 'XAU/USD',
    action: z.action,
    price: entry1, sl, tp1: tps[0].price, tp2: tps[1]?.price ?? null, // legacy mirror
    timeframe: '15m',
    confidence: z.score,
    fib_level: null,
    pattern: 'Limit Zone',
    strategy: 'Zone',
    note: z.reasons.slice(0, 3).join(' | '),
    rsi: null, atr: round2(atr),
    current_price: getPrice(),
    entry_valid_for: null,
    mtf: JSON.stringify({ h1: true, h4: false, d1: false }),
    timestamp: Date.now(),
    kind: 'zone', status: 'active', session: sessionKey,
    entries: JSON.stringify(entries), tps: JSON.stringify(tps),
  };

  db.prepare(`INSERT INTO signals
    (ticker,action,price,sl,tp1,tp2,timeframe,confidence,fib_level,pattern,strategy,note,rsi,atr,current_price,entry_valid_for,mtf,timestamp,outcome,kind,status,session,entries,tps)
    VALUES (@ticker,@action,@price,@sl,@tp1,@tp2,@timeframe,@confidence,@fib_level,@pattern,@strategy,@note,@rsi,@atr,@current_price,@entry_valid_for,@mtf,@timestamp,'open',@kind,@status,@session,@entries,@tps)`
  ).run(record);

  const id = db.prepare('SELECT last_insert_rowid() as id').get().id;
  const zone = { ...record, id, entries, tps };
  console.log(`🎯 ZONE [${sessionKey}] ${z.action} LIMIT ${entry1}+${entry2} SL ${sl} TPs [${tps.map(t => t.price).join(', ')}] conf ${z.score}%`);
  await sendZonePush(zone, 'zone_ready');
}

// ════════════════════════════════════════════════════════════════════
// LIFECYCLE WATCHER — fills, TP hits, SL, expiry. SL NEVER MOVES.
// ════════════════════════════════════════════════════════════════════
async function watchZones() {
  try {
    const zones = loadActiveZones();
    if (!zones.length) return;

    const m5 = await fetchCandles('5min', 60); // newest-first, for wick-accurate checks
    const price = getPrice();
    if (!m5 || !m5.length) return;

    for (const zone of zones) {
      const sell = zone.action === 'SELL';

      // Watermark: candle-time anchor set on first watcher contact.
      // Immune to any timezone offset between candle datetimes and server clock.
      if (!zone.watch_from) {
        const newest = m5.reduce((a, c) => Math.max(a, c.timestamp), 0);
        db.prepare('UPDATE signals SET watch_from=? WHERE id=?').run(newest, zone.id);
        console.log(`🔖 Zone #${zone.id} watermark set @ candle ${new Date(newest).toISOString()}`);
        continue; // start evaluating from next cycle
      }
      const sinceZone = m5.filter(c => c.timestamp > zone.watch_from);
      const candles = sinceZone.length ? sinceZone : (price ? [{ high: price, low: price, close: price, timestamp: Date.now() }] : []);
      if (!candles.length) continue;

      const hi = Math.max(...candles.map(c => c.high));
      const lo = Math.min(...candles.map(c => c.low));
      let changed = false;

      // 1) Entry fills (limit logic: SELL fills when price trades AT/ABOVE entry)
      for (const e of zone.entries) {
        if (e.status !== 'pending') continue;
        const filled = sell ? hi >= e.price : lo <= e.price;
        if (filled) {
          e.status = 'filled'; e.filled_at = new Date().toISOString();
          zone.status = 'filled';
          changed = true;
          console.log(`✅ Zone #${zone.id} entry filled @ ${e.price}`);
          await sendZonePush(zone, 'entry_filled', { price: e.price });
        }
      }

      const anyFilled = zone.entries.some(e => e.status === 'filled');

      if (anyFilled) {
        // 2) SL check first within same window? Pessimistic like your tracker:
        const slHit = sell ? hi >= zone.sl : lo <= zone.sl;

        // 3) TP hits (only count TPs beyond what price actually reached)
        for (const tp of zone.tps) {
          if (tp.status === 'hit') continue;
          const hit = sell ? lo <= tp.price : hi >= tp.price;
          if (hit && !(slHit && tp.level >= 3)) { // pessimistic: deep TPs don't count if SL also swept
            tp.status = 'hit'; tp.hit_at = new Date().toISOString();
            changed = true;
            console.log(`💰 Zone #${zone.id} TP${tp.level} hit @ ${tp.price}`);
            await sendZonePush(zone, 'tp_hit', { level: tp.level, price: tp.price });
          }
        }

        if (slHit) {
          zone.status = 'closed';
          zone.outcome = zone.tps.some(t => t.status === 'hit') ? 'tp1_hit' : 'sl_hit';
          zone.exit_price = zone.sl; zone.closed_at = Date.now();
          // PnL in R with default 40/20/20/10/10 weights (informational)
          const weights = [40, 20, 20, 10, 10];
          const avgEntry = (zone.entries[0].price + (zone.entries[1]?.price ?? zone.entries[0].price)) / 2;
          const risk = Math.abs(avgEntry - zone.sl);
          let pnl = 0, remaining = 100;
          zone.tps.forEach((t, i) => {
            if (t.status === 'hit') {
              const w = weights[i] ?? 0; remaining -= w;
              pnl += (Math.abs(t.price - avgEntry) / risk) * (w / 100);
            }
          });
          pnl -= remaining / 100; // rest stopped at original SL = -1R on remainder
          zone.pnl_r = Math.round(pnl * 100) / 100;
          changed = true;
          console.log(`🛑 Zone #${zone.id} SL hit — closed, pnl ${zone.pnl_r}R (SL never moved)`);
          await sendZonePush(zone, 'sl_hit');
        } else if (zone.tps.every(t => t.status === 'hit')) {
          zone.status = 'closed'; zone.outcome = 'tp2_hit';
          zone.exit_price = zone.tps[zone.tps.length - 1].price; zone.closed_at = Date.now();
          const avgEntry = (zone.entries[0].price + (zone.entries[1]?.price ?? zone.entries[0].price)) / 2;
          const risk = Math.abs(avgEntry - zone.sl);
          const weights = [40, 20, 20, 10, 10];
          zone.pnl_r = Math.round(zone.tps.reduce((s, t, i) =>
            s + (Math.abs(t.price - avgEntry) / risk) * ((weights[i] ?? 0) / 100), 0) * 100) / 100;
          changed = true;
          console.log(`🏆 Zone #${zone.id} ALL TPs hit — closed, pnl ${zone.pnl_r}R`);
        }
      } else {
        // 4) Expiry: session over (close hour + 1) without fill, or 24h TTL
        const sess = SESSIONS.find(s => s.key === zone.session);
        const nowH = new Date().getUTCHours();
        const ageH = (Date.now() - zone.timestamp) / 3600000;
        const sessionOver = sess && ageH > 1 && (
          (sess.close >= sess.open && (nowH >= sess.close + 1 || nowH < sess.open && ageH > 4)) ||
          ageH > ZONE_TTL_HOURS
        );
        if (ageH > ZONE_TTL_HOURS || sessionOver) {
          zone.status = 'cancelled'; zone.outcome = 'expired'; zone.closed_at = Date.now();
          changed = true;
          console.log(`⏰ Zone #${zone.id} cancelled (no fill, session over)`);
        }
      }

      if (changed) saveZoneState(zone);
    }
  } catch (e) {
    console.error('Zone watcher error:', e.message);
  }
}

// ── Session cron: fires zone generation at genH:genM UTC, once/day ────
const lastGen = {};
function sessionCronTick() {
  const now = new Date();
  const h = now.getUTCHours(), m = now.getUTCMinutes();
  const today = now.toISOString().slice(0, 10);
  for (const s of SESSIONS) {
    if (h === s.genH && m >= s.genM && m < s.genM + 5 && lastGen[s.key] !== today) {
      lastGen[s.key] = today;
      console.log(`⏰ Session cron: generating ${s.key} zones`);
      buildZonesForSession(s.key);
    }
  }
}

// ── Public API ────────────────────────────────────────────────────────
function init(deps) {
  db = deps.db; admin = deps.admin; fetchCandles = deps.fetchCandles; getPrice = deps.getPrice;
  migrate();

  setInterval(sessionCronTick, 60 * 1000);
  setInterval(watchZones, 60 * 1000);
  watchZones();

  // Debug/ops routes
  if (deps.app) {
    deps.app.get('/zones', (req, res) => {
      const rows = db.prepare(
        "SELECT * FROM signals WHERE kind='zone' ORDER BY timestamp DESC LIMIT ?"
      ).all(parseInt(req.query.limit) || 20);
      res.json(rows.map(rowToZone));
    });
    deps.app.get('/reset-zones', (req, res) => {
      const n = db.prepare("DELETE FROM signals WHERE kind='zone'").run().changes;
      console.log(`🧹 reset-zones: ${n} zone rows deleted`);
      res.json({ deleted: n });
    });

    deps.app.get('/zone-debug', async (req, res) => {
      const price = getPrice();
      const m15 = await fetchCandles('15min', 220);
      const atr = m15 && m15.length >= 20 ? atr14(m15) : null;
      const rsi = m15 ? rsi14(m15.map(c => c.close)) : null;
      res.json({
        price, atr, rsi,
        minConfidence: MIN_CONFIDENCE,
        distWindow: atr ? [round2(atr * MIN_DIST_ATR), round2(atr * MAX_DIST_ATR)] : null,
        hint: 'Call /force-zones and check Railway logs for per-level 🔬 scoring lines',
      });
    });

    deps.app.get('/force-zones', async (req, res) => {
      const session = ['tokyo', 'london', 'ny'].includes(req.query.session) ? req.query.session : 'london';
      await buildZonesForSession(session);
      const rows = db.prepare("SELECT * FROM signals WHERE kind='zone' AND session=? ORDER BY timestamp DESC LIMIT 5").all(session);
      res.json({ session, zones: rows.map(rowToZone) });
    });
  }

  console.log('🎯 Zone Engine v1 ready · sessions: Tokyo 23:45 / London 06:45 / NY 11:45 UTC · SL never moves');
}

module.exports = { init, buildZonesForSession, watchZones };
