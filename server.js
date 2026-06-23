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
  
  const emoji = signal.action === 'BUY' ? '🟢' : '🔴';
  const direction = signal.action === 'BUY' ? '▲' : '▼';
  
  // Title: Direction + Action + Symbol + Price
  const title = `${emoji} ${direction} ${signal.action} XAU/USD @ $${Number(signal.price).toFixed(2)}`;
  
  // Body: Strategy + Confidence + TP/SL info  
  const parts = [];
  if (signal.strategy) {
    // Shorten strategy names for push
    const shortStrategy = signal.strategy
      .replace('Setup 2 Breakout+Retest', 'Breakout+Retest')
      .replace('Setup 3 Double Top', 'Double Top')
      .replace('Setup 3 Double Bottom', 'Double Bottom')
      .replace('FIB', 'FIB Pullback');
    parts.push(shortStrategy);
  }
  if (signal.fib_level) parts.push(signal.fib_level);
  if (signal.confidence) parts.push(`${signal.confidence}% conf`);
  if (signal.tp1) parts.push(`TP $${Number(signal.tp1).toFixed(2)}`);
  
  const body = parts.join(' · ');
  
  // Sub-title for iOS (shows on lockscreen)
  const subtitle = signal.pattern && signal.pattern !== 'No pattern' 
    ? signal.pattern.replace(' (Liquidity Sweep Confirmed)', ' ✓')
    : null;
  
  const payload = {
    topic: 'signals',
    notification: { title, body },
    data: { 
      signal_id: String(signal.id || ''),
      action: signal.action || '',
      price: String(signal.price || ''),
      sl: String(signal.sl || ''),
      tp1: String(signal.tp1 || ''),
      tp2: String(signal.tp2 || ''),
      strategy: signal.strategy || '',
      confidence: String(signal.confidence || 0),
      timeframe: signal.timeframe || '',
      fib_level: signal.fib_level || '',
      pattern: signal.pattern || '',
    },
    apns: { 
      payload: { 
        aps: { 
          sound: 'default', 
          badge: 1,
          //'mutable-content': 1,
          alert: subtitle ? { title, subtitle, body } : { title, body },
        } 
      }, 
      headers: { 'apns-priority': '10' } 
    },
    android: { 
      priority: 'high', 
      notification: { 
        title, 
        body, 
        channelId: 'pulstrade_signals', 
        priority: 'max',
        defaultSound: true,
        defaultVibrateTimings: true,
        color: signal.action === 'BUY' ? '#10B981' : '#EF4444',
      } 
    },
  };
  
  admin.messaging().send(payload)
    .then(() => console.log(`✓ Push sent: ${title}`))
    .catch(e => console.error('Push error:', e.message));
}

// ════════════════════════════════════════════════════════════════════
// TELEGRAM SIGNAL BROADCASTER (A+ SETUPS ONLY)
// ════════════════════════════════════════════════════════════════════

const TELEGRAM_BOT_TOKEN  = process.env.TELEGRAM_BOT_TOKEN  || '';
const TELEGRAM_CHANNEL_ID = process.env.TELEGRAM_CHANNEL_ID || '';
const TELEGRAM_MIN_CONFIDENCE = parseInt(process.env.TELEGRAM_MIN_CONFIDENCE || '75', 10);
const APP_STORE_URL = 'https://apps.apple.com/de/app/pulstrade/id6762018999';

async function sendTelegramSignal(signal, retryCount = 0) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHANNEL_ID) {
    return;
  }

  if ((signal.confidence || 0) < TELEGRAM_MIN_CONFIDENCE) {
    console.log(`📵 Telegram skipped: ${signal.action} ${signal.timeframe} confidence ${signal.confidence}% < ${TELEGRAM_MIN_CONFIDENCE}% (App-only signal)`);
    return;
  }

  try {
    const directionEmoji = signal.action === 'BUY' ? '🟢' : '🔴';
    const arrow          = signal.action === 'BUY' ? '▲' : '▼';
    const setupGrade     = signal.confidence >= 78 ? 'A+' : 'A';

    const entry = Number(signal.price);
    const sl    = Number(signal.sl);
    const tp1   = Number(signal.tp1);
    const tp2   = Number(signal.tp2);
    const risk  = Math.abs(entry - sl);
    const rr1   = risk > 0 ? (Math.abs(tp1 - entry) / risk).toFixed(2) : '—';
    const rr2   = risk > 0 ? (Math.abs(tp2 - entry) / risk).toFixed(2) : '—';

    const strategyShort = (signal.strategy || '')
      .replace('Setup 2 Breakout+Retest', 'Breakout+Retest')
      .replace('Setup 3 Double Top', 'Double Top')
      .replace('Setup 3 Double Bottom', 'Double Bottom')
      .replace('FIB', 'FIB Pullback');

    const lines = [
      `<b>🥇 ${setupGrade} SETUP · ${signal.confidence}% CONFIDENCE</b>`,
      ``,
      `<b>${directionEmoji} ${arrow} ${signal.action} XAU/USD</b>`,
      `<i>${strategyShort}${signal.fib_level ? ' · FIB ' + signal.fib_level : ''} · ${signal.timeframe}</i>`,
      ``,
      `📍 <b>Entry:</b>     <code>$${entry.toFixed(2)}</code>`,
      `🛡️ <b>Stop Loss:</b> <code>$${sl.toFixed(2)}</code>`,
      `🎯 <b>Take Profit 1:</b> <code>$${tp1.toFixed(2)}</code>  <i>(${rr1}R)</i>`,
      `🎯 <b>Take Profit 2:</b> <code>$${tp2.toFixed(2)}</code>  <i>(${rr2}R)</i>`,
    ];

    if (signal.pattern && signal.pattern !== 'No pattern') {
      lines.push(``);
      lines.push(`📊 <b>Pattern:</b> ${signal.pattern}`);
    }
    if (signal.rsi) {
      lines.push(`📈 <b>RSI:</b> ${Number(signal.rsi).toFixed(1)}`);
    }

    lines.push(``);
    lines.push(`⚡ <i>Want push notifications + auto-trading?</i>`);
    lines.push(`<a href="${APP_STORE_URL}">📱 Get Pulstrade App — 7-day free trial</a>`);

    const message = lines.join('\n');

    const response = await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        chat_id: TELEGRAM_CHANNEL_ID,
        text: message,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      },
      { timeout: 10000 }
    );

    const messageId = response.data?.result?.message_id;
    console.log(`✅ Telegram posted: ${signal.action} ${signal.timeframe} ${signal.confidence}% (msg #${messageId})`);

    if (messageId && signal.id) {
      try {
        db.prepare('UPDATE signals SET telegram_msg_id = ? WHERE id = ?').run(messageId, signal.id);
      } catch (e) {
        // Column may not exist yet — silently skip
      }
    }

    return { success: true, messageId };

  } catch (error) {
    const status = error.response?.status;
    const errMsg = error.response?.data?.description || error.message;
    console.error(`❌ Telegram error [${status}]:`, errMsg);

    if (retryCount < 2 && (!status || status >= 500)) {
      await new Promise(r => setTimeout(r, 2000 * (retryCount + 1)));
      return sendTelegramSignal(signal, retryCount + 1);
    }
    return { success: false, error: errMsg };
  }
}

async function updateTelegramOutcome(signal, outcome) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHANNEL_ID) return;
  if (!signal.telegram_msg_id) return;

  try {
    const emoji = outcome === 'tp1_hit' || outcome === 'tp2_hit' ? '✅' : '❌';
    const label = outcome === 'tp1_hit' ? 'TP1 HIT' :
                  outcome === 'tp2_hit' ? 'TP2 HIT' :
                  outcome === 'sl_hit'  ? 'SL HIT'  : 'CLOSED';
    const pnl = signal.pnl_r ? `${signal.pnl_r > 0 ? '+' : ''}${signal.pnl_r}R` : '';

    const reply = `${emoji} <b>${label}</b> · ${pnl}\nClosed at <code>$${Number(signal.exit_price).toFixed(2)}</code>`;

    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        chat_id: TELEGRAM_CHANNEL_ID,
        text: reply,
        parse_mode: 'HTML',
        reply_to_message_id: signal.telegram_msg_id,
        disable_notification: false,
      },
      { timeout: 10000 }
    );
    console.log(`✅ Telegram outcome posted for signal #${signal.id}: ${label}`);
  } catch (e) {
    console.error('Telegram outcome error:', e.response?.data?.description || e.message);
  }
}

const app  = express();
const PORT = process.env.PORT || 3000;
const TWELVE_API_KEY = process.env.TWELVE_DATA_API_KEY || '';
const NEWS_API_KEY   = process.env.NEWS_API_KEY || '4ada05cd8e1d46f2bbb4f011506044fd';
const METAAPI_TOKEN  = process.env.METAAPI_TOKEN || '';
const METAAPI_URL    = 'https://mt-client-api-v1.london.agiliumtrade.ai';
const TICKER = 'XAU/USD';

// ════════════════════════════════════════════════════════════════════
// TRUMP X / TRUTH SOCIAL MONITOR (with AI Sentiment Analysis)
// ════════════════════════════════════════════════════════════════════

const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY || '';
const TRUTHSOCIAL_USER     = 'realDonaldTrump';
const TWITTER_USERNAME     = 'realDonaldTrump';
const TRUMP_POLL_INTERVAL  = 60 * 1000; // 60 seconds
const TRUMP_ENABLED        = process.env.TRUMP_MONITOR_ENABLED !== 'false'; // default ON

const seenTrumpPosts = new Set();

// ── 1. Truth Social Polling (RSS) ───────────────────────────
async function pollTruthSocial() {
  try {
    const url = `https://truthsocial.com/users/${TRUTHSOCIAL_USER}/statuses.rss`;
    const r = await axios.get(url, {
      timeout: 8000,
      headers: { 'User-Agent': 'Mozilla/5.0 PulstradeBot/1.0' }
    });
    if (!r.data) return [];

    const items = [];
    const itemBlocks = r.data.split(/<item>/).slice(1);
    for (const block of itemBlocks) {
      const closeIdx = block.indexOf('</item>');
      if (closeIdx === -1) continue;
      const item = block.substring(0, closeIdx);

      const titleMatch = item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s) ||
                         item.match(/<title>(.*?)<\/title>/s);
      const linkMatch  = item.match(/<link>(.*?)<\/link>/s);
      const dateMatch  = item.match(/<pubDate>(.*?)<\/pubDate>/s);
      const guidMatch  = item.match(/<guid[^>]*>(.*?)<\/guid>/s);
      const descMatch  = item.match(/<description><!\[CDATA\[(.*?)\]\]><\/description>/s) ||
                         item.match(/<description>(.*?)<\/description>/s);

      const text = (titleMatch?.[1] || descMatch?.[1] || '')
        .replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').trim();
      if (!text) continue;

      items.push({
        id: 'ts_' + (guidMatch?.[1] || linkMatch?.[1] || '').split('/').pop(),
        platform: 'truth_social',
        text: text.substring(0, 500),
        url: linkMatch?.[1] || '',
        posted_at: dateMatch?.[1] ? new Date(dateMatch[1]).getTime() : Date.now(),
      });
    }
    return items;
  } catch(e) {
    if (e.response?.status !== 404) {
      console.error('TruthSocial poll error:', e.message);
    }
    return [];
  }
}

// ── 2. Twitter/X Polling (via Nitter mirror) ────────────────
async function pollTwitter() {
  try {
    const instances = [
      'https://nitter.net',
      'https://nitter.privacydev.net',
      'https://nitter.poast.org',
    ];

    for (const base of instances) {
      try {
        const url = `${base}/${TWITTER_USERNAME}/rss`;
        const r = await axios.get(url, {
          timeout: 6000,
          headers: { 'User-Agent': 'Mozilla/5.0 PulstradeBot/1.0' }
        });
        if (!r.data) continue;

        const items = [];
        const itemBlocks = r.data.split(/<item>/).slice(1);
        for (const block of itemBlocks) {
          const closeIdx = block.indexOf('</item>');
          if (closeIdx === -1) continue;
          const item = block.substring(0, closeIdx);

          const titleMatch = item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s) ||
                             item.match(/<title>(.*?)<\/title>/s);
          const linkMatch  = item.match(/<link>(.*?)<\/link>/s);
          const dateMatch  = item.match(/<pubDate>(.*?)<\/pubDate>/s);
          const guidMatch  = item.match(/<guid[^>]*>(.*?)<\/guid>/s);

          const text = (titleMatch?.[1] || '')
            .replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').trim();
          if (!text || text.startsWith('RT by') || text.startsWith('R to ')) continue;

          items.push({
            id: 'tw_' + (guidMatch?.[1] || linkMatch?.[1] || '').split('/').pop(),
            platform: 'twitter',
            text: text.substring(0, 500),
            url: linkMatch?.[1] || '',
            posted_at: dateMatch?.[1] ? new Date(dateMatch[1]).getTime() : Date.now(),
          });
        }
        return items;
      } catch(e) { /* try next instance */ }
    }
    return [];
  } catch(e) {
    console.error('Twitter poll error:', e.message);
    return [];
  }
}

// ── 3. AI SENTIMENT ANALYSIS (Claude API) ───────────────────
async function analyzeTrumpPostForGold(text) {
  if (!ANTHROPIC_API_KEY) {
    return simpleSentimentFallback(text);
  }

  try {
    const prompt = `You are a gold-market analyst. Analyze this post by Donald Trump and predict its IMMEDIATE impact on XAU/USD (Gold) price within the next 30-60 minutes.

POST: "${text}"

Consider:
- Iran/Middle East mentions → BULLISH gold (safe haven)
- Iran de-escalation/peace → BEARISH gold (risk-on)
- Tariffs/trade war → BULLISH gold (uncertainty)
- Fed criticism / rate cut pressure → BULLISH gold (weak dollar)
- Dollar strength comments → BEARISH gold
- Nothing market-relevant → NEUTRAL

Respond in STRICT JSON format ONLY (no other text):
{"impact":"HIGH|MEDIUM|LOW|NONE","direction":"BULLISH|BEARISH|NEUTRAL","confidence":0.0-1.0,"reason":"max 80 chars","expected_move_pct":0.0-3.0}

If post is unrelated to gold/dollar/economy/geopolitics → return impact:"NONE"`;

    const r = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 200,
      messages: [{ role: 'user', content: prompt }]
    }, {
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      timeout: 15000
    });

    const responseText = r.data?.content?.[0]?.text || '';
    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON in Claude response');

    const result = JSON.parse(jsonMatch[0]);
    return {
      impact: result.impact || 'NONE',
      direction: result.direction || 'NEUTRAL',
      confidence: Math.max(0, Math.min(1, parseFloat(result.confidence) || 0)),
      reason: (result.reason || '').substring(0, 100),
      expected_move_pct: Math.max(0, Math.min(5, parseFloat(result.expected_move_pct) || 0))
    };
  } catch(e) {
    console.error('Claude sentiment error:', e.response?.data?.error?.message || e.message);
    return simpleSentimentFallback(text);
  }
}

function simpleSentimentFallback(text) {
  const lower = text.toLowerCase();
  let direction = 'NEUTRAL';
  let impact = 'NONE';
  const reasons = [];

  const bullish = ['iran', 'tariff', 'trade war', 'fed cut', 'dollar weak', 'china war', 'sanctions', 'inflation'];
  const bearish = ['peace', 'ceasefire', 'deal', 'agreement', 'rate hike', 'strong dollar', 'de-escalat'];

  for (const kw of bullish) if (lower.includes(kw)) { direction = 'BULLISH'; reasons.push(kw); }
  for (const kw of bearish) if (lower.includes(kw)) { direction = 'BEARISH'; reasons.push(kw); }

  if (reasons.length >= 2) impact = 'HIGH';
  else if (reasons.length === 1) impact = 'MEDIUM';
  else impact = 'NONE';

  return {
    impact, direction,
    confidence: reasons.length ? 0.6 : 0.3,
    reason: reasons.length ? `Keywords: ${reasons.slice(0,3).join(', ')}` : 'No relevant keywords',
    expected_move_pct: impact === 'HIGH' ? 1.0 : impact === 'MEDIUM' ? 0.5 : 0
  };
}

// ── 4. SEND TRUMP ALERT to Telegram + Push ──────────────────
async function sendTrumpAlert(post, analysis) {
  if (analysis.impact === 'NONE' || analysis.impact === 'LOW') {
    console.log(`📵 Trump alert skipped (impact ${analysis.impact}): "${post.text.substring(0,60)}..."`);
    return;
  }

  const arrow = analysis.direction === 'BULLISH' ? '🟢⬆️' :
                analysis.direction === 'BEARISH' ? '🔴⬇️' : '⚪';
  const platformIcon = post.platform === 'truth_social' ? '📢' : '🐦';
  const impactEmoji = analysis.impact === 'HIGH' ? '🚨' : '⚠️';
  const confPct = Math.round(analysis.confidence * 100);

  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHANNEL_ID) {
    try {
      const tgMsg = [
        `${impactEmoji} <b>TRUMP ALERT — Gold Impact</b>`,
        ``,
        `${platformIcon} <i>${post.platform === 'truth_social' ? 'Truth Social' : 'X/Twitter'} · just now</i>`,
        ``,
        `${arrow} <b>${analysis.direction} for Gold</b>`,
        `<b>Impact:</b> ${analysis.impact} · <b>Confidence:</b> ${confPct}%`,
        `<b>Expected move:</b> ${analysis.expected_move_pct > 0 ? '~' + analysis.expected_move_pct.toFixed(1) + '%' : 'unclear'}`,
        ``,
        `💬 <b>Why:</b> ${analysis.reason}`,
        ``,
        `<blockquote>${post.text.substring(0, 300)}${post.text.length > 300 ? '...' : ''}</blockquote>`,
        ``,
        post.url ? `<a href="${post.url}">View original post →</a>` : '',
        ``,
        `📱 <i>Get real-time alerts in the Pulstrade App</i>`,
        `<a href="https://apps.apple.com/de/app/pulstrade/id6762018999">Open App →</a>`,
      ].filter(Boolean).join('\n');

      await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        {
          chat_id: TELEGRAM_CHANNEL_ID,
          text: tgMsg,
          parse_mode: 'HTML',
          disable_web_page_preview: true
        },
        { timeout: 10000 }
      );
      console.log(`✅ Trump alert sent to Telegram: ${analysis.direction} (${analysis.impact})`);
    } catch(e) {
      console.error('Trump→Telegram error:', e.response?.data?.description || e.message);
    }
  }

  try {
    const pushTitle = `${impactEmoji} TRUMP ALERT · Gold ${analysis.direction}`;
    const pushBody  = `${analysis.reason} · ${confPct}% confidence`;
    admin.messaging().send({
      topic: 'all_users',
      notification: { title: pushTitle, body: pushBody },
      data: {
        type: 'trump_alert',
        platform: post.platform,
        direction: analysis.direction,
        impact: analysis.impact,
        url: post.url || '',
      },
      android: { priority: 'high' },
      apns: { headers: { 'apns-priority': '10' } }
    }).then(() => console.log(`✅ Trump alert push sent`))
      .catch(e => console.error('Trump push:', e.message));
  } catch(e) { console.error('Push setup:', e.message); }

  try {
    db.prepare(`UPDATE trump_events SET notified = 1 WHERE id = ?`).run(post.id);
  } catch(e) {}
}

// ── 5. MAIN POLL LOOP ───────────────────────────────────────
async function pollTrumpAndAnalyze() {
  if (!TRUMP_ENABLED) return;
  try {
    const [tsP, twP] = await Promise.all([pollTruthSocial(), pollTwitter()]);
    const allPosts = [...tsP, ...twP].sort((a, b) => a.posted_at - b.posted_at);

    const newPosts = allPosts.filter(p => p.id && !seenTrumpPosts.has(p.id));

    if (newPosts.length === 0) return;
    console.log(`🔍 Trump Monitor: ${newPosts.length} new post(s) detected`);

    for (const post of newPosts) {
      seenTrumpPosts.add(post.id);

      if (post.posted_at < Date.now() - 30*60*1000) {
        console.log(`⏭️  Trump post too old (${Math.round((Date.now()-post.posted_at)/60000)} min ago), skipping`);
        try {
          db.prepare(`INSERT OR IGNORE INTO trump_events
            (id, platform, text, url, posted_at, detected_at, impact)
            VALUES (?, ?, ?, ?, ?, ?, 'OLD')`)
            .run(post.id, post.platform, post.text, post.url, post.posted_at, Date.now());
        } catch(e) {}
        continue;
      }

      const analysis = await analyzeTrumpPostForGold(post.text);

      try {
        db.prepare(`INSERT OR IGNORE INTO trump_events
          (id, platform, text, url, posted_at, detected_at, impact, direction, confidence, reason, expected_move)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
          .run(
            post.id, post.platform, post.text, post.url,
            post.posted_at, Date.now(),
            analysis.impact, analysis.direction, analysis.confidence,
            analysis.reason, analysis.expected_move_pct
          );
      } catch(e) { console.error('Trump DB insert:', e.message); }

      console.log(`📊 Trump (${post.platform}): "${post.text.substring(0,80)}..." → ${analysis.direction} ${analysis.impact} ${Math.round(analysis.confidence*100)}%`);

      await sendTrumpAlert(post, analysis);

      await new Promise(r => setTimeout(r, 2000));
    }
  } catch(e) {
    console.error('Trump monitor error:', e.message);
  }
}

function initTrumpDb() {
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS trump_events (
      id            TEXT PRIMARY KEY,
      platform      TEXT NOT NULL,
      text          TEXT NOT NULL,
      url           TEXT,
      posted_at     INTEGER NOT NULL,
      detected_at   INTEGER NOT NULL,
      impact        TEXT,
      direction     TEXT,
      confidence    REAL,
      reason        TEXT,
      expected_move REAL,
      notified      INTEGER DEFAULT 0
    )`);
    const recent = db.prepare(`SELECT id FROM trump_events WHERE detected_at > ?`)
      .all(Date.now() - 7*24*3600000);
    recent.forEach(r => seenTrumpPosts.add(r.id));
    console.log(`📋 Trump DB ready · ${seenTrumpPosts.size} seen posts loaded`);
  } catch(e) {
    console.error('Trump DB init error:', e.message);
  }
}

if (TRUMP_ENABLED) {
  console.log(`🐘 Trump Monitor: ENABLED (poll every ${TRUMP_POLL_INTERVAL/1000}s)`);
  console.log(`   - Truth Social: @${TRUTHSOCIAL_USER}`);
  console.log(`   - Twitter/X:    @${TWITTER_USERNAME}`);
  console.log(`   - AI Analysis:  ${ANTHROPIC_API_KEY ? 'Claude API ✓' : 'Keyword fallback'}`);
  setTimeout(() => { initTrumpDb(); pollTrumpAndAnalyze(); }, 30000);
  setInterval(pollTrumpAndAnalyze, TRUMP_POLL_INTERVAL);
} else {
  console.log(`🐘 Trump Monitor: DISABLED (set TRUMP_MONITOR_ENABLED=true to enable)`);
}

app.use(cors());
app.use(express.json());

// DB Path: prefer persistent volume, fallback to /tmp (ephemeral)
const fs = require('fs');

function detectDbDir() {
  const candidates = ['/data', '/var/lib/data', '/storage', '/mnt/data'];
  for (const path of candidates) {
    try {
      if (fs.existsSync(path)) {
        const testFile = `${path}/.pulstrade_writetest`;
        fs.writeFileSync(testFile, 'test');
        fs.unlinkSync(testFile);
        return { dir: path, persistent: true };
      }
    } catch (e) { /* not writable */ }
  }
  return { dir: '/tmp', persistent: false };
}

const dbInfo = detectDbDir();
const DB_DIR = dbInfo.dir;
const DB_PATH = process.env.DB_PATH || `${DB_DIR}/pulstrade.db`;
console.log(`📂 Database path: ${DB_PATH} (${dbInfo.persistent ? 'PERSISTENT ✓' : 'EPHEMERAL ⚠️'})`);
if (!dbInfo.persistent) {
  console.log(`⚠️ WARNING: DB is on /tmp — data will be lost on next deploy!`);
  console.log(`   Set Railway Volume Mount Path to /data to enable persistence.`);
}
const db = new Database(DB_PATH);
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
  if (!cols.includes('telegram_msg_id')) db.exec("ALTER TABLE signals ADD COLUMN telegram_msg_id INTEGER");
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

  const bb = calcBollingerBands(closes);

  for (const [fibName, fibValue] of Object.entries(fibs)) {
    const tolerance = atr * 1.5;
    if (Math.abs(price - fibValue) > tolerance) continue;

    const action = uptrend ? 'BUY' : 'SELL';

    const isBuyZone  = price < swingLow  + fibRange * 0.50;
    const isSellZone = price > swingHigh - fibRange * 0.50;
    if (action==='BUY'  && !isBuyZone)  continue;
    if (action==='SELL' && !isSellZone) continue;

    const confirmations = [];
    let score = 0;

    const fibScores = { '61.8%':25, '50.0%':20, '38.2%':18, '78.6%':15 };
    const fibPts = fibScores[fibName] || 10;
    score += fibPts;
    confirmations.push(`FIB ${fibName}: +${fibPts}pts`);

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

    const pattern = detectPattern(candles, action);
    let patternConfirmed = false;
    if (pattern.score >= 18) {
      score += pattern.score;
      confirmations.push(`${pattern.name}: +${pattern.score}pts`);
      patternConfirmed = true;
    } else if (pattern.score > 0) {
      score += pattern.score;
    }

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

    const validConfirmations = [rsiConfirmed, emaConfirmed, patternConfirmed, bbConfirmed, momentumConfirmed].filter(Boolean).length;
    
    const hasPremiumSignal = patternConfirmed || bbConfirmed;
    
    const passes = (validConfirmations >= 3) || (validConfirmations >= 2 && hasPremiumSignal);
    
    if (!passes) {
      console.log(`🚫 [${tf.label}] ${action} ${fibName} blocked — ${validConfirmations}/5 confirmations, premium=${hasPremiumSignal}`);
      continue;
    }
    
    const setupQuality = validConfirmations >= 3 ? 'A+' : 'A';

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
      note: `${setupQuality} setup (${validConfirmations}/5${hasPremiumSignal ? ', premium' : ''}) | ${confirmations.slice(0,3).join(' | ')}`,
      rsi, atr,
    });
    console.log(`✅ [${tf.label}] ${setupQuality} ${action} ${fibName} — ${validConfirmations}/5 confirmations${hasPremiumSignal ? ' + PREMIUM' : ''}`);
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
  
  if (!ema50 || !ema200) return [];
  const emaDiff = Math.abs(ema50 - ema200) / ema200;
  if (emaDiff > 0.03) return [];

  let rangeHigh=-Infinity, rangeLow=Infinity;
  for (let i=0; i<50; i++) {
    if (!candles[i]) break;
    if (candles[i].high > rangeHigh) rangeHigh = candles[i].high;
    if (candles[i].low  < rangeLow)  rangeLow = candles[i].low;
  }
  const rangeSize = rangeHigh - rangeLow;
  if (rangeSize < atr * 2) return [];

  const bb = calcBollingerBands(closes);
  if (!bb) return [];

  const signals = [];

  const nearLow = price < rangeLow + rangeSize * 0.25;
  const atBBLower = price <= bb.lower * 1.005;

  if (nearLow && atBBLower && rsi < 55) {
    let score = 30;
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

  let consHigh=-Infinity, consLow=Infinity;
  for (let i=2; i<22; i++) {
    if (!candles[i]) break;
    if (candles[i].high > consHigh) consHigh = candles[i].high;
    if (candles[i].low  < consLow)  consLow = candles[i].low;
  }
  const consRange = consHigh - consLow;
  if (consRange < atr * 1.5) return [];

  const signals = [];

  if (price > consHigh && candles[0].close > candles[0].open) {
    const breakoutSize = price - consHigh;
    if (breakoutSize > atr * 0.3 && breakoutSize < atr * 3) {
      let score = 35;
      const reasons = ['Bullish Breakout: +35pts'];

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

  if (uptrend) {
    const nearEma20 = Math.abs(price - ema20) < atr * 2.5 && price < ema20;
    const nearEma50 = Math.abs(price - ema50) < atr * 3.0 && price < ema50;
    
    if (nearEma20 || nearEma50) {
      let score = 40;
      const reasons = [nearEma20 ? 'Pullback to EMA20: +40pts' : 'Pullback to EMA50: +40pts'];

      if (rsi >= 30 && rsi <= 55) { score += 20; reasons.push('RSI oversold: +20pts'); }
      else if (rsi < 30) { score += 15; reasons.push('RSI very oversold: +15pts'); }

      const pattern = detectPattern(candles, 'BUY');
      if (pattern.score > 0) { score += pattern.score; reasons.push(`${pattern.name}: +${pattern.score}pts`); }

      if (price > ema200) { score += 10; reasons.push('Above EMA200: +10pts'); }

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

  if (downtrend) {
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
// ════════════════════════════════════════════════════════════════════════

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
    aggBar:        null,
    aggIsBull:     null,
    retestDone:    false,
    retestBar:     null,
  };
}

function calcSMA(values, period) {
  if (values.length < period) return null;
  const slice = values.slice(0, period);
  return slice.reduce((a,b) => a+b, 0) / period;
}

function calcCVD(candles) {
  const chrono = [...candles].reverse();
  const cvdSeries = [];
  let cum = 0;
  for (const c of chrono) {
    const vol = c.volume || 1;
    const delta = c.close >= c.open ? vol : -vol;
    cum += delta;
    cvdSeries.push(cum);
  }
  const cvdNow = cvdSeries[cvdSeries.length - 1];
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
  
  const c0 = candles[0];
  const c1 = candles[1];
  const price = c0.close;
  const open0 = c0.open;
  
  const atr = calcATR(candles);
  const ema200 = calcEMA(closes, 200);
  const sma200 = calcSMA(closes, 200);
  if (!ema200 || !sma200) return signals;
  
  if (!setup2State[tf.label]) setup2State[tf.label] = freshState();
  const state = setup2State[tf.label];
  
  const rangeWindow = candles.slice(1, 21);
  if (rangeWindow.length < 20) return signals;
  const rangeHigh = Math.max(...rangeWindow.map(c => c.high));
  const rangeLow  = Math.min(...rangeWindow.map(c => c.low));
  const rangeSize = rangeHigh - rangeLow;
  const isAccumulation = rangeSize <= atr * 2.0;
  
  const body0 = Math.abs(c0.close - c0.open);
  const isAggressive = body0 >= atr * 1.5;
  
  const bullishBreak = isAccumulation && isAggressive
    && c1.close < ema200 && c1.close < sma200
    && c0.close > ema200 && c0.close > sma200
    && c0.close > c0.open;
  
  const bearishBreak = isAccumulation && isAggressive
    && c1.close > ema200 && c1.close > sma200
    && c0.close < ema200 && c0.close < sma200
    && c0.close < c0.open;
  
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
    return signals;
  }
  
  if (state.aggBar && !state.retestDone) {
    const barsSinceAgg = candles.findIndex(c => c.timestamp <= state.aggBar);
    
    if (barsSinceAgg < 0 || barsSinceAgg > 15) {
      console.log(`⏰ [${tf.label}] Setup 2: Aggressive candle expired (no retest in 15 bars)`);
      setup2State[tf.label] = freshState();
      return signals;
    }
    
    const retestTolerance = atr * 0.3;
    const breakLevel = state.aggBreakLevel;
    
    if (state.aggIsBull) {
      if (c0.low <= breakLevel + retestTolerance && c0.low >= breakLevel - retestTolerance) {
        state.retestDone = true;
        state.retestBar = c0.timestamp;
        console.log(`🔄 [${tf.label}] Setup 2: BULL retest hit @ ${c0.low.toFixed(2)} (level: ${breakLevel.toFixed(2)})`);
      }
    } else {
      if (c0.high >= breakLevel - retestTolerance && c0.high <= breakLevel + retestTolerance) {
        state.retestDone = true;
        state.retestBar = c0.timestamp;
        console.log(`🔄 [${tf.label}] Setup 2: BEAR retest hit @ ${c0.high.toFixed(2)} (level: ${breakLevel.toFixed(2)})`);
      }
    }
  }
  
  if (state.aggBar && state.retestDone && state.retestBar) {
    const barsSinceRetest = candles.findIndex(c => c.timestamp <= state.retestBar);
    
    if (barsSinceRetest < 0 || barsSinceRetest > 5) {
      console.log(`⏰ [${tf.label}] Setup 2: Retest expired (no trigger in 5 bars)`);
      setup2State[tf.label] = freshState();
      return signals;
    }
    
    const cvd = calcCVD(candles);
    
    if (state.aggIsBull) {
      const bullishClose = c0.close > c0.open;
      const cvdOK = !state.cvdRequired || cvd.cvdBullish;
      
      if (bullishClose && cvd.cvdBullish) {
        const entry = c0.close;
        const originalSL = state.aggCandleLow - atr * 0.1;
        const originalTP = state.aggBreakLevel + (state.aggRangeSize * 1.5);
        
        if (originalSL < entry && originalTP > entry) {
          armedSignals[tf.label] = {
            direction: 'BUY',
            armBarTimestamp: c0.timestamp,
            armPrice: entry,
            armLow: c0.low,
            armHigh: c0.high,
            originalSL, originalTP,
            atrSnapshot: atr,
            confidence: 80,
            strategy: 'Setup 2 Breakout+Retest',
            pattern: 'Aggressive Bull Break + Retest',
            fibLevel: null,
          };
          console.log(`🎯 [${tf.label}] Setup 2 BUY ARMED @ ${entry.toFixed(2)} — waiting for confirmation`);
        }
        setup2State[tf.label] = freshState();
        return signals;
      }
    } else {
      const bearishClose = c0.close < c0.open;
      
      if (bearishClose && cvd.cvdBearish) {
        const entry = c0.close;
        const originalSL = state.aggCandleHigh + atr * 0.1;
        const originalTP = state.aggBreakLevel - (state.aggRangeSize * 1.5);
        
        if (originalSL > entry && originalTP < entry) {
          armedSignals[tf.label] = {
            direction: 'SELL',
            armBarTimestamp: c0.timestamp,
            armPrice: entry,
            armLow: c0.low,
            armHigh: c0.high,
            originalSL, originalTP,
            atrSnapshot: atr,
            confidence: 80,
            strategy: 'Setup 2 Breakout+Retest',
            pattern: 'Aggressive Bear Break + Retest',
            fibLevel: null,
          };
          console.log(`🎯 [${tf.label}] Setup 2 SELL ARMED @ ${entry.toFixed(2)} — waiting for confirmation`);
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
// ════════════════════════════════════════════════════════════════════════

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

const S3_SLOPE_LOOKBACK = 10;
const S3_SLOPE_FLAT_THRESHOLD = 0.05;
const S3_EMA_SLOPE_LEN = 200;

// ════════════════════════════════════════════════════════════════════════
// CONFIRMATION DELAY (applies to Setup 2 + Setup 3)
// ════════════════════════════════════════════════════════════════════════
const CONFIRM_MIN_PULLBACK_ATR = 0.3;
const CONFIRM_MAX_PULLBACK_ATR = 1.5;
const CONFIRM_TIMEOUT_BARS     = 5;
const CONFIRM_SL_BUFFER_ATR    = 0.1;

const armedSignals = {
  '5m':  null,
  '15m': null,
};

function processArmedSignal(candles, tf) {
  const armed = armedSignals[tf.label];
  if (!armed) return null;
  
  const c0 = candles[0];
  const atr = calcATR(candles);
  if (!atr) return null;
  
  const armBarIdx = candles.findIndex(c => c.timestamp === armed.armBarTimestamp);
  const barsSinceArm = armBarIdx >= 0 ? armBarIdx : 999;
  
  if (barsSinceArm > CONFIRM_TIMEOUT_BARS) {
    console.log(`⏰ [${tf.label}] Armed ${armed.direction} signal timeout — discarded`);
    armedSignals[tf.label] = null;
    return null;
  }
  
  if (armed.direction === 'BUY') {
    const sinceArm = candles.slice(0, armBarIdx + 1);
    const lowestSinceArm = Math.min(...sinceArm.map(c => c.low));
    armed.armLow = Math.min(armed.armLow, lowestSinceArm);
    
    const pullback = armed.armPrice - armed.armLow;
    
    if (armed.armLow < armed.originalSL) {
      console.log(`🚫 [${tf.label}] Armed BUY invalidated — pullback exceeded original SL`);
      armedSignals[tf.label] = null;
      return null;
    }
    
    if (pullback > armed.atrSnapshot * CONFIRM_MAX_PULLBACK_ATR) {
      console.log(`🚫 [${tf.label}] Armed BUY invalidated — pullback ${pullback.toFixed(2)} > max ${(armed.atrSnapshot * CONFIRM_MAX_PULLBACK_ATR).toFixed(2)}`);
      armedSignals[tf.label] = null;
      return null;
    }
    
    const sufficientPullback   = pullback >= armed.atrSnapshot * CONFIRM_MIN_PULLBACK_ATR;
    const bullishClose         = c0.close > c0.open;
    const closeAboveArmPrice   = c0.close >= armed.armPrice - (armed.atrSnapshot * 0.2);
    const minBarSinceArm       = barsSinceArm >= 1;
    
    if (sufficientPullback && bullishClose && closeAboveArmPrice && minBarSinceArm) {
      const newSL = armed.armLow - (atr * CONFIRM_SL_BUFFER_ATR);
      const slDistance = c0.close - newSL;
      const tpDistance = armed.originalTP - c0.close;
      
      if (slDistance <= 0 || tpDistance <= 0) {
        console.log(`🚫 [${tf.label}] Armed BUY skipped — invalid SL/TP geometry after confirm`);
        armedSignals[tf.label] = null;
        return null;
      }
      
      const newRR = tpDistance / slDistance;
      
      const signal = {
        action: 'BUY',
        price: Math.round(c0.close * 100) / 100,
        sl: Math.round(newSL * 100) / 100,
        tp1: Math.round(armed.originalTP * 100) / 100,
        tp2: Math.round((armed.originalTP + (armed.originalTP - c0.close) * 0.6) * 100) / 100,
        confidence: Math.min(85, armed.confidence + 5),
        fib_level: armed.fibLevel || null,
        pattern: armed.pattern + ' (Liquidity Sweep Confirmed)',
        strategy: armed.strategy,
        note: `CONFIRMED after ${barsSinceArm}-bar sweep | RR ${newRR.toFixed(2)} | sweep low ${armed.armLow.toFixed(2)}`,
        rsi: calcRSI(candles.map(c => c.close)),
        atr,
      };
      
      console.log(`✅ [${tf.label}] CONFIRMED ${armed.strategy} BUY @ ${c0.close.toFixed(2)} | SL ${newSL.toFixed(2)} | RR ${newRR.toFixed(2)} (sweep ${armed.armLow.toFixed(2)})`);
      armedSignals[tf.label] = null;
      return signal;
    }
    
    return null;
  }
  
  if (armed.direction === 'SELL') {
    const sinceArm = candles.slice(0, armBarIdx + 1);
    const highestSinceArm = Math.max(...sinceArm.map(c => c.high));
    armed.armHigh = Math.max(armed.armHigh, highestSinceArm);
    
    const pullback = armed.armHigh - armed.armPrice;
    
    if (armed.armHigh > armed.originalSL) {
      console.log(`🚫 [${tf.label}] Armed SELL invalidated — pullback exceeded original SL`);
      armedSignals[tf.label] = null;
      return null;
    }
    
    if (pullback > armed.atrSnapshot * CONFIRM_MAX_PULLBACK_ATR) {
      console.log(`🚫 [${tf.label}] Armed SELL invalidated — pullback ${pullback.toFixed(2)} > max ${(armed.atrSnapshot * CONFIRM_MAX_PULLBACK_ATR).toFixed(2)}`);
      armedSignals[tf.label] = null;
      return null;
    }
    
    const sufficientPullback   = pullback >= armed.atrSnapshot * CONFIRM_MIN_PULLBACK_ATR;
    const bearishClose         = c0.close < c0.open;
    const closeBelowArmPrice   = c0.close <= armed.armPrice + (armed.atrSnapshot * 0.2);
    const minBarSinceArm       = barsSinceArm >= 1;
    
    if (sufficientPullback && bearishClose && closeBelowArmPrice && minBarSinceArm) {
      const newSL = armed.armHigh + (atr * CONFIRM_SL_BUFFER_ATR);
      const slDistance = newSL - c0.close;
      const tpDistance = c0.close - armed.originalTP;
      
      if (slDistance <= 0 || tpDistance <= 0) {
        console.log(`🚫 [${tf.label}] Armed SELL skipped — invalid SL/TP geometry`);
        armedSignals[tf.label] = null;
        return null;
      }
      
      const newRR = tpDistance / slDistance;
      
      const signal = {
        action: 'SELL',
        price: Math.round(c0.close * 100) / 100,
        sl: Math.round(newSL * 100) / 100,
        tp1: Math.round(armed.originalTP * 100) / 100,
        tp2: Math.round((armed.originalTP - (c0.close - armed.originalTP) * 0.6) * 100) / 100,
        confidence: Math.min(85, armed.confidence + 5),
        fib_level: armed.fibLevel || null,
        pattern: armed.pattern + ' (Liquidity Sweep Confirmed)',
        strategy: armed.strategy,
        note: `CONFIRMED after ${barsSinceArm}-bar sweep | RR ${newRR.toFixed(2)} | sweep high ${armed.armHigh.toFixed(2)}`,
        rsi: calcRSI(candles.map(c => c.close)),
        atr,
      };
      
      console.log(`✅ [${tf.label}] CONFIRMED ${armed.strategy} SELL @ ${c0.close.toFixed(2)} | SL ${newSL.toFixed(2)} | RR ${newRR.toFixed(2)} (sweep ${armed.armHigh.toFixed(2)})`);
      armedSignals[tf.label] = null;
      return signal;
    }
    
    return null;
  }
  
  return null;
}

function calcEMA200Slope(candles, lookback) {
  if (candles.length < S3_EMA_SLOPE_LEN + lookback + 5) return null;
  
  const closes = candles.map(c => c.close);
  
  const ema_now = calcEMA(closes, S3_EMA_SLOPE_LEN);
  
  const closes_past = candles.slice(lookback).map(c => c.close);
  const ema_past = calcEMA(closes_past, S3_EMA_SLOPE_LEN);
  
  if (!ema_now || !ema_past || ema_past === 0) return null;
  
  const slopePct = ((ema_now - ema_past) / ema_past) * 100;
  
  return {
    slopePct,
    magnitude: Math.abs(slopePct),
    isFlat: Math.abs(slopePct) < S3_SLOPE_FLAT_THRESHOLD,
  };
}

const setup3State = {
  '5m': null,
  '15m': null,
};

function freshSetup3State() {
  return {
    firstTop:        null,
    firstBottom:     null,
    secondTop:       null,
    secondBottom:    null,
    triggerArmedBarTimestamp: null,
    lastSignalBarTimestamp:   null,
  };
}

function detectPivotHighFromCandles(candles, left, right) {
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
  const c0 = candles[0];
  const c1 = candles[1];
  if (!c0 || !c1) return signals;
  
  const atr = calcATR(candles);
  const ema20 = calcEMA(closes, S3_EMA_TRIGGER_LEN);
  if (!ema20 || !atr) return signals;
  
  if (!setup3State[tf.label]) setup3State[tf.label] = freshSetup3State();
  const state = setup3State[tf.label];
  
  const slope = calcEMA200Slope(candles, S3_SLOPE_LOOKBACK);
  const trendIsExhausted = slope ? slope.isFlat : false;
  
  const ph = detectPivotHighFromCandles(candles, S3_PIVOT_LEFT, S3_PIVOT_RIGHT);
  const pl = detectPivotLowFromCandles(candles, S3_PIVOT_LEFT, S3_PIVOT_RIGHT);
  
  const phHasWick = ph && ph.range > 0 && (ph.upperWick / ph.range) >= S3_WICK_RATIO;
  const plHasWick = pl && pl.range > 0 && (pl.lowerWick / pl.range) >= S3_WICK_RATIO;
  
  if (ph && phHasWick) {
    if (!state.firstTop) {
      state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
      console.log(`📍 [${tf.label}] Setup 3: First TOP @ ${ph.price.toFixed(2)}`);
    } else {
      const barsSince = barsBetween(candles, state.firstTop.barTimestamp, ph.barTimestamp);
      const priceDiff = Math.abs(ph.price - state.firstTop.price);
      const tolerance = atr * S3_TOP_TOLERANCE_ATR;
      
      if (barsSince >= S3_MIN_BARS_BETWEEN && barsSince <= S3_MAX_BARS_BETWEEN && priceDiff <= tolerance) {
        state.secondTop = { price: ph.price, barTimestamp: ph.barTimestamp };
        state.triggerArmedBarTimestamp = c0.timestamp;
        console.log(`🎯 [${tf.label}] Setup 3: DOUBLE TOP confirmed @ ${ph.price.toFixed(2)} (1st: ${state.firstTop.price.toFixed(2)}) — armed`);
      } else if (barsSince > S3_MAX_BARS_BETWEEN) {
        state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
        state.secondTop = null;
        state.triggerArmedBarTimestamp = null;
      } else {
        state.firstTop = { price: ph.price, barTimestamp: ph.barTimestamp, lowSinceTop: ph.price };
      }
    }
  }
  
  if (state.firstTop && c0.low < state.firstTop.lowSinceTop) {
    state.firstTop.lowSinceTop = c0.low;
  }
  
  if (state.secondTop && state.triggerArmedBarTimestamp) {
    const barsSinceArm = barsBetween(candles, state.triggerArmedBarTimestamp, c0.timestamp);
    
    if (barsSinceArm > S3_MAX_BARS_TO_TRIGGER) {
      console.log(`⏰ [${tf.label}] Setup 3: SHORT trigger expired (${barsSinceArm} bars without EMA break)`);
      state.secondTop = null;
      state.triggerArmedBarTimestamp = null;
    } else if (barsSinceArm >= 1 && c0.close < ema20 && c1.close >= ema20 && !trendIsExhausted) {
      console.log(`🚫 [${tf.label}] Setup 3 SHORT trigger blocked — EMA200 slope ${slope?.slopePct.toFixed(3)}% (need <${S3_SLOPE_FLAT_THRESHOLD}%)`);
    } else if (barsSinceArm >= 1 && c0.close < ema20 && c1.close >= ema20 && trendIsExhausted) {
      const topHigh = Math.max(state.firstTop.price, state.secondTop.price);
      const middleLow = state.firstTop.lowSinceTop;
      const measuredMove = topHigh - middleLow;
      
      if (measuredMove > 0) {
        const entry = c0.close;
        const sl = topHigh + (atr * S3_SL_BUFFER_ATR);
        const tp1 = entry - (measuredMove * S3_TP_MULTIPLIER);
        const tp2 = entry - (measuredMove * 2.5);
        
        if (sl > entry && tp1 < entry) {
          armedSignals[tf.label] = {
            direction: 'SELL',
            armBarTimestamp: c0.timestamp,
            armPrice: entry,
            armLow: c0.low,
            armHigh: c0.high,
            originalSL: sl,
            originalTP: tp1,
            atrSnapshot: atr,
            confidence: 80,
            strategy: 'Setup 3 Double Top',
            pattern: 'Double Top + EMA20 Break',
            fibLevel: null,
          };
          console.log(`🎯 [${tf.label}] Setup 3 SELL ARMED @ ${entry.toFixed(2)} — waiting for confirmation`);
        }
        
        state.firstTop = null;
        state.secondTop = null;
        state.triggerArmedBarTimestamp = null;
        state.lastSignalBarTimestamp = c0.timestamp;
        return signals;
      }
    }
  }
  
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
  
  if (state.firstBottom && c0.high > state.firstBottom.highSinceBottom) {
    state.firstBottom.highSinceBottom = c0.high;
  }
  
  if (state.secondBottom && state.triggerArmedBarTimestamp) {
    const barsSinceArm = barsBetween(candles, state.triggerArmedBarTimestamp, c0.timestamp);
    
    if (barsSinceArm > S3_MAX_BARS_TO_TRIGGER) {
      console.log(`⏰ [${tf.label}] Setup 3: LONG trigger expired`);
      state.secondBottom = null;
      state.triggerArmedBarTimestamp = null;
    } else if (barsSinceArm >= 1 && c0.close > ema20 && c1.close <= ema20 && !trendIsExhausted) {
      console.log(`🚫 [${tf.label}] Setup 3 LONG trigger blocked — EMA200 slope ${slope?.slopePct.toFixed(3)}% (need <${S3_SLOPE_FLAT_THRESHOLD}%)`);
    } else if (barsSinceArm >= 1 && c0.close > ema20 && c1.close <= ema20 && trendIsExhausted) {
      const bottomLow = Math.min(state.firstBottom.price, state.secondBottom.price);
      const middleHigh = state.firstBottom.highSinceBottom;
      const measuredMove = middleHigh - bottomLow;
      
      if (measuredMove > 0) {
        const entry = c0.close;
        const sl = bottomLow - (atr * S3_SL_BUFFER_ATR);
        const tp1 = entry + (measuredMove * S3_TP_MULTIPLIER);
        const tp2 = entry + (measuredMove * 2.5);
        
        if (sl < entry && tp1 > entry) {
          armedSignals[tf.label] = {
            direction: 'BUY',
            armBarTimestamp: c0.timestamp,
            armPrice: entry,
            armLow: c0.low,
            armHigh: c0.high,
            originalSL: sl,
            originalTP: tp1,
            atrSnapshot: atr,
            confidence: 80,
            strategy: 'Setup 3 Double Bottom',
            pattern: 'Double Bottom + EMA20 Break',
            fibLevel: null,
          };
          console.log(`🎯 [${tf.label}] Setup 3 BUY ARMED @ ${entry.toFixed(2)} — waiting for confirmation`);
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
      if (tfCandles.length >= 20 && tfEma50) {
        const recent20Closes = tfCandles.slice(0, 20).map(c => c.close);
        const slopeStart = recent20Closes[recent20Closes.length - 1];
        const slopeEnd = recent20Closes[0];
        const slope = (slopeEnd - slopeStart) / slopeStart;
        
        const currentPrice = tfCloses[tfCloses.length - 1];
        const aboveEma50 = currentPrice > tfEma50;
        const belowEma50 = currentPrice < tfEma50;
        
        const SLOPE_THRESHOLD = 0.0005;
        
        if (slope > SLOPE_THRESHOLD && aboveEma50) {
          trend = 'UP';
        } else if (slope < -SLOPE_THRESHOLD && belowEma50) {
          trend = 'DOWN';
        }
      }
      tfTrends[tf.label] = trend;
      trendVotes[trend]++;
    } catch(e) { tfTrends[tf.label] = 'ERROR'; }
  }
  
  console.log(`📊 Trend Consensus: UP=${trendVotes.UP} DOWN=${trendVotes.DOWN} NEUTRAL=${trendVotes.NEUTRAL} | ${JSON.stringify(tfTrends)}`);
  
  const CONSENSUS_THRESHOLD = 3;
  let consensusDirection = null;
  if (trendVotes.UP >= CONSENSUS_THRESHOLD) consensusDirection = 'BUY';
  else if (trendVotes.DOWN >= CONSENSUS_THRESHOLD) consensusDirection = 'SELL';
  
  if (!consensusDirection) {
    console.log(`⚠️ No clear MTF consensus — FIB on 30m/1H/4H will be blocked, but Setup 2/3 + 5m/15m FIB still run`);
  } else {
    console.log(`✅ MTF Consensus: ${consensusDirection} (${consensusDirection === 'BUY' ? trendVotes.UP : trendVotes.DOWN}/5 timeframes)`);
  }

  for (const tf of timeframes) {
    try {
      const candles = await fetchCandles(tf.interval, 220);
      if (!candles || candles.length < 30) continue;

      const price = candles[0].close;

      if (cachedPrice.price) {
        const dev = Math.abs(price - cachedPrice.price) / cachedPrice.price;
        if (dev > 0.05) { console.log(`⚠️ [${tf.label}] Stale candles`); continue; }
      }

      const confirmedSignal = (tf.label === '5m' || tf.label === '15m')
                              ? processArmedSignal(candles, tf)
                              : null;
      
      const fibSignals    = scanFibPullback(candles, tf);
      const setup2Signals = (tf.label === '5m' || tf.label === '15m') 
                            ? scanSetup2BreakoutRetest(candles, tf) 
                            : [];
      const setup3Signals = (tf.label === '5m' || tf.label === '15m')
                            ? scanSetup3DoubleTop(candles, tf)
                            : [];
      const allSignals    = [...fibSignals, ...setup2Signals, ...setup3Signals, ...(confirmedSignal ? [confirmedSignal] : [])];

      console.log(`📦 [${tf.label}] Got ${allSignals.length} candidate signals`);
      for (const sig of allSignals) {
        console.log(`  → Checking ${sig.action} ${sig.strategy} conf=${sig.confidence} fib=${sig.fib_level || 'n/a'}`);
        
        const isTrendFollowing = sig.strategy === 'FIB';
        const isShortTermTF = (tf.label === '5m' || tf.label === '15m');
        const shouldFilterMTF = isTrendFollowing && !isShortTermTF;
        
        if (shouldFilterMTF) {
          if (!consensusDirection) {
            console.log(`  🚫 BLOCKED: FIB ${tf.label} — no MTF consensus, market mixed`);
            continue;
          }
          if (sig.action !== consensusDirection) {
            console.log(`  🚫 BLOCKED: ${sig.strategy} ${tf.label} against MTF consensus (need ${consensusDirection})`);
            continue;
          }
        } else {
          console.log(`  ✓ MTF check skipped: ${sig.strategy} on ${tf.label} (short-term/reversal allowed)`);
        }
        
        if (sig.confidence < tf.minScore) {
          console.log(`  🚫 BLOCKED: confidence ${sig.confidence} < minScore ${tf.minScore}`);
          continue;
        }

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
          try { sendTelegramSignal({ ...record, id: inserted.id }); } catch(tgErr) { console.error('Telegram call error:', tgErr.message); }
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
    // ZONE ENGINE PATCH: zones are tracked by zone_engine.js — exclude here
    const openSignals = db.prepare("SELECT * FROM signals WHERE (outcome = 'open' OR outcome IS NULL) AND (kind IS NULL OR kind != 'zone')").all();
    if (openSignals.length === 0) return;

    const candles = await fetchCandles('5min', 1000);
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
            const slHit = c.low <= sl;
            const tp1Hit = c.high >= tp1;
            const tp2Hit = tp2 && c.high >= tp2;
            
            if (slHit && (tp1Hit || tp2Hit)) {
              outcome = 'sl_hit'; exitPrice = sl; exitTime = c.timestamp; break;
            }
            if (slHit) { outcome='sl_hit'; exitPrice=sl; exitTime=c.timestamp; break; }
            if (tp2Hit) { outcome='tp2_hit'; exitPrice=tp2; exitTime=c.timestamp; break; }
            if (tp1Hit) { outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
          } else {
            const slHit = c.high >= sl;
            const tp1Hit = c.low <= tp1;
            const tp2Hit = tp2 && c.low <= tp2;
            
            if (slHit && (tp1Hit || tp2Hit)) {
              outcome = 'sl_hit'; exitPrice = sl; exitTime = c.timestamp; break;
            }
            if (slHit) { outcome='sl_hit'; exitPrice=sl; exitTime=c.timestamp; break; }
            if (tp2Hit) { outcome='tp2_hit'; exitPrice=tp2; exitTime=c.timestamp; break; }
            if (tp1Hit) { outcome='tp1_hit'; exitPrice=tp1; exitTime=c.timestamp; break; }
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
          const finalPnlR = Math.round(pnlR * 100) / 100;
          db.prepare(`UPDATE signals SET outcome=?, exit_price=?, closed_at=?, pnl_r=? WHERE id=?`)
            .run(outcome, exitPrice, exitTime, finalPnlR, signal.id);
          closedCount++;
          try {
            updateTelegramOutcome({ ...signal, exit_price: exitPrice, pnl_r: finalPnlR }, outcome);
          } catch(tgErr) { /* silent */ }
        }
      } catch(e) {}
    }
    if (closedCount > 0) console.log(`✓ Closed ${closedCount} signals`);
  } catch(e) { console.error('Tracker error:', e.message); }
}
trackSignalOutcomes();
setInterval(trackSignalOutcomes, 15 * 60 * 1000);

// ── Routes ─────────────────────────────────────────────────
app.get('/', (req,res) => res.json({ status:'Pulstrade Backend', version:'5.7.0-zone-engine' }));
app.get('/health', (req,res) => res.json({
  status:'ok',
  version: '5.7.0-zone-engine',
  dbPath: DB_PATH,
  dbPersistent: dbInfo.persistent,
  dbDir: DB_DIR,
  pathChecks: {
    '/data': fs.existsSync('/data'),
    '/var/lib/data': fs.existsSync('/var/lib/data'),
    '/storage': fs.existsSync('/storage'),
    '/mnt/data': fs.existsSync('/mnt/data'),
    '/tmp': fs.existsSync('/tmp'),
  },
  signals: db.prepare('SELECT COUNT(*) as c FROM signals').get().c,
  open:    db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome='open' OR outcome IS NULL").get().c,
  closed:  db.prepare("SELECT COUNT(*) as c FROM signals WHERE outcome IS NOT NULL AND outcome != 'open'").get().c,
  marketClosed: isMarketClosed(),
  priceCache: cachedPrice,
  strategies: ['FIB Pullback', 'Setup 2 Breakout+Retest', 'Setup 3 Double Top/Bottom', 'Zone Engine (Sniper)'],
  timeframes: ['5m', '15m', '30m', '1H', '4H'],
}));

app.get('/signals', (req,res) => {
  const limit  = parseInt(req.query.limit)||50;
  const ticker = req.query.ticker||null;
  let query='SELECT * FROM signals', params=[];
  if (ticker) { query+=' WHERE ticker=?'; params.push(ticker); }
  query+=' ORDER BY timestamp DESC LIMIT ?'; params.push(limit);
  // ZONE ENGINE PATCH: parse entries/tps JSON for Signal v2 clients
  res.json(db.prepare(query).all(...params).map(r=>({
    ...r,
    mtf: r.mtf ? JSON.parse(r.mtf) : null,
    entries: r.entries ? JSON.parse(r.entries) : null,
    tps: r.tps ? JSON.parse(r.tps) : null,
  })));
});

app.get('/signals/:id', (req,res) => {
  const row = db.prepare('SELECT * FROM signals WHERE id=?').get(req.params.id);
  if (!row) return res.status(404).json({error:'Not found'});
  // ZONE ENGINE PATCH: parse entries/tps JSON
  res.json({
    ...row,
    mtf: row.mtf ? JSON.parse(row.mtf) : null,
    entries: row.entries ? JSON.parse(row.entries) : null,
    tps: row.tps ? JSON.parse(row.tps) : null,
  });
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
        zoneStats: { total: 0, wins: 0, losses: 0, winRate: null, avgPnL: null, tpLadder: { tp1: 0, tp2: 0, tp3: 0, tp4: 0, tp5: 0 } },
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
    for (const strat of ['FIB', 'Range Bounce', 'Breakout', 'Zone']) {
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

    // ZONE ENGINE PATCH: zone (Sniper) performance incl. TP-ladder distribution
    const zones = all.filter(s => s.kind === 'zone');
    const zoneWins = zones.filter(s => s.outcome === 'tp1_hit' || s.outcome === 'tp2_hit');
    const zoneLosses = zones.filter(s => s.outcome === 'sl_hit');
    const zoneDecisive = zoneWins.length + zoneLosses.length;
    const tpHitCounts = [0, 0, 0, 0, 0];
    zones.forEach(z => {
      try {
        (JSON.parse(z.tps || '[]')).forEach((tp, i) => { if (tp.status === 'hit' && i < 5) tpHitCounts[i]++; });
      } catch (e) {}
    });
    const zoneStats = {
      total: zones.length,
      wins: zoneWins.length,
      losses: zoneLosses.length,
      winRate: zoneDecisive > 0 ? Math.round((zoneWins.length / zoneDecisive) * 1000) / 10 : null,
      avgPnL: zones.length > 0 ? Math.round((zones.reduce((a, s) => a + (s.pnl_r || 0), 0) / zones.length) * 100) / 100 : null,
      tpLadder: { tp1: tpHitCounts[0], tp2: tpHitCounts[1], tp3: tpHitCounts[2], tp4: tpHitCounts[3], tp5: tpHitCounts[4] },
    };

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
      zoneStats,
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
      let slope = 0;
      if (candles.length >= 20 && ema50) {
        const recent20 = candles.slice(0, 20).map(c => c.close);
        const slopeStart = recent20[recent20.length - 1];
        const slopeEnd = recent20[0];
        slope = (slopeEnd - slopeStart) / slopeStart;
        const currentPrice = closes[closes.length - 1];
        const aboveEma50 = currentPrice > ema50;
        const belowEma50 = currentPrice < ema50;
        const SLOPE_THRESHOLD = 0.0005;
        if (slope > SLOPE_THRESHOLD && aboveEma50) trend = 'UP';
        else if (slope < -SLOPE_THRESHOLD && belowEma50) trend = 'DOWN';
      }
      details[tf.label] = { 
        trend, 
        price: candles[0].close.toFixed(2),
        slope_pct: (slope * 100).toFixed(3),
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
app.get('/strategy-state', async (req, res) => {
  const slopes = {};
  for (const tf of [{label:'5m',interval:'5min'}, {label:'15m',interval:'15min'}]) {
    try {
      const c = await fetchCandles(tf.interval, 220);
      const slope = calcEMA200Slope(c, S3_SLOPE_LOOKBACK);
      slopes[tf.label] = slope ? {
        slopePct: parseFloat(slope.slopePct.toFixed(3)),
        magnitude: parseFloat(slope.magnitude.toFixed(3)),
        isFlat: slope.isFlat,
        threshold: S3_SLOPE_FLAT_THRESHOLD,
        setup3Allowed: slope.isFlat,
      } : null;
    } catch(e) { slopes[tf.label] = { error: e.message }; }
  }
  res.json({
    setup2: setup2State,
    setup3: setup3State,
    armedSignals,
    slopes,
    notes: {
      setup3Allowed: 'true means EMA200 is flat enough → Setup 3 can fire if pivots align',
      armedSignals: 'Triggered setups waiting for confirmation (liquidity sweep + stabilization)',
    },
  });
});

// ── DB CHECK — inspect schema and test insert ─────────────
app.get('/db-check', (req, res) => {
  try {
    const columns = db.prepare("PRAGMA table_info(signals)").all();
    const count = db.prepare("SELECT COUNT(*) as c FROM signals").get().c;
    
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

// ── TEST TELEGRAM — verify bot + channel work ──────────────
app.get('/test-telegram', async (req, res) => {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHANNEL_ID) {
    return res.status(500).json({
      error: 'Telegram not configured',
      missing: {
        TELEGRAM_BOT_TOKEN: !TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHANNEL_ID: !TELEGRAM_CHANNEL_ID,
      }
    });
  }
  const testSignal = {
    id: 9999,
    action: 'BUY',
    price: 4615.57,
    sl: 4610.61,
    tp1: 4625.50,
    tp2: 4632.12,
    timeframe: '5m',
    confidence: 80,
    fib_level: '61.8%',
    pattern: 'Bullish Engulfing',
    strategy: 'FIB',
    rsi: 37.8,
  };
  const result = await sendTelegramSignal(testSignal);
  res.json({ success: result?.success !== false, ...result, sentAs: testSignal });
});

// ── TEST TRUMP ALERT — manually trigger Trump analysis ─────
app.get('/test-trump', async (req, res) => {
  const testText = req.query.text || "Iran refuses to negotiate. We will impose massive new tariffs on China starting Monday. The Fed must cut rates immediately to support our great economy!";
  try {
    const analysis = await analyzeTrumpPostForGold(testText);
    const mockPost = {
      id: 'test_' + Date.now(),
      platform: 'truth_social',
      text: testText,
      url: '',
      posted_at: Date.now()
    };
    if (req.query.send === '1') {
      await sendTrumpAlert(mockPost, analysis);
    }
    res.json({ analyzed: testText, analysis, sent: req.query.send === '1' });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── TRUMP EVENTS HISTORY ───────────────────────────────────
app.get('/trump-events', (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const events = db.prepare(`SELECT * FROM trump_events
      ORDER BY detected_at DESC LIMIT ?`).all(Math.min(limit, 200));
    const stats = {
      total: db.prepare(`SELECT COUNT(*) as c FROM trump_events`).get().c,
      high_impact: db.prepare(`SELECT COUNT(*) as c FROM trump_events WHERE impact='HIGH'`).get().c,
      medium_impact: db.prepare(`SELECT COUNT(*) as c FROM trump_events WHERE impact='MEDIUM'`).get().c,
      bullish: db.prepare(`SELECT COUNT(*) as c FROM trump_events WHERE direction='BULLISH'`).get().c,
      bearish: db.prepare(`SELECT COUNT(*) as c FROM trump_events WHERE direction='BEARISH'`).get().c,
      notified: db.prepare(`SELECT COUNT(*) as c FROM trump_events WHERE notified=1`).get().c,
    };
    res.json({ stats, events });
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

// ── Market Overview Cache (DXY, Oil, BTC, SPX) ────────────────
const marketOverviewCache = { data: null, timestamp: 0 };
const MARKET_CACHE_TTL = 60 * 1000;

async function fetchMarketOverview() {
  const symbols = {
    dxy: 'DXY',
    oil: 'WTI/USD',
    btc: 'BTC/USD',
    spx: 'SPX',
  };
  
  const result = {};
  
  await Promise.all(Object.entries(symbols).map(async ([key, symbol]) => {
    try {
      const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=1day&outputsize=2&apikey=${TWELVE_API_KEY}`;
      const r = await axios.get(url, { timeout: 8000 });
      const values = r.data?.values;
      if (values && values.length >= 2) {
        const today = parseFloat(values[0].close);
        const prev = parseFloat(values[1].close);
        const change = prev !== 0 ? ((today - prev) / prev) * 100 : 0;
        result[key] = { price: today, change };
      } else {
        result[key] = null;
      }
    } catch(e) {
      console.log(`⚠️ Market overview ${key} (${symbol}) failed: ${e.message}`);
      result[key] = null;
    }
  }));
  
  return result;
}

app.get('/market-overview', async (req, res) => {
  const now = Date.now();
  if (marketOverviewCache.data && (now - marketOverviewCache.timestamp) < MARKET_CACHE_TTL) {
    return res.json({ ...marketOverviewCache.data, cached: true, age: Math.floor((now - marketOverviewCache.timestamp) / 1000) });
  }
  
  try {
    const data = await fetchMarketOverview();
    marketOverviewCache.data = data;
    marketOverviewCache.timestamp = now;
    res.json({ ...data, cached: false });
  } catch(e) {
    if (marketOverviewCache.data) {
      return res.json({ ...marketOverviewCache.data, cached: true, stale: true });
    }
    res.status(500).json({ error: e.message });
  }
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

// ── Zone Engine (Sniper limit-zone signals) ───────────────
const zoneEngine = require('./zone_engine');
zoneEngine.init({
  db,
  admin,
  fetchCandles,
  getPrice: () => cachedPrice.price,
  app,
});

app.listen(PORT, () => console.log(`Pulstrade Backend v5.7.0 — Zone Engine on port ${PORT}`));
