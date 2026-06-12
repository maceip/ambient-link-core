// face-chat-final relay.
//
// One Node process. For each configured thread (= a tmux session running a
// coding agent), it:
//   - mirrors the pane via `tmux pipe-pane → FIFO`
//   - keeps a per-thread ring buffer of recent output
//   - detects idle (no output for IDLE_MS) → broadcasts `thread_idle` event
//     with a snapshot of the last assistant message. This is the trigger the
//     phone-native DAT companion uses to peek a card on the HUD.
//   - exposes a WS endpoint `/face-chat/ws` for both surfaces:
//       glasses web app (subscribes to all threads, sends user input)
//       phone-native svc (subscribes to events only)
//
// Protocol (newline-delimited JSON, sent as WS text frames):
//   server → client:
//     { type: "hello",    threads: [{id,label,agent}], cursor: {threadId:int} }
//     { type: "snapshot", thread: id, content: string, cursor: int }
//     { type: "append",   thread: id, content: string, cursor: int }
//     { type: "thread_idle", thread: id, lastAssistant: string, at: ms }
//     { type: "thread_busy", thread: id, at: ms }
//   client → server:
//     { type: "subscribe", since: { threadId: cursor } }     // resume per-thread
//     { type: "input",     thread: id, text: string, enter?: bool }
//     { type: "special",   thread: id, key: string }         // e.g. Escape, Tab
//
// env:
//   FC_PORT          5180
//   FC_HOST          0.0.0.0
//   FC_THREADS_PATH  ./etc/threads.json
//   FC_RING          65536
//   FC_IDLE_MS       2000        — gap that flips a thread from busy→idle
//
// SIGHUP reloads threads.json (add/remove threads without restart).

import { WebSocketServer } from 'ws';
import webPush from 'web-push';
import { execSync } from 'node:child_process';
import { createReadStream, existsSync, unlinkSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';

const PORT          = Number(process.env.FC_PORT ?? 5180);
const HOST          = process.env.FC_HOST ?? '0.0.0.0';
const THREADS_PATH  = process.env.FC_THREADS_PATH ?? new URL('./etc/threads.json', import.meta.url).pathname;
const VAPID_PATH    = process.env.FC_VAPID_PATH   ?? new URL('./etc/vapid.json',   import.meta.url).pathname;
const SUBS_PATH     = process.env.FC_SUBS_PATH    ?? new URL('./etc/subs.json',    import.meta.url).pathname;
const RING_MAX      = Number(process.env.FC_RING ?? 65536);
const IDLE_MS       = Number(process.env.FC_IDLE_MS ?? 2000);

function sh(c)    { return execSync(c, { encoding: 'utf8' }); }
function trySh(c) { try { return sh(c); } catch { return null; } }
function shellQuote(s) { return "'" + String(s).replace(/'/g, "'\\''") + "'"; }

// ── per-thread state ─────────────────────────────────────────────────────
// One Thread per configured agent. Owns its FIFO, ring buffer, idle timer.
class Thread {
  constructor(cfg) {
    this.id      = cfg.id;
    this.label   = cfg.label ?? cfg.id;
    this.tmux    = cfg.tmux  ?? cfg.id;
    this.agent   = cfg.agent ?? 'generic';
    this.pipe    = `/tmp/face-chat-final-${this.tmux}.pipe`;
    this.ring    = Buffer.alloc(0);
    this.ringStart = 0;
    this.cursor    = 0;
    this.idleTimer = null;
    this.busy      = false;
    this.lastChunkAt = 0;
  }
  ensureSession() {
    const has = trySh(`tmux has-session -t ${this.tmux} 2>/dev/null && echo yes`);
    if (!has || !has.includes('yes')) {
      console.log(`[${this.id}] tmux session "${this.tmux}" missing — creating empty bash shell as placeholder`);
      sh(`tmux new-session -d -s ${this.tmux} -x 80 -y 24 bash`);
      trySh(`tmux set-option -t ${this.tmux} -g window-size manual`);
    }
  }
  startPipe() {
    if (existsSync(this.pipe)) { try { unlinkSync(this.pipe); } catch {} }
    sh(`mkfifo ${this.pipe}`);
    sh(`tmux pipe-pane -o -t ${this.tmux} 'cat > ${this.pipe}'`);
  }
  snapshot() {
    return trySh(`tmux capture-pane -p -e -t ${this.tmux} -S -500`) ?? '';
  }
  appendChunk(chunk, onAppend, onIdle, onBusy) {
    this.ring = Buffer.concat([this.ring, chunk]);
    this.cursor += chunk.length;
    if (this.ring.length > RING_MAX) {
      const drop = this.ring.length - RING_MAX;
      this.ring = this.ring.slice(drop);
      this.ringStart += drop;
    }
    this.lastChunkAt = Date.now();
    if (!this.busy) { this.busy = true; onBusy(this); }
    onAppend(this, chunk);
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      this.busy = false;
      onIdle(this);
    }, IDLE_MS);
  }
  since(cursor) {
    if (typeof cursor !== 'number' || cursor < this.ringStart || cursor > this.cursor) {
      return { stale: true, cursor: this.cursor, content: '' };
    }
    return { stale: false, cursor: this.cursor, content: this.ring.slice(cursor - this.ringStart).toString('utf8') };
  }
  sendInput(text, enter) {
    if (text) sh(`tmux send-keys -t ${this.tmux} -l ${shellQuote(text)}`);
    if (enter) sh(`tmux send-keys -t ${this.tmux} Enter`);
  }
  sendSpecial(key) {
    sh(`tmux send-keys -t ${this.tmux} ${shellQuote(key)}`);
  }
}

// ── ANSI-strip + last-assistant extraction ───────────────────────────────
// For the peek card we need a short, readable snippet of the most recent
// agent output. Strip ANSI, drop box-drawing borders, return the last few
// non-trivial lines from the snapshot. Agent-specific refinement can replace
// this per agent type later.
const ANSI = /\x1b\[[0-9;?]*[ -\/]*[@-~]|\x1b\][^\x07]*\x07|\x1b[=>]|\x1b\([AB012]/g;
function stripAnsi(s)        { return (s || '').replace(ANSI, '').replace(/\r/g, ''); }
function isBoxLine(s)        { return /^[\s│║╭╮╯╰─━╌╍┄┅═╔╗╚╝]+$/.test(s); }
function isPromptLine(s)     { return /^(>|❯|\$|#)\s*$/.test(s.trim()); }
function lastAssistant(text, maxLines = 12) {
  const lines = stripAnsi(text).split('\n').map(l => l.replace(/\s+$/, ''));
  const kept = [];
  for (let i = lines.length - 1; i >= 0 && kept.length < maxLines; i--) {
    const l = lines[i];
    if (!l) { if (kept.length) kept.push(''); continue; }
    if (isBoxLine(l) || isPromptLine(l)) continue;
    kept.push(l);
  }
  return kept.reverse().join('\n').trim();
}

// ── threads registry ─────────────────────────────────────────────────────
let threads = new Map();
function loadThreads() {
  const doc = JSON.parse(readFileSync(THREADS_PATH, 'utf8'));
  const next = new Map();
  for (const cfg of (doc.threads ?? [])) {
    const existing = threads.get(cfg.id);
    if (existing) { next.set(cfg.id, existing); continue; }
    const t = new Thread(cfg);
    t.ensureSession();
    t.startPipe();
    tailThread(t);
    console.log(`[relay] +thread ${t.id} tmux=${t.tmux} agent=${t.agent}`);
    next.set(cfg.id, t);
  }
  for (const [id, t] of threads) {
    if (!next.has(id)) {
      console.log(`[relay] -thread ${id}`);
      trySh(`tmux pipe-pane -t ${t.tmux}`);
      try { unlinkSync(t.pipe); } catch {}
      if (t.idleTimer) clearTimeout(t.idleTimer);
    }
  }
  threads = next;
}

// ── transport ────────────────────────────────────────────────────────────
const clients = new Set();
function broadcast(obj) {
  const j = JSON.stringify(obj);
  for (const c of clients) { try { c.send(j); } catch {} }
}

function tailThread(t) {
  const open = () => {
    const s = createReadStream(t.pipe);
    s.on('data', (chunk) => {
      t.appendChunk(
        chunk,
        (thread, c)  => broadcast({ type: 'append',  thread: thread.id, content: c.toString('utf8'), cursor: thread.cursor }),
        (thread)     => {
          const last = lastAssistant(thread.snapshot());
          broadcast({ type: 'thread_idle', thread: thread.id, lastAssistant: last, at: Date.now() });
          // Fan out as Web Push so a sleeping PWA wakes its SW. Body is short — the HUD card
          // shows the agent label + last assistant snippet.
          pushAll({
            title: thread.label || thread.id,
            body:  last ? last.slice(0, 140) : '(agent paused)',
            thread: thread.id,
            tag:    `fc-${thread.id}`,
          }).catch((e) => console.warn('[push] pushAll error:', e.message));
        },
        (thread)     => broadcast({ type: 'thread_busy', thread: thread.id, at: Date.now() }),
      );
    });
    s.on('end',   () => setTimeout(open, 100));
    s.on('error', () => setTimeout(open, 500));
  };
  open();
}

loadThreads();

// ── web push ─────────────────────────────────────────────────────────────
// VAPID + subscription store. Subscriptions persist to etc/subs.json keyed by endpoint
// so a relay restart doesn't lose them. On thread_idle we fan out to every subscriber.
let vapid = null;
try { vapid = JSON.parse(readFileSync(VAPID_PATH, 'utf8')); webPush.setVapidDetails(vapid.subject || 'mailto:relay@local', vapid.publicKey, vapid.privateKey); console.log('[push] vapid loaded'); }
catch (e) { console.warn('[push] no VAPID keys at', VAPID_PATH, '— push disabled. Generate with: node -e "console.log(JSON.stringify(require(\'web-push\').generateVAPIDKeys()))"'); }

let subs = new Map(); // endpoint → subscription object
function loadSubs() {
  try { for (const s of JSON.parse(readFileSync(SUBS_PATH, 'utf8'))) subs.set(s.endpoint, s); console.log(`[push] loaded ${subs.size} subscriptions`); }
  catch { /* none yet */ }
}
function saveSubs() {
  try { writeFileSync(SUBS_PATH, JSON.stringify([...subs.values()], null, 2)); } catch (e) { console.warn('[push] saveSubs:', e.message); }
}
loadSubs();
async function pushAll(payload) {
  if (!vapid || !subs.size) return;
  const body = JSON.stringify(payload);
  const dead = [];
  await Promise.all([...subs.values()].map(async (s) => {
    try { await webPush.sendNotification(s, body, { TTL: 60 }); }
    catch (e) {
      console.warn(`[push] send fail ${e.statusCode}: ${e.body || e.message}`);
      // 404/410 = endpoint expired; drop it so we don't keep retrying.
      if (e.statusCode === 404 || e.statusCode === 410) dead.push(s.endpoint);
    }
  }));
  if (dead.length) { for (const e of dead) subs.delete(e); saveSubs(); console.log(`[push] pruned ${dead.length} dead subscriptions`); }
}

// ── HTTP + WS on one port ───────────────────────────────────────────────
const httpServer = createServer(async (req, res) => {
  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET,POST,OPTIONS', 'access-control-allow-headers': 'content-type' };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/face-chat/push/vapid' && req.method === 'GET') {
    if (!vapid) { res.writeHead(503, cors); return res.end('vapid not configured'); }
    res.writeHead(200, { ...cors, 'content-type': 'application/json' });
    return res.end(JSON.stringify({ publicKey: vapid.publicKey }));
  }
  if (url.pathname === '/face-chat/push/subscribe' && req.method === 'POST') {
    let body = ''; for await (const c of req) body += c;
    let sub; try { sub = JSON.parse(body); } catch { res.writeHead(400, cors); return res.end('bad json'); }
    if (!sub?.endpoint) { res.writeHead(400, cors); return res.end('no endpoint'); }
    subs.set(sub.endpoint, sub); saveSubs();
    console.log(`[push] +sub (${subs.size} total) ${sub.endpoint.slice(0, 60)}…`);
    res.writeHead(204, cors); return res.end();
  }
  if (url.pathname === '/face-chat/push/test' && req.method === 'POST') {
    let body = ''; for await (const c of req) body += c;
    let payload = {}; try { payload = JSON.parse(body || '{}'); } catch {}
    await pushAll({ title: payload.title || 'fc test push', body: payload.body || 'manual test', thread: payload.thread || null });
    res.writeHead(200, cors); return res.end(`pushed to ${subs.size} subscriptions`);
  }
  if (url.pathname === '/face-chat/push/status' && req.method === 'GET') {
    res.writeHead(200, { ...cors, 'content-type': 'application/json' });
    return res.end(JSON.stringify({ vapid: !!vapid, subscriptions: subs.size, threads: [...threads.keys()] }));
  }
  res.writeHead(404, cors); res.end();
});

const wss = new WebSocketServer({ server: httpServer, path: '/face-chat/ws' });
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[relay] +client (${clients.size})`);
  const meta = [...threads.values()].map(t => ({ id: t.id, label: t.label, agent: t.agent }));
  const cursors = Object.fromEntries([...threads.values()].map(t => [t.id, t.cursor]));
  ws.send(JSON.stringify({ type: 'hello', threads: meta, cursor: cursors }));

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'subscribe') {
      // Per-thread resume. For each known thread, return delta-since OR fresh snapshot if stale.
      for (const t of threads.values()) {
        const since = msg.since?.[t.id];
        if (typeof since === 'number') {
          const r = t.since(since);
          if (!r.stale) {
            ws.send(JSON.stringify({ type: 'append', thread: t.id, content: r.content, cursor: r.cursor }));
            continue;
          }
        }
        ws.send(JSON.stringify({ type: 'snapshot', thread: t.id, content: t.snapshot(), cursor: t.cursor }));
      }
    } else if (msg.type === 'input') {
      const t = threads.get(msg.thread); if (!t) return;
      try { t.sendInput(msg.text || '', msg.enter !== false); }
      catch (e) { console.error(`[${t.id}] sendInput:`, e.message); }
    } else if (msg.type === 'special') {
      const t = threads.get(msg.thread); if (!t) return;
      try { t.sendSpecial(msg.key || 'Enter'); }
      catch (e) { console.error(`[${t.id}] sendSpecial:`, e.message); }
    }
  });

  ws.on('close', () => { clients.delete(ws); console.log(`[relay] -client (${clients.size})`); });
});
httpServer.listen(PORT, HOST, () => {
  console.log(`[relay] listen ${HOST}:${PORT}  ws=/face-chat/ws  http=/face-chat/push/{vapid,subscribe,test,status}  threads=[${[...threads.keys()].join(',')}]  idle=${IDLE_MS}ms`);
});

process.on('SIGHUP', () => { console.log('[relay] SIGHUP — reloading threads'); try { loadThreads(); } catch (e) { console.error(e); } });
function shutdown() {
  for (const t of threads.values()) {
    trySh(`tmux pipe-pane -t ${t.tmux}`);
    try { unlinkSync(t.pipe); } catch {}
  }
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
