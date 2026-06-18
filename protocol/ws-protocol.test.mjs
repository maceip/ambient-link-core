// Conformance test for the core relay protocol (HTTP + WS) defined in PROTOCOL.md.
// Vendor-neutral: runs against a live host daemon, not any glasses vendor.
// Run: node --test protocol/ws-protocol.test.mjs
//   AMBIENT_HOST / AMBIENT_WS override the target (default local host on :5181).
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';

const HOST = process.env.AMBIENT_HOST || 'http://127.0.0.1:5181';
const WS = process.env.AMBIENT_WS || 'ws://127.0.0.1:5181/ambient-link/ws';

async function get(path) {
  const res = await fetch(HOST + path);
  if (!res.ok) throw new Error(path + ' ' + res.status);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(HOST + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json().catch(() => ({}));
}

function once(ws, ev, ms = 8000) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout ' + ev)), ms);
    ws.addEventListener(ev, (e) => { clearTimeout(t); resolve(e); }, { once: true });
    ws.addEventListener('error', (e) => { clearTimeout(t); reject(e); }, { once: true });
  });
}

function waitForType(ws, type, deadlineMs = 6000) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + deadlineMs;
    function onMsg(ev) {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === type) {
          cleanup();
          resolve(msg);
        }
      } catch (_) { /* ignore */ }
    }
    function onTick() {
      if (Date.now() > deadline) {
        cleanup();
        reject(new Error('no ' + type + ' within ' + deadlineMs + 'ms'));
      }
    }
    function cleanup() {
      ws.removeEventListener('message', onMsg);
      clearInterval(timer);
    }
    ws.addEventListener('message', onMsg);
    const timer = setInterval(onTick, 100);
  });
}

describe('host HTTP', () => {
  it('healthz responds', async () => {
    const res = await fetch(HOST + '/healthz');
    assert.equal(res.status, 200);
  });

  it('status includes journal and relay_debug fields', async () => {
    const data = await get('/ambient-link/status');
    assert.ok('journal' in data);
    assert.ok('relay_debug' in data);
    assert.ok(Array.isArray(data.sessions));
    assert.ok(Array.isArray(data.delivery));
    assert.ok(Array.isArray(data.outbox));
  });

  it('sessions endpoint returns JSON instead of the SPA shell', async () => {
    const data = await get('/ambient-link/sessions');
    assert.ok(Array.isArray(data.sessions));
    assert.equal(typeof data.now, 'number');
  });

  it('root redirects to ambient-link app base', async () => {
    const res = await fetch(HOST + '/', { redirect: 'manual' });
    assert.equal(res.status, 302);
    assert.equal(res.headers.get('location'), '/ambient-link/');
  });

  it('index is served from ambient-link app base', async () => {
    const res = await fetch(HOST + '/ambient-link/');
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.match(html, /id="host-panel"/);
  });
});

describe('WS protocol', () => {
  let status;
  before(async () => {
    status = await get('/ambient-link/status');
  });

  it('hello includes threads and relay_debug flag', async () => {
    const ws = new WebSocket(WS);
    await once(ws, 'open');
    ws.send(JSON.stringify({ type: 'subscribe', since: { journal: status.journal || 0 } }));
    const ev = await once(ws, 'message');
    const hello = JSON.parse(ev.data);
    assert.equal(hello.type, 'hello');
    assert.ok(Array.isArray(hello.threads));
    assert.equal(!!hello.relay_debug, !!status.relay_debug);
    ws.close();
  });

  it('debug/yank delivers hud_yank to subscriber', async () => {
    const sess = (status.sessions || []).find((s) => s.state !== 'DEAD');
    const thread = sess ? sess.thread_id : 'web-proto-test';
    const ws = new WebSocket(WS);
    await once(ws, 'open');
    ws.send(JSON.stringify({ type: 'subscribe', since: { journal: status.journal || 0 } }));
    await once(ws, 'message'); // hello
    const marker = 'web-yank-' + Date.now();
    const cardP = waitForType(ws, 'hud_yank', 12000);
    await post('/ambient-link/debug/yank', {
      thread,
      label: 'web-test',
      agent: sess ? sess.agent : 'cursor',
      awaiting: 'question',
      lastAssistant: marker,
    });
    const card = await cardP;
    assert.equal(card.thread, thread);
    assert.equal(card.awaiting, 'question');
    assert.match(card.lastAssistant || '', new RegExp(marker));
    ws.close();
  });

  it('hud_yank request returns card to same client when mux has thread', async () => {
    const sess = (status.sessions || []).find((s) => s.state !== 'DEAD');
    if (!sess) return; // skip when no live agent session
    const thread = sess.thread_id;
    const ws = new WebSocket(WS);
    await once(ws, 'open');
    ws.send(JSON.stringify({ type: 'subscribe', since: { journal: status.journal || 0 } }));
    await once(ws, 'message'); // hello
    ws.send(JSON.stringify({ type: 'hud_yank', thread }));
    const card = await waitForType(ws, 'hud_yank', 8000);
    assert.equal(card.thread, thread);
    ws.close();
  });

  it('input frame returns quiet status without closing socket', async () => {
    const thread = 'web-input-test-' + Date.now();
    const id = 'client-' + Date.now();
    const ws = new WebSocket(WS);
    await once(ws, 'open');
    ws.send(JSON.stringify({ type: 'subscribe', since: { journal: status.journal || 0 } }));
    await once(ws, 'message');
    const statusP = waitForType(ws, 'input_status', 8000);
    ws.send(JSON.stringify({ type: 'input', thread, text: 'web-protocol-ping', enter: true, client_id: id }));
    const inputStatus = await statusP;
    assert.equal(inputStatus.id, id);
    assert.equal(inputStatus.status, 'failed');
    assert.equal(ws.readyState, WebSocket.OPEN);
    ws.close();
  });
});
