// relay-bridge: mirror live local relay sessions -> a remote relay so glasses
// surfaces (which only talk to the remote relay) see your real agent sessions.
//
// Vendor-neutral: any vendor app (meta / google / snapchat) reads the same
// remote relay this script feeds. No deps: Node 22+ ships global fetch + WebSocket.
// Run: node tools/relay-bridge.mjs

const LOCAL_HTTP = process.env.LOCAL_HTTP || 'http://127.0.0.1:5181';
const LOCAL_WS = process.env.LOCAL_WS || 'ws://127.0.0.1:5181/ambient-link/ws';
const REMOTE = process.env.REMOTE || 'https://public.computer';
const POLL_MS = Number(process.env.POLL_MS || 15000);

async function getJSON(base, path) {
  const res = await fetch(base + path);
  if (!res.ok) throw new Error(path + ' ' + res.status);
  return res.json();
}

async function postRemote(path, body) {
  const res = await fetch(REMOTE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok && res.status !== 204) {
    throw new Error(path + ' ' + res.status + ' ' + (await res.text().catch(() => '')));
  }
}

// Pull the latest hud_yank card per thread (carries lastAssistant preview text).
function pullCards(threadIds, journal) {
  return new Promise((resolve) => {
    const cards = {};
    if (!threadIds.length) return resolve(cards);
    let ws;
    try { ws = new WebSocket(LOCAL_WS); } catch { return resolve(cards); }
    const finish = () => { try { ws.close(); } catch {} resolve(cards); };
    const timer = setTimeout(finish, 4000);
    ws.addEventListener('open', () => {
      ws.send(JSON.stringify({ type: 'subscribe', since: { journal: journal || 0 } }));
    });
    ws.addEventListener('message', (ev) => {
      let m;
      try { m = JSON.parse(ev.data); } catch { return; }
      if (m.type === 'hello') {
        threadIds.forEach((th) => ws.send(JSON.stringify({ type: 'hud_yank', thread: th })));
      } else if (m.type === 'hud_yank' && m.thread) {
        cards[m.thread] = m;
      }
    });
    ws.addEventListener('error', () => { clearTimeout(timer); finish(); });
  });
}

function previewFor(session, card) {
  if (card && card.lastAssistant) return String(card.lastAssistant).slice(0, 200);
  if (session.state === 'BUSY') return 'thinking…';
  return '';
}

async function cycle() {
  const [sessData, status] = await Promise.all([
    getJSON(LOCAL_HTTP, '/ambient-link/sessions'),
    getJSON(LOCAL_HTTP, '/ambient-link/status'),
  ]);
  const live = (sessData.sessions || []).filter((s) => s.state !== 'DEAD');
  const cards = await pullCards(live.map((s) => s.thread_id), status.journal);

  let pushed = 0;
  for (const s of live) {
    const message = previewFor(s, cards[s.thread_id]);
    try {
      await postRemote('/ambient-link/ingest', {
        source: 'virtual',
        session_id: s.session_id,
        agent: s.agent,
        cwd: s.cwd,
        event_type: 'assistant_message',
        payload: { message },
        observed_at: Date.now(),
      });
      pushed++;
    } catch (e) {
      console.error('[bridge] push failed', s.label, e.message);
    }
  }
  console.log(`[bridge] ${new Date().toISOString()} mirrored ${pushed}/${live.length} live sessions -> ${REMOTE}`);
}

async function main() {
  console.log(`[bridge] local=${LOCAL_HTTP} remote=${REMOTE} poll=${POLL_MS}ms`);
  for (;;) {
    try { await cycle(); } catch (e) { console.error('[bridge] cycle error', e.message); }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

main();
