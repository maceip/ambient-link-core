// relay-bridge: mirror live local relay sessions -> a remote relay so glasses
// surfaces (which only talk to the remote relay) see your real agent sessions.
//
// Vendor-neutral: any vendor app (meta / google / snapchat) reads the same
// remote relay this script feeds. No deps: Node 22+ ships global fetch + WebSocket.
// Run: node tools/relay-bridge.mjs

const LOCAL_HTTP = process.env.LOCAL_HTTP || 'http://127.0.0.1:5181';
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

// The REST snapshot now carries the preview/last_assistant directly (data-plane
// parity), so no WS card dance is needed — read it straight off /sessions.
function previewFor(session) {
  if (session.preview) return String(session.preview).slice(0, 200);
  if (session.last_assistant) return String(session.last_assistant).slice(0, 200);
  if (session.state === 'BUSY') return 'thinking…';
  return '';
}

async function cycle() {
  const sessData = await getJSON(LOCAL_HTTP, '/ambient-link/sessions');
  const live = (sessData.sessions || []).filter((s) => s.state !== 'DEAD');

  let pushed = 0;
  for (const s of live) {
    const message = previewFor(s);
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
