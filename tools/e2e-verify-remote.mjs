// One-shot: connect to the REMOTE relay WS exactly like the deployed web app does,
// subscribe, yank the session card, and print what the web app would render.
const REMOTE_WS = process.env.REMOTE_WS || 'wss://public.computer/ambient-link/ws';
const THREAD = process.env.THREAD || 'cursor-8ace8bf731';

const ws = new WebSocket(REMOTE_WS);
const seen = [];
const done = (code) => { try { ws.close(); } catch {} 
  console.log('\n=== cards seen on remote WS ===');
  for (const c of seen) console.log(JSON.stringify(c));
  process.exit(code);
};
const timer = setTimeout(() => done(seen.length ? 0 : 2), 8000);
ws.addEventListener('open', () => {
  ws.send(JSON.stringify({ type: 'subscribe', since: { journal: 0 } }));
});
ws.addEventListener('message', (ev) => {
  let m; try { m = JSON.parse(ev.data); } catch { return; }
  if (m.type === 'hello') {
    console.log('[remote] hello; sessions in hello:', (m.sessions || []).map(s => s.thread_id || s.threadId).join(', ') || '(none)');
    ws.send(JSON.stringify({ type: 'hud_yank', thread: THREAD }));
  } else if (m.type === 'hud_yank') {
    console.log('[remote] hud_yank card for', m.thread, '→ lastAssistant:', JSON.stringify((m.lastAssistant || '').slice(0, 160)));
    seen.push({ thread: m.thread, agent: m.agent, lastAssistant: (m.lastAssistant || '').slice(0, 160), awaiting: m.awaiting });
    clearTimeout(timer); setTimeout(() => done(0), 500);
  }
});
ws.addEventListener('error', (e) => { console.error('[remote] ws error', e.message || e); done(3); });
