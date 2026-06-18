// Connect to a relay WS exactly like a glasses client does and print hello.threads.
// Vendor-neutral smoke check for the core relay protocol.
// Run: WS=wss://public.computer/ambient-link/ws node tools/ws-check.mjs
const WS = process.env.WS || 'wss://public.computer/ambient-link/ws';
const ws = new WebSocket(WS);
const t = setTimeout(() => { console.error('timeout: no hello'); process.exit(2); }, 10000);
ws.addEventListener('open', () => console.log('[ws] open ->', WS));
ws.addEventListener('message', (ev) => {
  let m; try { m = JSON.parse(ev.data); } catch { return; }
  if (m.type === 'hello') {
    clearTimeout(t);
    const threads = m.threads || [];
    console.log(`[ws] hello: ${threads.length} thread(s)`);
    for (const th of threads) console.log('  -', JSON.stringify({ id: th.id, label: th.label, agent: th.agent }));
    ws.close();
    process.exit(0);
  }
});
ws.addEventListener('error', (e) => { console.error('[ws] error', e.message || e); process.exit(1); });
