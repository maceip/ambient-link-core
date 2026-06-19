// Load the DEPLOYED meta web app (public.computer/hud/) in headless Chrome exactly
// as the glasses would, wait for the session list to populate from the live relay,
// dump the rendered rows, and screenshot it as proof.
const { chromium } = require('playwright');

(async () => {
  const URL = process.env.HUD_URL || 'https://public.computer/hud/';
  const OUT = process.env.OUT || 'e2e-webapp.png';
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const page = await browser.newPage({ viewport: { width: 440, height: 940, deviceScaleFactor: 2 } });
  page.on('console', (m) => { if (m.type() === 'error') console.log('[page error]', m.text()); });

  console.log('[shot] goto', URL);
  await page.goto(URL, { waitUntil: 'networkidle', timeout: 45000 });

  let found = true;
  try {
    await page.waitForSelector('#threads .thread-row', { timeout: 25000 });
  } catch { found = false; }
  await page.waitForTimeout(2500); // let WS hello + syncFromHost settle

  const rows = await page.$$eval('#threads .thread-row', (els) =>
    els.map((e) => e.innerText.replace(/\s+/g, ' ').trim()));
  console.log('[shot] session rows rendered:', JSON.stringify(rows, null, 2));
  console.log('[shot] row selector matched:', found);

  await page.screenshot({ path: OUT, fullPage: true });
  console.log('[shot] saved', OUT);
  await browser.close();
  process.exit(rows.length ? 0 : 1);
})().catch((e) => { console.error('[shot] FAILED', e.message); process.exit(2); });
