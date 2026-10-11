// The demo page in headless Chromium, through the latency proxy: one page predicting, one not.
//
//   PAGE=http://127.0.0.1:3000/v1/database/predict/route/ npx tsx tests/browser.test.mts
//   SHOTS=docs  ...also saves docs/demo-{light,dark}.png
import assert from 'node:assert/strict';
import { chromium, type Page } from 'playwright';
import { startProxy } from './proxy.mts';

const PAGE = process.env.PAGE ?? 'http://127.0.0.1:3000/v1/database/predict/route/';
const DELAY = Number(process.env.DELAY ?? 150);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let passed = 0;
async function test(name: string, fn: () => Promise<void>) {
  await fn();
  passed++;
  console.log(`  ok  ${name}`);
}

const proxy = await startProxy(3101, 3000, DELAY);
const browser = await chromium.launch();
async function open(predictOn: boolean, scheme: 'light' | 'dark' = 'light'): Promise<Page> {
  const page = await (await browser.newContext({ viewport: { width: 900, height: 720 }, colorScheme: scheme })).newPage();
  page.on('pageerror', (e) => console.error('[page]', e.message));
  await page.goto(`${PAGE}?host=ws://127.0.0.1:3101&predict=${predictOn ? 'on' : 'off'}`);
  await page.waitForSelector('#add');
  // Record what every frame showed, to catch flicker a person could see.
  // (A string, not a function: tsx's transform would add helpers the page doesn't have.)
  await page.evaluate(`
    window.frames_ = [];
    const loop = () => {
      window.frames_.push([...document.querySelectorAll('#tasks li .title')].map((e) => e.textContent));
      requestAnimationFrame(loop);
    };
    requestAnimationFrame(loop);
  `);
  return page;
}
const lastMs = async (p: Page) => Number((await p.textContent('#last'))!.match(/after (\d+) ms/)![1]);
async function add(p: Page, title: string) {
  await p.fill('#add input', title);
  await p.press('#add input', 'Enter');
}

try {
  const fast = await open(true);
  const slow = await open(false);
  const tag = `t${Date.now() % 100000}`;

  await test('with prediction a new task shows within a frame or two; without, after the round trip', async () => {
    await add(fast, `${tag} fast`);
    await fast.waitForSelector(`#tasks li >> text=${tag} fast`);
    await fast.waitForSelector('#last >> text=/after \\d+ ms/');
    const f = await lastMs(fast);
    // Let the fast page's task reach the slow page first, so the slow page times its own add.
    await slow.waitForSelector(`#tasks li >> text=${tag} fast`);
    await sleep(100);
    await add(slow, `${tag} slow`);
    await slow.waitForSelector(`#tasks li >> text=${tag} slow`);
    await slow.waitForSelector('#last >> text=/after \\d+ ms/');
    const s = await lastMs(slow);
    console.log(`      click to visible: predicted ${f} ms, unpredicted ${s} ms (+${DELAY} ms each way)`);
    assert.ok(f < 50, `predicted ${f} ms`);
    assert.ok(s >= 2 * DELAY, `unpredicted ${s} ms`);
  });

  await test('the predicted row is replaced by the real one without a visible flicker or duplicate', async () => {
    await fast.evaluate('window.frames_ = []');
    await add(fast, `${tag} smooth`);
    await fast.waitForFunction(() => document.querySelectorAll('#tasks li.pending').length === 0);
    await sleep(100);
    const frames = (await fast.evaluate('window.frames_')) as (string | null)[][];
    const counts = frames.map((f) => f.filter((t) => t === `${tag} smooth`).length);
    const first = counts.indexOf(1);
    assert.ok(first >= 0, 'it showed');
    assert.ok(counts.slice(first).every((c) => c === 1), `once in every frame after it appeared: ${counts.join('')}`);
  });

  await test('toggling shows at once, and the other page catches up after the round trip', async () => {
    const row = fast.locator('#tasks li', { hasText: `${tag} smooth` });
    await row.locator('input[type=checkbox]').click();
    assert.equal(await row.locator('input[type=checkbox]').isChecked(), true);
    assert.match((await row.getAttribute('class'))!, /pending/);
    await slow.waitForSelector(`#tasks li.done >> text=${tag} smooth`);
    await fast.waitForFunction(() => document.querySelectorAll('#tasks li.pending').length === 0);
  });

  await test("a refused action rolls back and says why (6 adds in 10 s; the limit is 5)", async () => {
    for (let i = 1; i <= 6; i++) await add(fast, `${tag} burst ${i}`);
    await fast.waitForSelector('#toast.show >> text=slow down');
    await fast.waitForFunction(() => document.querySelectorAll('#tasks li.pending').length === 0);
    const titles = await fast.$$eval('#tasks li .title', (els) => els.map((e) => e.textContent));
    const burst = titles.filter((t) => t?.includes(' burst '));
    assert.ok(burst.length < 6, `the refused add is gone: ${burst.length} burst tasks left`);
  });

  if (process.env.SHOTS) {
    await sleep(3600);
    for (const scheme of ['light', 'dark'] as const) {
      const p = await open(true, scheme);
      await p.waitForSelector('#tasks li');
      await add(p, 'Write the launch post');
      await sleep(40); // mid-flight: the new task is still a prediction
      await p.screenshot({ path: `${process.env.SHOTS}/demo-${scheme}.png` });
    }
  }
} finally {
  await browser.close();
  proxy.close();
}
console.log(`\n${passed} passed`);
