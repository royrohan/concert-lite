#!/usr/bin/env node
// Renders the getting-started GIFs (docs/getting-started/*.gif) from the real example files and captured command
// output, so they can be regenerated when the example changes:
//   node docs/getting-started/render/render.js [step numbers...]
// Needs Playwright (with Chromium) and ffmpeg. Inputs: examples/library, showcases/library and render/captured/
// (command output and a dashboard screenshot recorded from a real run; see README.md in this directory).
'use strict';
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { chromium } = require('playwright');

const HERE = __dirname;
const ROOT = path.resolve(HERE, '../../..');
const OUT = path.resolve(HERE, '..');
const CAP = path.join(HERE, 'captured');
const TOTAL = 8;
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8').replace(/\s+$/, '');
const cap = f => fs.readFileSync(path.join(CAP, f), 'utf8').replace(/\s+$/, '');
const lines = s => s.split('\n');

// ---------------------------------------------------------------------------------------------- timeline helpers

/** Line diff (LCS) as keep / del / ins operations. */
function diff(a, b) {
  const n = a.length, m = b.length, t = Array.from({ length: n + 1 }, () => new Int32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) t[i][j] = a[i] === b[j] ? t[i + 1][j + 1] + 1 : Math.max(t[i + 1][j], t[i][j + 1]);
  const ops = [];
  let i = 0, j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && a[i] === b[j]) { ops.push({ op: 'keep', text: a[i] }); i++; j++; }
    else if (j < m && (i === n || t[i][j + 1] >= t[i + 1][j])) { ops.push({ op: 'ins', text: b[j] }); j++; }
    else { ops.push({ op: 'del', text: a[i] }); i++; }
  }
  return ops;
}

/**
 * Editor frames that turn file a into file b one change at a time: deletions vanish, insertions are typed
 * `per` lines a frame and stay highlighted; the view follows the caret.
 */
function editFrames(base, a, b, { per = 1, rows = 24, dur = 0.09 } = {}) {
  const ops = diff(lines(a), lines(b));
  const insIdx = ops.map((o, k) => o.op === 'ins' ? k : -1).filter(k => k >= 0);
  const frames = [];
  const view = (revealed, caretOp) => {
    const out = [], ins = [];
    let caret = -1;
    ops.forEach((o, k) => {
      const done = o.op === 'ins' ? revealed.has(k) : o.op === 'del' ? !delKept(k, revealed) : true;
      if (o.op === 'keep' || (o.op === 'ins' && done)) {
        if (o.op === 'ins') ins.push(out.length);
        if (k === caretOp) caret = out.length;
        out.push(o.text);
      } else if (o.op === 'del' && !done) out.push(o.text);
    });
    return { lines: out, ins, caret, scroll: Math.max(0, caret - Math.floor(rows / 2)) };
  };
  // a deletion goes when the first insertion after it (in its hunk) starts, or right away when the hunk has none
  const delKept = (k, revealed) => {
    for (let x = k + 1; x < ops.length && ops[x].op !== 'keep'; x++) if (ops[x].op === 'ins') return !revealed.has(x);
    return false;
  };
  const revealed = new Set();
  frames.push({ state: { ...base, editor: { ...base.editor, ...view(revealed, -1), caret: -1 } }, dur: 1.2 });
  for (let k = 0; k < insIdx.length; k += per) {
    insIdx.slice(k, k + per).forEach(x => revealed.add(x));
    const v = view(revealed, insIdx[Math.min(k + per, insIdx.length) - 1]);
    frames.push({ state: { ...base, editor: { ...base.editor, ...v } }, dur });
  }
  return frames;
}

/** Editor frames typing `body` (array of line items) after `head`, `per` lines a frame. */
function typeFrames(base, head, body, { per = 2, rows = 24, dur = 0.09 } = {}) {
  const frames = [];
  for (let k = 0; k < body.length + per; k += per) {
    const shown = head.concat(body.slice(0, Math.min(k, body.length)));
    const caret = shown.length - 1;
    frames.push({ state: { ...base, editor: { ...base.editor, lines: shown, caret, ins: [], scroll: Math.max(0, caret - rows + 3) } }, dur });
  }
  return frames;
}

/** Terminal frames: the command typed, then its output `per` lines a frame. */
function termFrames(base, history, cmd, output, { per = 2, dur = 0.08, cls = l => '' } = {}) {
  const frames = [];
  for (let c = 0; c <= cmd.length; c += 6) {
    frames.push({ state: { ...base, terminal: { ...base.terminal, lines: [...history, { cmd: cmd.slice(0, c), caret: true }] } }, dur: 0.05 });
  }
  const out = lines(output).map(text => ({ text, cls: cls(text) }));
  for (let k = 0; k < out.length + per; k += per) {
    frames.push({ state: { ...base, terminal: { ...base.terminal, lines: [...history, { cmd }, ...out.slice(0, Math.min(k, out.length))] } }, dur });
  }
  const last = frames.at(-1).state;
  return { frames, history: last.terminal.lines };
}

const hold = (frames, sec) => { frames.at(-1).dur = sec; return frames; };
const outCls = t => /PASSED|BUILD SUCCESSFUL|^ok:| ok$|deployed|Started|Healthy|created/.test(t) ? 'ok'
  : /FAILED|error/i.test(t) ? 'warn' : /^\s*(>|==>)/.test(t) ? 'dim' : '';

function step(n, title, caption) { return { step: n, total: TOTAL, title, caption }; }

// ----------------------------------------------------------------------------------------------------- the steps

const MODEL = read('examples/library/loan.pure');
const PLAIN = MODEL
  .replace(/Class <<concert::sm\.root>>\n\{[\s\S]*?\n\}\n(library::Loan)/, 'Class $1')
  .replace(/Class <<concert::sm\.command>> /g, 'Class ')
  .replace(/\n\/\/ The getting-started guide.*\n/, '\n');
const PURE_TREE = [
  { name: 'concert-lite', cls: 'dir' }, { name: 'examples', depth: 1, cls: 'dir' }, { name: 'insurance', depth: 2, cls: 'dir' },
  { name: 'library', depth: 2, cls: 'dir' }, { name: 'loan.pure', depth: 3, cls: 'on' }, { name: 'order-events', depth: 2, cls: 'dir' },
  { name: 'trading-gen', depth: 2, cls: 'dir' }, { name: 'showcases', depth: 1, cls: 'dir' }, { name: 'core', depth: 1, cls: 'dir' },
  { name: 'platform', depth: 1, cls: 'dir' },
];
const pureEditor = { tab: 'loan.pure', tree: PURE_TREE };
const term = title => ({ view: 'terminal', terminal: { title: title || 'Terminal — concert-lite', lines: [] } });

function step1() {
  const base = { ...step(1, 'Write your model in Pure',
    'Create <code>examples/library/loan.pure</code>: the entity (<code>Loan</code>), its status enum and one class per ' +
    'command that changes it. Plain Pure: nothing Concert-specific yet.'), view: 'editor', editor: pureEditor };
  const frames = hold(typeFrames(base, [], lines(PLAIN), { per: 2 }), 1.5);
  const all = lines(PLAIN);
  frames.push({ state: { ...base, editor: { ...pureEditor, lines: all, caret: -1, ins: [], scroll: all.findIndex(l => l.startsWith('Enum')) - 1 } }, dur: 3.5 });
  return frames;
}

function step2() {
  const base = { ...step(2, 'Write your state machine in Pure',
    'Mark <code>Loan</code> <code>&lt;&lt;concert::sm.root&gt;&gt;</code> and declare its type, initial state and ' +
    '<code>FROM -event-&gt; TO : Command</code> transitions; mark each command <code>&lt;&lt;concert::sm.command&gt;&gt;</code>.'),
  view: 'editor', editor: pureEditor };
  const frames = editFrames(base, PLAIN, MODEL, { per: 1, dur: 0.14 });
  hold(frames, 1.5);
  // the whole machine declaration, its inserted lines still highlighted
  const last = frames.at(-1).state.editor;
  frames.push({ state: { ...base, editor: { ...last, caret: -1, scroll: last.lines.findIndex(l => l.includes('sm.root')) - 2 } }, dur: 4 });
  const t = { ...base, ...term() };
  frames.push(...hold(termFrames(t, [], './generate-concert-ecosystem check examples/library', cap('check.txt'), { cls: outCls }).frames, 3));
  return frames;
}

function step3() {
  const base = step(3, 'Run the generate task',
    'In the IDE’s Gradle panel double-click <b>Tasks › concert › generateEcosystems</b> (or run ' +
    '<code>./generate-concert-ecosystem examples/library --name library</code>). It writes <code>showcases/library</code>.');
  const nodes = [
    { name: '▾ concert-lite', depth: 0 }, { name: '▾ Tasks', depth: 1 }, { name: '▸ application', depth: 2 },
    { name: '▸ build', depth: 2 }, { name: '▾ concert', depth: 2 }, { name: '⚙ generateEcosystems', depth: 3 },
    { name: '▸ help', depth: 2 }, { name: '▸ verification', depth: 2 }, { name: '▸ Dependencies', depth: 1 },
  ];
  const ed = { tab: 'loan.pure', lines: lines(MODEL), scroll: 8, caret: -1, ins: [] };
  const g = (selected, consoleLines) => ({ ...base, view: 'gradle', gradle: { nodes, selected, editor: ed, console: consoleLines } });
  const target = '.gradle .n[data-i="5"]';
  const frames = [];
  for (let k = 0; k <= 12; k++) {
    frames.push({ state: { ...g(-1, []), cursor: { from: { x: 420, y: 160 }, sel: target, t: k / 12 } }, dur: 0.06 });
  }
  frames.push({ state: { ...g(5, []), cursor: { sel: target, t: 1, click: true } }, dur: 0.35 });
  const out = ['> Task :ecosystem-gen:generateEcosystems', 'generate showcases/library from examples/library', ...lines(cap('generate.txt')),
    'new ecosystems are added to settings.gradle.kts: reload the Gradle project in the IDE', '', 'BUILD SUCCESSFUL in 3s'];
  for (let k = 1; k <= out.length; k += 2) {
    frames.push({ state: { ...g(5, out.slice(0, k).map(text => ({ text, cls: outCls(text) }))), cursor: { sel: target, t: 1 } }, dur: 0.08 });
  }
  hold(frames, 2.5);
  // what it wrote: the module in the project tree, the regenerated spec open
  const tree = [
    { name: 'showcases', cls: 'dir' }, { name: 'library', depth: 1, cls: 'dir' }, { name: 'samples', depth: 2, cls: 'new' },
    { name: 'src/main/java/…/library', depth: 2, cls: 'dir' }, { name: 'LibraryEvents.java', depth: 3, cls: 'new' },
    { name: 'LibraryMachines.java', depth: 3, cls: 'new' }, { name: 'LibraryModels.java', depth: 3, cls: 'new' },
    { name: 'LibraryWorkerMain.java', depth: 3, cls: 'new' }, { name: 'LoanMachine.java  (yours)', depth: 3, cls: 'new' },
    { name: 'LoanSpec.java', depth: 3, cls: 'on' }, { name: 'src/main/pure', depth: 2, cls: 'new' },
    { name: 'src/test/java/…', depth: 2, cls: 'dir' }, { name: 'LibraryGeneratedTest.java', depth: 3, cls: 'new' },
    { name: 'build.gradle.kts', depth: 2, cls: 'new' }, { name: 'compose.yml', depth: 2, cls: 'new' },
    { name: 'send, send-flow', depth: 2, cls: 'new' }, { name: 'ecosystem.json', depth: 2, cls: 'new' },
  ];
  const spec = lines(read('showcases/library/src/main/java/io/concert/eco/library/LoanSpec.java'));
  const at = Math.max(0, spec.findIndex(l => l.includes('SPEC')) - 4);
  frames.push({ state: { ...base, view: 'editor', editor: { tab: 'LoanSpec.java  (regenerated)', tree, lines: spec, scroll: at, caret: -1, ins: [] } }, dur: 4 });
  return frames;
}

function step4() {
  const base = { ...step(4, 'Implement the state machine',
    'Open <code>LoanMachine.java</code> (yours: generated once, never overwritten) and fill in <code>onTransition</code>: ' +
    'apply each command to the loan and <code>reject(…)</code> what breaks a rule.'),
  view: 'editor', editor: { tab: 'LoanMachine.java', tree: null } };
  const stub = cap('LoanMachine.stub.java');
  const impl = read('showcases/library/src/main/java/io/concert/eco/library/LoanMachine.java');
  const frames = editFrames(base, stub, impl, { per: 1, dur: 0.11 });
  // start on the TODOs, end on the whole method
  frames[0].state.editor.scroll = lines(stub).findIndex(l => l.includes('protected void onTransition')) - 3;
  frames[0].dur = 2;
  return hold(frames, 4);
}

/** A file shown with folded regions: keep = [from, to) line ranges (0-based), the rest folded with a label. */
function folded(file, keep, labels) {
  const src = lines(read(file));
  const out = [];
  let pos = 0;
  keep.forEach(([from, to], i) => {
    if (from > pos) out.push({ text: labels[i] || '…', fold: true });
    for (let k = from; k < to; k++) out.push({ text: src[k], no: k + 1 });
    pos = to;
  });
  if (pos < src.length) out.push({ text: labels[keep.length] || '…', fold: true });
  return out;
}
const lineOf = (file, needle, from = 0) => lines(read(file)).findIndex((l, i) => i >= from && l.includes(needle));

function step5() {
  const f = 'showcases/library/src/test/java/io/concert/eco/library/LoanMachineTest.java';
  const cls = lineOf(f, '/** The loan machine'), firstTest = lineOf(f, '@Test'), helpers = lineOf(f, 'private TransitionResult send');
  const shown = folded(f, [[0, 1], [cls, cls + 2], [firstTest - 1, helpers - 1]],
    ['', 'imports', 'fields, @BeforeEach / @AfterEach: Temporal’s test server, a LoanMachine worker, an in-memory store',
      'send(...) and data(...) helpers']);
  const head = shown.slice(0, 5);
  const base = { ...step(5, 'Write your tests',
    'Unit-test the rules in <code>src/test</code>: events go through the real workflow on Temporal’s test server. ' +
    'The generated <code>LibraryGeneratedTest</code> already plays every sample flow.'),
  view: 'editor', editor: { tab: 'LoanMachineTest.java', tree: null } };
  const frames = hold(typeFrames(base, head, shown.slice(5), { per: 3, rows: 24 }), 2.5);
  frames.push(...hold(termFrames({ ...base, ...term() }, [], './gradlew :library:test', cap('test.txt'), { per: 1, dur: 0.18, cls: outCls }).frames, 3.5));
  return frames;
}

function step6() {
  const f = 'showcases/library/src/test/java/io/concert/eco/library/LoanFlowIT.java';
  const tag = lineOf(f, '@Tag("integration")'), t1 = lineOf(f, '@Test'), t2 = lineOf(f, '@Test', t1 + 1);
  const send = lineOf(f, '/** Puts one event'), sendEnd = lineOf(f, 'Thread.sleep(200)') + 2;
  const shown = folded(f, [[tag, tag + 2], [t1, t2 - 1], [send, sendEnd]],
    ['package, imports, javadoc', 'fields', 'aRejectedRenewalLeavesTheLoanAsItWas()', 'awaitState(...): polls /api/entities/loan:<id> on the dashboard']);
  const base = { ...step(6, 'Write your integration test',
    'Tag it <code>@Tag("integration")</code>: it puts events on the flow’s Kinesis stream and reads the loan back from the dashboard. ' +
    '<code>extra.gradle.kts</code> gives it its own task; it runs against the stack from steps 7–8.'),
  view: 'editor', editor: { tab: 'LoanFlowIT.java', tree: null } };
  const frames = hold(typeFrames(base, [], shown, { per: 3, rows: 24 }), 2.5);
  const extra = lines(read('showcases/library/extra.gradle.kts'));
  frames.push(...hold(typeFrames({ ...base, editor: { tab: 'extra.gradle.kts', tree: null } }, [], extra, { per: 2 }), 3.5));
  return frames;
}

function step7() {
  const base = { ...step(7, 'Deploy with code',
    '<code>./deploy-concert-ecosystem library</code> builds the module, starts the flow’s coordinator and worker ' +
    'containers with the platform (Temporal, Kinesis, Postgres, dashboard), waits for health and prints the links.'),
  ...term() };
  const out = cap('deploy.txt');
  return hold(termFrames(base, [], './deploy-concert-ecosystem library --no-analytics', out, { per: 3, dur: 0.07, cls: outCls }).frames, 5);
}

function step8() {
  const base = { ...step(8, 'Start the containers and send events',
    'The flow runs in its own containers. Send the sample flow, watch the loan reach <code>RETURNED</code> in the ' +
    'dashboard, then run the integration test against the running stack.'), ...term() };
  const frames = [];
  let r = termFrames(base, [], 'docker compose -f docker-compose.yml -f showcases/library/compose.yml ps', cap('compose-ps.txt'),
    { per: 2, cls: t => /library/.test(t) ? 'ok' : '' });
  frames.push(...hold(r.frames, 2.5));
  r = termFrames(base, r.history.slice(-4), './showcases/library/send-flow all', cap('send-flow.txt'), { per: 1, dur: 0.3, cls: outCls });
  frames.push(...hold(r.frames, 2));
  const img = 'data:image/png;base64,' + fs.readFileSync(path.join(CAP, 'dashboard-entity.png')).toString('base64');
  const b = (scroll, box) => ({ ...base, view: 'browser', browser: { url: 'http://localhost:8088/#entity/loan:LOAN-1001', img, scroll, box } });
  frames.push({ state: b(0, { x: 20, y: 160, w: 300, h: 26 }), dur: 2.5 });
  for (let s = 0; s <= 300; s += 50) frames.push({ state: b(s, null), dur: 0.08 });
  frames.push({ state: b(300, { x: 32, y: 716, w: 124, h: 56 }), dur: 3 });
  r = termFrames(base, [], './gradlew :library:integrationTest', cap('it.txt'), { per: 1, dur: 0.3, cls: outCls });
  frames.push(...hold(r.frames, 4));
  return frames;
}

const STEPS = [
  [1, '01-model', step1], [2, '02-state-machine', step2], [3, '03-generate', step3], [4, '04-implement', step4],
  [5, '05-tests', step5], [6, '06-integration-test', step6], [7, '07-deploy', step7], [8, '08-run', step8],
];

// ------------------------------------------------------------------------------------------------------- render

(async () => {
  const only = process.argv.slice(2).map(Number);
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1000, height: 620 }, deviceScaleFactor: 1 });
  await page.goto('file://' + path.join(HERE, 'page.html'));
  for (const [n, name, build] of STEPS) {
    if (only.length && !only.includes(n)) continue;
    const frames = build();
    const dir = fs.mkdtempSync(path.join(require('os').tmpdir(), 'gs-'));
    const list = [];
    for (let i = 0; i < frames.length; i++) {
      await page.evaluate(s => window.render(s), frames[i].state);
      const png = path.join(dir, `f${String(i).padStart(4, '0')}.png`);
      await page.screenshot({ path: png });
      list.push(`file '${png}'`, `duration ${frames[i].dur.toFixed(2)}`);
    }
    list.push(`file '${path.join(dir, `f${String(frames.length - 1).padStart(4, '0')}.png`)}'`);
    fs.writeFileSync(path.join(dir, 'list.txt'), list.join('\n'));
    const gif = path.join(OUT, `${name}.gif`);
    execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'concat', '-safe', '0', '-i', path.join(dir, 'list.txt'),
      '-vf', 'fps=10,split[a][b];[a]palettegen=max_colors=128:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle',
      '-loop', '0', gif]);
    fs.rmSync(dir, { recursive: true });
    const secs = frames.reduce((s, f) => s + f.dur, 0);
    console.log(`${name}.gif  ${frames.length} frames, ${secs.toFixed(1)} s, ${(fs.statSync(gif).size / 1024).toFixed(0)} KB`);
  }
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
