const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = readFileSync(path.join(__dirname, '../app.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));

class Element {
  constructor() {
    this.value = '';
    this.children = [];
    this.listeners = {};
    this.attributes = {};
    this.classList = { toggle() {} };
  }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  emit(name, key) { this.listeners[name]?.({ key, preventDefault() {} }); }
  setCustomValidity(message) { this.validationMessage = message; }
  reportValidity() { this.reported = true; }
  blur() { this.emit('blur'); }
  setAttribute(name, value) { this.attributes[name] = value; }
  removeAttribute(name) { delete this.attributes[name]; }
  replaceChildren() { this.children = []; this.value = ''; }
  appendChild(child) { this.children.push(child); }
}

function snapshot(run_at) {
  const stamp = run_at.replaceAll('-', '').replaceAll(':', '').slice(0, 13) + 'Z';
  return { run_at, charts: {
    metric_svg: `snapshots/${stamp}_metric.svg`,
    imperial_svg: `snapshots/${stamp}_imperial.svg`
  } };
}
const history = [
  '2026-09-21T21:40:00Z', '2026-09-22T07:00:00Z',
  '2026-09-22T21:35:00Z', '2026-09-22T21:45:00Z',
  '2026-09-23T07:00:00Z', '2026-09-23T21:40:00Z', '2026-09-23T21:45:00Z'
].map(snapshot);

async function setup(snapshots = history) {
  const elements = Object.fromEntries([
    'chart', 'chartTitle', 'metricBtn', 'imperialBtn', 'prevSnapshotBtn', 'nextSnapshotBtn',
    'snapshotDay', 'snapshotTime'
  ].map(id => [id, new Element()]));
  const requests = {};
  vm.runInNewContext(source, {
    document: { getElementById: id => elements[id], createElement: () => new Element() },
    window: { location: { hostname: 'localhost' } },
    localStorage: { getItem() {}, setItem() {} },
    fetch: url => new Promise((resolve, reject) => {
      requests[url.split('?')[0]] = { resolve: data => resolve({ ok: true, json: async () => data }), reject };
    }),
    Date, Intl
  });
  if (snapshots instanceof Error) requests['./station_history.json'].reject(snapshots);
  else requests['./station_history.json'].resolve({ snapshots });
  await tick();
  return { ...elements, requests, enter(value, event = 'keydown') {
    elements.snapshotTime.value = value;
    elements.snapshotTime.emit('input');
    elements.snapshotTime.emit(event, 'Enter');
  } };
}

test('starts at the latest snapshot with one Pacific day per option', async () => {
  const ui = await setup([...history].reverse());
  assert.equal(ui.snapshotDay.value, '2026-09-23');
  assert.equal(ui.snapshotTime.value, '14:45');
  assert.deepEqual(ui.snapshotDay.children.map(o => o.textContent), ['Weds', 'Tues', 'Mon']);
  assert.equal(ui.nextSnapshotBtn.disabled, true);
  assert.equal(ui.prevSnapshotBtn.disabled, false);
});

test('direct entry, nearest available minute, blur, and compact input agree with the chart', async () => {
  const ui = await setup();
  for (const [input, expected, event] of [
    ['14:40', '14:40'], ['14:42', '14:40'], ['1444', '14:45', 'blur'], ['0:00', '00:00']
  ]) {
    ui.enter(input, event);
    assert.equal(ui.snapshotTime.value, expected);
    assert.match(ui.chartTitle.textContent, new RegExp('at ' + expected));
    assert.ok(ui.chart.src.includes('20260923T' + (expected === '00:00' ? '0700' : expected.replace(':', '').replace('14', '21')) + 'Z_metric.svg'));
  }
});

test('day changes preserve clock time and arrows synchronize across Pacific midnight', async () => {
  const ui = await setup();
  ui.snapshotDay.value = '2026-09-22';
  ui.snapshotDay.emit('change');
  assert.equal(ui.snapshotTime.value, '14:45');
  assert.match(ui.chart.src, /20260922T2145Z_metric/);
  ui.enter('0000');
  assert.equal(ui.snapshotTime.value, '00:00');
  ui.prevSnapshotBtn.emit('click');
  assert.equal(ui.snapshotDay.value, '2026-09-21');
  assert.equal(ui.snapshotTime.value, '14:40');
  assert.equal(ui.prevSnapshotBtn.disabled, true);
  ui.nextSnapshotBtn.emit('click');
  assert.equal(ui.snapshotDay.value, '2026-09-22');
  assert.equal(ui.snapshotTime.value, '00:00');
});

test('requests outside the available range stay on the selected day', async () => {
  const ui = await setup();
  ui.enter('23:59');
  assert.equal(ui.snapshotTime.value, '14:45');
  ui.snapshotDay.value = '2026-09-21';
  ui.snapshotDay.emit('change');
  ui.enter('00:00');
  assert.equal(ui.snapshotDay.value, '2026-09-21');
  assert.equal(ui.snapshotTime.value, '14:40');
});

test('invalid times never navigate; Escape and blur restore the actual time', async () => {
  const ui = await setup();
  const original = ui.chart.src;
  for (const invalid of ['', '24:00', '12:60', '99:99', '1:2', '-100', 'noon']) {
    ui.enter(invalid);
    assert.equal(ui.chart.src, original);
    assert.equal(ui.snapshotTime.attributes['aria-invalid'], 'true');
    assert.ok(ui.snapshotTime.reported);
  }
  ui.snapshotTime.emit('keydown', 'Escape');
  assert.equal(ui.snapshotTime.value, '14:45');
  assert.equal(ui.snapshotTime.validationMessage, '');
  ui.enter('oops', 'blur');
  assert.equal(ui.snapshotTime.value, '14:45');
});

test('unit changes retain the chosen snapshot and late latest-state fetch cannot replace its title', async () => {
  const ui = await setup();
  ui.snapshotDay.value = '2026-09-22';
  ui.snapshotDay.emit('change');
  ui.enter('14:35');
  ui.imperialBtn.emit('click');
  assert.match(ui.chart.src, /20260922T2135Z_imperial/);
  const title = ui.chartTitle.textContent;
  ui.requests['./station_state.json'].resolve({ generated_at: history.at(-1).run_at });
  await tick();
  assert.equal(ui.chartTitle.textContent, title);
  assert.equal(ui.snapshotDay.value, '2026-09-22');
  assert.equal(ui.snapshotTime.value, '14:35');
});

test('malformed snapshots are excluded and missing history keeps the latest chart usable', async () => {
  for (const input of [[], new Error('offline'), [snapshot('invalid'), {
    ...history[0], charts: { ...history[0].charts, metric_svg: 'https://other.example/chart.svg' }
  }]]) {
    const ui = await setup(input);
    assert.equal(ui.snapshotDay.disabled, true);
    assert.equal(ui.snapshotTime.disabled, true);
    ui.imperialBtn.emit('click');
    assert.match(ui.chart.src, /^\.\/sba_wwtemp_chart_imperial\.svg/);
  }
});

test('Pacific DST spring gap selects an available time and repeated fall times stay navigable', async () => {
  let ui = await setup(['2026-03-08T09:55:00Z', '2026-03-08T10:00:00Z'].map(snapshot));
  ui.enter('02:30');
  assert.equal(ui.snapshotTime.value, '03:00');
  ui = await setup(['2026-11-01T08:30:00Z', '2026-11-01T09:30:00Z'].map(snapshot));
  ui.enter('01:30');
  assert.match(ui.chart.src, /T0930Z/);
  ui.prevSnapshotBtn.emit('click');
  ui.enter('01:30');
  assert.match(ui.chart.src, /T0830Z/);
});
