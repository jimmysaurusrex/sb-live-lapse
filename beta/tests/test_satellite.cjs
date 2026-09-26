const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const source = readFileSync(path.join(__dirname, '../app.js'), 'utf8');
const html = readFileSync(path.join(__dirname, '../index.html'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));

class Element {
  constructor() {
    this.listeners = {};
    this.value = '';
    this.children = [];
    this.classList = { toggle() {} };
    this.attributes = {};
    this.hidden = true;
    this.complete = false;
    this.naturalWidth = 0;
  }
  set src(value) { this._src = value; this.complete = false; this.naturalWidth = 0; }
  get src() { return this._src; }
  addEventListener(name, listener) { (this.listeners[name] ||= []).push(listener); }
  emit(name, detail = {}) {
    for (const listener of this.listeners[name] || []) listener(detail);
    this['on' + name]?.(detail);
  }
  loaded() { this.complete = true; this.naturalWidth = 600; this.emit('load'); }
  replaceChildren() { this.children = []; }
  appendChild(child) { this.children.push(child); }
  setCustomValidity() {}
  setAttribute(name, value) { this.attributes[name] = value; }
  removeAttribute(name) { delete this.attributes[name]; if (name === 'src') this._src = undefined; }
}

function setup(connection = {}, observerSupported = true, cameras = false) {
  const ids = ['chart', 'chartTitle', 'metricBtn', 'imperialBtn', 'prevSnapshotBtn', 'nextSnapshotBtn',
    'snapshotDay', 'snapshotTime', 'satelliteSection', 'satelliteImage', 'satelliteStatus', 'loadSatellite', 'playSatellite', 'satelliteCaption'];
  if (cameras) for (const key of ['gibraltar', 'tvhill', 'ortega']) {
    ids.push(...['Section', 'Image', 'Status', 'Load', 'Caption'].map(suffix => key + suffix));
  }
  const elements = Object.fromEntries(ids.map(id => [id, new Element()]));
  const document = Object.assign(new Element(), {
    readyState: 'interactive', visibilityState: 'visible',
    getElementById: id => elements[id], createElement: () => new Element()
  });
  const window = Object.assign(new Element(), { location: { hostname: 'localhost' } });
  let intersection;
  const intersections = new Map();
  class Observer {
    constructor(callback) { this.callback = callback; }
    observe(element) {
      intersections.set(element, this.callback);
      if (element === elements.satelliteSection) intersection = this.callback;
    }
  }
  if (observerSupported) window.IntersectionObserver = Observer;
  const requests = [], timers = new Map(), revoked = [];
  let timerId = 0, urlId = 0;
  vm.runInNewContext(source, {
    document, window, navigator: { connection }, IntersectionObserver: Observer,
    AbortController, Date, Intl,
    URL: { createObjectURL: () => 'blob:satellite' + (++urlId), revokeObjectURL: value => revoked.push(value) },
    localStorage: { getItem() {}, setItem() {} },
    setTimeout: callback => { timers.set(++timerId, callback); return timerId; },
    clearTimeout: id => timers.delete(id),
    setInterval: () => 1,
    fetch: (url, options) => new Promise((resolve, reject) => {
      requests.push({ url, options, resolve, reject });
    })
  });
  const satelliteRequests = () => requests.filter(r => r.url.startsWith('./satellite/'));
  const flush = () => { const pending = [...timers.values()]; timers.clear(); pending.forEach(fn => fn()); };
  const history = { snapshots: [{ run_at: '2026-09-23T21:40:00Z', charts: {
    metric_svg: 'snapshots/20260923T2140Z_metric.svg', imperial_svg: 'snapshots/20260923T2140Z_imperial.svg'
  } }] };
  async function resolveData(kind) {
    const request = requests.find(r => r.url.startsWith('./station_' + kind));
    request.resolve({ ok: true, json: async () => kind === 'history' ? history : { generated_at: '2026-09-23T21:40:00Z' } });
    await tick();
  }
  async function ready() {
    await resolveData('history');
    await resolveData('state');
    elements.chart.loaded();
    window.emit('load');
    intersection?.([{ isIntersecting: true }]);
  }
  const manifest = { image: '20260924003021-visible-v1.jpg', observed_at: '2026-09-24T00:30:21Z',
    mode: 'visible', loop: '20260924003021-20260923233021-7-v1.gif', loop_bytes: 321400 };
  async function resolveManifest(data = manifest) {
    satelliteRequests().findLast(r => r.url.endsWith('latest.json')).resolve({ ok: true, json: async () => data });
    await tick();
  }
  async function resolveImage(type = 'image/jpeg') {
    satelliteRequests().at(-1).resolve({ ok: true, headers: { get: () => type }, blob: async () => ({}) });
    await tick();
    elements.satelliteImage.loaded();
  }
  async function loaded() {
    await ready(); flush(); await resolveManifest(); await resolveImage();
  }
  return { ...elements, window, document, requests, satelliteRequests, flush, resolveData, ready, revoked,
    resolveManifest, resolveImage, loaded,
    cameraVisible(key, value) { intersections.get(elements[key + 'Section'])?.([{ isIntersecting: value }]); },
    visible(value) { intersection?.([{ isIntersecting: value }]); } };
}

test('HTML cannot start a satellite download and contains no preload or connection hint', () => {
  const tag = html.match(/<img id="satelliteImage"[\s\S]*?>/)[0];
  assert.doesNotMatch(tag, /\s(?:src|srcset)\s*=/);
  assert.doesNotMatch(html, /rel="(?:preload|prefetch|preconnect|dns-prefetch)"/);
  assert.ok(html.indexOf('id="satelliteSection"') > html.indexOf('id="chart"'));
});

test('waits for window load, both data fetches, the selected chart, and an actual viewport intersection', async () => {
  const ui = setup();
  ui.visible(true);
  ui.chart.loaded();
  ui.window.emit('load');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0);
  await ui.resolveData('history');
  ui.chart.loaded();
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0, 'latest-state fetch still pending');
  ui.visible(false);
  await ui.resolveData('state');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0, 'section is below the viewport');
  ui.visible(true);
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
  assert.equal(ui.satelliteRequests()[0].options.priority, 'low');
  assert.equal(ui.satelliteRequests()[0].options.credentials, 'omit');
});

test('data readiness alone cannot bypass page and chart image loading', async () => {
  const ui = setup();
  await ui.resolveData('history');
  await ui.resolveData('state');
  ui.visible(true);
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0);
  ui.window.emit('load');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0);
  ui.chart.loaded();
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
});

test('changing the chart cancels a scheduled or active satellite request and waits for the new chart', async () => {
  const ui = setup();
  await ui.ready();
  ui.imperialBtn.emit('click');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0, 'timer must recheck the changed chart');
  ui.chart.loaded();
  ui.flush();
  const first = ui.satelliteRequests()[0];
  ui.metricBtn.emit('click');
  assert.equal(first.options.signal.aborted, true);
  first.reject(new Error('aborted'));
  await tick();
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
  ui.chart.loaded();
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 2);
});

test('slow and data-saving connections need an explicit tap after the chart is ready', async () => {
  for (const connection of [{ saveData: true }, { effectiveType: 'slow-2g' }, { effectiveType: '2g' }, { effectiveType: '3g' }]) {
    const ui = setup(connection);
    ui.loadSatellite.emit('click');
    ui.flush();
    assert.equal(ui.satelliteRequests().length, 0, 'even a tap cannot bypass core loading');
    const fresh = setup(connection);
    await fresh.ready();
    fresh.flush();
    assert.equal(fresh.satelliteRequests().length, 0);
    assert.equal(fresh.loadSatellite.hidden, false);
    assert.equal(fresh.loadSatellite.disabled, false);
    fresh.loadSatellite.emit('click');
    fresh.flush();
    assert.equal(fresh.satelliteRequests().length, 1);
  }
});

test('browser without intersection observation uses a manual load button', async () => {
  const ui = setup({}, false);
  await ui.ready();
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 0);
  assert.equal(ui.loadSatellite.hidden, false);
  ui.loadSatellite.emit('click');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
});

test('failed downloads do not retry automatically or affect the chart', async () => {
  const ui = setup();
  await ui.ready();
  ui.flush();
  const originalChart = ui.chart.src;
  ui.satelliteRequests()[0].reject(new Error('offline'));
  await tick();
  ui.visible(false);
  ui.visible(true);
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
  assert.equal(ui.chart.src, originalChart);
  assert.match(ui.satelliteStatus.textContent, /unavailable/);
  ui.loadSatellite.emit('click');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 2);
});

test('successful still loads only one local image and no loop until requested', async () => {
  const ui = setup();
  await ui.loaded();
  assert.equal(ui.satelliteImage.src, 'blob:satellite1');
  assert.equal(ui.satelliteImage.hidden, false);
  assert.equal(ui.satelliteStatus.hidden, true);
  assert.equal(ui.satelliteRequests().length, 2, 'one manifest plus one still');
  assert.ok(ui.satelliteRequests().every(r => r.url.startsWith('./satellite/')));
  assert.equal(ui.playSatellite.hidden, false);
  ui.imperialBtn.emit('click'); ui.chart.loaded(); ui.flush();
  assert.equal(ui.satelliteRequests().length, 2);
});

test('loop is opt-in, gives download size in a tooltip, stops for chart navigation, and reuses its download', async () => {
  const ui = setup(); await ui.loaded();
  assert.equal(ui.playSatellite.textContent, 'Play last hour');
  assert.match(ui.playSatellite.title, /314 KB/);
  ui.playSatellite.emit('click'); ui.flush();
  await tick();
  assert.match(ui.satelliteRequests().at(-1).url, /\.gif$/);
  await ui.resolveImage('image/gif');
  assert.equal(ui.satelliteImage.src, 'blob:satellite2');
  assert.equal(ui.playSatellite.textContent, 'Stop loop');
  ui.imperialBtn.emit('click');
  assert.equal(ui.satelliteImage.src, 'blob:satellite1');
  ui.chart.loaded(); ui.flush();
  ui.playSatellite.emit('click'); ui.flush();
  assert.equal(ui.satelliteImage.src, 'blob:satellite2');
  assert.equal(ui.satelliteRequests().length, 3);
  ui.playSatellite.emit('click');
  assert.equal(ui.satelliteImage.src, 'blob:satellite1');
});

test('changing charts aborts image and loop bodies as well as the metadata request', async () => {
  const ui = setup(); await ui.ready(); ui.flush(); await ui.resolveManifest();
  const still = ui.satelliteRequests().at(-1);
  ui.imperialBtn.emit('click');
  assert.equal(still.options.signal.aborted, true);
  still.resolve({ ok: true, headers: { get: () => 'image/jpeg' }, blob: async () => ({}) });
  await tick();
  assert.equal(ui.satelliteImage.src, undefined, 'late body cannot render');
  ui.chart.loaded(); ui.flush(); await ui.resolveManifest(); await ui.resolveImage();
  ui.playSatellite.emit('click'); ui.flush(); await tick();
  const loop = ui.satelliteRequests().at(-1);
  ui.metricBtn.emit('click');
  assert.equal(loop.options.signal.aborted, true);
  assert.equal(ui.satelliteImage.src, 'blob:satellite1');
});

test('unsafe manifest paths fail without sending image requests', async () => {
  const ui = setup(); await ui.ready(); ui.flush();
  await ui.resolveManifest({ image: 'https://example.com/image.jpg', observed_at: '2026-09-24T00:30:21Z', mode: 'visible' });
  assert.equal(ui.satelliteRequests().length, 1);
  assert.match(ui.satelliteStatus.textContent, /unavailable/);
});

test('a delayed satellite frame is explicitly labeled and keeps its own time', async () => {
  const ui = setup(); await ui.ready(); ui.flush();
  await ui.resolveManifest({ image: '20200101003021-night-v1.jpg', observed_at: '2020-01-01T00:30:21Z', mode: 'night', twilight: true });
  await ui.resolveImage();
  assert.match(ui.satelliteCaption.textContent, /delayed/);
  assert.match(ui.satelliteCaption.textContent, /Night low clouds/);
  assert.match(ui.satelliteCaption.textContent, /Twilight/);
  const caption = ui.satelliteCaption.textContent;
  ui.imperialBtn.emit('click'); ui.chart.loaded(); ui.flush();
  assert.equal(ui.satelliteCaption.textContent, caption);
});

test('hidden tabs pause downloads until visible again', async () => {
  const ui = setup();
  await ui.ready();
  ui.flush();
  ui.document.visibilityState = 'hidden';
  ui.document.emit('visibilitychange');
  assert.equal(ui.satelliteRequests()[0].options.signal.aborted, true);
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 1);
  ui.document.visibilityState = 'visible';
  ui.document.emit('visibilitychange');
  ui.flush();
  assert.equal(ui.satelliteRequests().length, 2);
});


test('all camera images have no eager source and follow the requested three-row order', () => {
  for (const key of ['gibraltar', 'tvhill', 'ortega']) {
    const tag = html.match(new RegExp('<img id="' + key + 'Image"[\\s\\S]*?>'))[0];
    assert.doesNotMatch(tag, /\s(?:src|srcset)\s*=/);
  }
  const ids = ['chart', 'satelliteSection', 'gibraltarSection', 'tvhillSection', 'ortegaSection'];
  const positions = ids.map(id => html.indexOf('id="' + id + '"'));
  assert.deepEqual(positions, [...positions].sort((a, b) => a - b));
});

test('each camera waits for page, chart, both data requests and its own viewport; navigation aborts all feeds', async () => {
  const ui = setup({}, true, true);
  for (const key of ['gibraltar', 'tvhill', 'ortega']) ui.cameraVisible(key, true);
  const requests = () => ui.requests.filter(r => r.url.startsWith('./cameras/'));
  ui.flush(); assert.equal(requests().length, 0);
  await ui.resolveData('history');
  ui.chart.loaded(); ui.window.emit('load'); ui.flush();
  assert.equal(requests().length, 0);
  ui.cameraVisible('ortega', false);
  await ui.resolveData('state'); ui.flush();
  assert.deepEqual(requests().map(r => r.url), ['./cameras/gibraltar.json', './cameras/tvhill.json']);
  ui.cameraVisible('ortega', true); ui.flush();
  assert.equal(requests().length, 3);
  const first = [...requests()];
  ui.imperialBtn.emit('click');
  assert.ok(first.every(r => r.options.signal.aborted));
  ui.flush(); assert.equal(requests().length, 3);
  ui.chart.loaded(); ui.flush(); assert.equal(requests().length, 6);
});

test('camera images are one small local fetch; stale frames are labeled and no automatic image polling occurs', async () => {
  const ui = setup({}, true, true);
  await ui.ready(); ui.cameraVisible('gibraltar', true); ui.flush();
  const request = ui.requests.find(r => r.url === './cameras/gibraltar.json');
  request.resolve({ ok: true, json: async () => ({ image: 'gibraltar-1790386900-v1.jpg', observed_at: '2020-01-01T00:00:00Z' }) });
  await tick();
  const body = ui.requests.at(-1);
  assert.equal(body.url, './cameras/gibraltar-1790386900-v1.jpg');
  assert.equal(body.options.priority, 'low');
  body.resolve({ ok: true, headers: { get: () => 'image/jpeg' }, blob: async () => ({}) });
  await tick(); ui.gibraltarImage.loaded();
  assert.equal(ui.gibraltarImage.hidden, false);
  assert.equal(ui.gibraltarStatus.hidden, false);
  assert.match(ui.gibraltarStatus.textContent, /Delayed/);
  ui.flush(); ui.cameraVisible('gibraltar', true); ui.flush();
  assert.equal(ui.requests.filter(r => r.url.startsWith('./cameras/')).length, 2);
});

test('camera save-data mode requires a tap and rejects paths outside its own feed', async () => {
  const ui = setup({ saveData: true }, true, true);
  await ui.ready(); ui.cameraVisible('tvhill', true); ui.flush();
  assert.equal(ui.requests.filter(r => r.url.startsWith('./cameras/')).length, 0);
  assert.equal(ui.tvhillLoad.hidden, false);
  ui.tvhillLoad.emit('click'); ui.flush();
  ui.requests.at(-1).resolve({ ok: true, json: async () => ({ image: '../app.js', observed_at: '2026-09-26T01:00:00Z' }) });
  await tick();
  assert.equal(ui.requests.filter(r => r.url.startsWith('./cameras/')).length, 1);
  assert.equal(ui.tvhillLoad.textContent, 'Retry camera image');
});
