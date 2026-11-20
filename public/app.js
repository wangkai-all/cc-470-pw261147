/* Dashboard client: polls /api/status, renders pipeline/gates/switch/rollback
   state, and runs interactive consistency demos. Untrusted strings (snapshot
   ids, gate details, event payloads) are inserted via textContent only. */
'use strict';

const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = text;
  return n;
};
const fmtTime = (iso) => (iso ? new Date(iso).toLocaleTimeString('en-GB') : '—');
const fmtNum = (n) => Number(n || 0).toLocaleString('en-US');

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `${res.status}`);
  return data;
}

/* ---------------- status rendering ---------------- */

const STATE_ICON = { current: '●', staged: '◆', superseded: '○', building: '…', gating: '…', failed: '✗' };

function pill(state) {
  const p = el('span', `pill ${state}`);
  p.append(el('span', 'icon', STATE_ICON[state] || '?'), el('span', null, state));
  return p;
}

function renderStatus(st) {
  $('health').classList.remove('down');
  $('health-text').textContent = 'live';
  $('hero-current').textContent = st.current ? st.current.id : 'none';
  $('t-records').textContent = st.current ? fmtNum(st.current.recordCount) : '—';
  $('t-readers').textContent = fmtNum(st.metrics.activeReaders);
  $('t-queries').textContent = fmtNum(st.metrics.queriesTotal);
  $('t-switches').textContent = fmtNum(st.metrics.switchesTotal);
  $('t-rollbacks').textContent = fmtNum(st.metrics.rollbacksTotal);
  $('t-failed').textContent = fmtNum(st.metrics.failedBuilds);
  $('t-rejected').textContent = fmtNum(st.metrics.rejectedPublishes);
  renderBuilds(st.builds);
  renderPublishSelect(st);
  renderVersions(st);
  renderChart(st);
  renderEvents(st.events);
}

function renderBuilds(builds) {
  const box = $('builds');
  box.textContent = '';
  if (!builds.length) {
    box.append(el('div', 'empty', 'No builds yet — start one from the controls.'));
    return;
  }
  for (const b of builds.slice(0, 6)) {
    const card = el('div', 'build');
    const row = el('div', 'row');
    row.append(
      el('span', 'id', b.id),
      pill(b.state),
      el('span', 'meta', `${fmtNum(b.recordCount)} rows → ${b.snapshotId}`),
      el('span', 'meta', fmtTime(b.startedAt))
    );
    if (b.injectBadData) row.append(el('span', 'meta', '(bad data injected)'));
    card.append(row);
    if (b.state === 'building' || b.state === 'gating') {
      const meter = el('div', 'meter');
      const fill = el('div');
      fill.style.width = `${Math.round((b.progress || 0) * 100)}%`;
      meter.append(fill);
      card.append(meter);
    }
    if (b.gates) {
      const gates = el('div', 'gates');
      for (const g of b.gates) {
        const chip = el('span', `gate ${g.passed ? 'pass' : 'fail'}`);
        chip.append(el('span', 'icon', g.passed ? '✓' : '✗'), el('span', null, `${g.name}: ${g.detail}`));
        gates.append(chip);
      }
      card.append(gates);
    }
    if (b.error) card.append(el('div', 'meta', `error: ${b.error}`));
    box.append(card);
  }
}

function renderPublishSelect(st) {
  const sel = $('publish-select');
  const prev = sel.value;
  sel.textContent = '';
  const staged = st.snapshots.filter((s) => s.state === 'staged');
  if (!staged.length) {
    const opt = el('option', null, 'no staged snapshot');
    opt.disabled = true;
    sel.append(opt);
    $('btn-publish').disabled = true;
    return;
  }
  for (const s of staged) {
    const opt = el('option', null, `${s.id} (${fmtNum(s.recordCount)} rows)`);
    opt.value = s.id;
    sel.append(opt);
  }
  if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
  $('btn-publish').disabled = false;
}

function renderVersions(st) {
  const tbody = $('versions');
  tbody.textContent = '';
  const queries = st.metrics.queriesPerSnapshot || {};
  if (!st.snapshots.length) {
    const tr = el('tr');
    const td = el('td', null, 'No snapshots yet.');
    td.colSpan = 8;
    tr.append(td);
    tbody.append(tr);
    return;
  }
  for (const s of st.snapshots) {
    const tr = el('tr', s.state === 'current' ? 'now' : null);
    const gatesPassed = (s.gates || []).filter((g) => g.passed).length;
    const gatesTotal = (s.gates || []).length;
    const cells = [
      el('td', 'id', s.id),
      el('td'),
      el('td', null, fmtNum(s.refs)),
      el('td', null, fmtNum(s.recordCount)),
      el('td', null, fmtNum(queries[s.id] || 0)),
      el('td', null, gatesTotal ? `${gatesPassed}/${gatesTotal} passed` : '—'),
      el('td', null, fmtTime(s.builtAt)),
      el('td'),
    ];
    cells[1].append(pill(s.state));
    if (s.state === 'staged') {
      const btn = el('button', 'secondary', 'publish');
      btn.style.margin = '0';
      btn.onclick = () => publish(`build-${s.id.split('-')[1]}`);
      cells[7].append(btn);
    } else if (s.state === 'superseded') {
      const btn = el('button', 'secondary', 'roll back here');
      btn.style.margin = '0';
      btn.onclick = () => rollbackTo(s.id);
      cells[7].append(btn);
    }
    tr.append(...cells);
    tbody.append(tr);
  }
}

/* ---- bar chart: queries per snapshot (single series) ---- */

function renderChart(st) {
  const box = $('chart');
  box.textContent = '';
  const queries = st.metrics.queriesPerSnapshot || {};
  const rows = st.snapshots
    .map((s) => ({ id: s.id, current: s.state === 'current', n: queries[s.id] || 0 }))
    .filter((r) => r.n > 0)
    .sort((a, b) => b.n - a.n || a.id.localeCompare(b.id));
  if (!rows.length) {
    box.append(el('div', 'empty', 'No queries served yet.'));
    return;
  }
  const max = Math.max(...rows.map((r) => r.n));
  for (const r of rows) {
    const row = el('div', 'bar-row');
    row.tabIndex = 0;
    const label = el('div', `snap${r.current ? ' now' : ''}`, r.id + (r.current ? ' ●' : ''));
    const track = el('div', 'bar-track');
    const bar = el('div', 'bar');
    bar.style.width = `${Math.max((r.n / max) * 100, 1.5)}%`;
    track.append(bar);
    const val = el('div', 'val', fmtNum(r.n));
    row.append(label, track, val);
    const show = (e) => showTooltip(e, r);
    row.addEventListener('pointermove', show);
    row.addEventListener('focus', show);
    row.addEventListener('pointerleave', hideTooltip);
    row.addEventListener('blur', hideTooltip);
    box.append(row);
  }
  const axis = el('div', 'axis');
  const ticks = el('div', 'ticks');
  const mid = Math.round(max / 2);
  for (const t of [0, mid, max]) ticks.append(el('span', null, fmtNum(t)));
  axis.append(el('span'), ticks, el('span'));
  box.append(axis);
}

const tooltip = $('tooltip');
function showTooltip(e, r) {
  tooltip.textContent = '';
  const v = el('div', 't-val', `${fmtNum(r.n)} queries`);
  const l = el('div', 't-label', `snapshot ${r.id}${r.current ? ' (current)' : ''}`);
  tooltip.append(v, l);
  tooltip.style.display = 'block';
  const x = e.clientX ?? e.target.getBoundingClientRect().left;
  const y = e.clientY ?? e.target.getBoundingClientRect().top;
  tooltip.style.left = `${Math.min(x + 12, window.innerWidth - 180)}px`;
  tooltip.style.top = `${y + 14}px`;
}
function hideTooltip() {
  tooltip.style.display = 'none';
}

/* ---- events timeline ---- */

const EVENT_STYLE = {
  build_started: ['info', '▶'],
  build_completed: ['good', '✓'],
  build_failed: ['bad', '✗'],
  build_rejected: ['bad', '✗'],
  gates_started: ['info', '▶'],
  gate_result: null, // depends on passed
  snapshot_staged: ['good', '✓'],
  publish_started: ['info', '▶'],
  publish_succeeded: ['good', '✓'],
  publish_failed: ['bad', '✗'],
  rollback_started: ['info', '▶'],
  rollback_succeeded: ['good', '✓'],
  recovery: ['warn', '!'],
  snapshot_gc: ['warn', '!'],
};

function renderEvents(events) {
  const box = $('events');
  box.textContent = '';
  const shown = events.filter((e) => e.type !== 'build_progress').slice(-40).reverse();
  if (!shown.length) {
    box.append(el('div', 'empty', 'No events yet.'));
    return;
  }
  for (const ev of shown) {
    let style = EVENT_STYLE[ev.type] || ['info', '•'];
    if (ev.type === 'gate_result') style = ev.passed ? ['good', '✓'] : ['bad', '✗'];
    const row = el('div', 'event');
    row.append(el('span', 'ts', fmtTime(ev.ts)), el('span', `icon ${style[0]}`, style[1]));
    const what = el('div', 'what', ev.type.replaceAll('_', ' '));
    const detail = el('span', 'detail', ' ' + eventDetail(ev));
    what.append(detail);
    row.append(what);
    box.append(row);
  }
}

function eventDetail(ev) {
  const parts = [];
  for (const k of ['buildId', 'snapshotId', 'gate', 'detail', 'reason', 'error', 'mode', 'currentId', 'previousId', 'recordCount']) {
    if (ev[k] !== undefined) parts.push(`${k}=${ev[k]}`);
  }
  return parts.join('  ');
}

/* ---------------- actions ---------------- */

async function act(fn, note) {
  try {
    await fn();
  } catch (err) {
    alert(`${note || 'action'} failed: ${err.message}`);
  }
  refresh();
}

function publish(buildId) {
  return act(() => api('POST', '/api/publish', buildId ? { buildId } : {}), 'publish');
}
function rollbackTo(targetId) {
  return act(() => api('POST', '/api/rollback', targetId ? { targetId } : {}), 'rollback');
}

$('btn-build').onclick = () =>
  act(async () => {
    await api('POST', '/api/build', {
      recordCount: Number($('build-count').value) || 200,
      injectBadData: $('build-bad').checked,
    });
  }, 'build');

$('btn-publish').onclick = () => {
  const snapId = $('publish-select').value;
  const buildId = snapId && snapId.startsWith('snap-') ? `build-${snapId.slice(5)}` : null;
  return act(() => api('POST', '/api/publish', buildId ? { buildId } : {}), 'publish');
};

$('btn-rollback').onclick = () => rollbackTo();

$('btn-fault-fail').onclick = () =>
  act(() => api('POST', '/api/admin/fault', { failNextPublishBeforeRename: true }), 'arm fault');
$('btn-fault-crash').onclick = () =>
  act(() => api('POST', '/api/admin/fault', { crashAfterRenameOnNextPublish: true }), 'arm fault');

/* ---------------- query tester & demos ---------------- */

function checkConsistency(res) {
  const versions = new Set(res.rows.map((r) => r.version));
  return {
    ok: versions.size <= 1 && (versions.size === 0 || `snap-${[...versions][0]}` === res.snapshotId),
    versions: [...versions],
  };
}

function showQueryResult(res) {
  const box = $('query-result');
  box.textContent = '';
  const c = checkConsistency(res);
  const verdict = el('div', `verdict ${c.ok ? 'ok' : 'bad'}`);
  verdict.append(
    el('span', 'icon', c.ok ? '✓' : '✗'),
    el(
      'span',
      null,
      c.ok
        ? `consistent — ${fmtNum(res.rowCount)} rows, all from ${res.snapshotId} (${res.elapsedMs} ms)`
        : `MIXED VERSIONS in one response: ${c.versions.join(', ')}`
    )
  );
  box.append(verdict);
  const pre = el('pre', 'rows');
  pre.textContent = res.rows
    .slice(0, 6)
    .map((r) => JSON.stringify(r))
    .join('\n');
  box.append(pre);
}

$('btn-query').onclick = async () => {
  const p = new URLSearchParams({
    q: $('q-text').value,
    limit: $('q-limit').value,
    slow: $('q-slow').value,
  });
  if ($('q-snap').value.trim()) p.set('snapshot', $('q-snap').value.trim());
  try {
    showQueryResult(await api('GET', `/api/query?${p}`));
  } catch (err) {
    const box = $('query-result');
    box.textContent = '';
    const v = el('div', 'verdict bad');
    v.append(el('span', 'icon', '✗'), el('span', null, err.message));
    box.append(v);
  }
};

$('btn-burst').onclick = async () => {
  const log = $('demo-log');
  log.textContent = '';
  const line = el('div', null, 'burst: firing 20 concurrent queries…');
  log.append(line);
  const results = await Promise.all(
    Array.from({ length: 20 }, (_, i) => api('GET', `/api/query?limit=10&slow=${i % 3}`))
  );
  const snaps = new Set(results.map((r) => r.snapshotId));
  const allConsistent = results.every((r) => checkConsistency(r).ok);
  line.textContent = `burst: 20 responses, snapshots seen: ${[...snaps].join(', ') || 'none'} — `;
  const tag = el('span', allConsistent ? 'ok' : 'bad', allConsistent ? 'every response single-version ✓' : 'MIXED VERSION ✗');
  line.append(tag);
};

$('btn-demo').onclick = async () => {
  const log = $('demo-log');
  log.textContent = '';
  const say = (msg, cls) => log.append(el('div', cls, msg));
  try {
    const st = await api('GET', '/api/status');
    const startSnap = st.current?.id;
    if (!startSnap) return say('no current snapshot — build & publish one first', 'bad');
    say(`1. starting long query (slow scan) on ${startSnap}…`);
    const longQuery = api('GET', '/api/query?slow=8&limit=10000');
    await new Promise((r) => setTimeout(r, 200));
    say('2. building a new version in staging…');
    const { build } = await api('POST', '/api/build?wait=1', { recordCount: 200 });
    if (build.state !== 'staged') return say(`   build ${build.id} failed gates — demo aborted`, 'bad');
    say(`3. gates passed → switching current to ${build.snapshotId} mid-query…`);
    await api('POST', '/api/publish', { buildId: build.id });
    const res = await longQuery;
    say(`4. long query finished: ${res.rowCount} rows, all from ${res.snapshotId} (${res.elapsedMs} ms)`);
    const fresh = await api('GET', '/api/query?limit=5');
    say(`5. new query is served by ${fresh.snapshotId}`);
    if (res.snapshotId === startSnap && fresh.snapshotId === build.snapshotId) {
      say('✓ no mixed versions: in-flight query kept its snapshot, new queries moved on', 'ok');
    } else {
      say('✗ unexpected version drift', 'bad');
    }
  } catch (err) {
    say(`demo failed: ${err.message}`, 'bad');
  }
};

/* ---------------- polling ---------------- */

async function refresh() {
  try {
    renderStatus(await api('GET', '/api/status'));
  } catch {
    $('health').classList.add('down');
    $('health-text').textContent = 'server unreachable';
  }
}
refresh();
setInterval(refresh, 1000);
