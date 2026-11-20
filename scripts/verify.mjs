// End-to-end verification of the snapshot service's guarantees:
//
//   S1  build -> gates -> publish -> query serves the new version
//   S2  quality-gate failure rejects the build, old version keeps serving
//   S3  LONG QUERY started before a switch finishes on its pinned snapshot
//   S4  concurrent queries + pinned retries during a switch: no mixed versions
//   S5  PARALLEL publishes are serialized; stale expectation -> 409
//   S6  publish failure keeps serving the old version
//   S7  crash AFTER pointer rename: recovery converges to the new version
//   S8  graceful restart: recovery restores the same current version
//   S9  rollback switches back to the previous snapshot
//   S10 GC respects reader refs; rolled-off snapshots answer 410
//
// Usage: node scripts/verify.mjs
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 4100 + (process.pid % 500);
const BASE = `http://127.0.0.1:${PORT}`;
const RETENTION = 2; // small window so GC behavior is observable in tests

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;

function assert(cond, msg) {
  if (!cond) throw new Error(`assertion failed: ${msg}`);
}

async function api(method, p, body) {
  const res = await fetch(BASE + p, {
    method,
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  if (!res.ok) {
    const err = new Error(`${method} ${p} -> ${res.status}: ${text}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

async function expectHttp(promise, status, what) {
  try {
    await promise;
  } catch (err) {
    assert(err.status === status, `${what}: expected HTTP ${status}, got ${err.status} (${err.message})`);
    return;
  }
  throw new Error(`${what}: expected HTTP ${status} but request succeeded`);
}

async function waitFor(fn, { timeout = 15000, interval = 100, what = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const v = await fn().catch(() => null);
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${what}`);
    await sleep(interval);
  }
}

// ---- server lifecycle ------------------------------------------------------
let child = null;
let childExited = null;

async function startServer(dataDir) {
  child = spawn(process.execPath, [path.join(ROOT, 'src', 'server.js')], {
    env: {
      ...process.env,
      PORT: String(PORT),
      DATA_DIR: dataDir,
      RETENTION: String(RETENTION),
      BUILD_CHUNK_DELAY_MS: '1',
    },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.stderr.on('data', (d) => process.stderr.write(`[server] ${d}`));
  childExited = new Promise((r) => child.once('exit', (code, sig) => r({ code, sig })));
  await waitFor(async () => (await api('GET', '/api/health').catch(() => null))?.ok, { what: 'server healthy' });
}

async function stopServer(signal = 'SIGTERM') {
  if (!child) return;
  child.kill(signal);
  await Promise.race([childExited, sleep(5000)]);
  child = null;
}

// ---- scenario runner -------------------------------------------------------
const results = [];
async function scenario(name, fn) {
  const t0 = Date.now();
  try {
    await fn();
    results.push({ name, ok: true, ms: Date.now() - t0 });
    console.log(`  PASS  ${name} (${Date.now() - t0}ms)`);
  } catch (err) {
    failures++;
    results.push({ name, ok: false, ms: Date.now() - t0, error: err.message });
    console.log(`  FAIL  ${name}\n        ${err.message}`);
  }
}

// Build and wait for it to finish; returns the build view.
async function buildAndWait(body) {
  const { build } = await api('POST', '/api/build?wait=1', { chunkDelayMs: 1, ...body });
  return build;
}

async function currentId() {
  return (await api('GET', '/api/status')).current?.id ?? null;
}

// Assert a query response contains rows from exactly one snapshot.
function assertSingleVersion(res, what) {
  const versions = new Set(res.rows.map((r) => r.version));
  assert(versions.size <= 1, `${what}: response mixes versions ${[...versions].join(', ')}`);
  if (versions.size === 1) {
    assert(
      `snap-${[...versions][0]}` === res.snapshotId,
      `${what}: rows are version ${[...versions][0]} but response claims ${res.snapshotId}`
    );
  }
}

// =============================================================================
async function main() {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'snap-verify-'));
  console.log(`verify: data dir ${dataDir}, port ${PORT}\n`);
  await startServer(dataDir);

  try {
    // -- S1 ------------------------------------------------------------------
    let v1;
    await scenario('S1: build -> gates -> publish -> query new version', async () => {
      const b = await buildAndWait({ recordCount: 200 });
      assert(b.state === 'staged', `build state ${b.state}, gates: ${JSON.stringify(b.gates)}`);
      assert(b.gates.every((g) => g.passed), 'all gates should pass on clean data');
      const pub = await api('POST', '/api/publish', { buildId: b.id });
      assert(pub.current === b.snapshotId, `current is ${pub.current}`);
      const q = await api('GET', '/api/query?limit=500');
      assert(q.snapshotId === b.snapshotId, `query served ${q.snapshotId}`);
      assert(q.rows.length === 200, `expected 200 rows, got ${q.rows.length}`);
      assertSingleVersion(q, 'S1');
      v1 = b.snapshotId;
    });

    // -- S2 ------------------------------------------------------------------
    await scenario('S2: gate failure rejects build, old version keeps serving', async () => {
      const before = await currentId();
      const bad = await buildAndWait({ recordCount: 200, injectBadData: true });
      assert(bad.state === 'failed', `bad build should fail gates, state=${bad.state}`);
      const failedGates = bad.gates.filter((g) => !g.passed).map((g) => g.name);
      assert(failedGates.length > 0, 'expected at least one failed gate');
      await expectHttp(api('POST', '/api/publish', { buildId: bad.id }), 409, 'publish of failed build');
      assert((await currentId()) === before, 'current must be unchanged after rejected build');
      const q = await api('GET', '/api/query?limit=10');
      assert(q.snapshotId === before, 'queries still served by old version');
    });

    // -- S3 ------------------------------------------------------------------
    await scenario('S3: long query stays on its pinned snapshot across a switch', async () => {
      const pinned = await currentId();
      const longQuery = api('GET', '/api/query?slow=5&limit=200'); // ~1s scan
      await sleep(150); // let it get going
      const b = await buildAndWait({ recordCount: 200 });
      await api('POST', '/api/publish', { buildId: b.id }); // switch mid-query
      const res = await longQuery;
      assert(res.snapshotId === pinned, `long query served ${res.snapshotId}, expected pinned ${pinned}`);
      assert(res.rows.length === 200, `expected full 200-row scan, got ${res.rows.length}`);
      assertSingleVersion(res, 'S3 long query');
      const after = await api('GET', '/api/query?limit=10');
      assert(after.snapshotId === b.snapshotId, 'new queries see the new version');
    });

    // -- S4 ------------------------------------------------------------------
    await scenario('S4: concurrent queries + pinned retries during switch: no mixed versions', async () => {
      const before = await currentId();
      const inFlight = [];
      for (let i = 0; i < 30; i++) {
        inFlight.push(api('GET', `/api/query?limit=60&slow=${i % 4}`));
      }
      await sleep(30);
      const b = await buildAndWait({ recordCount: 150 });
      await api('POST', '/api/publish', { buildId: b.id });
      const settled = await Promise.all(inFlight);
      for (const [i, res] of settled.entries()) {
        assertSingleVersion(res, `S4 query#${i}`);
        assert(
          res.snapshotId === before || res.snapshotId === b.snapshotId,
          `S4 query#${i} served unexpected snapshot ${res.snapshotId}`
        );
      }
      // Client-side retry pattern: pin the retry to the snapshot of the 1st attempt.
      const first = await api('GET', '/api/query?limit=5');
      const retry = await api('GET', `/api/query?limit=5&snapshot=${first.snapshotId}`);
      assert(retry.snapshotId === first.snapshotId, 'pinned retry must land on the same snapshot');
      assertSingleVersion(retry, 'S4 pinned retry');
    });

    // -- S5 ------------------------------------------------------------------
    await scenario('S5: parallel publishes are serialized, stale expectation rejected', async () => {
      const base = await currentId();
      const [ba, bb] = await Promise.all([
        buildAndWait({ recordCount: 120 }),
        buildAndWait({ recordCount: 120 }),
      ]);
      assert(ba.state === 'staged' && bb.state === 'staged', 'both builds staged');
      const [pa, pb] = await Promise.allSettled([
        api('POST', '/api/publish', { buildId: ba.id, expectedCurrentId: base }),
        api('POST', '/api/publish', { buildId: bb.id, expectedCurrentId: base }),
      ]);
      const oks = [pa, pb].filter((p) => p.status === 'fulfilled');
      const conflicts = [pa, pb].filter((p) => p.status === 'rejected' && p.reason.status === 409);
      assert(oks.length === 1 && conflicts.length === 1, `expected exactly one winner, got oks=${oks.length} conflicts=${conflicts.length}`);
      // The loser can still be published explicitly (serialized, last wins).
      const loserBuild = pa.status === 'fulfilled' ? bb : ba;
      const second = await api('POST', '/api/publish', { buildId: loserBuild.id });
      assert(second.current === loserBuild.snapshotId, 'second publish wins last');
      const st = await api('GET', '/api/status');
      const q = await api('GET', '/api/query?limit=5');
      assert(q.snapshotId === st.current.id, 'status and query agree on current');
      assertSingleVersion(q, 'S5');
    });

    // -- S6 ------------------------------------------------------------------
    await scenario('S6: publish failure keeps serving the old version', async () => {
      const before = await currentId();
      const b = await buildAndWait({ recordCount: 100 });
      await api('POST', '/api/admin/fault', { failNextPublishBeforeRename: true });
      await expectHttp(api('POST', '/api/publish', { buildId: b.id }), 500, 'injected publish failure');
      assert((await currentId()) === before, 'current unchanged after failed publish');
      const q = await api('GET', '/api/query?limit=10');
      assert(q.snapshotId === before, 'old version still served');
      const st = await api('GET', '/api/status');
      assert(st.events.some((e) => e.type === 'publish_failed'), 'publish_failed event recorded');
      const retry = await api('POST', '/api/publish', { buildId: b.id }); // fault was one-shot
      assert(retry.current === b.snapshotId, 'publish retry succeeds after fault cleared');
    });

    // -- S7 ------------------------------------------------------------------
    await scenario('S7: crash after pointer rename -> recovery converges to new version', async () => {
      const b = await buildAndWait({ recordCount: 100 });
      await api('POST', '/api/admin/fault', { crashAfterRenameOnNextPublish: true });
      await api('POST', '/api/publish', { buildId: b.id }).catch(() => {}); // connection dies with the process
      await waitFor(() => childExited, { what: 'server crash' });
      await startServer(dataDir);
      assert((await currentId()) === b.snapshotId, 'recovered current = renamed pointer target');
      const q = await api('GET', '/api/query?limit=20');
      assert(q.snapshotId === b.snapshotId, 'queries serve recovered version');
      assertSingleVersion(q, 'S7');
      const st = await api('GET', '/api/status');
      assert(st.events.some((e) => e.type === 'recovery'), 'recovery event recorded');
    });

    // -- S8 ------------------------------------------------------------------
    await scenario('S8: graceful restart restores the same current version', async () => {
      const before = await currentId();
      await stopServer('SIGTERM');
      await startServer(dataDir);
      assert((await currentId()) === before, 'current restored after restart');
      const q = await api('GET', '/api/query?limit=5');
      assert(q.snapshotId === before, 'queries resume on the same version');
    });

    // -- S9 ------------------------------------------------------------------
    await scenario('S9: rollback switches back to the previous snapshot', async () => {
      const st = await api('GET', '/api/status');
      const cur = st.current.id;
      const previous = [...st.history].reverse().find((id) => id !== cur && st.snapshots.some((s) => s.id === id));
      assert(previous, `need a retained previous snapshot, history=${st.history.join(',')}`);
      const rb = await api('POST', '/api/rollback', {});
      assert(rb.current === previous, `rollback landed on ${rb.current}, expected ${previous}`);
      const q = await api('GET', '/api/query?limit=10');
      assert(q.snapshotId === previous, 'queries now serve the rolled-back version');
      assertSingleVersion(q, 'S9');
      const st2 = await api('GET', '/api/status');
      assert(st2.events.some((e) => e.type === 'rollback_succeeded'), 'rollback event recorded');
      // roll forward again by re-publishing a staged build if one exists
      const staged = st2.snapshots.find((s) => s.state === 'staged');
      if (staged) await api('POST', '/api/publish', { buildId: `build-${staged.id.split('-')[1]}` });
    });

    // -- S10 -----------------------------------------------------------------
    await scenario('S10: GC respects reader refs; rolled-off snapshots answer 410', async () => {
      const pinned = await currentId();
      const pinnedMeta = (await api('GET', '/api/status')).snapshots.find((s) => s.id === pinned);
      // Pin the current snapshot with a long query, then publish RETENTION+1
      // new versions — the pinned snapshot must survive while being read.
      const longQuery = api('GET', `/api/query?snapshot=${pinned}&slow=5&limit=1000`);
      await sleep(100);
      for (let i = 0; i < RETENTION + 1; i++) {
        const b = await buildAndWait({ recordCount: 200 });
        await api('POST', '/api/publish', { buildId: b.id });
      }
      const res = await longQuery;
      assert(res.snapshotId === pinned, 'pinned long query completed on its snapshot');
      assert(res.rows.length === pinnedMeta.recordCount, `full scan despite GC pressure, got ${res.rows.length}/${pinnedMeta.recordCount} rows`);
      assertSingleVersion(res, 'S10 pinned');
      // After release + further switches, it rolls off retention -> 410.
      for (let i = 0; i < RETENTION + 1; i++) {
        const b = await buildAndWait({ recordCount: 60 });
        await api('POST', '/api/publish', { buildId: b.id });
      }
      await expectHttp(api('GET', `/api/query?snapshot=${pinned}&limit=1`), 410, 'query on GC-ed snapshot');
      const st = await api('GET', '/api/status');
      assert(st.events.some((e) => e.type === 'snapshot_gc'), 'gc event recorded');
    });
  } finally {
    await stopServer();
  }

  console.log('\n================ summary ================');
  for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  ${r.name}`);
  const passed = results.filter((r) => r.ok).length;
  console.log(`\n${passed}/${results.length} scenarios passed`);
  if (failures > 0) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
