// Core of the service.
//
// Consistency model
// -----------------
// * Snapshots are IMMUTABLE directories: staging/build-N -> (gates pass) ->
//   snapshots/snap-N. Files inside a sealed snapshot are never modified.
// * The "current" pointer is a tiny file swapped via write-tmp + fsync +
//   rename (atomic on POSIX). A crash mid-switch leaves either the old or the
//   new pointer fully in place — never a torn one.
// * Readers PIN a snapshot at request start (refs++, synchronous, no await
//   between lookup and increment — race-free on Node's single thread) and
//   read only that snapshot's files for the whole request, however long it
//   runs. Switches never disturb in-flight reads.
// * A superseded snapshot is garbage-collected only when refs === 0 AND it
//   has rolled off the retention window, so long queries never lose their
//   files and rollback targets survive.
// * Every row carries its build's version; the query path asserts that all
//   returned rows belong to the pinned snapshot — a mixed-version response
//   is turned into a 500 instead of reaching the client.
import fs from 'node:fs/promises';
import { createReadStream, createWriteStream } from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { EventEmitter, once } from 'node:events';
import { generateRecords } from './source.js';
import { analyzeRecords, evaluateGates } from './gates.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MAX_EVENTS = 500;

export function httpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

async function pathExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

// Atomic file replace: write temp file in the SAME directory, fsync, rename.
async function writeFileAtomic(file, data) {
  const tmp = `${file}.tmp`;
  const fh = await fs.open(tmp, 'w');
  try {
    await fh.writeFile(data);
    await fh.sync();
  } finally {
    await fh.close();
  }
  await fs.rename(tmp, file);
}

export class SnapshotManager extends EventEmitter {
  constructor(dataDir, opts = {}) {
    super();
    this.dataDir = dataDir;
    this.snapshotsDir = path.join(dataDir, 'snapshots');
    this.stagingDir = path.join(dataDir, 'staging');
    this.currentFile = path.join(dataDir, 'CURRENT');
    this.eventsFile = path.join(dataDir, 'events.jsonl');
    this.opts = {
      minRecords: 50,
      retention: 10, // superseded snapshots kept for rollback
      buildChunkSize: 50,
      buildChunkDelayMs: 25, // simulates batch build latency; tests set ~1ms
      ...opts,
    };
    this.seq = 0;
    this.current = null; // {id, seq, dir, manifest, refs, state}
    this.snapshots = new Map(); // id -> snapshot meta (staged/current/superseded)
    this.builds = new Map(); // buildId -> build state
    this.events = [];
    this.history = []; // published snapshot ids, in switch order
    this.metrics = {
      queriesTotal: 0,
      queriesPerSnapshot: {},
      switchesTotal: 0,
      publishesTotal: 0,
      rollbacksTotal: 0,
      failedBuilds: 0,
      rejectedPublishes: 0,
    };
    this.faults = { failNextPublishBeforeRename: false, crashAfterRenameOnNextPublish: false };
    this._eventSeq = 0;
    this._switchChain = Promise.resolve(); // serializes publish/rollback
  }

  // ---------------------------------------------------------------- lifecycle

  async init() {
    await fs.mkdir(this.snapshotsDir, { recursive: true });
    await fs.mkdir(this.stagingDir, { recursive: true });

    // Reload every sealed snapshot that has a complete manifest. A dir
    // without a manifest is a leftover of a crashed promote -> ignore it.
    const dirs = await fs.readdir(this.snapshotsDir).catch(() => []);
    for (const d of dirs) {
      try {
        const manifest = JSON.parse(await fs.readFile(path.join(this.snapshotsDir, d, 'manifest.json'), 'utf8'));
        this.snapshots.set(manifest.id, {
          id: manifest.id,
          seq: manifest.seq,
          dir: path.join(this.snapshotsDir, d),
          manifest,
          refs: 0,
          state: 'superseded',
        });
        this.seq = Math.max(this.seq, manifest.seq);
      } catch {
        // incomplete snapshot dir (crash during promote) — not loadable
      }
    }

    // Recover the current pointer; fall back to the newest sealed snapshot if
    // the pointer is missing/corrupt/dangling.
    let pointedId = null;
    try {
      pointedId = (await fs.readFile(this.currentFile, 'utf8')).trim();
    } catch {
      /* no pointer yet */
    }
    if (pointedId && this.snapshots.has(pointedId)) {
      this.current = this.snapshots.get(pointedId);
      this.current.state = 'current';
      this.#event('recovery', { mode: 'pointer', currentId: pointedId });
    } else if (this.snapshots.size > 0) {
      this.current = [...this.snapshots.values()].sort((a, b) => b.seq - a.seq)[0];
      this.current.state = 'current';
      await writeFileAtomic(this.currentFile, this.current.id);
      this.#event('recovery', { mode: 'fallback', currentId: this.current.id, lostPointer: pointedId });
    } else {
      this.#event('recovery', { mode: 'empty' });
    }

    // Best-effort publish history for "rollback to previous".
    this.history = [...this.snapshots.values()]
      .filter((s) => s.seq <= (this.current?.seq ?? 0))
      .sort((a, b) => a.seq - b.seq)
      .map((s) => s.id);

    // Drop staging leftovers from crashed builds — they were never promoted,
    // so they were never visible to readers.
    for (const d of await fs.readdir(this.stagingDir).catch(() => [])) {
      await fs.rm(path.join(this.stagingDir, d), { recursive: true, force: true }).catch(() => {});
    }
  }

  // ------------------------------------------------------------------ events

  #event(type, detail = {}) {
    const ev = { seq: ++this._eventSeq, ts: new Date().toISOString(), type, ...detail };
    this.events.push(ev);
    if (this.events.length > MAX_EVENTS) this.events.splice(0, this.events.length - MAX_EVENTS);
    this.emit('event', ev);
    fs.appendFile(this.eventsFile, JSON.stringify(ev) + '\n').catch(() => {});
    return ev;
  }

  // ------------------------------------------------------------------- build

  startBuild({ recordCount = 200, injectBadData = false, chunkDelayMs } = {}) {
    recordCount = Math.max(1, Math.min(100000, Math.floor(Number(recordCount) || 0)));
    const seq = ++this.seq;
    const build = {
      id: `build-${seq}`,
      seq,
      snapshotId: `snap-${seq}`,
      state: 'building', // building -> gating -> staged | failed
      progress: 0,
      recordCount,
      injectBadData: !!injectBadData,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      gates: null,
      stats: null,
      error: null,
      done: null,
    };
    this.builds.set(build.id, build);
    this.#event('build_started', { buildId: build.id, recordCount, injectBadData: build.injectBadData });
    build.done = this.#runBuild(build, chunkDelayMs).catch((err) => {
      build.state = 'failed';
      build.error = String(err?.message ?? err);
      build.finishedAt = new Date().toISOString();
      this.metrics.failedBuilds++;
      this.#event('build_failed', { buildId: build.id, error: build.error });
    });
    return build;
  }

  async #runBuild(build, chunkDelayMs) {
    const delay = chunkDelayMs ?? this.opts.buildChunkDelayMs;
    const stagingPath = path.join(this.stagingDir, build.id);
    await fs.mkdir(stagingPath, { recursive: true });
    const recordsPath = path.join(stagingPath, 'records.jsonl');

    // 1) BUILD: extract the batch into the staging area, chunked so progress
    //    is observable on the dashboard.
    const out = createWriteStream(recordsPath);
    let written = 0;
    try {
      for (const rec of generateRecords({ seq: build.seq, count: build.recordCount, injectBadData: build.injectBadData })) {
        if (!out.write(JSON.stringify(rec) + '\n')) await once(out, 'drain');
        written++;
        if (written % this.opts.buildChunkSize === 0) {
          build.progress = written / build.recordCount;
          this.#event('build_progress', { buildId: build.id, written, total: build.recordCount });
          if (delay) await sleep(delay);
        }
      }
    } finally {
      await new Promise((resolve) => out.end(resolve));
    }
    build.progress = 1;
    this.#event('build_completed', { buildId: build.id, written });

    // 2) GATES: quality checks against the staged data. Current version is
    //    untouched while this runs.
    build.state = 'gating';
    this.#event('gates_started', { buildId: build.id });
    const stats = await analyzeRecords(recordsPath, build.seq);
    const gates = evaluateGates(stats, { minRecords: this.opts.minRecords });
    build.stats = stats;
    build.gates = gates;
    for (const g of gates) {
      this.#event('gate_result', { buildId: build.id, gate: g.name, passed: g.passed, detail: g.detail });
    }
    if (!gates.every((g) => g.passed)) {
      build.state = 'failed';
      build.finishedAt = new Date().toISOString();
      this.metrics.failedBuilds++;
      this.#event('build_rejected', {
        buildId: build.id,
        failedGates: gates.filter((g) => !g.passed).map((g) => g.name),
      });
      return; // staging dir kept for inspection; current version untouched
    }

    // 3) PROMOTE: manifest + atomic directory rename seals the snapshot.
    const manifest = {
      id: build.snapshotId,
      seq: build.seq,
      recordCount: stats.recordCount,
      builtAt: new Date().toISOString(),
      gates: gates.map((g) => ({ name: g.name, passed: g.passed, detail: g.detail })),
    };
    await fs.writeFile(path.join(stagingPath, 'manifest.json'), JSON.stringify(manifest, null, 2));
    const finalDir = path.join(this.snapshotsDir, build.snapshotId);
    await fs.rename(stagingPath, finalDir);
    this.snapshots.set(build.snapshotId, {
      id: build.snapshotId,
      seq: build.seq,
      dir: finalDir,
      manifest,
      refs: 0,
      state: 'staged',
    });
    build.state = 'staged';
    build.finishedAt = new Date().toISOString();
    this.#event('snapshot_staged', { buildId: build.id, snapshotId: build.snapshotId, recordCount: stats.recordCount });
  }

  // -------------------------------------------------------- publish/rollback

  async publish({ buildId, expectedCurrentId } = {}) {
    return this.#withSwitchLock(async () => {
      let snap;
      if (buildId) {
        const build = this.builds.get(buildId);
        if (!build) throw httpError(404, `unknown build: ${buildId}`);
        if (build.state !== 'staged') {
          this.metrics.rejectedPublishes++;
          throw httpError(409, `build ${buildId} is not publishable (state: ${build.state}) — quality gates must pass first`);
        }
        snap = this.snapshots.get(build.snapshotId);
      } else {
        snap =
          [...this.snapshots.values()]
            .filter((s) => s.state === 'staged')
            .sort((a, b) => b.seq - a.seq)[0] ?? null;
        if (!snap) {
          this.metrics.rejectedPublishes++;
          throw httpError(409, 'no staged snapshot to publish');
        }
      }
      if (!snap) throw httpError(409, 'staged snapshot data missing');
      if (snap === this.current) throw httpError(409, `${snap.id} is already current`);
      if (expectedCurrentId !== undefined && expectedCurrentId !== (this.current?.id ?? null)) {
        this.metrics.rejectedPublishes++;
        throw httpError(409, `stale expectation: current is ${this.current?.id ?? 'none'}, not ${expectedCurrentId}`);
      }
      return this.#switchTo(snap, 'publish');
    });
  }

  async rollback({ targetId } = {}) {
    return this.#withSwitchLock(async () => {
      let target = null;
      if (targetId) {
        target = this.snapshots.get(targetId) ?? null;
        if (!target) throw httpError(410, `snapshot ${targetId} is no longer retained`);
      } else {
        for (let i = this.history.length - 1; i >= 0; i--) {
          const cand = this.snapshots.get(this.history[i]);
          if (cand && cand !== this.current) {
            target = cand;
            break;
          }
        }
        if (!target) throw httpError(409, 'no previous snapshot to roll back to');
      }
      if (target === this.current) throw httpError(409, `${target.id} is already current`);
      if (!(await pathExists(path.join(target.dir, 'records.jsonl')))) {
        throw httpError(410, `snapshot ${target.id} data has been garbage-collected`);
      }
      return this.#switchTo(target, 'rollback');
    });
  }

  // The ONLY place the current pointer changes. Serialized by the switch
  // lock; atomic on disk; instantaneous in memory.
  async #switchTo(snap, kind) {
    const prev = this.current;
    this.#event(`${kind}_started`, { snapshotId: snap.id, previousId: prev?.id ?? null });

    // Fault injection: fail BEFORE the atomic rename — the old pointer stays
    // fully intact and readers keep being served the old version.
    if (kind === 'publish' && this.faults.failNextPublishBeforeRename) {
      this.faults.failNextPublishBeforeRename = false;
      this.metrics.rejectedPublishes++;
      this.#event('publish_failed', { snapshotId: snap.id, reason: 'injected fault before pointer rename' });
      throw httpError(500, 'publish failed (injected fault) — old version still current');
    }

    await writeFileAtomic(this.currentFile, snap.id);

    // Fault injection: hard crash AFTER the rename but BEFORE the response.
    // On restart, recovery must converge to the renamed pointer.
    if (kind === 'publish' && this.faults.crashAfterRenameOnNextPublish) {
      this.faults.crashAfterRenameOnNextPublish = false;
      process.kill(process.pid, 'SIGKILL');
    }

    // Synchronous in-memory swap: readers that pinned `prev` keep their ref
    // and their files; readers arriving from this line on pin `snap`.
    this.current = snap;
    snap.state = 'current';
    if (prev && prev !== snap) prev.state = 'superseded';
    this.history.push(snap.id);
    this.metrics.switchesTotal++;
    if (kind === 'publish') this.metrics.publishesTotal++;
    if (kind === 'rollback') this.metrics.rollbacksTotal++;
    this.#gcSweep();
    this.#event(`${kind}_succeeded`, { snapshotId: snap.id, previousId: prev?.id ?? null });
    return { current: snap.id, previous: prev?.id ?? null };
  }

  // Promise-chain mutex: publish and rollback are strictly serialized, so
  // concurrent publishes can never interleave into a torn state.
  async #withSwitchLock(fn) {
    const prev = this._switchChain;
    let release;
    this._switchChain = new Promise((r) => (release = r));
    await prev;
    try {
      return await fn();
    } finally {
      release();
    }
  }

  // --------------------------------------------------------------- read path

  acquire(snapshotId) {
    const snap = snapshotId ? this.snapshots.get(snapshotId) : this.current;
    if (!snap) return null;
    snap.refs++; // synchronous — no await between lookup and increment
    return snap;
  }

  release(snap) {
    snap.refs--;
    if (snap.refs === 0) this.#gcSweep();
  }

  async query({ q = '', limit = 200, slowMs = 0, snapshotId } = {}) {
    const snap = this.acquire(snapshotId);
    if (!snap) {
      throw httpError(
        snapshotId ? 410 : 503,
        snapshotId
          ? `snapshot ${snapshotId} is no longer available (rolled off retention)`
          : 'no snapshot published yet'
      );
    }
    const started = Date.now();
    try {
      const rows = [];
      const needle = q.trim().toLowerCase();
      const rl = readline.createInterface({
        input: createReadStream(path.join(snap.dir, 'records.jsonl')),
        crlfDelay: Infinity,
      });
      for await (const line of rl) {
        if (!line) continue;
        let rec;
        try {
          rec = JSON.parse(line);
        } catch {
          continue;
        }
        if (needle) {
          const hay = `${rec.name ?? ''} ${rec.category ?? ''}`.toLowerCase();
          if (!hay.includes(needle)) continue;
        }
        rows.push(rec);
        if (slowMs) await sleep(slowMs); // simulates a long-running scan
        if (rows.length >= limit) break;
      }
      // Hard consistency guard: every returned row must belong to the pinned
      // snapshot. A violation becomes a 500, never a mixed-version response.
      const foreign = rows.find((r) => r.version !== snap.seq);
      if (foreign) {
        throw httpError(500, `consistency violation: row ${foreign.id} is version ${foreign.version}, pinned ${snap.seq}`);
      }
      this.metrics.queriesTotal++;
      this.metrics.queriesPerSnapshot[snap.id] = (this.metrics.queriesPerSnapshot[snap.id] ?? 0) + 1;
      return { snapshotId: snap.id, seq: snap.seq, rowCount: rows.length, rows, elapsedMs: Date.now() - started };
    } finally {
      this.release(snap);
    }
  }

  // --------------------------------------------------------------------- gc

  #gcSweep() {
    const superseded = [...this.snapshots.values()]
      .filter((s) => s.state === 'superseded')
      .sort((a, b) => b.seq - a.seq);
    // Keep the newest `retention` superseded snapshots as rollback targets;
    // collect the rest once no reader holds a reference.
    for (const victim of superseded.slice(this.opts.retention)) {
      if (victim.refs > 0) continue; // long query still reading — keep its files
      this.snapshots.delete(victim.id);
      fs.rm(victim.dir, { recursive: true, force: true }).catch(() => {});
      this.#event('snapshot_gc', { snapshotId: victim.id });
    }
  }

  // ------------------------------------------------------------------ status

  #buildView(b) {
    return {
      id: b.id,
      seq: b.seq,
      snapshotId: b.snapshotId,
      state: b.state,
      progress: b.progress,
      recordCount: b.recordCount,
      injectBadData: b.injectBadData,
      startedAt: b.startedAt,
      finishedAt: b.finishedAt,
      gates: b.gates,
      stats: b.stats,
      error: b.error,
    };
  }

  getStatus() {
    const snapView = (s) => ({
      id: s.id,
      seq: s.seq,
      state: s.state,
      refs: s.refs,
      recordCount: s.manifest.recordCount,
      builtAt: s.manifest.builtAt,
      gates: s.manifest.gates ?? null,
    });
    return {
      now: new Date().toISOString(),
      current: this.current ? snapView(this.current) : null,
      snapshots: [...this.snapshots.values()].map(snapView).sort((a, b) => b.seq - a.seq),
      builds: [...this.builds.values()].map((b) => this.#buildView(b)).sort((a, b) => b.seq - a.seq).slice(0, 20),
      events: this.events.slice(-120),
      history: [...this.history],
      metrics: {
        ...this.metrics,
        activeReaders: [...this.snapshots.values()].reduce((n, s) => n + s.refs, 0),
      },
      config: { minRecords: this.opts.minRecords, retention: this.opts.retention },
    };
  }
}
