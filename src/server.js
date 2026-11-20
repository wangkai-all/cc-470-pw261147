import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { SnapshotManager } from './snapshot-manager.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');

const mgr = new SnapshotManager(DATA_DIR, {
  minRecords: Number(process.env.MIN_RECORDS || 50),
  retention: Number(process.env.RETENTION || 10),
  buildChunkDelayMs: Number(process.env.BUILD_CHUNK_DELAY_MS || 25),
});
await mgr.init();

const STATIC = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/style.css': ['style.css', 'text/css; charset=utf-8'],
};

function json(res, status, body, headers = {}) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', ...headers });
  res.end(JSON.stringify(body));
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  try {
    // ---- static dashboard ----
    if (req.method === 'GET' && STATIC[url.pathname]) {
      const [file, type] = STATIC[url.pathname];
      res.writeHead(200, { 'content-type': type });
      res.end(await fs.readFile(path.join(PUBLIC_DIR, file)));
      return;
    }

    // ---- read path ----
    if (req.method === 'GET' && url.pathname === '/api/health') {
      json(res, 200, { ok: true, current: mgr.current?.id ?? null });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/api/status') {
      json(res, 200, mgr.getStatus());
      return;
    }
    if (req.method === 'GET' && url.pathname === '/api/query') {
      const result = await mgr.query({
        q: url.searchParams.get('q') ?? '',
        limit: Math.min(Number(url.searchParams.get('limit')) || 200, 10000),
        slowMs: Math.min(Number(url.searchParams.get('slow')) || 0, 100),
        snapshotId: url.searchParams.get('snapshot') || undefined,
      });
      // The snapshot id is echoed in a header so clients (and retries) can
      // pin subsequent requests to the exact same version.
      json(res, 200, result, { 'x-snapshot-id': result.snapshotId });
      return;
    }

    // ---- pipeline ----
    if (req.method === 'POST' && url.pathname === '/api/build') {
      const body = await readBody(req);
      const build = mgr.startBuild(body);
      if (url.searchParams.has('wait')) await build.done;
      json(res, 202, { build: mgr.getStatus().builds.find((b) => b.id === build.id) });
      return;
    }
    if (req.method === 'POST' && url.pathname === '/api/publish') {
      const body = await readBody(req);
      json(res, 200, await mgr.publish(body));
      return;
    }
    if (req.method === 'POST' && url.pathname === '/api/rollback') {
      const body = await readBody(req);
      json(res, 200, await mgr.rollback(body));
      return;
    }

    // ---- fault injection (demo & verification) ----
    if (req.method === 'POST' && url.pathname === '/api/admin/fault') {
      const body = await readBody(req);
      Object.assign(mgr.faults, {
        failNextPublishBeforeRename: !!body.failNextPublishBeforeRename,
        crashAfterRenameOnNextPublish: !!body.crashAfterRenameOnNextPublish,
      });
      json(res, 200, { faults: mgr.faults });
      return;
    }

    json(res, 404, { error: `no route: ${req.method} ${url.pathname}` });
  } catch (err) {
    json(res, err.status || 500, { error: err.message });
  }
});

server.listen(PORT, () => {
  console.log(`snapshot-query-service listening on :${PORT}`);
  console.log(`data dir: ${DATA_DIR}`);
  console.log(`current snapshot: ${mgr.current?.id ?? '(none yet — build & publish one)'}`);
});
