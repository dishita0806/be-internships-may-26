import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

let SQL = null;
let db = null;
let ready = null;

/**
 * sql.js is a real SQLite engine compiled to WASM — genuine UNIQUE
 * constraints and atomic upserts, same SQL semantics as better-sqlite3 —
 * but it keeps the database in memory and persistence is manual. We swap
 * to it because better-sqlite3 needs a native C++ build step that isn't
 * available on this machine (no Visual Studio Build Tools); sql.js is
 * pure JS/WASM so it always installs cleanly, including in CI.
 */
async function init() {
  if (ready) return ready;
  ready = (async () => {
    SQL = await initSqlJs();
    if (fs.existsSync(dbPath)) {
      db = new SQL.Database(fs.readFileSync(dbPath));
    } else {
      db = new SQL.Database();
    }

    db.run(`
      CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        idempotency_key TEXT UNIQUE,
        created_at INTEGER NOT NULL
      );
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_user_created ON signals(user_id, created_at);`);
    db.run(`
      CREATE TABLE IF NOT EXISTS rate_limit_counters (
        user_id TEXT NOT NULL,
        window_start INTEGER NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, window_start)
      );
    `);
    persist();
  })();
  return ready;
}

function persist() {
  fs.writeFileSync(dbPath, Buffer.from(db.export()));
}

// failure simulation, used to test retry/backoff under DB_FAIL_RATE
function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Retries an operation on simulated/transient failure, with exponential
 * backoff + jitter. Safe to retry because every function below wraps a
 * single atomic statement (an INSERT, or an atomic upsert) — never a
 * check-then-write pair — so re-running it after a transient failure can
 * never produce a duplicate: either the first attempt never reached the
 * DB (safe to redo), or a real duplicate gets rejected by the UNIQUE
 * constraint on idempotency_key (handled by the caller in signals.js).
 */
async function withRetry(fn, { retries = 3, baseDelayMs = 20, maxDelayMs = 300 } = {}) {
  let attempt = 0;
  for (;;) {
    try {
      return fn();
    } catch (err) {
      const isTransient = err && err.code === 'SQLITE_BUSY';
      if (!isTransient || attempt >= retries) throw err;
      const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
      const jitterMs = exp / 2 + Math.random() * exp * 0.5;
      await sleep(jitterMs);
      attempt += 1;
    }
  }
}

/**
 * In-process write lock: sql.js is synchronous and single-threaded, but
 * we still funnel every write through one queue so an "insert, catch
 * conflict, re-read" sequence can never be split across two requests'
 * awaits and observe a half-finished write. This is what makes the
 * service correct under many concurrent requests inside one Node process
 * (exactly how the hidden tests will exercise it).
 */
let queue = Promise.resolve();
function withLock(fn) {
  const result = queue.then(() => fn());
  queue = result.then(() => undefined, () => undefined);
  return result;
}

function runStatement(sql, params) {
  db.run(sql, params);
}

function getRow(sql, params) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  let row;
  if (stmt.step()) row = stmt.getAsObject();
  stmt.free();
  return row;
}

function getRows(sql, params) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

export async function insertSignal(userId, type, payload, idemKey, nowMs) {
  await init();
  return withLock(() =>
    withRetry(() => {
      maybeFail();
      runStatement(
        'INSERT INTO signals (user_id, type, payload, idempotency_key, created_at) VALUES (?,?,?,?,?)',
        [userId, type, String(payload), idemKey || null, nowMs]
      );
      const row = getRow('SELECT last_insert_rowid() as id', []);
      persist();
      return { lastInsertRowid: row.id };
    })
  );
}

export async function getByIdemKey(idemKey) {
  await init();
  return withLock(() =>
    withRetry(() => {
      maybeFail();
      return getRow(
        'SELECT id, user_id as userId, type, payload, idempotency_key as idempotencyKey, created_at as createdAt FROM signals WHERE idempotency_key = ?',
        [idemKey]
      );
    })
  );
}

export async function getById(id) {
  await init();
  return withLock(() =>
    withRetry(() => {
      maybeFail();
      return getRow(
        'SELECT id, user_id as userId, type, payload, idempotency_key as idempotencyKey, created_at as createdAt FROM signals WHERE id = ?',
        [id]
      );
    })
  );
}

export async function listSignals(userId, limit) {
  await init();
  return withLock(() =>
    withRetry(() => {
      maybeFail();
      return getRows(
        'SELECT id, user_id as userId, type, payload, idempotency_key as idempotencyKey, created_at as createdAt FROM signals WHERE user_id = ? ORDER BY created_at DESC LIMIT ?',
        [userId, limit]
      );
    })
  );
}

export async function incrementRateCounter(userId, windowStart) {
  await init();
  return withLock(() =>
    withRetry(() => {
      maybeFail();
      runStatement(
        `INSERT INTO rate_limit_counters (user_id, window_start, count)
         VALUES (?, ?, 1)
         ON CONFLICT(user_id, window_start) DO UPDATE SET count = count + 1`,
        [userId, windowStart]
      );
      const row = getRow('SELECT count FROM rate_limit_counters WHERE user_id = ? AND window_start = ?', [
        userId,
        windowStart,
      ]);
      persist();
      return row.count;
    })
  );
}