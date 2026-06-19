import { incrementRateCounter } from './db.js';

const RATE = Number(process.env.RATE_LIMIT_PER_MIN || 5);
const WINDOW_MS = 60_000;

/**
 * Fixed-window rate limit, backed by an atomic DB upsert instead of an
 * in-memory Map.
 *
 * Why the old in-memory version was unsafe: `buckets.get/set` is a
 * classic check-then-act race. Two parallel requests for the same userId
 * can both read the same `ent.cnt`, both decide they're under the limit,
 * and both increment — letting more than RATE requests through in a
 * burst. It's also not multi-instance safe: each server process has its
 * own separate Map, so a 5/min limit silently becomes 5/min PER INSTANCE
 * once you run more than one copy of this service.
 *
 * Why this version is safe: incrementRateCounter does the
 * read-modify-write as a single SQL statement (INSERT ... ON CONFLICT DO
 * UPDATE SET count = count + 1), funneled through db.js's write lock so
 * it executes atomically with respect to every other DB operation. The
 * increment itself IS the check — there's no gap between "read the
 * count" and "write the new count" for two requests to interleave in.
 *
 * Multi-instance safety: the same pattern (atomic upsert against a SHARED
 * store) is what makes this safe across multiple running instances too —
 * the requirement is just that all instances increment counters in the
 * same shared store instead of local memory. Here that's one SQLite file;
 * in a real horizontally-scaled deployment you'd point this at Redis
 * (INCR + EXPIRE, or a Lua script for atomicity) or a shared Postgres
 * table with the same upsert pattern — the code shape barely changes,
 * only which store backs it. See SCALE.md.
 */
export async function checkAndConsume(userId, nowMs = Date.now()) {
  const windowStart = Math.floor(nowMs / WINDOW_MS) * WINDOW_MS;
  const count = await incrementRateCounter(userId, windowStart);

  const ok = count <= RATE;
  const remaining = Math.max(RATE - count, 0);
  const resetMs = windowStart + WINDOW_MS;

  return { ok, remaining, resetMs };
}