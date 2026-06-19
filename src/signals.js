import { insertSignal, getByIdemKey, listSignals } from './db.js';
import { checkAndConsume } from './rateLimit.js';

function nowMs() {
  return Date.now();
}

/**
 * sql.js (and better-sqlite3) both surface a UNIQUE constraint violation
 * as an error whose message contains "UNIQUE constraint failed" — that's
 * our signal that a concurrent/earlier request already won the race to
 * insert this idempotency key.
 */
function isUniqueConstraintError(err) {
  return err && /UNIQUE constraint failed/i.test(err.message || '');
}

export async function postSignal(req, reply) {
  const idem = req.headers['idempotency-key'] || null;
  const { userId, type, payload } = req.body || {};
  if (!userId || !type || typeof payload === 'undefined') {
    return reply.code(400).send({ error: 'invalid_body' });
  }

  const { ok, remaining, resetMs } = await checkAndConsume(userId, nowMs());
  if (!ok) return reply.code(429).send({ error: 'rate_limited', remaining, resetMs });

  try {
    const t = nowMs();

    // Atomic idempotency: try the insert first, don't check-then-insert.
    // We do NOT do `if (idem) { check; if not found, insert }` — that has
    // a gap where two concurrent requests with the same key can both
    // check, both see nothing yet, and both insert, creating duplicate
    // rows. Instead, just attempt the insert. The UNIQUE constraint on
    // idempotency_key (defined in db.js) guarantees the database itself
    // rejects a second insert with the same key, even if it arrives at
    // the exact same instant as the first. The loser of that race simply
    // catches the conflict and re-reads what the winner wrote.
    try {
      const info = await insertSignal(userId, type, payload, idem, t);
      return {
        id: info.lastInsertRowid,
        userId,
        type,
        payload: String(payload),
        idempotencyKey: idem,
        createdAt: t,
      };
    } catch (e) {
      if (idem && isUniqueConstraintError(e)) {
        // Someone else already created this exact (idempotency-key)
        // resource — possibly a concurrent request, possibly a client
        // retry of this very request after a dropped response. Return
        // the existing resource instead of creating a duplicate or
        // erroring out, so the client sees one consistent result either
        // way.
        const existing = await getByIdemKey(idem);
        if (existing) return existing;
      }
      throw e;
    }
  } catch (e) {
    req.log.error({ err: e, ctx: 'insertSignal' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}

export async function getSignals(req, reply) {
  const { userId, limit = 20 } = req.query || {};
  if (!userId) return reply.code(400).send({ error: 'missing_userId' });
  const lim = Math.min(Number(limit) || 20, 100);
  try {
    const rows = await listSignals(userId, lim);
    return { items: rows };
  } catch (e) {
    req.log.error({ err: e, ctx: 'listSignals' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}