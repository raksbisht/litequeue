import { Driver, expiresAt } from './Driver.js'

// node:sqlite ships with Node 22.13+. It prints an "experimental" warning on
// load, which is noise for queue users, so we silence that one warning only.
let DatabaseSync = null
let loadError = null
{
  const original = process.emitWarning
  process.emitWarning = function (warning, ...args) {
    const type = typeof args[0] === 'string' ? args[0] : args[0]?.type
    if (type === 'ExperimentalWarning' && String(warning?.message ?? warning).includes('SQLite')) return
    return original.call(this, warning, ...args)
  }
  try {
    ;({ DatabaseSync } = await import('node:sqlite'))
  } catch (error) {
    loadError = error
  } finally {
    process.emitWarning = original
  }
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    queue        TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    reserved_at  INTEGER,
    available_at INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS jobs_queue_index ON jobs (queue);

  CREATE TABLE IF NOT EXISTS failed_jobs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid      TEXT,
    queue     TEXT    NOT NULL,
    payload   TEXT    NOT NULL,
    exception TEXT    NOT NULL,
    failed_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS locks (
    key        TEXT PRIMARY KEY,
    owner      TEXT    NOT NULL,
    expires_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS hits (
    key        TEXT PRIMARY KEY,
    count      INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
  );
`

/**
 * Laravel: DatabaseQueue + DatabaseFailedJobProvider + database cache store,
 * in one SQLite file. Safe to use from several worker processes at once:
 * every reservation happens inside a write transaction (SQLite has no
 * SKIP LOCKED, so BEGIN IMMEDIATE plays the role of Laravel's popping lock).
 */
export class SqliteDriver extends Driver {
  constructor({ path = 'queue.sqlite', busyTimeout = 5000 } = {}) {
    super()
    if (!DatabaseSync) {
      throw new Error(
        `The sqlite driver needs Node.js 22.13 or newer (node:sqlite could not be loaded: ${loadError?.message}). ` +
          'Upgrade Node, or use another driver.',
      )
    }
    this.path = path
    this.db = new DatabaseSync(path)
    this.db.exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = ${Number(busyTimeout)};`)
    this.db.exec(SCHEMA)

    const sql = {
      push: 'INSERT INTO jobs (queue, payload, attempts, available_at, created_at) VALUES (?, ?, ?, ?, ?)',
      // Same conditions as DatabaseQueue::getNextAvailableJob(): available, or reserved but expired.
      pop: `UPDATE jobs SET reserved_at = ?, attempts = attempts + 1
            WHERE id = (
              SELECT id FROM jobs
              WHERE queue = ?
                AND ((reserved_at IS NULL AND available_at <= ?) OR reserved_at <= ?)
              ORDER BY id LIMIT 1
            )
            RETURNING id, queue, payload, attempts`,
      find: 'SELECT id, queue, payload, attempts FROM jobs WHERE id = ?',
      delete: 'DELETE FROM jobs WHERE id = ?',
      clear: 'DELETE FROM jobs WHERE queue = ?',
      stats: `SELECT queue,
                COUNT(*) AS size,
                SUM(reserved_at IS NULL AND available_at <= ?) AS pending,
                SUM(reserved_at IS NULL AND available_at > ?) AS delayed,
                SUM(reserved_at IS NOT NULL) AS reserved
              FROM jobs WHERE (? IS NULL OR queue = ?) GROUP BY queue ORDER BY queue`,
      insertFailed: 'INSERT INTO failed_jobs (uuid, queue, payload, exception, failed_at) VALUES (?, ?, ?, ?, ?)',
      failed: 'SELECT id, uuid, queue, payload, exception, failed_at AS failedAt FROM failed_jobs ORDER BY id DESC',
      findFailed: 'SELECT id, uuid, queue, payload, exception, failed_at AS failedAt FROM failed_jobs WHERE id = ?',
      forgetFailed: 'DELETE FROM failed_jobs WHERE id = ?',
      flushFailed: 'DELETE FROM failed_jobs',
      acquireLock: `INSERT INTO locks (key, owner, expires_at) VALUES (?, ?, ?)
                    ON CONFLICT (key) DO UPDATE SET owner = excluded.owner, expires_at = excluded.expires_at
                    WHERE locks.expires_at <= ?`,
      releaseLock: 'DELETE FROM locks WHERE key = ? AND owner = ?',
      hit: `INSERT INTO hits (key, count, expires_at) VALUES (?, 1, ?)
            ON CONFLICT (key) DO UPDATE SET
              count = CASE WHEN hits.expires_at <= ? THEN 1 ELSE hits.count + 1 END,
              expires_at = CASE WHEN hits.expires_at <= ? THEN excluded.expires_at ELSE hits.expires_at END
            RETURNING count`,
      getHits: 'SELECT count, expires_at FROM hits WHERE key = ?',
      clearHits: 'DELETE FROM hits WHERE key = ?',
      getMeta: 'SELECT value FROM meta WHERE key = ?',
      setMeta: 'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value',
      deleteMeta: 'DELETE FROM meta WHERE key = ?',
    }
    this.stmt = Object.fromEntries(Object.entries(sql).map(([name, text]) => [name, this.db.prepare(text)]))
  }

  #transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const result = fn()
      this.db.exec('COMMIT')
      return result
    } catch (error) {
      try {
        this.db.exec('ROLLBACK')
      } catch {}
      throw error
    }
  }

  #insert(queue, payload, delaySeconds, attempts) {
    const now = Date.now()
    return Number(this.stmt.push.run(queue, payload, attempts, now + delaySeconds * 1000, now).lastInsertRowid)
  }

  async push(queue, payload, delaySeconds = 0, attempts = 0) {
    return this.#insert(queue, payload, delaySeconds, attempts)
  }

  async pop(queue, retryAfterSeconds) {
    const now = Date.now()
    const expired = retryAfterSeconds ? now - retryAfterSeconds * 1000 : -1
    const row = this.#transaction(() => this.stmt.pop.get(now, queue, now, expired))
    return row ? { id: row.id, queue: row.queue, payload: row.payload, attempts: row.attempts } : null
  }

  async release(id, delaySeconds = 0) {
    return this.#transaction(() => {
      const row = this.stmt.find.get(id)
      if (!row) return null
      this.stmt.delete.run(id)
      return this.#insert(row.queue, row.payload, delaySeconds, row.attempts)
    })
  }

  async delete(id) {
    this.stmt.delete.run(id)
  }

  async clear(queue) {
    return Number(this.stmt.clear.run(queue).changes)
  }

  async stats(queue = null) {
    const now = Date.now()
    return this.stmt.stats.all(now, now, queue, queue).map((r) => ({
      queue: r.queue,
      size: Number(r.size),
      pending: Number(r.pending),
      delayed: Number(r.delayed),
      reserved: Number(r.reserved),
    }))
  }

  async fail({ id, queue, payload, error }) {
    let uuid = null
    try {
      uuid = JSON.parse(payload).uuid ?? null
    } catch {}
    this.#transaction(() => {
      this.stmt.insertFailed.run(uuid, queue, payload, error?.stack ?? String(error), Date.now())
      this.stmt.delete.run(id)
    })
  }

  async failed() {
    return this.stmt.failed.all().map((row) => ({ ...row }))
  }

  async findFailed(id) {
    const row = this.stmt.findFailed.get(Number(id))
    return row ? { ...row } : null
  }

  async forgetFailed(id) {
    return this.stmt.forgetFailed.run(Number(id)).changes > 0
  }

  async flushFailed() {
    return Number(this.stmt.flushFailed.run().changes)
  }

  async acquireLock(key, ttlSeconds, owner) {
    const now = Date.now()
    return this.stmt.acquireLock.run(key, owner, expiresAt(ttlSeconds, now), now).changes > 0
  }

  async releaseLock(key, owner) {
    this.stmt.releaseLock.run(key, owner)
  }

  async hit(key, decaySeconds) {
    const now = Date.now()
    return this.stmt.hit.get(key, expiresAt(decaySeconds, now), now, now).count
  }

  async attempts(key) {
    const row = this.stmt.getHits.get(key)
    return row && row.expires_at > Date.now() ? row.count : 0
  }

  async availableIn(key) {
    const row = this.stmt.getHits.get(key)
    return row ? Math.max(0, Math.ceil((row.expires_at - Date.now()) / 1000)) : 0
  }

  async clearHits(key) {
    this.stmt.clearHits.run(key)
  }

  async getMeta(key) {
    return this.stmt.getMeta.get(key)?.value ?? null
  }

  async setMeta(key, value) {
    if (value === null || value === undefined) this.stmt.deleteMeta.run(key)
    else this.stmt.setMeta.run(key, String(value))
  }

  async close() {
    try {
      this.db.close()
    } catch {} // already closed
  }
}
