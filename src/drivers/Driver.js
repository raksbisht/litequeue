/**
 * The contract every storage backend implements. Extend this to add a driver
 * (Postgres, Redis, ...) and register it with Queue.extend().
 *
 * Laravel splits this across Queue (jobs), FailedJobProvider (failed jobs) and
 * the cache (locks, rate limits, restart and pause signals). litequeue keeps them
 * in one driver so a single SQLite file is all you need.
 *
 * Times passed in are seconds. `payload` is an opaque JSON string.
 * A reserved job looks like: { id, queue, payload, attempts }.
 */
export class Driver {
  /** True for drivers that run jobs at dispatch time instead of storing them. */
  sync = false

  // --- Jobs (Laravel: DatabaseQueue) ----------------------------------------

  /** Store a job. Return its id. */
  async push(queue, payload, delaySeconds = 0, attempts = 0) { this.#todo('push') }

  /**
   * Atomically reserve the next job on `queue` and increment its attempts.
   * A job is available when it is not reserved and its delay has passed, or
   * when it was reserved more than `retryAfterSeconds` ago (its worker probably
   * died). Oldest id first. Return null when nothing is available.
   */
  async pop(queue, retryAfterSeconds) { this.#todo('pop') }

  /**
   * Put a reserved job back. Like Laravel's deleteAndRelease(), this deletes the
   * row and inserts a new one with the same payload and attempts, so a released
   * job goes to the back of the queue.
   */
  async release(id, delaySeconds = 0) { this.#todo('release') }

  /** Remove a finished job. */
  async delete(id) { this.#todo('delete') }

  /** Delete every job on a queue, and release their unique locks (see uniqueLockOf). Return how many were removed. */
  async clear(queue) { this.#todo('clear') }

  /** Counts for each queue: [{ queue, size, pending, delayed, reserved }]. Filter by queue if given. */
  async stats(queue) { this.#todo('stats') }

  // --- Failed jobs (Laravel: FailedJobProvider) -----------------------------

  /** Move a job to failed jobs: { id, queue, payload, error }. */
  async fail(record) { this.#todo('fail') }
  /** Newest first: [{ id, uuid, queue, payload, exception, failedAt }]. */
  async failed() { this.#todo('failed') }
  async findFailed(id) { this.#todo('findFailed') }
  /** Return true if something was removed. */
  async forgetFailed(id) { this.#todo('forgetFailed') }
  /** Return how many were removed. */
  async flushFailed() { this.#todo('flushFailed') }

  // --- Cache-like storage (Laravel: Cache locks, RateLimiter, cache keys) ---

  /** Take a lock if it is free or expired. ttlSeconds 0 means it never expires. Return true on success. */
  async acquireLock(key, ttlSeconds, owner) { this.#todo('acquireLock') }
  /** Release a lock, but only if `owner` still holds it. */
  async releaseLock(key, owner) { this.#todo('releaseLock') }

  /** Atomically count a hit in a window of decaySeconds. Return the new count. */
  async hit(key, decaySeconds) { this.#todo('hit') }
  /** Hits in the current window (0 if the window has passed). */
  async attempts(key) { this.#todo('attempts') }
  /** Seconds until the current window ends (0 if none). */
  async availableIn(key) { this.#todo('availableIn') }
  async clearHits(key) { this.#todo('clearHits') }

  /** Small key/value store. setMeta(key, null) deletes the key. */
  async getMeta(key) { this.#todo('getMeta') }
  async setMeta(key, value) { this.#todo('setMeta') }

  async close() {}

  #todo(method) {
    throw new Error(`${this.constructor.name} does not implement ${method}()`)
  }
}

/** Far-future timestamp used for locks and keys that never expire. */
export const FOREVER = 8.64e15

/** The unique lock a stored job holds, if any: { key, owner }. Used by clear() so it doesn't leave locks behind. */
export function uniqueLockOf(payload) {
  try {
    const { uniqueKey, uuid } = JSON.parse(payload)
    return uniqueKey ? { key: uniqueKey, owner: uuid } : null
  } catch {
    return null
  }
}

export function expiresAt(ttlSeconds, now = Date.now()) {
  return ttlSeconds > 0 ? now + ttlSeconds * 1000 : FOREVER
}
