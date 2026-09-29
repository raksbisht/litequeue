import { Driver, expiresAt } from './Driver.js'

/** Keeps everything in memory. Handy for tests and scripts. Lost when the process exits. */
export class MemoryDriver extends Driver {
  #jobs = []
  #failed = []
  #locks = new Map()
  #hits = new Map()
  #meta = new Map()
  #nextId = 1
  #nextFailedId = 1

  async push(queue, payload, delaySeconds = 0, attempts = 0) {
    const now = Date.now()
    const job = { id: this.#nextId++, queue, payload, attempts, reservedAt: null, availableAt: now + delaySeconds * 1000 }
    this.#jobs.push(job)
    return job.id
  }

  async pop(queue, retryAfterSeconds) {
    const now = Date.now()
    const expired = retryAfterSeconds ? now - retryAfterSeconds * 1000 : -Infinity
    const job = this.#jobs.find(
      (j) => j.queue === queue && ((j.reservedAt === null && j.availableAt <= now) || (j.reservedAt !== null && j.reservedAt <= expired)),
    )
    if (!job) return null
    job.reservedAt = now
    job.attempts++
    return { id: job.id, queue: job.queue, payload: job.payload, attempts: job.attempts }
  }

  async release(id, delaySeconds = 0) {
    const job = this.#jobs.find((j) => j.id === id)
    if (!job) return null
    await this.delete(id)
    return this.push(job.queue, job.payload, delaySeconds, job.attempts)
  }

  async delete(id) {
    this.#jobs = this.#jobs.filter((j) => j.id !== id)
  }

  async clear(queue) {
    const before = this.#jobs.length
    this.#jobs = this.#jobs.filter((j) => j.queue !== queue)
    return before - this.#jobs.length
  }

  async stats(queue) {
    const now = Date.now()
    const byQueue = new Map()
    for (const j of this.#jobs) {
      if (queue && j.queue !== queue) continue
      const s = byQueue.get(j.queue) ?? { queue: j.queue, size: 0, pending: 0, delayed: 0, reserved: 0 }
      s.size++
      if (j.reservedAt !== null) s.reserved++
      else if (j.availableAt > now) s.delayed++
      else s.pending++
      byQueue.set(j.queue, s)
    }
    return [...byQueue.values()].sort((a, b) => a.queue.localeCompare(b.queue))
  }

  async fail({ id, queue, payload, error }) {
    let uuid = null
    try {
      uuid = JSON.parse(payload).uuid ?? null
    } catch {}
    this.#failed.push({ id: this.#nextFailedId++, uuid, queue, payload, exception: error?.stack ?? String(error), failedAt: Date.now() })
    await this.delete(id)
  }

  async failed() {
    return [...this.#failed].reverse()
  }

  async findFailed(id) {
    return this.#failed.find((f) => f.id === Number(id)) ?? null
  }

  async forgetFailed(id) {
    const before = this.#failed.length
    this.#failed = this.#failed.filter((f) => f.id !== Number(id))
    return this.#failed.length < before
  }

  async flushFailed() {
    const count = this.#failed.length
    this.#failed = []
    return count
  }

  async acquireLock(key, ttlSeconds, owner) {
    const now = Date.now()
    const lock = this.#locks.get(key)
    if (lock && lock.expiresAt > now) return false
    this.#locks.set(key, { owner, expiresAt: expiresAt(ttlSeconds, now) })
    return true
  }

  async releaseLock(key, owner) {
    if (this.#locks.get(key)?.owner === owner) this.#locks.delete(key)
  }

  async hit(key, decaySeconds) {
    const now = Date.now()
    const entry = this.#hits.get(key)
    if (!entry || entry.expiresAt <= now) {
      this.#hits.set(key, { count: 1, expiresAt: expiresAt(decaySeconds, now) })
      return 1
    }
    return ++entry.count
  }

  async attempts(key) {
    const entry = this.#hits.get(key)
    return entry && entry.expiresAt > Date.now() ? entry.count : 0
  }

  async availableIn(key) {
    const entry = this.#hits.get(key)
    return entry ? Math.max(0, Math.ceil((entry.expiresAt - Date.now()) / 1000)) : 0
  }

  async clearHits(key) {
    this.#hits.delete(key)
  }

  async getMeta(key) {
    return this.#meta.get(key) ?? null
  }

  async setMeta(key, value) {
    if (value === null || value === undefined) this.#meta.delete(key)
    else this.#meta.set(key, String(value))
  }
}

/** Laravel: SyncQueue. Runs jobs immediately when they are dispatched. Errors reach the caller. */
export class SyncDriver extends MemoryDriver {
  sync = true
}
