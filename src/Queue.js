import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import { JobNotRegisteredError } from './errors.js'
import { PendingDispatch } from './PendingDispatch.js'
import { QueueJob } from './QueueJob.js'
import { setDefaultQueue } from './registry.js'
import { Worker } from './Worker.js'
import { MemoryDriver, SyncDriver } from './drivers/MemoryDriver.js'
import { SqliteDriver } from './drivers/SqliteDriver.js'

const driverFactories = {
  sqlite: (options) => new SqliteDriver(options),
  memory: () => new MemoryDriver(),
  sync: () => new SyncDriver(),
}

/**
 * Connects job classes to a storage driver. Plays the part of Laravel's
 * Queue (payloads, pushing), QueueManager (drivers, pausing) and the
 * queue:* console commands.
 *
 *   const queue = createQueue({ driver: 'sqlite', path: 'queue.sqlite', jobs: [SendEmail] })
 *
 * Events (Laravel event in brackets):
 *   job:queued [JobQueued]              job:unique-skipped [UniqueJobSkipped]
 *   job:processing [JobProcessing]      job:processed [JobProcessed]
 *   job:exception-occurred [JobExceptionOccurred]
 *   job:released-after-exception [JobReleasedAfterException]
 *   job:released [JobReleased]          job:failed [JobFailed]
 *   job:timed-out [JobTimedOut]         job:debounced [JobDebounced]
 *   job:attempted [JobAttempted]
 *   worker:starting [WorkerStarting]    worker:stopping [WorkerStopping]
 *   worker:idle [WorkerIdle]            worker:error
 *   worker:queue-paused [WorkerQueuePaused]  worker:queue-resumed [WorkerQueueResumed]
 *   queue:paused [QueuePaused]          queue:resumed [QueueResumed]
 *   warning
 */
export class Queue extends EventEmitter {
  #limiters = new Map()

  constructor(options = {}) {
    super()
    const { driver = 'sqlite', retryAfter = 90, jobs = [], ...driverOptions } = options
    this.retryAfter = retryAfter
    this.driver = typeof driver === 'string' ? Queue.createDriver(driver, driverOptions) : driver
    this.jobs = new Map()
    this.register(...jobs)
  }

  /** Laravel: Queue::extend(). Queue.extend('postgres', (options) => new PostgresDriver(options)) */
  static extend(name, factory) {
    driverFactories[name] = factory
  }

  static createDriver(name, options = {}) {
    const factory = driverFactories[name]
    if (!factory) {
      throw new Error(`Unknown queue driver "${name}". Available: ${Object.keys(driverFactories).join(', ')}.`)
    }
    return factory(options)
  }

  /** Workers can only run jobs whose classes they know about. */
  register(...classes) {
    for (const JobClass of classes.flat()) {
      const name = JobClass.getJobName()
      const existing = this.jobs.get(name)
      if (existing && existing !== JobClass) {
        this.warn(`Two different jobs are registered as "${name}". The last one wins, so give each job its own name.`)
      }
      this.jobs.set(name, JobClass)
    }
    return this
  }

  resolve(name) {
    const JobClass = this.jobs.get(name)
    if (!JobClass) {
      throw new JobNotRegisteredError(`Job "${name}" is not registered. Add it to createQueue({ jobs: [...] }).`)
    }
    return JobClass
  }

  // --- Dispatching ----------------------------------------------------------

  /** Push a job instance. Returns the job id, or null if a unique job was already queued. */
  async dispatch(job, { queue, delay, chain = [] } = {}) {
    const JobClass = job.constructor
    this.register(JobClass)
    const queueName = queue ?? JobClass.queue ?? 'default'
    const payload = this.createPayload(job, chain.map((j) => this.#chainEntry(j, queue)))
    delay ??= JobClass.delay

    // Laravel: PendingDispatch::shouldDispatch() + UniqueLock
    if (JobClass.unique) {
      const key = `unique:${payload.displayName}:${job.uniqueId() ?? ''}`
      if (!(await this.driver.acquireLock(key, JobClass.uniqueFor ?? 0, payload.uuid))) {
        this.emit('job:unique-skipped', { name: payload.displayName, queue: queueName, key })
        return null
      }
      payload.uniqueKey = key
    }

    // Laravel: PendingDispatch::acquireDebounceLock() + DebounceLock
    if (JobClass.debounceFor != null) {
      if (JobClass.unique) throw new Error('A debounced job cannot also be unique.')
      const key = `debounce:${payload.displayName}:${job.debounceId() ?? ''}`
      await this.driver.setMeta(key, payload.uuid)
      payload.debounceKey = key
      payload.debounceOwner = payload.uuid
      delay ??= (await this.#debounceMaxWaitExceeded(key, JobClass.maxDebounceWait)) ? 0 : JobClass.debounceFor
    }

    if (this.driver.sync) {
      await this.#runNow(job, payload, queueName)
      return payload.uuid
    }

    let id
    try {
      id = await this.driver.push(queueName, JSON.stringify(payload), delay ?? 0)
    } catch (error) {
      // The job was never stored, so don't let its lock block the next dispatch.
      if (payload.uniqueKey) await this.driver.releaseLock(payload.uniqueKey, payload.uuid)
      throw error
    }
    this.emit('job:queued', { id, uuid: payload.uuid, name: payload.displayName, queue: queueName, delay: delay ?? 0 })
    return id
  }

  /** Laravel: dispatchSync(). Run a job in this process right now. Errors are thrown to the caller. */
  async dispatchSync(job) {
    const JobClass = job.constructor
    this.register(JobClass)
    await this.#runNow(job, this.createPayload(job, []), JobClass.queue ?? 'default')
  }

  /** Laravel: Bus::chain(). Jobs run one after another; if one fails for good, the rest don't run. */
  chain(jobs, { queue } = {}) {
    const [first, ...rest] = jobs
    if (!first) throw new Error('chain() needs at least one job.')
    const pending = new PendingDispatch(this, first, { chain: rest })
    if (queue) pending.onQueue(queue)
    return pending
  }

  /** Used by workers to dispatch the next job of a chain. */
  async dispatchChain([next, ...rest]) {
    const JobClass = this.resolve(next.job)
    return this.dispatch(new JobClass(next.data), { queue: next.queue, chain: rest })
  }

  /** Laravel: Queue::createObjectPayload(). Same keys, minus the PHP serialization. */
  createPayload(job, chain) {
    const JobClass = job.constructor
    const until = job.retryUntil()
    return {
      uuid: randomUUID(),
      displayName: job.displayName(),
      job: JobClass.getJobName(),
      maxTries: JobClass.tries ?? null,
      maxExceptions: JobClass.maxExceptions ?? null,
      failOnTimeout: Boolean(JobClass.failOnTimeout),
      backoff: JobClass.backoff ?? null,
      timeout: JobClass.timeout ?? null,
      retryUntil: until == null ? null : +until,
      data: job.data,
      createdAt: Date.now(),
      chain,
    }
  }

  // --- Workers --------------------------------------------------------------

  worker(options) {
    return new Worker(this, options)
  }

  /** Laravel: queue:work. Start a worker and resolve when it stops. */
  work(options) {
    return this.worker(options).daemon()
  }

  /** Laravel: queue:restart. Running workers exit after their current job. */
  restart() {
    return this.driver.setMeta('restart', `${Date.now()}-${randomUUID()}`)
  }

  /** Laravel: queue:pause. Workers skip this queue until resumed (or for ttlSeconds). */
  async pause(queue, ttlSeconds = 0) {
    await this.driver.setMeta(`paused:${queue}`, ttlSeconds > 0 ? Date.now() + ttlSeconds * 1000 : 'forever')
    this.emit('queue:paused', { queue, ttl: ttlSeconds })
  }

  /** Laravel: queue:resume. */
  async resume(queue) {
    await this.driver.setMeta(`paused:${queue}`, null)
    this.emit('queue:resumed', { queue })
  }

  async isPaused(queue) {
    const value = await this.driver.getMeta(`paused:${queue}`)
    return value !== null && (value === 'forever' || Number(value) > Date.now())
  }

  async getPausedQueues(queues) {
    const paused = []
    for (const queue of queues) if (await this.isPaused(queue)) paused.push(queue)
    return paused
  }

  // --- Rate limiters (Laravel: RateLimiter::for) ------------------------------

  /** Define a named limiter for the RateLimited middleware: queue.limiter('api', (job) => Limit.perMinute(10)) */
  limiter(name, callback) {
    this.#limiters.set(name, callback)
    return this
  }

  limiterFor(name) {
    return this.#limiters.get(name) ?? null
  }

  // --- Inspecting and managing (Laravel: queue:* commands) ------------------

  async size(queue = 'default') {
    return (await this.driver.stats(queue))[0]?.size ?? 0
  }

  async pendingSize(queue = 'default') {
    return (await this.driver.stats(queue))[0]?.pending ?? 0
  }

  async delayedSize(queue = 'default') {
    return (await this.driver.stats(queue))[0]?.delayed ?? 0
  }

  async reservedSize(queue = 'default') {
    return (await this.driver.stats(queue))[0]?.reserved ?? 0
  }

  /** [{ queue, size, pending, delayed, reserved }] for every queue with jobs. */
  stats() {
    return this.driver.stats()
  }

  /** Laravel: queue:clear. */
  clear(queue = 'default') {
    return this.driver.clear(queue)
  }

  /** Laravel: queue:failed. */
  failed() {
    return this.driver.failed()
  }

  /**
   * Laravel: queue:retry. Pushes failed jobs back with attempts reset and a
   * fresh retryUntil. Pass ids, or 'all'. Returns the ids retried.
   */
  async retry(ids) {
    const records = ids === 'all' ? await this.driver.failed() : await Promise.all([ids].flat().map((id) => this.driver.findFailed(id)))
    const retried = []
    for (const record of records.filter(Boolean)) {
      const payload = JSON.parse(record.payload)
      if (payload.retryUntil && this.jobs.has(payload.job)) {
        const until = new (this.jobs.get(payload.job))(payload.data).retryUntil()
        payload.retryUntil = until == null ? null : +until
      }
      await this.driver.clearHits(`job-exceptions:${payload.uuid}`)
      await this.driver.push(record.queue, JSON.stringify(payload), 0, 0)
      await this.driver.forgetFailed(record.id)
      retried.push(record.id)
    }
    return retried
  }

  /** Laravel: queue:forget. */
  forget(id) {
    return this.driver.forgetFailed(id)
  }

  /** Laravel: queue:flush. */
  flush() {
    return this.driver.flushFailed()
  }

  close() {
    return this.driver.close()
  }

  warn(message) {
    if (this.listenerCount('warning') > 0) this.emit('warning', message)
    else console.warn(`[litequeue] ${message}`)
  }

  // --- Internals ------------------------------------------------------------

  async #debounceMaxWaitExceeded(key, maxWait) {
    if (maxWait == null) return false
    const first = await this.driver.getMeta(`${key}:first`)
    if (first === null) {
      await this.driver.setMeta(`${key}:first`, Date.now())
      return false
    }
    if (Date.now() - Number(first) >= maxWait * 1000) {
      await this.driver.setMeta(`${key}:first`, null)
      return true
    }
    return false
  }

  #chainEntry(job, queue) {
    if (typeof job?.constructor?.getJobName !== 'function') return job // already serialized
    this.register(job.constructor)
    return { job: job.constructor.getJobName(), data: job.data, queue: queue ?? job.constructor.queue ?? 'default' }
  }

  /** Laravel: SyncQueue::executeJob(). Failures call failed() and are re-thrown. */
  async #runNow(instance, payload, queueName) {
    const worker = new Worker(this)
    const job = new QueueJob({ id: null, queue: queueName, attempts: 1, payload, signal: new AbortController().signal, manager: this })
    instance.job = job
    const info = { id: null, uuid: payload.uuid, name: payload.displayName, queue: queueName, attempts: 1, job }
    this.emit('job:processing', info)
    try {
      await worker.call(instance, job)
    } catch (error) {
      await worker.failJob(job, instance, error)
      throw error
    }
    if (job.hasFailed()) await worker.failJob(job, instance, job.failure)
    this.emit('job:processed', { ...info, duration: 0 })
  }
}

/** Create a queue and make it the one Job.dispatch() uses. */
export function createQueue(options) {
  const queue = new Queue(options)
  setDefaultQueue(queue)
  return queue
}
