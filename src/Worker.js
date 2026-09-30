import { MaxAttemptsExceededError, TimeoutExceededError } from './errors.js'
import { QueueJob } from './QueueJob.js'

/** Laravel: WorkerOptions (same defaults). `queue` and `concurrency` are litequeue additions. */
export const WORKER_DEFAULTS = {
  queue: ['default'], // checked in order, so list high priority first
  concurrency: 1, // jobs processed at the same time in this process
  backoff: 0,
  memory: 128, // MB
  timeout: 60,
  sleep: 3,
  tries: 1, // Laravel: maxTries
  stopWhenEmpty: false,
  stopWhenEmptyFor: 0,
  maxJobs: 0,
  maxTime: 0,
  rest: 0,
  killOnTimeout: true, // Laravel: Worker::$killOnTimeout
}

/** Laravel: WorkerStopReason, plus the exit codes Worker uses. */
export const STOP_REASONS = {
  interrupted: 0,
  memory: 12,
  restart: 0,
  'queue-empty': 0,
  'queue-empty-for': 0,
  'max-time': 0,
  'max-jobs': 0,
  'timed-out': 1,
}

/**
 * Laravel: Illuminate\Queue\Worker, with CallQueuedHandler folded in.
 * Method names follow Laravel's so the two are easy to compare.
 */
export class Worker {
  #wakers = new Set()
  #pausedQueues = new Set()
  #claimed = 0 // jobs reserved so far, so parallel loops don't pass maxJobs

  constructor(queue, options = {}) {
    this.queue = queue
    this.driver = queue.driver
    const given = Object.fromEntries(Object.entries(options).filter(([, v]) => v !== undefined))
    this.options = { ...WORKER_DEFAULTS, ...given }
    if (typeof this.options.queue === 'string') {
      this.options.queue = this.options.queue.split(',').map((q) => q.trim()).filter(Boolean)
    }
    this.jobsProcessed = 0
    this.shouldQuit = false
    this.stopReason = null
  }

  /** Stop after the jobs currently running finish. Laravel: $shouldQuit (SIGTERM). */
  stop(reason = 'interrupted') {
    this.stopReason ??= reason
    this.shouldQuit = true
    for (const wake of this.#wakers) wake()
  }

  /** Laravel: daemon(). */
  async daemon() {
    if (this.driver.sync) throw new Error('The sync driver runs jobs when they are dispatched; there is nothing for a worker to do.')
    this.lastRestart = await this.driver.getMeta('restart')
    this.startTime = Date.now()
    this.lastJobProcessedAt = null
    this.#warnAboutRetryAfter()
    this.#emit('worker:starting', { queues: this.options.queue, options: this.options })

    const loops = Array.from({ length: Math.max(1, this.options.concurrency) }, () => this.#loop())
    await Promise.all(loops)

    const reason = this.stopReason ?? 'interrupted'
    const status = STOP_REASONS[reason] ?? 0
    this.#emit('worker:stopping', { status, reason, processed: this.jobsProcessed })
    return { status, reason, processed: this.jobsProcessed }
  }

  run() {
    return this.daemon()
  }

  async #loop() {
    while (!this.shouldQuit) {
      const { maxJobs } = this.options
      if (maxJobs && this.#claimed >= maxJobs) break
      this.#claimed++
      const job = await this.getNextJob()
      if (!job) this.#claimed--

      if (job) {
        this.jobsProcessed++
        await this.process(job)
        this.lastJobProcessedAt = Date.now()
        if (this.options.rest > 0) await this.sleep(this.options.rest)
      } else {
        this.#emit('worker:idle', { queues: this.options.queue })
        if (!this.options.stopWhenEmpty) await this.sleep(this.options.sleep)
      }

      const reason = await this.stopIfNecessary(job)
      if (reason) this.stop(reason)
    }
  }

  /** Laravel: stopIfNecessary(). Same checks, same order. */
  async stopIfNecessary(job) {
    const { memory, stopWhenEmpty, stopWhenEmptyFor, maxTime, maxJobs } = this.options
    const now = Date.now()
    if (this.shouldQuit) return this.stopReason
    if (memory && process.memoryUsage().rss / 1024 / 1024 >= memory) return 'memory'
    if ((await this.driver.getMeta('restart')) !== this.lastRestart) return 'restart'
    if (stopWhenEmpty && !job) return 'queue-empty'
    if (stopWhenEmptyFor && !job && now - (this.lastJobProcessedAt ?? this.startTime) >= stopWhenEmptyFor * 1000) return 'queue-empty-for'
    if (maxTime && now - this.startTime >= maxTime * 1000) return 'max-time'
    if (maxJobs && this.jobsProcessed >= maxJobs) return 'max-jobs'
    return null
  }

  /** Laravel: getNextJob(). Tries each queue in order, skipping paused ones. */
  async getNextJob() {
    try {
      const paused = await this.queue.getPausedQueues(this.options.queue)
      this.#raisePausedQueueEvents(paused)
      for (const name of this.options.queue) {
        if (paused.includes(name)) continue
        const reserved = await this.driver.pop(name, this.queue.retryAfter)
        if (reserved) return reserved
      }
    } catch (error) {
      this.#emit('worker:error', error)
      await this.sleep(1)
    }
    return null
  }

  #raisePausedQueueEvents(paused) {
    for (const name of paused) if (!this.#pausedQueues.has(name)) this.#emit('worker:queue-paused', { queue: name })
    for (const name of this.#pausedQueues) if (!paused.includes(name)) this.#emit('worker:queue-resumed', { queue: name })
    this.#pausedQueues = new Set(paused)
  }

  sleep(seconds) {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer)
        this.#wakers.delete(done)
        resolve()
      }
      const timer = setTimeout(done, seconds * 1000)
      this.#wakers.add(done)
    })
  }

  /** Laravel: process(). Runs one reserved job: { id, queue, payload, attempts }. */
  async process(reserved) {
    let payload
    try {
      payload = JSON.parse(reserved.payload)
    } catch (error) {
      await this.driver.fail({ id: reserved.id, queue: reserved.queue, payload: reserved.payload, error })
      this.#emit('job:failed', { id: reserved.id, name: 'unknown', queue: reserved.queue, attempts: reserved.attempts, error })
      return
    }

    const abort = new AbortController()
    const job = new QueueJob({ ...reserved, payload, signal: abort.signal, manager: this.queue })
    const info = this.#info(job)
    let instance = null
    let exceptionOccurred = null

    try {
      this.#emit('job:processing', info)
      this.markJobAsFailedIfAlreadyExceedsMaxAttempts(job)

      const JobClass = this.queue.resolve(payload.job)
      instance = new JobClass(payload.data)
      instance.job = job

      const started = performance.now()
      await this.#withTimeout(() => this.call(instance, job), this.timeoutForJob(job), abort, job)
      const duration = Math.round((performance.now() - started) * 100) / 100

      await this.#settle(job, instance)
      this.#emit('job:processed', { ...info, duration })
      if (job.isReleased() && !job.isDeleted() && !job.hasFailed()) this.#emit('job:released', { ...info, delay: job.releaseDelay })
    } catch (error) {
      exceptionOccurred = error
      if (error instanceof TimeoutExceededError && this.options.killOnTimeout) {
        await this.#handleTimeout(job, instance, error)
      } else {
        await this.handleJobException(job, instance, error)
      }
    } finally {
      this.#emit('job:attempted', { ...info, error: exceptionOccurred })
    }
  }

  /**
   * Laravel: CallQueuedHandler::call(). Debounce check, middleware pipeline,
   * unique lock release, then the next job in the chain.
   */
  async call(instance, job) {
    const JobClass = instance.constructor
    const payload = job.payload()

    if (await this.#shouldBeDebounced(payload)) {
      job.delete()
      this.#emit('job:debounced', this.#info(job))
      return
    }

    let uniqueLockReleased = false
    const releaseUniqueForProcessing = async () => {
      if (!uniqueLockReleased && JobClass.uniqueUntilProcessing) {
        uniqueLockReleased = true
        await this.#releaseUniqueLock(payload)
      }
    }

    try {
      await this.#pipeline(instance, async () => {
        await releaseUniqueForProcessing()
        if (payload.debounceKey) await this.driver.setMeta(`${payload.debounceKey}:first`, null)
        return instance.handle(job)
      })
    } finally {
      if (!job.isReleased()) await releaseUniqueForProcessing()
    }

    // Timed out: the worker has already moved on (Laravel would have killed the process).
    if (job.signal.aborted) return

    if (!job.isReleased() && !JobClass.uniqueUntilProcessing) await this.#releaseUniqueLock(payload)
    if (!job.hasFailed() && !job.isReleased() && payload.chain?.length) await this.queue.dispatchChain(payload.chain)
    if (!job.isDeletedOrReleased()) job.delete()
  }

  #pipeline(instance, destination) {
    const stack = (instance.middleware() ?? []).map((m) => (typeof m === 'function' ? m : m.handle.bind(m)))
    const next = (i) => (i < stack.length ? stack[i](instance, () => next(i + 1)) : destination())
    return next(0)
  }

  /** Apply what the job asked for: fail, release or delete. */
  async #settle(job, instance) {
    if (job.hasFailed()) return this.failJob(job, instance, job.failure)
    if (job.isReleased()) return this.driver.release(job.id, job.releaseDelay)
    return this.driver.delete(job.id)
  }

  /** Laravel: handleJobException(). */
  async handleJobException(job, instance, error) {
    if (!job.hasFailed()) {
      this.markJobAsFailedIfWillExceedMaxAttempts(job, error)
      await this.markJobAsFailedIfWillExceedMaxExceptions(job, error)
    }

    this.#emit('job:exception-occurred', { ...this.#info(job), error })

    if (job.hasFailed()) return this.failJob(job, instance, job.failure)
    if (job.isDeleted()) return this.driver.delete(job.id)
    if (job.isReleased()) return this.driver.release(job.id, job.releaseDelay)

    const backoff = this.calculateBackoff(job)
    await this.driver.release(job.id, backoff)
    this.#emit('job:released-after-exception', { ...this.#info(job), delay: backoff, error })
  }

  /**
   * Laravel: the SIGALRM handler in registerTimeoutHandler(). Mark the job
   * failed if it has no chances left, then stop the worker. A job that isn't
   * failed stays reserved and runs again once retryAfter passes.
   */
  async #handleTimeout(job, instance, error) {
    if (!job.hasFailed()) {
      this.markJobAsFailedIfWillExceedMaxAttempts(job, error)
      await this.markJobAsFailedIfWillExceedMaxExceptions(job, error)
      if (job.shouldFailOnTimeout()) job.fail(error)
    }
    this.#emit('job:timed-out', { ...this.#info(job), timeout: this.timeoutForJob(job) })
    if (job.hasFailed()) await this.failJob(job, instance, job.failure)
    this.stop('timed-out')
  }

  /** Laravel: markJobAsFailedIfAlreadyExceedsMaxAttempts(). Throws after failing. */
  markJobAsFailedIfAlreadyExceedsMaxAttempts(job) {
    const maxTries = job.maxTries() ?? this.options.tries
    const retryUntil = job.retryUntil()
    if (retryUntil && Date.now() <= retryUntil) return
    if (!retryUntil && (maxTries === 0 || job.attempts() <= maxTries)) return
    const error = new MaxAttemptsExceededError(
      `${job.displayName()} has been attempted too many times. The job may have previously timed out.`,
    )
    job.fail(error)
    throw error
  }

  /** Laravel: markJobAsFailedIfWillExceedMaxAttempts(). */
  markJobAsFailedIfWillExceedMaxAttempts(job, error) {
    const maxTries = job.maxTries() ?? this.options.tries
    const retryUntil = job.retryUntil()
    if (retryUntil && retryUntil <= Date.now()) job.fail(error)
    if (!retryUntil && maxTries > 0 && job.attempts() >= maxTries) job.fail(error)
  }

  /** Laravel: markJobAsFailedIfWillExceedMaxExceptions(). Counts exceptions per job uuid. */
  async markJobAsFailedIfWillExceedMaxExceptions(job, error) {
    const maxExceptions = job.maxExceptions()
    if (!maxExceptions || !job.uuid()) return
    const key = `job-exceptions:${job.uuid()}`
    if (maxExceptions <= (await this.driver.hit(key, 86400))) {
      await this.driver.clearHits(key)
      job.fail(error)
    }
  }

  /** Laravel: calculateBackoff(). The last value repeats for later attempts. */
  calculateBackoff(job) {
    const backoff = job.backoff() ?? this.options.backoff
    const list = Array.isArray(backoff) ? backoff : String(backoff).split(',').map(Number)
    return list[job.attempts() - 1] ?? list.at(-1) ?? 0
  }

  timeoutForJob(job) {
    return job.timeout() ?? this.options.timeout
  }

  /**
   * Laravel: Jobs\Job::fail(). Deletes the job, records it in failed jobs,
   * calls the job's failed() method and fires job:failed.
   */
  async failJob(job, instance, error) {
    job.fail(error)
    const payload = job.payload()
    try {
      await this.driver.fail({ id: job.id, queue: job.queue, payload: JSON.stringify(payload), error })
      const JobClass = instance?.constructor ?? this.queue.jobs.get(payload.job)
      if (!JobClass?.uniqueUntilProcessing) await this.#releaseUniqueLock(payload)
      const target = instance ?? (JobClass ? new JobClass(payload.data) : null)
      if (target) {
        try {
          await target.failed(error)
        } catch (hookError) {
          this.#emit('worker:error', hookError)
        }
      }
    } finally {
      this.#emit('job:failed', { ...this.#info(job), error })
    }
  }

  async #shouldBeDebounced(payload) {
    if (!payload.debounceKey || !payload.debounceOwner) return false
    const current = await this.driver.getMeta(payload.debounceKey)
    // Fail open, like Laravel: if the key is gone, run the job.
    return current !== null && current !== payload.debounceOwner
  }

  async #releaseUniqueLock(payload) {
    if (payload.uniqueKey) await this.driver.releaseLock(payload.uniqueKey, payload.uuid)
  }

  async #withTimeout(run, seconds, abort, job) {
    if (!seconds) return run()
    let timer
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        const error = new TimeoutExceededError(`${job.displayName()} has timed out.`)
        abort.abort(error)
        reject(error)
      }, seconds * 1000)
    })
    try {
      return await Promise.race([run(), timeout])
    } finally {
      clearTimeout(timer)
    }
  }

  #info(job) {
    return { id: job.id, uuid: job.uuid(), name: job.displayName(), queue: job.queue, attempts: job.attempts(), job }
  }

  #emit(event, data) {
    this.queue.emit(event, data)
  }

  // Not in Laravel's Worker, but its docs warn about it: a job running longer
  // than retryAfter gets picked up by a second worker while the first is busy.
  #warnAboutRetryAfter() {
    const retryAfter = this.queue.retryAfter
    if (!retryAfter) return
    for (const [name, JobClass] of this.queue.jobs) {
      const timeout = JobClass.timeout ?? this.options.timeout
      if (!timeout || timeout >= retryAfter) {
        this.queue.warn(
          `${name} can run for ${timeout ? `${timeout}s` : 'ever'}, but retryAfter is ${retryAfter}s. ` +
            'A second worker may start it while the first is still running. Set retryAfter a few seconds longer than the longest timeout.',
        )
      }
    }
  }
}
