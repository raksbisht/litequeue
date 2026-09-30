/**
 * Job middleware, ported from Illuminate\Queue\Middleware. Each class has the
 * same constructor arguments and fluent methods as its Laravel namesake.
 * Middleware receives (job, next): `job` is your job instance, and
 * job.release() / job.fail() / job.delete() work as in Laravel.
 *
 * Plain functions work too: middleware() { return [async (job, next) => next()] }
 */

const store = (job) => {
  const manager = job.job?.manager
  if (!manager) throw new Error('This middleware needs a queue. It only works inside a worker or a sync dispatch.')
  return manager
}

/** Laravel: WithoutOverlapping. Only one job with the same key runs at a time. */
export class WithoutOverlapping {
  prefix = 'laravel-queue-overlap:'
  shareKey = false

  constructor(key = '', releaseAfter = 0, expiresAfter = 0) {
    this.key = String(key)
    this.releaseAfterSeconds = releaseAfter
    this.expiresAfter = expiresAfter
  }

  async handle(job, next) {
    const { driver } = store(job)
    const key = this.getLockKey(job)
    const owner = `${job.job.uuid()}:${job.attempts()}`
    if (await driver.acquireLock(key, this.expiresAfter, owner)) {
      try {
        return await next(job)
      } finally {
        await driver.releaseLock(key, owner)
      }
    } else if (this.releaseAfterSeconds !== null) {
      job.release(this.releaseAfterSeconds)
    }
  }

  /** Seconds to wait before trying a blocked job again. */
  releaseAfter(seconds) {
    this.releaseAfterSeconds = seconds
    return this
  }

  /** Drop blocked jobs instead of releasing them. */
  dontRelease() {
    this.releaseAfterSeconds = null
    return this
  }

  /** Seconds before the lock expires on its own, in case a worker dies holding it. 0 means never. */
  expireAfter(seconds) {
    this.expiresAfter = seconds
    return this
  }

  withPrefix(prefix) {
    this.prefix = prefix
    return this
  }

  /** Share the lock across different job classes that use the same key. */
  shared() {
    this.shareKey = true
    return this
  }

  getLockKey(job) {
    return this.shareKey ? `${this.prefix}${this.key}` : `${this.prefix}${job.displayName()}:${this.key}`
  }
}

/**
 * Laravel: ThrottlesExceptions. After maxAttempts exceptions within
 * decaySeconds, stop calling the job and release it until the window ends.
 * Exceptions caught here release the job instead of counting as exceptions,
 * so pair it with retryUntil() rather than a small `tries`.
 */
export class ThrottlesExceptions {
  prefix = 'laravel_throttles_exceptions:'
  byJobKey = false
  retryAfterMinutes = 0
  key = null
  whenCallback = null
  reportCallback = null
  deleteWhenCallbacks = []
  failWhenCallbacks = []

  constructor(maxAttempts = 10, decaySeconds = 600) {
    this.maxAttempts = maxAttempts
    this.decaySeconds = decaySeconds
  }

  async handle(job, next) {
    const { driver } = store(job)
    const key = this.getKey(job)

    if ((await driver.attempts(key)) >= this.maxAttempts) {
      return job.release((await driver.availableIn(key)) + 3)
    }

    try {
      const result = await next(job)
      await driver.clearHits(key)
      return result
    } catch (error) {
      if (this.whenCallback && !(await this.whenCallback(error))) throw error
      if (this.reportCallback && (await this.reportCallback(error))) store(job).emit('worker:error', error)
      if (this.deleteWhenCallbacks.some((cb) => cb(error))) return job.delete()
      if (this.failWhenCallbacks.some((cb) => cb(error))) return job.fail(error)
      await driver.hit(key, this.decaySeconds)
      const minutes = typeof this.retryAfterMinutes === 'function' ? this.retryAfterMinutes(error) : this.retryAfterMinutes
      return job.release(minutes * 60)
    }
  }

  /** Only throttle errors the callback returns true for; others are thrown as usual. */
  when(callback) {
    this.whenCallback = callback
    return this
  }

  /** Delete the job (no retry, no failure) for these errors. Pass a class or a callback. */
  deleteWhen(check) {
    this.deleteWhenCallbacks.push(typeof check === 'function' && check.prototype instanceof Error ? (e) => e instanceof check : check)
    return this
  }

  /** Fail the job for these errors. Pass a class or a callback. */
  failWhen(check) {
    this.failWhenCallbacks.push(typeof check === 'function' && check.prototype instanceof Error ? (e) => e instanceof check : check)
    return this
  }

  /** Report throttled errors on the worker:error event. */
  report(callback = () => true) {
    this.reportCallback = callback
    return this
  }

  /** Minutes to wait before retrying after an exception (a number or a function of the error). Laravel uses minutes here too. */
  backoff(minutes) {
    this.retryAfterMinutes = minutes
    return this
  }

  /** Share the throttle across jobs with this key (e.g. the service they call). */
  by(key) {
    this.key = key
    return this
  }

  /** Throttle each dispatched job separately. */
  byJob() {
    this.byJobKey = true
    return this
  }

  withPrefix(prefix) {
    this.prefix = prefix
    return this
  }

  getKey(job) {
    if (this.key) return `${this.prefix}${this.key}`
    if (this.byJobKey) return `${this.prefix}${job.job.uuid()}`
    return `${this.prefix}${job.displayName()}`
  }
}

/** Laravel: Illuminate\Cache\RateLimiting\Limit. */
export class Limit {
  constructor(key = '', maxAttempts = 60, decaySeconds = 60) {
    this.key = key
    this.maxAttempts = maxAttempts
    this.decaySeconds = decaySeconds
  }

  static perSecond(maxAttempts, decaySeconds = 1) {
    return new Limit('', maxAttempts, decaySeconds)
  }

  static perMinute(maxAttempts, decayMinutes = 1) {
    return new Limit('', maxAttempts, 60 * decayMinutes)
  }

  static perMinutes(decayMinutes, maxAttempts) {
    return new Limit('', maxAttempts, 60 * decayMinutes)
  }

  static perHour(maxAttempts, decayHours = 1) {
    return new Limit('', maxAttempts, 3600 * decayHours)
  }

  static perDay(maxAttempts, decayDays = 1) {
    return new Limit('', maxAttempts, 86400 * decayDays)
  }

  /** No limit. */
  static none() {
    return new Unlimited()
  }

  by(key) {
    this.key = key
    return this
  }
}

export class Unlimited extends Limit {
  constructor() {
    super('', Number.MAX_SAFE_INTEGER, 0)
  }
}

/**
 * Laravel: RateLimited. Uses a named limiter defined on the queue:
 *
 *   queue.limiter('backups', (job) => Limit.perHour(1).by(job.data.userId))
 *   middleware() { return [new RateLimited('backups')] }
 */
export class RateLimited {
  releaseAfterSeconds = null
  shouldRelease = true

  constructor(limiterName) {
    this.limiterName = String(limiterName)
  }

  async handle(job, next) {
    const manager = store(job)
    const limiter = manager.limiterFor(this.limiterName)
    if (!limiter) return next(job)

    const response = await limiter(job)
    if (response instanceof Unlimited) return next(job)

    const limits = [response].flat().map((limit) => ({
      key: `laravel_rate_limited:${this.limiterName}:${limit.key}`,
      maxAttempts: limit.maxAttempts,
      decaySeconds: limit.decaySeconds,
    }))

    for (const limit of limits) {
      if ((await manager.driver.attempts(limit.key)) >= limit.maxAttempts) {
        if (!this.shouldRelease) return false
        return job.release(this.releaseAfterSeconds || (await manager.driver.availableIn(limit.key)) + 3)
      }
    }
    for (const limit of limits) await manager.driver.hit(limit.key, limit.decaySeconds)
    return next(job)
  }

  releaseAfter(seconds) {
    this.releaseAfterSeconds = seconds
    return this
  }

  /** Drop rate-limited jobs instead of releasing them. */
  dontRelease() {
    this.shouldRelease = false
    return this
  }
}

/** Laravel: Skip. Skip.when(condition) / Skip.unless(condition). The job is deleted without running. */
export class Skip {
  constructor(skip = false) {
    this.skip = skip
  }

  static when(condition) {
    return new Skip(condition)
  }

  static unless(condition) {
    return new Skip(async () => !(await (typeof condition === 'function' ? condition() : condition)))
  }

  async handle(job, next) {
    const skip = typeof this.skip === 'function' ? await this.skip() : this.skip
    return skip ? false : next(job)
  }
}

/** Laravel: Release. Release.when(condition, seconds) / Release.unless(condition, seconds). */
export class Release {
  constructor(release = false, releaseAfter = 0) {
    this.release = release
    this.releaseAfter = releaseAfter
  }

  static when(condition, releaseAfter = 0) {
    return new Release(condition, releaseAfter)
  }

  static unless(condition, releaseAfter = 0) {
    return new Release(async () => !(await (typeof condition === 'function' ? condition() : condition)), releaseAfter)
  }

  async handle(job, next) {
    const release = typeof this.release === 'function' ? await this.release() : this.release
    return release ? job.release(this.releaseAfter) : next(job)
  }
}

/**
 * Laravel: FailOnException. Fail the job (no more retries) when it throws one
 * of these error classes, or when the callback returns true.
 *
 *   new FailOnException([AuthorizationError])
 */
export class FailOnException {
  constructor(check) {
    this.callback = Array.isArray(check) ? (error) => check.some((ErrorClass) => error instanceof ErrorClass) : check
  }

  async handle(job, next) {
    try {
      return await next(job)
    } catch (error) {
      if ((await this.callback(error, job)) === true) job.fail(error)
      throw error
    }
  }
}

// Plain-function helpers, for code without `new`. Each returns the matching
// middleware above, so the fluent methods work the same:
//   withoutOverlapping(key).expireAfter(180)
export const withoutOverlapping = (...args) => new WithoutOverlapping(...args)
export const throttlesExceptions = (...args) => new ThrottlesExceptions(...args)
export const rateLimited = (limiterName) => new RateLimited(limiterName)
export const failOnException = (check) => new FailOnException(check)
export const skipWhen = (condition) => Skip.when(condition)
export const skipUnless = (condition) => Skip.unless(condition)
export const releaseWhen = (condition, releaseAfter) => Release.when(condition, releaseAfter)
export const releaseUnless = (condition, releaseAfter) => Release.unless(condition, releaseAfter)
