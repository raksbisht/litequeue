import { PendingDispatch } from './PendingDispatch.js'
import { getDefaultQueue } from './registry.js'

/**
 * Base class for queued jobs. Laravel: a class using Dispatchable,
 * InteractsWithQueue, Queueable and ShouldQueue.
 *
 *   class SendWelcomeEmail extends Job {
 *     static tries = 3
 *     static backoff = [10, 60]
 *
 *     async handle() {
 *       await mailer.send(this.data.email)
 *     }
 *   }
 *
 *   await SendWelcomeEmail.dispatch({ email: 'ada@example.com' })
 *
 * Options left undefined fall back to the worker's options. Times are in seconds.
 */
export class Job {
  /** Name stored in the queue. Defaults to the class name. Set it if your code is minified. */
  static jobName
  /** Queue to push to. Default "default". */
  static queue
  /** Laravel $tries. Max attempts; 0 means unlimited. Ignored while retryUntil() is in the future. */
  static tries
  /** Laravel $backoff. Seconds before a retry: a number, or [10, 60, 300] per attempt (the last value repeats). */
  static backoff
  /** Laravel $timeout. Seconds before the attempt times out. 0 means no timeout. */
  static timeout
  /** Laravel $maxExceptions. Fail after this many thrown errors (or timeouts), even with attempts left. */
  static maxExceptions
  /** Laravel $failOnTimeout. Fail right away on a timeout instead of retrying. */
  static failOnTimeout = false
  /** Laravel $delay. Default delay for every dispatch. */
  static delay
  /** Laravel ShouldBeUnique. Only one copy per uniqueId() can be queued at a time. */
  static unique = false
  /** Laravel $uniqueFor. Seconds the unique lock lives. 0 means until the job finishes. */
  static uniqueFor = 0
  /** Laravel ShouldBeUniqueUntilProcessing. Release the unique lock when the job starts instead of when it ends. */
  static uniqueUntilProcessing = false
  /** Laravel #[DebounceFor]. Wait this long; if the job is dispatched again meanwhile, only the newest runs. */
  static debounceFor
  /** Laravel DebounceFor maxWait. Run anyway once this many seconds have passed since the first dispatch. */
  static maxDebounceWait

  /** Set by the worker while the job runs. Laravel: $this->job. */
  job = null

  constructor(data = {}) {
    this.data = data
  }

  static getJobName() {
    return (Object.hasOwn(this, 'jobName') && this.jobName) || this.name
  }

  /**
   * Define a job from a function, no class needed. Laravel: a queued closure.
   *
   *   const sendEmail = Job.define('send-email', { tries: 3 }, async (data, job) => {
   *     await mailer.send(data.email, { signal: job.signal })
   *   })
   *
   *   await sendEmail.dispatch({ email: 'ada@example.com' })
   *
   * Options are the static fields above, plus the functions middleware(job),
   * retryUntil(job), failed(error, job), uniqueId(job) and debounceId(job).
   * Leave the options out for the defaults: Job.define('name', handler).
   * Or pass everything in one object:
   *
   *   Job.define({ name: 'send-email', tries: 3, handle: async (data, job) => {} })
   */
  static define(name, options, handler) {
    if (name && typeof name === 'object') {
      ;({ name, handle: handler, ...options } = name)
    } else if (typeof options === 'function') {
      ;[options, handler] = [{}, options]
    }
    if (!name || typeof name !== 'string') throw new TypeError('Job.define() needs a job name as its first argument.')
    if (typeof handler !== 'function') throw new TypeError(`Job.define("${name}") needs a function to run.`)

    const { middleware, retryUntil, failed, uniqueId, debounceId, ...settings } = options ?? {}
    const known = ['queue', 'tries', 'backoff', 'timeout', 'maxExceptions', 'failOnTimeout', 'delay', 'unique', 'uniqueFor', 'uniqueUntilProcessing', 'debounceFor', 'maxDebounceWait']
    for (const key of Object.keys(settings)) {
      if (!known.includes(key)) throw new TypeError(`Job.define("${name}"): unknown option "${key}".`)
    }

    // `this` is missing when define is pulled out of Job: const { define } = Job
    const Base = typeof this === 'function' ? this : Job
    class Defined extends Base {
      async handle() {
        return handler(this.data, this)
      }
    }
    Defined.jobName = name
    for (const [key, value] of Object.entries(settings)) {
      if (value !== undefined) Defined[key] = value
    }
    const hooks = { middleware, retryUntil, uniqueId, debounceId }
    for (const [method, fn] of Object.entries(hooks)) {
      if (fn) Defined.prototype[method] = function () { return fn(this) }
    }
    if (failed) Defined.prototype.failed = function (error) { return failed(error, this) }
    return Defined
  }

  displayName() {
    return this.constructor.getJobName()
  }

  // --- Dispatchable ---------------------------------------------------------

  /** Push the job onto the queue. Chain .onQueue() / .delay() before awaiting. */
  static dispatch(data) {
    return new PendingDispatch(getDefaultQueue(), new this(data))
  }

  static dispatchIf(condition, data) {
    return condition ? this.dispatch(data) : Promise.resolve(null)
  }

  static dispatchUnless(condition, data) {
    return condition ? Promise.resolve(null) : this.dispatch(data)
  }

  /** A job with its data, not yet dispatched. Same as `new SendInvoice(data)`, for chains: queue.chain([SendInvoice.with({ id: 1 })]) */
  static with(data) {
    return new this(data)
  }

  /** Run the job right now in this process, skipping the queue. */
  static dispatchSync(data) {
    return getDefaultQueue().dispatchSync(new this(data))
  }

  // --- InteractsWithQueue ---------------------------------------------------

  attempts() {
    return this.job ? this.job.attempts() : 1
  }

  release(delay = 0) {
    this.job?.release(delay)
  }

  fail(error = null) {
    this.job?.fail(error)
  }

  delete() {
    this.job?.delete()
  }

  /** Aborted when the attempt times out. Pass it to fetch() and friends. */
  get signal() {
    return this.job?.signal ?? new AbortController().signal
  }

  // --- Things to override ---------------------------------------------------

  /** Your job logic. Throw to retry. */
  async handle() {
    throw new Error(`${this.constructor.getJobName()} must implement handle()`)
  }

  /** Called once when the job fails for good. */
  async failed(error) {}

  /** Middleware run around handle(). Objects with handle(job, next), or functions (job, next). */
  middleware() {
    return []
  }

  /** Return a Date to keep retrying until then instead of counting tries. Evaluated at dispatch. */
  retryUntil() {
    return null
  }

  /** Key for unique jobs. */
  uniqueId() {
    return ''
  }

  /** Key for debounced jobs. */
  debounceId() {
    return ''
  }
}
