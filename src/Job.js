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
