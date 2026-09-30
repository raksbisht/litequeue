import { ManuallyFailedError } from './errors.js'

/**
 * A job that a worker has reserved from the queue. This is Laravel's
 * Illuminate\Queue\Jobs\Job: it knows its attempts and payload, and records
 * whether it was released, deleted or failed while running.
 *
 * Your job class reaches it as `this.job` (like InteractsWithQueue), but you
 * normally call the shortcuts on your job instead: this.attempts(),
 * this.release(), this.fail(), this.delete().
 */
export class QueueJob {
  #released = false
  #deleted = false
  #failed = false
  releaseDelay = 0
  failure = null

  constructor({ id, queue, attempts, payload, signal, manager }) {
    /** The Queue this job came from. Middleware uses it for locks and rate limiters. */
    this.manager = manager
    this.id = id
    this.queue = queue
    this.attemptsCount = attempts
    this.payloadData = payload
    this.signal = signal
  }

  getJobId() {
    return this.id
  }

  getQueue() {
    return this.queue
  }

  uuid() {
    return this.payloadData.uuid ?? null
  }

  payload() {
    return this.payloadData
  }

  displayName() {
    return this.payloadData.displayName
  }

  attempts() {
    return this.attemptsCount
  }

  maxTries() {
    return this.payloadData.maxTries ?? null
  }

  maxExceptions() {
    return this.payloadData.maxExceptions ?? null
  }

  backoff() {
    return this.payloadData.backoff ?? null
  }

  timeout() {
    return this.payloadData.timeout ?? null
  }

  retryUntil() {
    return this.payloadData.retryUntil ?? null
  }

  shouldFailOnTimeout() {
    return Boolean(this.payloadData.failOnTimeout)
  }

  /** Put the job back on the queue to run again after the delay (seconds). */
  release(delay = 0) {
    this.#released = true
    this.releaseDelay = delay
  }

  /** Remove the job without failing it. */
  delete() {
    this.#deleted = true
  }

  /** Fail the job now. It won't be retried. */
  fail(error = null) {
    this.#failed = true
    this.failure = error instanceof Error ? error : error ? new Error(String(error)) : new ManuallyFailedError('This job was failed manually.')
  }

  isReleased() {
    return this.#released
  }

  isDeleted() {
    return this.#deleted
  }

  isDeletedOrReleased() {
    return this.#deleted || this.#released
  }

  hasFailed() {
    return this.#failed
  }
}
