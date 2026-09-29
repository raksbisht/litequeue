/**
 * Returned by Job.dispatch(). Collects options, then pushes the job once,
 * at the end of the current tick, whether or not you await it (Laravel
 * dispatches when PendingDispatch is destructed).
 *
 *   await SendInvoice.dispatch({ id: 1 }).onQueue('high').delay(60)
 */
export class PendingDispatch {
  #queue
  #job
  #options
  #promise = null

  constructor(queue, job, options = {}) {
    this.#queue = queue
    this.#job = job
    this.#options = { ...options }
    queueMicrotask(() => this.#send())
  }

  onQueue(name) {
    this.#options.queue = name
    return this
  }

  delay(seconds) {
    this.#options.delay = seconds
    return this
  }

  #send() {
    this.#promise ??= this.#queue.dispatch(this.#job, this.#options)
    return this.#promise
  }

  then(resolve, reject) {
    return this.#send().then(resolve, reject)
  }

  catch(reject) {
    return this.#send().catch(reject)
  }
}
