export class LitequeueError extends Error {
  constructor(message) {
    super(message)
    this.name = this.constructor.name
  }
}

/** Laravel: MaxAttemptsExceededException. The job was attempted too many times, usually after a crash or timeout. */
export class MaxAttemptsExceededError extends LitequeueError {}

/** Laravel: TimeoutExceededException. The job ran longer than its timeout. */
export class TimeoutExceededError extends LitequeueError {}

/** Laravel: ManuallyFailedException. The job called fail() without an error. */
export class ManuallyFailedError extends LitequeueError {}

/** A worker found a job name it doesn't know. Register the class in createQueue({ jobs }). */
export class JobNotRegisteredError extends LitequeueError {}
