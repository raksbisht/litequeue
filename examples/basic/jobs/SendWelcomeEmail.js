import { Job } from '../../../src/index.js' // in your app: from 'jobline'

export class SendWelcomeEmail extends Job {
  static queue = 'emails'
  static tries = 3
  static backoff = [1, 5] // 1s before the 2nd attempt, 5s before the 3rd

  async handle() {
    // Pretend the mail provider is flaky on the first attempt.
    if (this.attempts() === 1) throw new Error('Mail provider timed out')
    console.log(`    -> welcome email sent to ${this.data.email}`)
  }

  async failed(error) {
    console.log(`    -> giving up on ${this.data.email}: ${error.message}`)
  }
}
