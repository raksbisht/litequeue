import { Job } from '../../../src/index.js' // in your app: from '@raksbisht/litequeue'

export const SendWelcomeEmail = Job.define({
  name: 'send-welcome-email',
  queue: 'emails',
  tries: 3,
  backoff: [1, 5], // 1s before the 2nd attempt, 5s before the 3rd
  handle: async (data, job) => {
    // Pretend the mail provider is flaky on the first attempt.
    if (job.attempts() === 1) throw new Error('Mail provider timed out')
    console.log(`    -> welcome email sent to ${data.email}`)
  },
  failed: async (error, job) => {
    console.log(`    -> giving up on ${job.data.email}: ${error.message}`)
  },
})

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// export class SendWelcomeEmail extends Job {
//   static queue = 'emails'
//   static tries = 3
//   static backoff = [1, 5]
//
//   async handle() {
//     if (this.attempts() === 1) throw new Error('Mail provider timed out')
//     console.log(`    -> welcome email sent to ${this.data.email}`)
//   }
//
//   async failed(error) {
//     console.log(`    -> giving up on ${this.data.email}: ${error.message}`)
//   }
// }
