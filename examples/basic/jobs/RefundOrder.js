import { Job, withoutOverlapping } from '../../../src/index.js' // in your app: from '@raksbisht/litequeue'

// Refunding twice is expensive. `unique` stops duplicates from being queued,
// withoutOverlapping stops two refunds for the same order running at once.
export const RefundOrder = Job.define({
  name: 'refund-order',
  queue: 'high',
  unique: true,
  tries: 5,
  uniqueId: (job) => String(job.data.orderId),
  middleware: (job) => [withoutOverlapping(job.data.orderId).releaseAfter(10).expireAfter(180)],
  handle: async (data) => {
    console.log(`    -> refunded order #${data.orderId}`)
  },
})

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// import { Job, WithoutOverlapping } from '@raksbisht/litequeue'
//
// export class RefundOrder extends Job {
//   static queue = 'high'
//   static unique = true
//   static tries = 5
//
//   uniqueId() {
//     return String(this.data.orderId)
//   }
//
//   middleware() {
//     return [new WithoutOverlapping(this.data.orderId).releaseAfter(10).expireAfter(180)]
//   }
//
//   async handle() {
//     console.log(`    -> refunded order #${this.data.orderId}`)
//   }
// }
