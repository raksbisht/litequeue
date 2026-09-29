import { Job, WithoutOverlapping } from '../../../src/index.js' // in your app: from 'jobline'

// Refunding twice is expensive. `unique` stops duplicates from being queued,
// withoutOverlapping stops two refunds for the same order running at once.
export class RefundOrder extends Job {
  static queue = 'high'
  static unique = true
  static tries = 5

  uniqueId() {
    return String(this.data.orderId)
  }

  middleware() {
    return [new WithoutOverlapping(this.data.orderId).releaseAfter(10).expireAfter(180)]
  }

  async handle() {
    console.log(`    -> refunded order #${this.data.orderId}`)
  }
}
