import { createQueue } from '../../src/index.js' // in your app: from 'jobline'
import { SendWelcomeEmail } from './jobs/SendWelcomeEmail.js'
import { RefundOrder } from './jobs/RefundOrder.js'

export default createQueue({
  driver: 'sqlite', // the default; 'memory', 'sync' or your own driver also work
  path: 'queue.sqlite',
  retryAfter: 90,
  jobs: [SendWelcomeEmail, RefundOrder],
})
