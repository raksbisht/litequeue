import queue from './jobline.config.js'
import { SendWelcomeEmail } from './jobs/SendWelcomeEmail.js'
import { RefundOrder } from './jobs/RefundOrder.js'

await SendWelcomeEmail.dispatch({ email: 'ada@example.com' })
await RefundOrder.dispatch({ orderId: 42 })
const duplicate = await RefundOrder.dispatch({ orderId: 42 }) // already queued, so skipped

console.log(`Queued. The duplicate refund was ${duplicate === null ? 'skipped' : 'queued (!)'}.`)
console.log('Now run: node ../../bin/jobline.js work --queue high,emails --stop-when-empty-for 3')
await queue.close()
