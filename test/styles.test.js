// The same jobs written both ways (Job.define and classes) must behave exactly alike.
// These are the README examples, with stand-ins for the mailer and database.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  createQueue,
  Job,
  Limit,
  skipWhen,
  withoutOverlapping,
  throttlesExceptions,
  rateLimited,
  failOnException,
  WithoutOverlapping,
  ThrottlesExceptions,
  RateLimited,
  Skip,
  FailOnException,
} from '../src/index.js'

class AuthorizationError extends Error {}
const log = []
const Invoice = { find: async (id) => ({ id }) }
const mailer = {
  send: async (invoice, { signal }) => {
    assert.ok(signal instanceof AbortSignal)
    log.push(`mail ${invoice.id}`)
  },
}

function functionJobs(p) {
  return {
    SendInvoice: Job.define({
      name: `${p}send-invoice`,
      queue: 'emails',
      tries: 5,
      backoff: [10, 60, 300],
      timeout: 30,
      handle: async (data, job) => {
        const invoice = await Invoice.find(data.invoiceId)
        await mailer.send(invoice, { signal: job.signal })
      },
      failed: async (error, job) => log.push(`alert ${job.data.invoiceId}`),
    }),
    RefundOrder: Job.define({
      name: `${p}refund-order`,
      unique: true,
      uniqueId: (job) => String(job.data.orderId),
      handle: async (data) => log.push(`refund ${data.orderId}`),
    }),
    ReindexUser: Job.define({
      name: `${p}reindex-user`,
      debounceFor: 0.05,
      maxDebounceWait: 300,
      debounceId: (job) => String(job.data.userId),
      handle: async (data) => log.push(`reindex ${data.v}`),
    }),
    SyncAccount: Job.define({
      name: `${p}sync-account`,
      tries: 0,
      retryUntil: () => new Date(Date.now() + 3600e3),
      middleware: (job) => [
        skipWhen(() => job.data.dryRun),
        withoutOverlapping(job.data.accountId).releaseAfter(10).expireAfter(180),
        throttlesExceptions(5, 300).by('crm-api').backoff(1),
        rateLimited('crm'),
        failOnException([AuthorizationError]),
      ],
      handle: async (data) => {
        log.push(`sync ${data.accountId}`)
        if (data.deny) throw new AuthorizationError('denied')
      },
    }),
    Step: Job.define(`${p}step`, async (data) => log.push(`step ${data.id}`)),
    chain: (Step) => [Step.with({ id: 1 }), Step.with({ id: 2 })],
  }
}

function classJobs(p) {
  class SendInvoice extends Job {
    static jobName = `${p}send-invoice`
    static queue = 'emails'
    static tries = 5
    static backoff = [10, 60, 300]
    static timeout = 30
    async handle() {
      const invoice = await Invoice.find(this.data.invoiceId)
      await mailer.send(invoice, { signal: this.signal })
    }
    async failed() {
      log.push(`alert ${this.data.invoiceId}`)
    }
  }
  class RefundOrder extends Job {
    static jobName = `${p}refund-order`
    static unique = true
    uniqueId() {
      return String(this.data.orderId)
    }
    async handle() {
      log.push(`refund ${this.data.orderId}`)
    }
  }
  class ReindexUser extends Job {
    static jobName = `${p}reindex-user`
    static debounceFor = 0.05
    static maxDebounceWait = 300
    debounceId() {
      return String(this.data.userId)
    }
    async handle() {
      log.push(`reindex ${this.data.v}`)
    }
  }
  class SyncAccount extends Job {
    static jobName = `${p}sync-account`
    static tries = 0
    retryUntil() {
      return new Date(Date.now() + 3600e3)
    }
    middleware() {
      return [
        Skip.when(() => this.data.dryRun),
        new WithoutOverlapping(this.data.accountId).releaseAfter(10).expireAfter(180),
        new ThrottlesExceptions(5, 300).by('crm-api').backoff(1),
        new RateLimited('crm'),
        new FailOnException([AuthorizationError]),
      ]
    }
    async handle() {
      log.push(`sync ${this.data.accountId}`)
      if (this.data.deny) throw new AuthorizationError('denied')
    }
  }
  class Step extends Job {
    static jobName = `${p}step`
    async handle() {
      log.push(`step ${this.data.id}`)
    }
  }
  return { SendInvoice, RefundOrder, ReindexUser, SyncAccount, Step, chain: (S) => [new S({ id: 1 }), new S({ id: 2 })] }
}

async function run(jobs) {
  log.length = 0
  const { chain, ...J } = jobs
  const queue = createQueue({ driver: 'memory', jobs: Object.values(J) })
  queue.on('warning', () => {})
  queue.limiter('crm', (job) => Limit.perMinute(60).by(job.data.accountId))

  await J.SendInvoice.dispatch({ invoiceId: 42 })
  log.push(`unique ${Boolean(await J.RefundOrder.dispatch({ orderId: 42 }))} ${await J.RefundOrder.dispatch({ orderId: 42 })}`)
  await J.ReindexUser.dispatch({ userId: 1, v: 1 })
  await J.ReindexUser.dispatch({ userId: 1, v: 2 })
  await J.SyncAccount.dispatch({ accountId: 9 })
  await J.SyncAccount.dispatch({ accountId: 9, dryRun: true })
  await J.SyncAccount.dispatch({ accountId: 8, deny: true })
  await queue.chain(chain(J.Step))

  const drain = () => queue.work({ queue: 'emails,default', stopWhenEmpty: true, sleep: 0.01 })
  await drain()
  await new Promise((r) => setTimeout(r, 120)) // let the debounced job become due
  await drain()
  log.push(`failed ${(await queue.failed()).length}`)
  await queue.close()
  return [...log]
}

test('function jobs and class jobs behave the same (README examples)', async () => {
  const fromFunctions = await run(functionJobs('f-'))
  const fromClasses = await run(classJobs('c-'))
  assert.deepEqual(fromFunctions, [
    'unique true null',
    'mail 42',
    'refund 42',
    'sync 9',
    'sync 8',
    'step 1',
    'step 2',
    'reindex 2',
    'failed 1',
  ])
  assert.deepEqual(fromClasses, fromFunctions)
})
