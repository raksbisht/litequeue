import { test, describe, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import { createQueue, Job } from '../src/index.js'

const log = []

const hello = Job.define('hello', async (data) => {
  log.push(`hello ${data.name}`)
})

const flaky = Job.define(
  'flaky',
  {
    tries: 3,
    failed: (error, job) => log.push(`failed after ${job.attempts()}: ${error.message}`),
  },
  async (data, job) => {
    log.push(`attempt ${job.attempts()}`)
    throw new Error('boom')
  },
)

const refund = Job.define(
  'refund',
  { unique: true, uniqueId: (job) => String(job.data.orderId), queue: 'billing' },
  async () => {},
)

describe('Job.define', () => {
  let queue

  beforeEach(() => {
    log.length = 0
    queue = createQueue({ driver: 'memory', jobs: [hello, flaky, refund] })
    queue.on('warning', () => {})
  })

  afterEach(() => queue.close())

  const drain = () => queue.work({ stopWhenEmpty: true, sleep: 0.01 })

  test('runs a function job with its data', async () => {
    await hello.dispatch({ name: 'ada' })
    const result = await drain()
    assert.equal(result.processed, 1)
    assert.deepEqual(log, ['hello ada'])
  })

  test('options and hooks work like the class version', async () => {
    await flaky.dispatch({})
    await drain()
    assert.deepEqual(log, ['attempt 1', 'attempt 2', 'attempt 3', 'failed after 3: boom'])
    assert.equal((await queue.failed()).length, 1)
  })

  test('static options and unique ids', async () => {
    assert.ok(await refund.dispatch({ orderId: 1 }))
    assert.equal(await refund.dispatch({ orderId: 1 }), null)
    assert.ok(await refund.dispatch({ orderId: 2 }))
    assert.equal(await queue.size('billing'), 2)
  })

  test('one object with name, options and handle', async () => {
    const one = Job.define({
      name: 'one-object',
      tries: 2,
      handle: async (data, job) => {
        log.push(`one ${data.n} attempt ${job.attempts()}`)
        throw new Error('nope')
      },
      failed: (error) => log.push(`failed: ${error.message}`),
    })
    queue.register(one)
    await one.dispatch({ n: 1 })
    await drain()
    assert.deepEqual(log, ['one 1 attempt 1', 'one 1 attempt 2', 'failed: nope'])
    assert.equal(one.getJobName(), 'one-object')
    assert.equal(one.tries, 2)
  })

  test('the object form needs a name and a handle function', () => {
    assert.throws(() => Job.define({ handle() {} }), /job name/)
    assert.throws(() => Job.define({ name: 'x' }), /function to run/)
    assert.throws(() => Job.define({ name: 'x', trys: 1, handle() {} }), /unknown option "trys"/)
  })

  test('the job is stored under the name you gave it', () => {
    assert.equal(hello.getJobName(), 'hello')
    assert.ok(hello.prototype instanceof Job)
  })

  test('define works when pulled out of Job', () => {
    const { define } = Job
    assert.equal(define('loose', () => {}).getJobName(), 'loose')
  })

  test('an option set to undefined keeps the default', () => {
    const job = Job.define({ name: 'undef', failOnTimeout: undefined, tries: undefined, handle() {} })
    assert.equal(job.failOnTimeout, false)
    assert.equal(job.tries, undefined)
  })

  test('two different jobs with one name trigger a warning', () => {
    const warnings = []
    const q = createQueue({ driver: 'memory' })
    q.on('warning', (message) => warnings.push(message))
    const one = Job.define('twin', () => {})
    const two = Job.define('twin', () => {})
    q.register(one, one) // the same job twice is fine
    assert.equal(warnings.length, 0)
    q.register(two)
    assert.equal(warnings.length, 1)
    assert.match(warnings[0], /"twin"/)
    return q.close()
  })

  test('bad arguments and unknown options are rejected', () => {
    assert.throws(() => Job.define(), /job name/)
    assert.throws(() => Job.define('x'), /function to run/)
    assert.throws(() => Job.define('x', { trys: 3 }, () => {}), /unknown option "trys"/)
  })
})
