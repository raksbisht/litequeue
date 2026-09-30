import { test, describe, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import {
  createQueue,
  Job,
  Limit,
  WithoutOverlapping,
  ThrottlesExceptions,
  RateLimited,
  FailOnException,
  Skip,
  Release,
  withoutOverlapping,
  throttlesExceptions,
  rateLimited,
  failOnException,
  skipWhen,
  skipUnless,
  releaseWhen,
  releaseUnless,
} from '../src/index.js'

const log = []

describe('function-style helpers', () => {
  let queue
  const drain = () => queue.work({ stopWhenEmpty: true, sleep: 0.01 })

  beforeEach(() => {
    log.length = 0
    queue = createQueue({ driver: 'memory' })
    queue.on('warning', () => {})
  })

  afterEach(() => queue.close())

  test('middleware helpers return the same middleware as the classes', () => {
    assert.ok(withoutOverlapping('k').expireAfter(10) instanceof WithoutOverlapping)
    assert.ok(throttlesExceptions(5, 300).by('api') instanceof ThrottlesExceptions)
    assert.ok(rateLimited('api') instanceof RateLimited)
    assert.ok(failOnException([TypeError]) instanceof FailOnException)
    assert.ok(skipWhen(true) instanceof Skip)
    assert.ok(skipUnless(true) instanceof Skip)
    assert.ok(releaseWhen(true, 5) instanceof Release)
    assert.ok(releaseUnless(true, 5) instanceof Release)
  })

  test('skipWhen, failOnException and rateLimited work inside Job.define', async () => {
    const skipped = Job.define({ name: 'skipped', middleware: (job) => [skipWhen(() => job.data.dry)], handle: () => log.push('ran') })
    const strict = Job.define({
      name: 'strict',
      tries: 5,
      middleware: () => [failOnException([TypeError])],
      handle: () => {
        log.push('strict')
        throw new TypeError('bad')
      },
    })
    const limited = Job.define({ name: 'limited', middleware: () => [rateLimited('one')], handle: (d) => log.push(`limited ${d.n}`) })
    queue.limiter('one', () => Limit.perMinute(1))
    queue.register(skipped, strict, limited)

    await skipped.dispatch({ dry: true })
    await strict.dispatch({})
    await limited.dispatch({ n: 1 })
    await limited.dispatch({ n: 2 })
    await drain()

    assert.deepEqual(log, ['strict', 'limited 1'])
    assert.equal((await queue.failed()).length, 1)
    assert.equal(await queue.delayedSize(), 1) // the second limited job was released for later
  })

  test('Job.with() builds a chain without new', async () => {
    const step = Job.define('step', (d) => log.push(`step ${d.n}`))
    queue.register(step)
    await queue.chain([step.with({ n: 1 }), step.with({ n: 2 }), step.with({ n: 3 })])
    await drain()
    assert.deepEqual(log, ['step 1', 'step 2', 'step 3'])
  })

  test('Job.with() works on classes too', () => {
    class Plain extends Job {}
    const job = Plain.with({ a: 1 })
    assert.ok(job instanceof Plain)
    assert.deepEqual(job.data, { a: 1 })
  })
})
