// Class versions of the tests in define.test.js and functional.test.js, plus
// mixing both styles in one queue. Argument checks for Job.define() stay in
// define.test.js, since classes have no equivalent.
import { test, describe, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import {
  createQueue,
  Job,
  Limit,
  Skip,
  FailOnException,
  RateLimited,
  skipWhen,
  failOnException,
  rateLimited,
} from '../src/index.js'

const log = []

class Hello extends Job {
  static jobName = 'hello'
  async handle() {
    log.push(`hello ${this.data.name}`)
  }
}

class Flaky extends Job {
  static jobName = 'flaky'
  static tries = 3
  async handle() {
    log.push(`attempt ${this.attempts()}`)
    throw new Error('boom')
  }
  async failed(error) {
    log.push(`failed after ${this.attempts()}: ${error.message}`)
  }
}

class Refund extends Job {
  static jobName = 'refund'
  static unique = true
  static queue = 'billing'
  uniqueId() {
    return String(this.data.orderId)
  }
  async handle() {}
}

describe('class jobs (same checks as define.test.js)', () => {
  let queue
  const drain = () => queue.work({ stopWhenEmpty: true, sleep: 0.01 })

  beforeEach(() => {
    log.length = 0
    queue = createQueue({ driver: 'memory', jobs: [Hello, Flaky, Refund] })
    queue.on('warning', () => {})
  })

  afterEach(() => queue.close())

  test('runs a class job with its data', async () => {
    await Hello.dispatch({ name: 'ada' })
    const result = await drain()
    assert.equal(result.processed, 1)
    assert.deepEqual(log, ['hello ada'])
  })

  test('options and hooks', async () => {
    await Flaky.dispatch({})
    await drain()
    assert.deepEqual(log, ['attempt 1', 'attempt 2', 'attempt 3', 'failed after 3: boom'])
    assert.equal((await queue.failed()).length, 1)
  })

  test('static options and unique ids', async () => {
    assert.ok(await Refund.dispatch({ orderId: 1 }))
    assert.equal(await Refund.dispatch({ orderId: 1 }), null)
    assert.ok(await Refund.dispatch({ orderId: 2 }))
    assert.equal(await queue.size('billing'), 2)
  })

  test('the job is stored under its jobName', () => {
    assert.equal(Hello.getJobName(), 'hello')
  })

  test('two different classes with one name trigger a warning', () => {
    const warnings = []
    const q = createQueue({ driver: 'memory' })
    q.on('warning', (message) => warnings.push(message))
    class One extends Job {
      static jobName = 'twin'
    }
    class Two extends Job {
      static jobName = 'twin'
    }
    q.register(One, One)
    assert.equal(warnings.length, 0)
    q.register(Two)
    assert.equal(warnings.length, 1)
    assert.match(warnings[0], /"twin"/)
    return q.close()
  })

  test('Skip, FailOnException and RateLimited work inside a class (same checks as functional.test.js)', async () => {
    class Skipped extends Job {
      static jobName = 'skipped'
      middleware() {
        return [Skip.when(() => this.data.dry)]
      }
      async handle() {
        log.push('ran')
      }
    }
    class Strict extends Job {
      static jobName = 'strict'
      static tries = 5
      middleware() {
        return [new FailOnException([TypeError])]
      }
      async handle() {
        log.push('strict')
        throw new TypeError('bad')
      }
    }
    class Limited extends Job {
      static jobName = 'limited'
      middleware() {
        return [new RateLimited('one')]
      }
      async handle() {
        log.push(`limited ${this.data.n}`)
      }
    }
    queue.limiter('one', () => Limit.perMinute(1))
    queue.register(Skipped, Strict, Limited)

    await Skipped.dispatch({ dry: true })
    await Strict.dispatch({})
    await Limited.dispatch({ n: 1 })
    await Limited.dispatch({ n: 2 })
    await drain()

    assert.deepEqual(log, ['strict', 'limited 1'])
    assert.equal((await queue.failed()).length, 1)
    assert.equal(await queue.delayedSize(), 1)
  })
})

describe('mixing both styles', () => {
  let queue
  const drain = () => queue.work({ stopWhenEmpty: true, sleep: 0.01 })

  beforeEach(() => {
    log.length = 0
    queue = createQueue({ driver: 'memory' })
    queue.on('warning', () => {})
  })

  afterEach(() => queue.close())

  test('class jobs can use the function-style middleware helpers', async () => {
    class Mixed extends Job {
      static jobName = 'mixed'
      static tries = 5
      middleware() {
        return [skipWhen(() => this.data.skip), failOnException([TypeError]), rateLimited('none')]
      }
      async handle() {
        log.push(`mixed ${this.data.n}`)
        if (this.data.bad) throw new TypeError('bad')
      }
    }
    queue.register(Mixed)
    await Mixed.dispatch({ n: 1, skip: true })
    await Mixed.dispatch({ n: 2 })
    await Mixed.dispatch({ n: 3, bad: true })
    await drain()
    assert.deepEqual(log, ['mixed 2', 'mixed 3'])
    assert.equal((await queue.failed()).length, 1)
  })

  test('function jobs can use the middleware classes', async () => {
    const mixed = Job.define({
      name: 'mixed-fn',
      tries: 5,
      middleware: (job) => [Skip.when(() => job.data.skip), new FailOnException([TypeError])],
      handle: (data) => {
        log.push(`mixed ${data.n}`)
        if (data.bad) throw new TypeError('bad')
      },
    })
    queue.register(mixed)
    await mixed.dispatch({ n: 1, skip: true })
    await mixed.dispatch({ n: 2 })
    await mixed.dispatch({ n: 3, bad: true })
    await drain()
    assert.deepEqual(log, ['mixed 2', 'mixed 3'])
    assert.equal((await queue.failed()).length, 1)
  })

  test('one chain can hold class jobs and function jobs', async () => {
    const fnStep = Job.define('fn-step', (data) => log.push(`fn ${data.n}`))
    class ClassStep extends Job {
      static jobName = 'class-step'
      async handle() {
        log.push(`class ${this.data.n}`)
      }
    }
    queue.register(fnStep, ClassStep)
    await queue.chain([fnStep.with({ n: 1 }), new ClassStep({ n: 2 }), ClassStep.with({ n: 3 }), new fnStep({ n: 4 })])
    await drain()
    assert.deepEqual(log, ['fn 1', 'class 2', 'class 3', 'fn 4'])
  })
})
