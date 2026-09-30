import { test, describe, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  createQueue,
  Job,
  Queue,
  MemoryDriver,
  WithoutOverlapping,
  ThrottlesExceptions,
  RateLimited,
  Limit,
  Skip,
  Release,
  FailOnException,
  withoutOverlapping,
  throttlesExceptions,
  rateLimited,
  failOnException,
  skipWhen,
  releaseWhen,
} from '../src/index.js'

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const log = []

class AuthError extends Error {}

function classJobs() {
  class Hello extends Job {
    async handle() {
      log.push(`hello ${this.data.name}`)
    }
  }

  class Flaky extends Job {
    static tries = 3
    async handle() {
      log.push(`flaky attempt ${this.attempts()}`)
      if (this.attempts() < this.data.succeedOn) throw new Error(`boom ${this.attempts()}`)
    }
    async failed(error) {
      log.push(`flaky failed: ${error.message}`)
    }
  }

  class Slow extends Job {
    static timeout = 0.05
    static tries = 3
    async handle() {
      await sleep(150)
      log.push(this.signal.aborted ? 'slow saw abort' : 'slow finished')
    }
  }

  class SlowFailOnTimeout extends Slow {
    static failOnTimeout = true
  }

  class Releaser extends Job {
    static tries = 0
    static maxExceptions = 2
    async handle() {
      log.push(`releaser ${this.attempts()}`)
      if (this.attempts() === 1) return this.release(0)
      throw new Error('nope')
    }
  }

  class Deadline extends Job {
    static tries = 1
    retryUntil() {
      return new Date(Date.now() + 80)
    }
    async handle() {
      throw new Error('always')
    }
  }

  class Refund extends Job {
    static unique = true
    uniqueId() {
      return String(this.data.orderId)
    }
    async handle() {
      log.push(`refund ${this.data.orderId}`)
    }
  }

  class RefundUntilProcessing extends Refund {
    static uniqueUntilProcessing = true
    async handle() {
      log.push(`start ${this.data.orderId}`)
      this.data.redispatched = await RefundUntilProcessing.dispatch({ orderId: this.data.orderId })
      log.push(`redispatched ${this.data.redispatched !== null}`)
    }
  }

  class Step extends Job {
    async handle() {
      log.push(`step ${this.data.n}`)
      if (this.data.fail) throw new Error('step failed')
    }
  }

  class Overlap extends Job {
    static tries = 10
    middleware() {
      return [new WithoutOverlapping('shared').releaseAfter(0.02)]
    }
    async handle() {
      log.push(`overlap start ${this.data.n}`)
      await sleep(30)
      log.push(`overlap end ${this.data.n}`)
    }
  }

  class Skippy extends Job {
    middleware() {
      return [Skip.when(() => this.data.skip)]
    }
    async handle() {
      log.push('skippy ran')
    }
  }

  class Waiter extends Job {
    static tries = 5
    middleware() {
      return [Release.when(this.attempts() < 2, 0)]
    }
    async handle() {
      log.push(`waiter ran on attempt ${this.attempts()}`)
    }
  }

  class Strict extends Job {
    static tries = 5
    middleware() {
      return [new FailOnException([AuthError])]
    }
    async handle() {
      throw new AuthError('forbidden')
    }
  }

  class CallsService extends Job {
    static tries = 0
    middleware() {
      return [new ThrottlesExceptions(2, 60).by('service')]
    }
    async handle() {
      log.push(`call ${this.attempts()}`)
      throw new Error('service down')
    }
  }

  class Backup extends Job {
    static tries = 5
    middleware() {
      return [new RateLimited('backups')]
    }
    async handle() {
      log.push(`backup ${this.data.n}`)
    }
  }

  class GivesUp extends Job {
    async handle() {
      this.fail()
    }
  }

  class Search extends Job {
    static debounceFor = 0.1
    debounceId() {
      return this.data.userId
    }
    async handle() {
      log.push(`index ${this.data.version}`)
    }
  }

  class SearchMaxWait extends Search {
    static maxDebounceWait = 0.15
  }

    const ALL = [Hello, Flaky, Slow, SlowFailOnTimeout, Releaser, Deadline, Refund, RefundUntilProcessing, Step, Overlap, Skippy, Waiter, Strict, CallsService, Backup, GivesUp, Search, SearchMaxWait]
    return { ALL, Hello, Flaky, Slow, SlowFailOnTimeout, Releaser, Deadline, Refund, RefundUntilProcessing, Step, Overlap, Skippy, Waiter, Strict, CallsService, Backup, GivesUp, Search, SearchMaxWait }
}

// The same jobs as classJobs(), written with Job.define(). Every test runs with both.
function functionJobs() {
  const Hello = Job.define('Hello', async (data) => {
    log.push(`hello ${data.name}`)
  })

  const Flaky = Job.define({
    name: 'Flaky',
    tries: 3,
    handle: async (data, job) => {
      log.push(`flaky attempt ${job.attempts()}`)
      if (job.attempts() < data.succeedOn) throw new Error(`boom ${job.attempts()}`)
    },
    failed: async (error) => {
      log.push(`flaky failed: ${error.message}`)
    },
  })

  const slowHandle = async (data, job) => {
    await sleep(150)
    log.push(job.signal.aborted ? 'slow saw abort' : 'slow finished')
  }
  const Slow = Job.define({ name: 'Slow', timeout: 0.05, tries: 3, handle: slowHandle })
  const SlowFailOnTimeout = Job.define({ name: 'SlowFailOnTimeout', timeout: 0.05, tries: 3, failOnTimeout: true, handle: slowHandle })

  const Releaser = Job.define({
    name: 'Releaser',
    tries: 0,
    maxExceptions: 2,
    handle: async (data, job) => {
      log.push(`releaser ${job.attempts()}`)
      if (job.attempts() === 1) return job.release(0)
      throw new Error('nope')
    },
  })

  const Deadline = Job.define({
    name: 'Deadline',
    tries: 1,
    retryUntil: () => new Date(Date.now() + 80),
    handle: async () => {
      throw new Error('always')
    },
  })

  const Refund = Job.define({
    name: 'Refund',
    unique: true,
    uniqueId: (job) => String(job.data.orderId),
    handle: async (data) => {
      log.push(`refund ${data.orderId}`)
    },
  })

  const RefundUntilProcessing = Job.define({
    name: 'RefundUntilProcessing',
    unique: true,
    uniqueUntilProcessing: true,
    uniqueId: (job) => String(job.data.orderId),
    handle: async (data) => {
      log.push(`start ${data.orderId}`)
      data.redispatched = await RefundUntilProcessing.dispatch({ orderId: data.orderId })
      log.push(`redispatched ${data.redispatched !== null}`)
    },
  })

  const Step = Job.define('Step', async (data) => {
    log.push(`step ${data.n}`)
    if (data.fail) throw new Error('step failed')
  })

  const Overlap = Job.define({
    name: 'Overlap',
    tries: 10,
    middleware: () => [withoutOverlapping('shared').releaseAfter(0.02)],
    handle: async (data) => {
      log.push(`overlap start ${data.n}`)
      await sleep(30)
      log.push(`overlap end ${data.n}`)
    },
  })

  const Skippy = Job.define({
    name: 'Skippy',
    middleware: (job) => [skipWhen(() => job.data.skip)],
    handle: async () => {
      log.push('skippy ran')
    },
  })

  const Waiter = Job.define({
    name: 'Waiter',
    tries: 5,
    middleware: (job) => [releaseWhen(job.attempts() < 2, 0)],
    handle: async (data, job) => {
      log.push(`waiter ran on attempt ${job.attempts()}`)
    },
  })

  const Strict = Job.define({
    name: 'Strict',
    tries: 5,
    middleware: () => [failOnException([AuthError])],
    handle: async () => {
      throw new AuthError('forbidden')
    },
  })

  const CallsService = Job.define({
    name: 'CallsService',
    tries: 0,
    middleware: () => [throttlesExceptions(2, 60).by('service')],
    handle: async (data, job) => {
      log.push(`call ${job.attempts()}`)
      throw new Error('service down')
    },
  })

  const Backup = Job.define({
    name: 'Backup',
    tries: 5,
    middleware: () => [rateLimited('backups')],
    handle: async (data) => {
      log.push(`backup ${data.n}`)
    },
  })

  const GivesUp = Job.define('GivesUp', async (data, job) => {
    job.fail()
  })

  const searchHandle = async (data) => {
    log.push(`index ${data.version}`)
  }
  const Search = Job.define({ name: 'Search', debounceFor: 0.1, debounceId: (job) => job.data.userId, handle: searchHandle })
  const SearchMaxWait = Job.define({
    name: 'SearchMaxWait',
    debounceFor: 0.1,
    maxDebounceWait: 0.15,
    debounceId: (job) => job.data.userId,
    handle: searchHandle,
  })

  const ALL = [Hello, Flaky, Slow, SlowFailOnTimeout, Releaser, Deadline, Refund, RefundUntilProcessing, Step, Overlap, Skippy, Waiter, Strict, CallsService, Backup, GivesUp, Search, SearchMaxWait]
  return { ALL, Hello, Flaky, Slow, SlowFailOnTimeout, Releaser, Deadline, Refund, RefundUntilProcessing, Step, Overlap, Skippy, Waiter, Strict, CallsService, Backup, GivesUp, Search, SearchMaxWait }
}

const styles = { class: classJobs(), function: functionJobs() }

const drivers = {
  memory: () => ({ driver: 'memory' }),
  sqlite: (dir) => ({ driver: 'sqlite', path: join(dir, 'queue.sqlite') }),
}

for (const [style, jobs] of Object.entries(styles)) {
  const { ALL, Hello, Flaky, Slow, SlowFailOnTimeout, Releaser, Deadline, Refund, RefundUntilProcessing, Step, Overlap, Skippy, Waiter, Strict, CallsService, Backup, GivesUp, Search, SearchMaxWait } = jobs

  for (const [name, config] of Object.entries(drivers)) {
    describe(`${name} driver (${style} jobs)`, () => {
      let dir
      let queue

      beforeEach(() => {
        log.length = 0
        dir = mkdtempSync(join(tmpdir(), 'litequeue-'))
        queue = createQueue({ ...config(dir), jobs: ALL })
        queue.on('warning', () => {})
      })

      afterEach(async () => {
        await queue.close()
        rmSync(dir, { recursive: true, force: true })
      })

      const drain = (options = {}) => queue.work({ stopWhenEmpty: true, sleep: 0.01, ...options })

      test('dispatches and processes a job', async () => {
        assert.ok(await Hello.dispatch({ name: 'ada' }))
        assert.equal(await queue.size(), 1)
        const result = await drain()
        assert.equal(result.processed, 1)
        assert.equal(result.reason, 'queue-empty')
        assert.deepEqual(log, ['hello ada'])
        assert.equal(await queue.size(), 0)
      })

      test('payload has the same keys as Laravel', async () => {
        await Flaky.dispatch({ succeedOn: 1 })
        const reserved = await queue.driver.pop('default', 90)
        const payload = JSON.parse(reserved.payload)
        for (const key of ['uuid', 'displayName', 'job', 'maxTries', 'maxExceptions', 'failOnTimeout', 'backoff', 'timeout', 'retryUntil', 'data', 'createdAt']) {
          assert.ok(key in payload, key)
        }
        assert.equal(payload.maxTries, 3)
      })

      test('onQueue and priority order', async () => {
        await Hello.dispatch({ name: 'low' })
        await Hello.dispatch({ name: 'high' }).onQueue('high')
        await drain({ queue: 'high,default' })
        assert.deepEqual(log, ['hello high', 'hello low'])
      })

      test('size, pending, delayed and reserved counts', async () => {
        await Hello.dispatch({ name: 'a' })
        await Hello.dispatch({ name: 'b' }).delay(60)
        await Hello.dispatch({ name: 'c' })
        await queue.driver.pop('default', 90)
        assert.deepEqual(
          { size: await queue.size(), pending: await queue.pendingSize(), delayed: await queue.delayedSize(), reserved: await queue.reservedSize() },
          { size: 3, pending: 1, delayed: 1, reserved: 1 },
        )
      })

      test('delayed jobs wait', async () => {
        await Hello.dispatch({ name: 'later' }).delay(0.15)
        await drain()
        assert.deepEqual(log, [])
        await sleep(200)
        await drain()
        assert.deepEqual(log, ['hello later'])
      })

      test('a released job goes to the back of the queue, like deleteAndRelease()', async () => {
        await Flaky.dispatch({ succeedOn: 2 })
        await Hello.dispatch({ name: 'second' })
        await drain()
        assert.deepEqual(log, ['flaky attempt 1', 'hello second', 'flaky attempt 2'])
      })

      test('fails after tries and calls failed()', async () => {
        await Flaky.dispatch({ succeedOn: 99 })
        await drain()
        assert.equal(log.at(-1), 'flaky failed: boom 3')
        const failed = await queue.failed()
        assert.equal(failed.length, 1)
        assert.match(failed[0].exception, /boom 3/)
      })

      test('backoff uses the worker default and the last value repeats', async () => {
        const delays = []
        queue.on('job:released-after-exception', ({ delay }) => delays.push(delay))
        await Flaky.dispatch({ succeedOn: 99 })
        await drain({ backoff: [0, 0.01] })
        await sleep(30)
        await drain({ backoff: [0, 0.01] })
        assert.deepEqual(delays, [0, 0.01])
      })

      test('retry resets attempts and removes the failed record', async () => {
        await Flaky.dispatch({ succeedOn: 99 })
        await drain()
        const [record] = await queue.failed()
        assert.deepEqual(await queue.retry(record.id), [record.id])
        assert.equal((await queue.failed()).length, 0)
        log.length = 0
        await drain()
        assert.deepEqual(log.slice(0, 1), ['flaky attempt 1'])
      })

      test('this.fail() without an error uses ManuallyFailedError', async () => {
        await GivesUp.dispatch()
        await drain()
        const [record] = await queue.failed()
        assert.match(record.exception, /ManuallyFailedError/)
      })

      test('a timeout stops the worker and leaves the job for retryAfter', async () => {
        const q = new Queue({ driver: queue.driver, retryAfter: 0.1, jobs: ALL })
        q.on('warning', () => {})
        const events = []
        q.on('job:timed-out', () => events.push('timed-out'))
        await q.dispatch(new Slow())
        const result = await q.work({ sleep: 0.01 })
        assert.equal(result.reason, 'timed-out')
        assert.equal(result.status, 1)
        assert.deepEqual(events, ['timed-out'])
        assert.equal(await q.reservedSize(), 1, 'still reserved, not released')
        await sleep(250)
        assert.equal(await q.pendingSize() + (await q.reservedSize()), 1)
        assert.ok(log.includes('slow saw abort'))
      })

      test('failOnTimeout fails the job on its first timeout', async () => {
        await SlowFailOnTimeout.dispatch()
        const result = await drain()
        assert.equal(result.reason, 'timed-out')
        const [record] = await queue.failed()
        assert.match(record.exception, /TimeoutExceededError/)
        await sleep(150)
      })

      test('killOnTimeout: false treats a timeout like any other error', async () => {
        await Slow.dispatch()
        const result = await drain({ killOnTimeout: false })
        assert.equal(result.reason, 'queue-empty')
        const [record] = await queue.failed()
        assert.match(record.exception, /TimeoutExceededError/)
        await sleep(150)
      })

      test('release() does not count as an exception; maxExceptions does', async () => {
        await Releaser.dispatch()
        await drain()
        assert.deepEqual(log, ['releaser 1', 'releaser 2', 'releaser 3'])
        assert.equal((await queue.failed()).length, 1)
      })

      test('retryUntil overrides tries', async () => {
        await Deadline.dispatch()
        await drain({ maxTime: 0.05 })
        assert.equal((await queue.failed()).length, 0, 'still retrying before the deadline')
        await sleep(100)
        await drain()
        assert.equal((await queue.failed()).length, 1)
      })

      test('unique jobs are only queued once and unlock when done', async () => {
        assert.ok(await Refund.dispatch({ orderId: 7 }))
        assert.equal(await Refund.dispatch({ orderId: 7 }), null)
        assert.ok(await Refund.dispatch({ orderId: 8 }))
        await drain()
        assert.deepEqual(log, ['refund 7', 'refund 8'])
        assert.ok(await Refund.dispatch({ orderId: 7 }))
      })

      test('uniqueUntilProcessing unlocks when the job starts', async () => {
        await RefundUntilProcessing.dispatch({ orderId: 1 })
        await drain({ maxJobs: 1 })
        assert.deepEqual(log, ['start 1', 'redispatched true'])
      })

      test('debounced jobs: only the newest dispatch runs', async () => {
        await Search.dispatch({ userId: 1, version: 1 })
        await Search.dispatch({ userId: 1, version: 2 })
        await Search.dispatch({ userId: 1, version: 3 })
        const debounced = []
        queue.on('job:debounced', () => debounced.push(1))
        await sleep(150)
        await drain()
        assert.deepEqual(log, ['index 3'])
        assert.equal(debounced.length, 2)
      })

      test('maxDebounceWait runs the job once the wait is too long', async () => {
        await SearchMaxWait.dispatch({ userId: 1, version: 1 })
        await sleep(80)
        await SearchMaxWait.dispatch({ userId: 1, version: 2 })
        await sleep(80)
        await SearchMaxWait.dispatch({ userId: 1, version: 3 }) // 160ms after the first: no delay
        await drain()
        assert.deepEqual(log, ['index 3'])
      })

      test('chains run in order and stop on failure', async () => {
        await queue.chain([new Step({ n: 1 }), new Step({ n: 2, fail: true }), new Step({ n: 3 })])
        await drain()
        assert.deepEqual(log, ['step 1', 'step 2'])
        assert.equal((await queue.failed()).length, 1)
      })

      test('WithoutOverlapping keeps jobs from running at the same time', async () => {
        await Overlap.dispatch({ n: 1 })
        await Overlap.dispatch({ n: 2 })
        await drain({ concurrency: 2, stopWhenEmpty: false, maxTime: 0.3 })
        assert.deepEqual(log, ['overlap start 1', 'overlap end 1', 'overlap start 2', 'overlap end 2'])
      })

      test('Skip.when deletes the job without running it', async () => {
        await Skippy.dispatch({ skip: true })
        await Skippy.dispatch({ skip: false })
        await drain()
        assert.deepEqual(log, ['skippy ran'])
        assert.equal((await queue.failed()).length, 0)
      })

      test('Release.when puts the job back', async () => {
        await Waiter.dispatch()
        await drain()
        assert.deepEqual(log, ['waiter ran on attempt 2'])
      })

      test('FailOnException stops retries for listed errors', async () => {
        await Strict.dispatch()
        await drain()
        const failed = await queue.failed()
        assert.equal(failed.length, 1)
        assert.match(failed[0].exception, /forbidden/)
      })

      test('ThrottlesExceptions stops calling a failing service', async () => {
        const released = []
        queue.on('job:released', ({ delay }) => released.push(delay))
        await CallsService.dispatch()
        await drain({ maxJobs: 3 })
        assert.deepEqual(log, ['call 1', 'call 2'], 'third attempt is throttled, not called')
        assert.ok(released.at(-1) > 60, 'released until the window ends')
      })

      test('RateLimited with a named limiter', async () => {
        queue.limiter('backups', () => Limit.perMinute(2))
        for (let n = 1; n <= 3; n++) await Backup.dispatch({ n })
        await drain()
        assert.deepEqual(log, ['backup 1', 'backup 2'])
        assert.equal(await queue.delayedSize(), 1)
      })

      test('paused queues are skipped until resumed', async () => {
        await Hello.dispatch({ name: 'waiting' })
        await queue.pause('default')
        await drain()
        assert.deepEqual(log, [])
        await queue.resume('default')
        await drain()
        assert.deepEqual(log, ['hello waiting'])
      })

      test('restart signal stops a running worker', async () => {
        const running = queue.worker({ sleep: 0.01 }).daemon()
        await sleep(30)
        await queue.restart()
        assert.equal((await running).reason, 'restart')
      })

      test('stopWhenEmptyFor', async () => {
        const started = Date.now()
        const result = await queue.work({ sleep: 0.01, stopWhenEmptyFor: 0.1 })
        assert.equal(result.reason, 'queue-empty-for')
        assert.ok(Date.now() - started >= 100)
      })

      test('max jobs', async () => {
        await Hello.dispatch({ name: 'a' })
        await Hello.dispatch({ name: 'b' })
        const result = await drain({ maxJobs: 1 })
        assert.equal(result.reason, 'max-jobs')
        assert.deepEqual(log, ['hello a'])
      })

      test('unknown jobs fail instead of crashing the worker', async () => {
        await queue.driver.push('default', JSON.stringify({ uuid: 'x', job: 'Ghost', displayName: 'Ghost', data: {} }), 0)
        await drain()
        const [record] = await queue.failed()
        assert.match(record.exception, /JobNotRegisteredError/)
      })

      test('a job abandoned by a crashed worker is picked up after retryAfter', async () => {
        const q = new Queue({ driver: queue.driver, retryAfter: 0.05, jobs: [Hello] })
        q.on('warning', () => {})
        await q.dispatch(new Hello({ name: 'again' }))
        assert.ok(await q.driver.pop('default', 0.05)) // a worker takes it, then "dies"
        assert.equal(await q.driver.pop('default', 0.05), null, 'still reserved')
        await sleep(80)
        await q.work({ stopWhenEmpty: true, sleep: 0.01, tries: 2 })
        assert.deepEqual(log, ['hello again'])
      })

      test('an abandoned job with no attempts left fails with MaxAttemptsExceededError', async () => {
        const q = new Queue({ driver: queue.driver, retryAfter: 0.05, jobs: [Hello] })
        q.on('warning', () => {})
        await q.dispatch(new Hello({ name: 'once' }))
        await q.driver.pop('default', 0.05)
        await sleep(80)
        await q.work({ stopWhenEmpty: true, sleep: 0.01 })
        assert.deepEqual(log, [])
        const [record] = await q.failed()
        assert.match(record.exception, /MaxAttemptsExceededError/)
      })

      test('warns when a timeout is longer than retryAfter', async () => {
        const q = new Queue({ driver: queue.driver, retryAfter: 30, jobs: [Hello] })
        const warnings = []
        q.on('warning', (message) => warnings.push(message))
        await q.work({ stopWhenEmpty: true, sleep: 0.01, timeout: 60 })
        assert.equal(warnings.length, 1)
        assert.match(warnings[0], /retryAfter is 30s/)
      })
    })
  }

  describe(`sqlite with several connections (${style} jobs)`, () => {
    test('two workers on the same file never run a job twice', async () => {
      log.length = 0
      const dir = mkdtempSync(join(tmpdir(), 'litequeue-'))
      const path = join(dir, 'queue.sqlite')
      const a = new Queue({ driver: 'sqlite', path, jobs: [Hello] })
      const b = new Queue({ driver: 'sqlite', path, jobs: [Hello] })
      for (let i = 0; i < 50; i++) await a.dispatch(new Hello({ name: i }))
      await Promise.all([
        a.work({ stopWhenEmpty: true, sleep: 0.01, concurrency: 3 }),
        b.work({ stopWhenEmpty: true, sleep: 0.01, concurrency: 3 }),
      ])
      assert.equal(log.length, 50)
      assert.equal(new Set(log).size, 50)
      await a.close()
      await b.close()
      rmSync(dir, { recursive: true, force: true })
    })
  })

  describe(`sync driver (${style} jobs)`, () => {
    test('runs on dispatch, calls failed() and throws to the caller', async () => {
      log.length = 0
      createQueue({ driver: 'sync' })
      await Hello.dispatch({ name: 'now' })
      assert.deepEqual(log, ['hello now'])
      await assert.rejects(Flaky.dispatch({ succeedOn: 99 }), /boom 1/)
      assert.equal(log.at(-1), 'flaky failed: boom 1')
    })

    test('dispatchSync skips the queue on any driver', async () => {
      log.length = 0
      const queue = createQueue({ driver: new MemoryDriver() })
      await Hello.dispatchSync({ name: 'inline' })
      assert.deepEqual(log, ['hello inline'])
      assert.equal(await queue.size(), 0)
    })
  })

  describe(`custom drivers (${style} jobs)`, () => {
    test('Queue.extend registers a driver by name', () => {
      Queue.extend('custom', () => new MemoryDriver())
      assert.ok(createQueue({ driver: 'custom' }).driver instanceof MemoryDriver)
    })
  })
}
