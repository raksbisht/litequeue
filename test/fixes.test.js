import { test, describe } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createQueue, Job } from '../src/index.js'

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const drivers = {
  memory: () => ({ driver: 'memory' }),
  sqlite: (dir) => ({ driver: 'sqlite', path: join(dir, 'queue.sqlite') }),
}

// Each test builds its jobs in both styles, so the fixes are checked for both.
const styles = {
  function: {
    unique: (name, queue) => Job.define({ name, queue, unique: true, uniqueId: (job) => String(job.data.id ?? 'x'), handle() {} }),
    slow: (name) => Job.define(name, () => sleep(20)),
  },
  class: {
    unique: (name, queue) =>
      class extends Job {
        static jobName = name
        static queue = queue
        static unique = true
        uniqueId() {
          return String(this.data.id ?? 'x')
        }
        async handle() {}
      },
    slow: (name) =>
      class extends Job {
        static jobName = name
        async handle() {
          await sleep(20)
        }
      },
  },
}

for (const [name, config] of Object.entries(drivers))
for (const [style, make] of Object.entries(styles)) {
  describe(`${name} driver fixes (${style} jobs)`, () => {
    const withQueue = async (fn) => {
      const dir = mkdtempSync(join(tmpdir(), 'litequeue-fix-'))
      const queue = createQueue(config(dir))
      queue.on('warning', () => {})
      try {
        await fn(queue)
      } finally {
        await queue.close()
        rmSync(dir, { recursive: true, force: true })
      }
    }

    test('clear() releases unique locks, so the job can be queued again', () =>
      withQueue(async (queue) => {
        const refund = make.unique('refund')
        queue.register(refund)
        assert.ok(await refund.dispatch({ id: 1 }))
        assert.equal(await refund.dispatch({ id: 1 }), null)
        assert.equal(await queue.clear('default'), 1)
        assert.ok(await refund.dispatch({ id: 1 }), 'still blocked after clear()')
      }))

    test('clear() leaves locks of other queues alone', () =>
      withQueue(async (queue) => {
        const refund = make.unique('refund', 'billing')
        queue.register(refund)
        await refund.dispatch({})
        await queue.clear('default')
        assert.equal(await refund.dispatch({}), null)
      }))

    test('maxJobs is not overshot with concurrency', () =>
      withQueue(async (queue) => {
        const slow = make.slow('slow')
        queue.register(slow)
        for (let i = 0; i < 10; i++) await slow.dispatch({})
        const result = await queue.work({ concurrency: 5, maxJobs: 3, sleep: 0.01 })
        assert.equal(result.processed, 3)
        assert.equal(result.reason, 'max-jobs')
        assert.equal(await queue.size(), 7)
      }))

    test('a failed push releases the unique lock', () =>
      withQueue(async (queue) => {
        const refund = make.unique('refund')
        queue.register(refund)
        const push = queue.driver.push
        queue.driver.push = async () => {
          throw new Error('disk full')
        }
        await assert.rejects(refund.dispatch({}), /disk full/)
        queue.driver.push = push
        assert.ok(await refund.dispatch({}), 'lock was left behind')
      }))
  })
}
