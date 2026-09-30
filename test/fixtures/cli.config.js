// Queue used by the CLI tests. LITEQUEUE_TEST_DIR and LITEQUEUE_TEST_STYLE are set by the test.
import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { createQueue, Job } from '../../src/index.js'

const dir = process.env.LITEQUEUE_TEST_DIR
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

class MarkClass extends Job {
  static jobName = 'Mark'
  async handle() {
    writeFileSync(join(dir, `done-${this.data.name}`), 'ok')
  }
}

class BoomClass extends Job {
  static jobName = 'Boom'
  async handle() {
    throw new Error('boom')
  }
}

class SlowDoneClass extends Job {
  static jobName = 'SlowDone'
  async handle() {
    writeFileSync(join(dir, 'slow-started'), 'ok')
    await sleep(1000)
    writeFileSync(join(dir, 'slow-finished'), 'ok')
  }
}

const MarkFunction = Job.define('Mark', async (data) => {
  writeFileSync(join(dir, `done-${data.name}`), 'ok')
})

const BoomFunction = Job.define('Boom', async () => {
  throw new Error('boom')
})

const SlowDoneFunction = Job.define('SlowDone', async () => {
  writeFileSync(join(dir, 'slow-started'), 'ok')
  await sleep(1000)
  writeFileSync(join(dir, 'slow-finished'), 'ok')
})

// The same jobs in both styles. LITEQUEUE_TEST_STYLE picks one ('class' or 'function').
const useFunctions = process.env.LITEQUEUE_TEST_STYLE === 'function'
export const Mark = useFunctions ? MarkFunction : MarkClass
export const Boom = useFunctions ? BoomFunction : BoomClass
export const SlowDone = useFunctions ? SlowDoneFunction : SlowDoneClass

export default createQueue({
  path: join(dir, 'queue.sqlite'),
  jobs: [Mark, Boom, SlowDone],
})
