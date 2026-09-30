import { test, describe, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { spawn } from 'node:child_process'

// Shared by cli.test.js (class jobs) and cli-function.test.js (function jobs).
const style = process.env.LITEQUEUE_TEST_STYLE ?? 'class'
const dir = mkdtempSync(join(tmpdir(), 'litequeue-cli-'))
process.env.LITEQUEUE_TEST_DIR = dir

const config = resolve('test/fixtures/cli.config.js')
const bin = resolve('bin/litequeue.js')
const { default: queue, Mark, Boom, SlowDone } = await import(config)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// Runs the CLI as a separate process, like a user would.
function cli(...args) {
  const child = spawn(process.execPath, [bin, ...args, '--config', config], {
    env: { ...process.env, NO_COLOR: '1' },
  })
  let out = ''
  child.stdout.on('data', (d) => (out += d))
  child.stderr.on('data', (d) => (out += d))
  const done = new Promise((res) => child.on('close', (code) => res({ code, out })))
  return Object.assign(done, { child })
}

async function waitFor(check, ms = 5000) {
  const end = Date.now() + ms
  while (Date.now() < end) {
    if (check()) return
    await sleep(25)
  }
  throw new Error('timed out waiting')
}

before(async () => {
  await queue.clear('default')
  await queue.flush()
})

after(async () => {
  await queue.close()
  rmSync(dir, { recursive: true, force: true })
})

describe(`command line (${style} jobs)`, () => {
  test('size shows queued jobs', async () => {
    await Mark.dispatch({ name: 'a' })
    const { code, out } = await cli('size')
    assert.equal(code, 0)
    assert.match(out, /default\s+1\s+1/)
  })

  test('work --stop-when-empty runs jobs and exits 0', async () => {
    const { code, out } = await cli('work', '--stop-when-empty', '--sleep', '0.1')
    assert.equal(code, 0)
    assert.match(out, /Mark/)
    assert.ok(existsSync(join(dir, 'done-a')))
    assert.match((await cli('size')).out, /empty/)
  })

  test('failed, retry, forget and flush manage failed jobs', async () => {
    await Boom.dispatch({})
    await cli('work', '--stop-when-empty', '--sleep', '0.1')

    const list = await cli('failed')
    assert.match(list.out, /Boom/)
    assert.match(list.out, /boom/)

    const retry = await cli('retry', 'all')
    assert.match(retry.out, /Pushed 1 job/)
    assert.match((await cli('size')).out, /default\s+1/)

    await cli('work', '--stop-when-empty', '--sleep', '0.1')
    const id = (await queue.failed())[0].id
    assert.match((await cli('forget', String(id))).out, /Failed job deleted/)
    assert.match((await cli('forget', String(id))).out, /No failed job/)

    await Boom.dispatch({})
    await cli('work', '--stop-when-empty', '--sleep', '0.1')
    assert.match((await cli('flush')).out, /Deleted 1 failed job/)
    assert.match((await cli('failed')).out, /No failed jobs/)
  })

  test('clear deletes the jobs on a queue', async () => {
    await Mark.dispatch({ name: 'x' })
    await Mark.dispatch({ name: 'y' })
    assert.match((await cli('clear')).out, /Deleted 2 job\(s\) from "default"/)
  })

  test('an unknown command exits 1 with a message', async () => {
    const { code, out } = await cli('nope')
    assert.equal(code, 1)
    assert.match(out, /Unknown command "nope"/)
  })

  test('a missing config file exits 1', async () => {
    const child = spawn(process.execPath, [bin, 'size', '--config', join(dir, 'missing.js')])
    let out = ''
    child.stderr.on('data', (d) => (out += d))
    const code = await new Promise((res) => child.on('close', res))
    assert.equal(code, 1)
    assert.match(out, /Could not find/)
  })
})

describe(`shutdown signals (${style} jobs)`, () => {
  for (const signal of ['SIGTERM', 'SIGINT']) {
    test(`${signal} lets the running job finish, then exits`, async () => {
      rmSync(join(dir, 'slow-started'), { force: true })
      rmSync(join(dir, 'slow-finished'), { force: true })
      await SlowDone.dispatch({})

      const run = cli('work', '--sleep', '0.1')
      await waitFor(() => existsSync(join(dir, 'slow-started')))
      run.child.kill(signal)
      const { code, out } = await run

      assert.ok(existsSync(join(dir, 'slow-finished')), 'job was cut off')
      assert.equal(code, 0)
      assert.match(out, /Finishing current jobs/)
    })
  }
})
