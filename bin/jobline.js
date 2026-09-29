#!/usr/bin/env node
import { parseArgs } from 'node:util'
import { resolve } from 'node:path'
import { existsSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import { Queue } from '../src/Queue.js'

const HELP = `
jobline <command> [options]

Commands
  work                 Start a worker
  size [queue]         Show pending, delayed and reserved jobs per queue
  failed               List failed jobs
  retry <id...|all>    Put failed jobs back on their queue
  forget <id>          Delete one failed job
  flush                Delete all failed jobs
  clear [queue]        Delete every job on a queue
  restart              Tell running workers to exit after their current job
  pause <queue>        Stop workers from taking jobs from a queue
  resume <queue>       Let workers take jobs from a paused queue again

Worker options
  --queue <names>      Queues to work, highest priority first  (default: default)
  --concurrency <n>    Jobs to run at the same time             (default: 1)
  --sleep <sec>        Wait when all queues are empty           (default: 3)
  --tries <n>          Attempts for jobs that don't set tries   (default: 1)
  --backoff <sec,...>  Delay before retries, e.g. 10,60         (default: 0)
  --timeout <sec>      Timeout for jobs that don't set one      (default: 60)
  --max-jobs <n>       Exit after this many jobs
  --max-time <sec>     Exit after this many seconds
  --memory <mb>        Exit when memory use passes this      (default: 128)
  --rest <sec>         Pause between jobs                      (default: 0)
  --stop-when-empty    Exit when the queues are empty
  --stop-when-empty-for <sec>  Exit after the queues have been empty this long
  --no-kill-on-timeout Treat timeouts as normal errors instead of stopping the worker

Global options
  --config <file>      Module that exports your queue  (default: jobline.config.js)
`

const { values: opts, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    config: { type: 'string', default: 'jobline.config.js' },
    queue: { type: 'string' },
    concurrency: { type: 'string' },
    sleep: { type: 'string' },
    tries: { type: 'string' },
    backoff: { type: 'string' },
    timeout: { type: 'string' },
    'max-jobs': { type: 'string' },
    'max-time': { type: 'string' },
    memory: { type: 'string' },
    'stop-when-empty': { type: 'boolean' },
    'stop-when-empty-for': { type: 'string' },
    rest: { type: 'string' },
    'no-kill-on-timeout': { type: 'boolean' },
    for: { type: 'string' },
    help: { type: 'boolean', short: 'h' },
  },
})

const [command, ...args] = positionals
const color = process.stdout.isTTY && !process.env.NO_COLOR
const paint = (code, text) => (color ? `\x1b[${code}m${text}\x1b[0m` : text)
const gray = (t) => paint(90, t)
const green = (t) => paint(32, t)
const yellow = (t) => paint(33, t)
const red = (t) => paint(31, t)
const num = (v) => (v === undefined ? undefined : Number(v))

if (!command || opts.help) {
  console.log(HELP)
  process.exit(0)
}

const queue = await loadQueue(opts.config)

try {
  switch (command) {
    case 'work':
      process.exitCode = await work()
      break
    case 'size': {
      const stats = (await queue.stats()).filter((s) => !args[0] || s.queue === args[0])
      if (!stats.length) console.log(args[0] ? `"${args[0]}" is empty.` : 'All queues are empty.')
      else console.log(gray(`${'queue'.padEnd(20)} ${'size'.padStart(7)} ${'pending'.padStart(8)} ${'delayed'.padStart(8)} ${'reserved'.padStart(9)}`))
      for (const s of stats) {
        const paused = (await queue.isPaused(s.queue)) ? yellow(' paused') : ''
        console.log(`${s.queue.padEnd(20)} ${String(s.size).padStart(7)} ${String(s.pending).padStart(8)} ${String(s.delayed).padStart(8)} ${String(s.reserved).padStart(9)}${paused}`)
      }
      break
    }
    case 'pause': {
      if (!args[0]) throw new Error('Pass the queue to pause.')
      await queue.pause(args[0], num(opts.for) ?? 0)
      console.log(`Workers will stop taking jobs from "${args[0]}"${opts.for ? ` for ${opts.for}s` : ''}.`)
      break
    }
    case 'resume': {
      if (!args[0]) throw new Error('Pass the queue to resume.')
      await queue.resume(args[0])
      console.log(`Workers will take jobs from "${args[0]}" again.`)
      break
    }
    case 'failed': {
      const failed = await queue.failed()
      if (!failed.length) console.log('No failed jobs.')
      for (const f of failed) {
        const name = safeParse(f.payload)?.job ?? 'unknown'
        const reason = f.exception.split('\n')[0]
        console.log(`${gray(`#${f.id}`)}  ${stamp(f.failedAt)}  ${name}  ${gray(`on ${f.queue}`)}\n     ${red(reason)}`)
      }
      break
    }
    case 'retry': {
      if (!args.length) throw new Error('Pass failed job ids, or "all".')
      const retried = await queue.retry(args[0] === 'all' ? 'all' : args)
      console.log(retried.length ? `Pushed ${retried.length} job(s) back onto the queue.` : 'Nothing to retry.')
      break
    }
    case 'forget': {
      if (!args[0]) throw new Error('Pass a failed job id.')
      console.log((await queue.forget(args[0])) ? 'Failed job deleted.' : 'No failed job with that id.')
      break
    }
    case 'flush':
      console.log(`Deleted ${await queue.flush()} failed job(s).`)
      break
    case 'clear': {
      const name = args[0] ?? 'default'
      console.log(`Deleted ${await queue.clear(name)} job(s) from "${name}".`)
      break
    }
    case 'restart':
      await queue.restart()
      console.log('Workers will restart after their current job.')
      break
    default:
      throw new Error(`Unknown command "${command}". Run "jobline --help".`)
  }
} catch (error) {
  console.error(red(error.message))
  process.exitCode = 1
} finally {
  await queue.close()
  // A timed-out job may still be running; like Laravel, exit instead of waiting for it.
  if (command === 'work') process.exit(process.exitCode ?? 0)
}

async function work() {
  const worker = queue.worker({
    queue: opts.queue,
    concurrency: num(opts.concurrency),
    sleep: num(opts.sleep),
    tries: num(opts.tries),
    backoff: opts.backoff?.includes(',') ? opts.backoff.split(',').map(Number) : num(opts.backoff),
    timeout: num(opts.timeout),
    maxJobs: num(opts['max-jobs']),
    maxTime: num(opts['max-time']),
    memory: num(opts.memory),
    rest: num(opts.rest),
    stopWhenEmpty: opts['stop-when-empty'],
    stopWhenEmptyFor: num(opts['stop-when-empty-for']),
    killOnTimeout: opts['no-kill-on-timeout'] ? false : undefined,
  })

  const line = (name, status, extra = '') => {
    const left = `  ${gray(stamp(Date.now()))} ${name} `
    const right = ` ${extra}${status}`
    const width = Math.max(4, (process.stdout.columns || 80) - visible(left) - visible(right))
    console.log(left + gray('.'.repeat(width)) + right)
  }

  queue.on('warning', (message) => console.warn(yellow(`  WARN  ${message}`)))
  queue.on('worker:error', (error) => console.error(red(`  ERROR ${error.stack ?? error}`)))
  queue.on('worker:starting', ({ queues, options }) =>
    console.log(`\n  Processing jobs from [${queues.join(', ')}]${options.concurrency > 1 ? ` with concurrency ${options.concurrency}` : ''}.\n`),
  )
  queue.on('job:processing', ({ name }) => line(name, yellow('RUNNING')))
  queue.on('job:processed', ({ name, duration, job }) => {
    if (job.hasFailed() || job.isReleased()) return
    line(name, green('DONE'), gray(`${duration.toFixed(1)}ms `))
  })
  queue.on('job:released', ({ name, delay }) => line(name, gray('RELEASED'), gray(`in ${delay}s `)))
  queue.on('job:released-after-exception', ({ name, delay, error }) => line(name, yellow('RETRY'), gray(`${error.message} · in ${delay}s `)))
  queue.on('job:debounced', ({ name }) => line(name, gray('DEBOUNCED')))
  queue.on('job:timed-out', ({ name, timeout }) => line(name, red('TIMED OUT'), gray(`after ${timeout}s `)))
  queue.on('job:failed', ({ name, error }) => line(name, red('FAIL'), gray(`${error.message} `)))
  queue.on('worker:queue-paused', ({ queue: name }) => console.log(yellow(`  Queue "${name}" is paused.`)))
  queue.on('worker:queue-resumed', ({ queue: name }) => console.log(green(`  Queue "${name}" resumed.`)))
  queue.on('worker:stopping', ({ processed, reason, status }) =>
    console.log(`\n  Worker stopping (${reason}, exit ${status}) after ${processed} job(s).\n`),
  )

  let signals = 0
  const onSignal = () => {
    if (++signals > 1) process.exit(1)
    console.log(gray('\n  Finishing current jobs. Press Ctrl+C again to force quit.'))
    worker.stop('interrupted')
  }
  process.on('SIGINT', onSignal)
  process.on('SIGTERM', onSignal)

  const { status } = await worker.daemon()
  return status
}

async function loadQueue(file) {
  const path = resolve(file)
  if (!existsSync(path)) {
    console.error(red(`Could not find ${file}.`))
    console.error(`Create it and export your queue:\n\n  export default createQueue({ jobs: [SendEmail] })\n`)
    process.exit(1)
  }
  const mod = await import(pathToFileURL(path).href)
  const queue = mod.default ?? mod.queue
  if (!(queue instanceof Queue) && typeof queue?.worker !== 'function') {
    console.error(red(`${file} must export a queue as its default export.`))
    process.exit(1)
  }
  return queue
}

function stamp(ms) {
  return new Date(ms).toISOString().replace('T', ' ').slice(0, 19)
}

function visible(text) {
  return text.replace(/\x1b\[\d+m/g, '').length
}

function safeParse(json) {
  try {
    return JSON.parse(json)
  } catch {
    return null
  }
}
