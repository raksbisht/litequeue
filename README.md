# litequeue

Simple background jobs for Node.js.

Retries, backoff, timeouts, unique jobs, middleware and chains, stored in **SQLite with zero dependencies**. No Redis to install, no server to run.

> **Simple projects on a single server:** use the default SQLite setup, with nothing else to configure. Support for MySQL, Postgres and Redis is coming soon.

Requires Node.js 22.13 or newer.

## Quick start

```
npm install @raksbisht/litequeue
```

**1. Write a job.** Put everything in one object. Throw to retry.

```js
// jobs/SendInvoice.js
import { Job } from '@raksbisht/litequeue'

export const SendInvoice = Job.define({
  name: 'send-invoice',
  queue: 'emails',
  tries: 5,
  backoff: [10, 60, 300],
  timeout: 30,
  handle: async (data, job) => {
    const invoice = await Invoice.find(data.invoiceId)
    await mailer.send(invoice, { signal: job.signal })
  },
  failed: async (error, job) => {
    await alertTeam(`Invoice ${job.data.invoiceId} could not be sent: ${error.message}`)
  },
})

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// export class SendInvoice extends Job {
//   static queue = 'emails'
//   static tries = 5
//   static backoff = [10, 60, 300]
//   static timeout = 30
//
//   async handle() {
//     const invoice = await Invoice.find(this.data.invoiceId)
//     await mailer.send(invoice, { signal: this.signal })
//   }
//
//   async failed(error) {
//     await alertTeam(`Invoice ${this.data.invoiceId} could not be sent: ${error.message}`)
//   }
// }
```

**2. Create the queue** in `litequeue.config.js`. The CLI loads this file.

```js
// litequeue.config.js
import { createQueue } from '@raksbisht/litequeue'
import { SendInvoice } from './jobs/SendInvoice.js'

export default createQueue({
  driver: 'sqlite',        // default
  path: 'queue.sqlite',    // default
  retryAfter: 90,          // seconds before a stuck job is retried
  jobs: [SendInvoice],
})
```

**3. Dispatch from your app.**

```js
import './litequeue.config.js'
import { SendInvoice } from './jobs/SendInvoice.js'

await SendInvoice.dispatch({ invoiceId: 42 })
await SendInvoice.dispatch({ invoiceId: 43 }).onQueue('high').delay(60)
```

**4. Run a worker.**

```
npx litequeue work --queue high,emails
```

Dispatching only saves the job to the queue. The worker is a separate process that picks jobs up and runs them, so nothing runs until it is started. Run it from the same project as your app (same code, same `queue.sqlite` file). If no worker is running, jobs wait in the queue.

## Job options

All times are in seconds. Options you leave out fall back to the worker's options. Pass them to `Job.define()` (`tries: 3`). In a class, they are `static` fields (`static tries = 3`).

| Option | What it does |
| --- | --- |
| `queue` | Queue name to push to. Default `'default'`. |
| `tries` | Max attempts. `0` means unlimited. Worker default is `1`. |
| `backoff` | Delay before a retry: a number, or an array per attempt like `[10, 60, 300]`. The last value repeats. |
| `timeout` | Seconds before the attempt times out. `0` means no timeout. Worker default is `60`. |
| `failOnTimeout` | Fail on the first timeout instead of retrying. |
| `maxExceptions` | Fail after this many thrown errors (timeouts count), even with attempts left. Releases don't count. |
| `delay` | Default delay for every dispatch. |
| `unique`, `uniqueFor`, `uniqueUntilProcessing` | Only one copy per `uniqueId` can be queued. `uniqueFor: 0` keeps the lock until the job finishes. |
| `debounceFor`, `maxDebounceWait` | Wait before running; if the job is dispatched again meanwhile, only the newest runs. |
| `name` | Name stored in the queue. Required in `Job.define()`. A class uses `static jobName`, or its class name. |
| `retryUntil` | A function that returns a `Date`. The job keeps retrying until then, ignoring `tries`. Evaluated at dispatch. |

Your `handle` function gets `(data, job)`. `job` has `job.attempts()`, `job.release(seconds)`, `job.fail(error)`, `job.delete()` and `job.signal` (an `AbortSignal` that fires on timeout). In a class, the same helpers are on `this`, and the data is `this.data`.

## Workers

```
litequeue work [options]

  --queue high,default         Queues to work, highest priority first
  --concurrency 5              Jobs to run at the same time in this process
  --sleep 3                    Seconds to wait when the queues are empty
  --tries 1                    Attempts for jobs that don't set tries
  --timeout 60                 Timeout for jobs that don't set one
  --config file.js             Where your queue is exported (default litequeue.config.js)
```

Run `npx litequeue work --help` to see every option. A worker stops (exit code `12`) when its memory passes 128 MB, so your process manager restarts it. Change the limit with `--memory <mb>`.

Exit codes: `0` for normal stops, `12` for the memory limit, `1` after a timeout. Workers shut down gracefully on `SIGINT`/`SIGTERM`. Run them under a process manager (systemd, PM2, Supervisor, Docker) so they come back after they exit.

From code: `await queue.work({ queue: 'high,default', concurrency: 5 })`.

### Deploying, pausing, inspecting

```
npx litequeue restart            # workers exit after their current job and reload your code
npx litequeue pause emails       # workers skip this queue (add --for 600 to pause for 10 minutes)
npx litequeue resume emails
npx litequeue size               # pending, delayed and reserved jobs per queue
```

## Failed jobs

A job fails for good when it runs out of tries, passes its `retryUntil` date, hits `maxExceptions`, times out with `failOnTimeout`, or calls `job.fail()`. It moves to failed jobs and its `failed` function runs.

```
npx litequeue failed          # list them
npx litequeue retry 12 15     # push some back (attempts reset, retryUntil refreshed)
npx litequeue retry all
npx litequeue forget 12
npx litequeue flush
```

## Unique jobs

```js
const RefundOrder = Job.define({
  name: 'refund-order',
  unique: true,
  uniqueId: (job) => String(job.data.orderId),
  handle: async (data) => { /* ... */ },
})

await RefundOrder.dispatch({ orderId: 42 }) // returns the job id
await RefundOrder.dispatch({ orderId: 42 }) // returns null: already queued

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// class RefundOrder extends Job {
//   static unique = true
//
//   uniqueId() {
//     return String(this.data.orderId)
//   }
//
//   async handle() { /* ... */ }
// }
```

The lock is released when the job finishes or fails for good, not when it's released for a retry. With `uniqueUntilProcessing: true`, it's released as soon as the job starts.

## Debounced jobs

Useful when the same event fires in bursts, like reindexing a user's search data after every edit.

```js
const ReindexUser = Job.define({
  name: 'reindex-user',
  debounceFor: 30,      // wait 30s after the last dispatch
  maxDebounceWait: 300, // but never wait more than 5 minutes in total
  debounceId: (job) => String(job.data.userId),
  handle: async (data) => { /* ... */ },
})

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// class ReindexUser extends Job {
//   static debounceFor = 30
//   static maxDebounceWait = 300
//
//   debounceId() {
//     return String(this.data.userId)
//   }
//
//   async handle() { /* ... */ }
// }
```

Every dispatch is queued, but when a job runs, it checks whether a newer one with the same `debounceId` was dispatched. If so, it deletes itself (`job:debounced`) and lets the newest one do the work.

## Middleware

```js
import { Job, Limit, skipWhen, withoutOverlapping, throttlesExceptions, rateLimited, failOnException } from '@raksbisht/litequeue'

const SyncAccount = Job.define({
  name: 'sync-account',
  tries: 0,
  retryUntil: () => new Date(Date.now() + 60 * 60 * 1000),
  middleware: (job) => [
    skipWhen(() => job.data.dryRun),
    withoutOverlapping(job.data.accountId).releaseAfter(10).expireAfter(180),
    throttlesExceptions(5, 300).by('crm-api').backoff(1),
    rateLimited('crm'),
    failOnException([AuthorizationError]),
  ],
  handle: async (data) => { /* ... */ },
})

queue.limiter('crm', (job) => Limit.perMinute(60).by(job.data.accountId))

// ---------------------------------------------------------------
// For class lovers, the same job as a class (works the same way):
//
// import { WithoutOverlapping, ThrottlesExceptions, RateLimited, Skip, FailOnException } from '@raksbisht/litequeue'
//
// class SyncAccount extends Job {
//   static tries = 0
//
//   retryUntil() {
//     return new Date(Date.now() + 60 * 60 * 1000)
//   }
//
//   middleware() {
//     return [
//       Skip.when(() => this.data.dryRun),
//       new WithoutOverlapping(this.data.accountId).releaseAfter(10).expireAfter(180),
//       new ThrottlesExceptions(5, 300).by('crm-api').backoff(1),
//       new RateLimited('crm'),
//       new FailOnException([AuthorizationError]),
//     ]
//   }
//
//   async handle() { /* ... */ }
// }
```

Details that trip people up: `WithoutOverlapping` defaults to `releaseAfter(0)` and a lock that never expires, so set `expireAfter()` in case a worker dies holding it. `ThrottlesExceptions` catches errors and releases the job instead of counting exceptions, and its `backoff()` is in minutes. Released jobs use up attempts, so pair these with `retryUntil` or `tries: 0`.

Your own middleware can be a function `(job, next) => ...` or an object with `handle(job, next)`. Also available: `skipUnless`, `releaseWhen` and `releaseUnless`. The classes (`new WithoutOverlapping(...)` and so on) still work.

## Chains

```js
await queue.chain([
  ProcessVideo.with({ id: 7 }),
  GenerateThumbnails.with({ id: 7 }),
  NotifyUploader.with({ id: 7 }),
])


// ---------------------------------------------------------------
// With class jobs, use `new` (or `.with()`, which works on classes too):
//
// await queue.chain([
//   new ProcessVideo({ id: 7 }),
//   new GenerateThumbnails({ id: 7 }),
//   new NotifyUploader({ id: 7 }),
// ])
```

Each job runs after the previous one succeeds. If one fails for good, the rest don't run.

## Drivers

| Driver | Use it for |
| --- | --- |
| `sqlite` (default) | Real apps on one machine. Safe with several worker processes on the same file. |
| `memory` | Tests and scripts. Lost when the process exits. |
| `sync` | Runs jobs right away when dispatched and throws errors to the caller. |

To add a driver, extend `Driver` (`src/drivers/Driver.js` documents every method) and register it with `Queue.extend('postgres', (options) => new PostgresDriver(options))`. `src/drivers/MemoryDriver.js` is a short, complete reference. The one hard rule: `pop()` must be atomic, so two workers never reserve the same job.

## Events

```js
queue.on('job:processed', ({ name, duration }) => metrics.timing(name, duration))
queue.on('job:failed', ({ name, error }) => sentry.captureException(error))
```

`job:queued`, `job:unique-skipped`, `job:processing`, `job:processed`, `job:exception-occurred`, `job:released-after-exception`, `job:released`, `job:failed`, `job:timed-out`, `job:debounced`, `job:attempted`, `worker:starting`, `worker:stopping`, `worker:idle`, `worker:error`, `worker:queue-paused`, `worker:queue-resumed`, `queue:paused`, `queue:resumed`, `warning`.

## Things worth knowing

**Keep `retryAfter` a few seconds longer than your longest timeout.** Otherwise a second worker picks up a job while the first is still running it. The worker warns you at startup.

**Pass `job.signal` to anything that accepts one** (`fetch`, database drivers, child processes) so a timed-out job actually stops.

**Pass IDs, not objects.** `data` is stored as JSON. Load fresh records inside `handle`. Dates become strings, functions are dropped, and a `BigInt` throws.

**Make jobs safe to run twice.** A worker can die mid-job, and the retry repeats the work. Check whether it's already done first.

## Credits

litequeue is a port of the queue component of the [Laravel framework](https://github.com/laravel/framework) by Taylor Otwell and contributors (MIT license). Many of the ideas behind it are explained in *Laravel Queues in Action* by Mohamed Said.

## License

MIT
