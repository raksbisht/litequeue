# jobline

Laravel-style background jobs for Node.js.

A Laravel-inspired queue for Node.js. Job classes, retries, backoff, timeouts, unique and debounced jobs, job middleware, chains, pausing and failed-job handling. Storage is a pluggable driver, and the default is **SQLite with zero dependencies** (it uses Node's built-in `node:sqlite`). No Redis to install, no server to run.

```js
import { Job } from 'jobline'

export class SendWelcomeEmail extends Job {
  static tries = 3
  static backoff = [10, 60]

  async handle() {
    await mailer.send(this.data.email, 'Welcome!', { signal: this.signal })
  }
}

await SendWelcomeEmail.dispatch({ email: 'ada@example.com' })
```

```
$ npx jobline work

  Processing jobs from [default].

  2026-09-29 14:02:11 SendWelcomeEmail ........................ RUNNING
  2026-09-29 14:02:11 SendWelcomeEmail .................. 12.4ms DONE
```

Requires Node.js 22.13 or newer.

## Install

```
npm install jobline
```

## Quick start

**1. Write a job.** Put your logic in `handle()`. Throw to retry.

```js
// jobs/SendInvoice.js
import { Job } from 'jobline'

export class SendInvoice extends Job {
  static queue = 'emails'
  static tries = 5
  static backoff = [10, 60, 300]
  static timeout = 30

  async handle() {
    const invoice = await Invoice.find(this.data.invoiceId)
    await mailer.send(invoice, { signal: this.signal })
  }

  async failed(error) {
    await alertTeam(`Invoice ${this.data.invoiceId} could not be sent: ${error.message}`)
  }
}
```

**2. Create the queue** in `jobline.config.js`. The CLI loads this file.

```js
// jobline.config.js
import { createQueue } from 'jobline'
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
import './jobline.config.js'
import { SendInvoice } from './jobs/SendInvoice.js'

await SendInvoice.dispatch({ invoiceId: 42 })
await SendInvoice.dispatch({ invoiceId: 43 }).onQueue('high').delay(60)
```

**4. Run a worker.**

```
npx jobline work --queue high,emails,default
```

## Coming from Laravel?

jobline is modeled on Laravel's queue, so the concepts, option names, middleware and CLI commands carry over (`queue:work` becomes `jobline work`, `$tries` becomes `static tries`, and so on). A few things differ:

- **Timeouts.** Node can't interrupt a running function, so jobline aborts `this.signal` and stops the worker with exit code 1 so your process manager restarts it. Pass `--no-kill-on-timeout` to treat timeouts as ordinary errors.
- **One store for everything.** Locks, rate limits and pause/restart signals live in the same driver as the jobs, so one SQLite file is enough.
- **Plain JSON, no closures.** Jobs carry plain JSON in `this.data`.
- **Concurrency.** Use `--concurrency` to run several jobs in one process.
- **Not ported yet:** batches, `queue:monitor`, `queue:prune-failed`, encrypted jobs, and the Redis, SQS and Beanstalkd drivers.

## Job options

All times are in seconds. Options you leave out fall back to the worker's options.

| Option | What it does |
| --- | --- |
| `static queue` | Queue name to push to. Default `'default'`. |
| `static tries` | Max attempts. `0` means unlimited. Worker default is `1`. |
| `static backoff` | Delay before a retry: a number, or an array per attempt like `[10, 60, 300]`. The last value repeats. |
| `static timeout` | Seconds before the attempt times out. `0` means no timeout. Worker default is `60`. |
| `static failOnTimeout` | Fail on the first timeout instead of retrying. |
| `static maxExceptions` | Fail after this many thrown errors (timeouts count), even with attempts left. Releases don't count. |
| `static delay` | Default delay for every dispatch. |
| `static unique`, `uniqueFor`, `uniqueUntilProcessing` | Only one copy per `uniqueId()` can be queued. `uniqueFor = 0` keeps the lock until the job finishes. |
| `static debounceFor`, `maxDebounceWait` | Wait before running; if the job is dispatched again meanwhile, only the newest runs. |
| `static jobName` | Name stored in the queue. Defaults to the class name; set it if you minify code. |
| `retryUntil()` | Return a `Date`. The job keeps retrying until then, ignoring `tries`. Evaluated at dispatch. |

Inside `handle()` you have `this.data`, `this.attempts()`, `this.release(seconds)`, `this.fail(error)`, `this.delete()` and `this.signal` (an `AbortSignal` that fires on timeout).

## Workers

```
jobline work [options]

  --queue high,default         Queues to work, highest priority first
  --concurrency 5              Jobs to run at the same time in this process
  --sleep 3                    Seconds to wait when the queues are empty
  --rest 0                     Seconds to pause between jobs
  --tries 1                    Attempts for jobs that don't set tries
  --backoff 10,60              Delay before retries
  --timeout 60                 Timeout for jobs that don't set one
  --memory 128                 Exit when memory use passes this many MB
  --max-jobs 1000              Exit after this many jobs
  --max-time 3600              Exit after this many seconds
  --stop-when-empty            Exit when the queues are empty
  --stop-when-empty-for 60     Exit after the queues have been empty this long
  --no-kill-on-timeout         Treat timeouts as normal errors
  --config file.js             Where your queue is exported (default jobline.config.js)
```

Exit codes: `0` for normal stops, `12` for the memory limit, `1` after a timeout. Workers shut down gracefully on `SIGINT`/`SIGTERM`. Run them under a process manager (systemd, PM2, Supervisor, Docker) so they come back after they exit.

From code: `await queue.work({ queue: 'high,default', concurrency: 5 })`.

### Deploying, pausing, inspecting

```
npx jobline restart            # workers exit after their current job and reload your code
npx jobline pause emails       # workers skip this queue (add --for 600 to pause for 10 minutes)
npx jobline resume emails
npx jobline size               # pending, delayed and reserved jobs per queue
```

## Failed jobs

A job fails for good when it runs out of tries, passes its `retryUntil()` date, hits `maxExceptions`, times out with `failOnTimeout`, or calls `this.fail()`. It moves to failed jobs and its `failed(error)` method runs.

```
npx jobline failed          # list them
npx jobline retry 12 15     # push some back (attempts reset, retryUntil refreshed)
npx jobline retry all
npx jobline forget 12
npx jobline flush
```

## Unique jobs

```js
class RefundOrder extends Job {
  static unique = true

  uniqueId() {
    return String(this.data.orderId)
  }
}

await RefundOrder.dispatch({ orderId: 42 }) // returns the job id
await RefundOrder.dispatch({ orderId: 42 }) // returns null: already queued
```

The lock is released when the job finishes or fails for good, not when it's released for a retry. With `uniqueUntilProcessing`, it's released as soon as the job starts.

## Debounced jobs

Useful when the same event fires in bursts, like reindexing a user's search data after every edit.

```js
class ReindexUser extends Job {
  static debounceFor = 30      // wait 30s after the last dispatch
  static maxDebounceWait = 300 // but never wait more than 5 minutes in total

  debounceId() {
    return String(this.data.userId)
  }
}
```

Every dispatch is queued, but when a job runs, it checks whether a newer one with the same `debounceId()` was dispatched. If so, it deletes itself (`job:debounced`) and lets the newest one do the work.

## Middleware

```js
import { Job, WithoutOverlapping, ThrottlesExceptions, RateLimited, Limit, Skip, Release, FailOnException } from 'jobline'

class SyncAccount extends Job {
  static tries = 0

  retryUntil() {
    return new Date(Date.now() + 60 * 60 * 1000)
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
}

queue.limiter('crm', (job) => Limit.perMinute(60).by(job.data.accountId))
```

Details that trip people up: `WithoutOverlapping` defaults to `releaseAfter(0)` and a lock that never expires, so set `expireAfter()` in case a worker dies holding it. `ThrottlesExceptions` catches errors and releases the job instead of counting exceptions, and its `backoff()` is in minutes. Released jobs use up attempts, so pair these with `retryUntil()` or `tries = 0`.

Your own middleware can be a function `(job, next) => ...` or an object with `handle(job, next)`.

## Chains

```js
await queue.chain([
  new ProcessVideo({ id: 7 }),
  new GenerateThumbnails({ id: 7 }),
  new NotifyUploader({ id: 7 }),
])
```

## Drivers

| Driver | Use it for |
| --- | --- |
| `sqlite` (default) | Real apps on one machine. Safe with several worker processes on the same file. Works like Laravel's `database` driver. |
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

**Pass `this.signal` to anything that accepts one** (`fetch`, database drivers, child processes) so a timed-out job actually stops.

**Pass IDs, not objects.** `this.data` is stored as JSON. Load fresh records inside `handle()`.

**Make jobs safe to run twice.** A worker can die mid-job, and the retry repeats the work. Check whether it's already done first.

## Credits

jobline is a port of the queue component of the [Laravel framework](https://github.com/laravel/framework) by Taylor Otwell and contributors (MIT license). Many of the ideas behind it are explained in *Laravel Queues in Action* by Mohamed Said.

## License

MIT
