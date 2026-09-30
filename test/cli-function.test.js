// Command-line tests with Job.define() jobs. The tests live in fixtures/cli-suite.js.
process.env.LITEQUEUE_TEST_STYLE = 'function'
await import('./fixtures/cli-suite.js')
