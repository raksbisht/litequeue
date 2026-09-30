// Command-line tests with class jobs. The tests live in fixtures/cli-suite.js.
process.env.LITEQUEUE_TEST_STYLE = 'class'
await import('./fixtures/cli-suite.js')
