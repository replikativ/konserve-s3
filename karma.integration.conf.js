// Karma runner for the :integration build — the browser-compliance test in
// headless Chrome against a live S3 endpoint (MinIO in CI).
//
//   npx shadow-cljs release integration                 # build target/integration.js
//   npx karma start karma.integration.conf.js --single-run
//
// Unlike karma.conf.js (network-free), this drives real GET/PUT/DELETE/list
// against the bucket configured via goog-define in the test namespace. In CI
// (Docker, running as root) Chrome needs --no-sandbox, and the compliance suite
// makes many network round-trips, so the no-activity timeout is raised.
module.exports = function (config) {
    config.set({
        basePath: 'target',
        files: ['integration.js'],
        frameworks: ['cljs-test'],
        plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
        colors: true,
        logLevel: config.LOG_INFO,
        // Network compliance can idle between round-trips longer than the 10s
        // default; give it room without hanging CI forever.
        browserNoActivityTimeout: 120000,
        browsers: ['ChromeHeadlessNoSandbox'],
        customLaunchers: {
            ChromeHeadlessNoSandbox: {
                base: 'ChromeHeadless',
                flags: ['--no-sandbox', '--disable-gpu']
            }
        },
        client: {
            args: ["shadow.test.karma.init"],
            singleRun: true
        }
    })
};
