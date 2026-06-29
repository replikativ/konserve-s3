// Karma runner for the :ci (shadow :karma) build — browser-side tests in
// headless Chrome. Mirrors konserve's own karma.conf.js.
//
//   npx shadow-cljs release ci   # build target/ci.js
//   npx karma start --single-run # run it in ChromeHeadless
//
// Only the network-free tests run here (parser + shared storage helpers); the
// compliance suite is node-only (it needs process.env + a reachable bucket) and
// is excluded from the browser builds in shadow-cljs.edn.
module.exports = function (config) {
    config.set({
        browsers: ['ChromeHeadless'],
        basePath: 'target',
        files: ['ci.js'],
        frameworks: ['cljs-test'],
        plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
        colors: true,
        logLevel: config.LOG_INFO,
        client: {
            args: ["shadow.test.karma.init"],
            singleRun: true
        }
    })
};
