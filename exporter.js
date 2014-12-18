exports.handleUncaughtExceptions = function (e) {
    console.log('Caught exception in Main process: %s'.bold, e.toString());
    if (e instanceof Error) {
        console.log(e.stack);
    }
    process.exit(99);
};

exports.export = function () {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    exports.opts = require('./options.js').opts();
    process.on('exit', exports.printSummary);
};

if (require.main === module) {
    exports.export();
}