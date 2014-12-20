// Process exit signals:
// 0 - Operation successful
// 1 - No documents found to export
// 2 - Uncaught Exception
// 3 - invalid options specified
// 4 - source or target status = invalid / red / not ready
// 10 - driver interface is not implemented properly
// 11 - driver doesn't exist
// 131-254 - reserved for driver specific problems

var util = require('util');
var async = require('async');
var log = require('./log.js');
var options = require('./options.js');
var drivers = require('./drivers.js');

/**
 * The environment object that will be passed on the the drivers for all operations.
 * The properties available here are only a minimum set and can be extended by each driver however best suited.
 *
 * @constructor
 */
function Environment() {
    this.options = {};
    this.statistics = {
        source: {
            version: "0.0",
            status: "red",
            docs: 0,
            retries: 0
        },
        target: {
            version: "0.0",
            status: "red",
            docs: 0,
            retries: 0
        },
        numCalls: 0,
        hits: {
            fetched: 0,
            processed: 0,
            total: 0
        },
        memory: {
            peak: 0,
            ratio: 0
        }
    };
}

exports.env = new Environment();

exports.handleUncaughtExceptions = function (e) {
    console.log('Caught exception in Main process: %s'.bold, e.toString());
    if (e instanceof Error) {
        console.log(e.stack);
    }
    process.exit(99);
};

exports.printSummary = function() {
    // TODO make this more pluggable with descriptions added from the drivers
    log.info('Number of calls:\t%s', exports.env.statistics.numCalls);
    if (exports.opts && exports.env.statistics.sourceStats && exports.env.statistics.sourceStats.retries) {
        log.info('Retries to source:\t%s', exports.env.statistics.sourceStats.retries);
    }
    if (exports.opts && exports.env.statistics.targetStats && exports.env.statistics.targetStats.retries) {
        log.info('Retries to target:\t%s', exports.opts.targetStats.retries);
    }
    log.info('Fetched Entries:\t%s documents', exports.env.statistics.hits.fetched);
    log.info('Processed Entries:\t%s documents', exports.env.statistics.hits.processed);
    log.info('Source DB Size:\t\t%s documents', exports.env.statistics.hits.total);
    if (exports.opts && exports.env.statistics.sourceStats && exports.env.statistics.sourceStats.count) {
        log.info('Unique Entries:\t\t%s documents', exports.env.statistics.sourceStats.count.uniques);
        log.info('Duplicate Entries:\t%s documents', exports.env.statistics.sourceStats.count.duplicates);
    }
    if (exports.env.statistics.memory.peak) {
        log.info('Peak Memory Used:\t%s bytes (%s%%)', exports.env.statistics.memory.peak, Math.round(exports.memoryRatio * 100));
        log.info('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
    }
};

/**
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 *
 * @param {function} callback Function to be called as soon as memory is available again.
 */
exports.waitOnTargetDriver = function (callback) {
    if (global.gc && exports.getMemoryStats() > exports.opts.memoryLimit) {
        global.gc();
        setTimeout(function () {
            exports.waitOnTargetDriver(callback);
        }, 100);
    }
    else {
        callback();
    }
};

exports.run = function () {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    process.on('exit', exports.printSummary);

    async.auto({
        read_options: function(callback) {
            log.debug('Reading options');
            options.read(function(optionTree) {
                callback(null, optionTree);
            });
        },
        verify_options: [ "read_options", function(callback, results) {
            log.debug('Passing options to drivers for verification');
            options.verify(results.read_options, function() {
                exports.env.options = results.read_options;
                callback();
            });
        }],
        reset_source: [ "verify_options", function(callback) {
            log.debug('Resetting source driver to begin operations');
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.reset(function( ){
                callback(null, source);
            });
        }],
        get_source_statistics: [ "reset_source", function(callback, results) {
            log.debug('Fetching source statistics before starting run');
            results.reset_source.getSourceStats(exports.env, function (sourceStats) {
                exports.env.statistics.source = util._extend(exports.env.statistics.source, sourceStats);
                callback();
            });
        }],
        reset_target: ["verify_options", function (callback) {
            log.debug('Resetting target driver to begin operations');
            var target = drivers.get(exports.env.options.drivers.target).driver;
            target.reset(function () {
                callback(null, target);
            });
        }],
        get_taget_statistics: [ "reset_target", function(callback, results) {
            log.debug('Fetching target statistics before starting run');
            results.reset_target.getTargetStats(exports.env, function (targetStats) {
                exports.env.statistics.target = util._extend(exports.env.statistics.target, targetStats);
                callback();
            });
        }]
    }, function() {
        console.log(exports.env);
    });
};

if (require.main === module) {
    exports.run();
}