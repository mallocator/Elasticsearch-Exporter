var util = require('util');
var async = require('async');
var log = require('./log.js');
var args = require('./args.js');
var options = require('./options.js');
var drivers = require('./drivers.js');
var cluster = require('./cluster.js');

/**
 * The environment object that will be passed on the the drivers for all operations.
 * The properties available here are only a minimum set and can be extended by each driver however best suited.
 *
 * @constructor
 */
function Environment() {
    this.options = {
        log: {
            count: false
        }
    };
    this.statistics = {
        source: {
            version: "0.0",
            status: "Red",
            docs: {
                processed: 0,
                total: 0
            },
            retries: 0
        },
        target: {
            version: "0.0",
            status: "Red",
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

exports.memUsage = null;
exports.env = null;
exports.status = "ready";
exports.queue = [];

exports.handleUncaughtExceptions = function (e) {
    log.error('Caught exception in Main process: %s'.bold, e.toString());
    if (e instanceof Error) {
        log.info(e.stack);
    }
    log.die(2);
};

exports.read_options = function (callback) {
    options.read(function (optionTree) {
        if (!optionTree) {
            callback('options have been returned empty');
        } else {
            callback(null, optionTree);
        }
    });
};

exports.verify_options = function (callback, results) {
    log.debug('Passing options to drivers for verification');
    options.verify(results.read_options, function (err) {
        exports.env = new Environment();
        exports.env.options = results.read_options;
        callback(err);
    });
};

exports.reset_source = function (callback) {
    async.retry(exports.env.options.errors.retry, function (callback) {
        log.debug('Resetting source driver to begin operations');
        var source = drivers.get(exports.env.options.drivers.source).driver;
        source.reset(exports.env, function (err) {
            callback(err);
        });
    }, callback);
};

exports.reset_target = function (callback) {
    async.retry(exports.env.options.errors.retry, function (callback) {
        log.debug('Resetting target driver to begin operations');
        var target = drivers.get(exports.env.options.drivers.target).driver;
        target.reset(exports.env, function (err) {
            callback(err);
        });
    }, callback);
};

exports.get_source_statistics = function (callback) {
    async.retry(exports.env.options.errors.retry, function (callback) {
        log.debug('Fetching source statistics before starting run');
        var source = drivers.get(exports.env.options.drivers.source).driver;
        source.getSourceStats(exports.env, function (err, sourceStats) {
            exports.env.statistics.source = util._extend(exports.env.statistics.source, sourceStats);
            callback(err);
        });
    }, callback);
};

exports.get_target_statistics = function (callback) {
    async.retry(exports.env.options.errors.retry, function (callback) {
        log.debug('Fetching target statistics before starting run');
        var target = drivers.get(exports.env.options.drivers.target).driver;
        target.getTargetStats(exports.env, function (err, targetStats) {
            exports.env.statistics.target = util._extend(exports.env.statistics.target, targetStats);
            callback(err);
        });
    }, callback);
};

exports.check_source_health = function (callback) {
    log.debug("Checking source database health");
    if (exports.env.statistics.source.status == "red") {
        callback("The source database is experiencing and error and cannot proceed");
    }
    else if (exports.env.statistics.source.docs.total === 0) {
        callback("The source driver has not reported any documents that can be exported. Not exporting.");
    } else {
        callback(null);
    }
};

exports.check_target_health = function (callback) {
    log.debug("Checking target database health");
    if (exports.env.statistics.target.status == "red") {
        callback("The target database is experiencing and error and cannot proceed");
    } else {
        callback(null);
    }
};

exports.get_metadata = function (callback) {
    // TODO validate metadata format
    async.retry(exports.env.options.errors.retry, function (callback) {
        if (exports.env.options.mapping) {
            log.debug("Using mapping overridden through options");
            callback(null, exports.env.options.mapping);
        } else {
            log.debug("Fetching mapping from source database");
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.getMeta(exports.env, callback);
        }
    }, callback);
};

exports.store_metadata = function (callback, results) {
    async.retry(exports.env.options.errors.retry, function (callback) {
        if (!exports.env.options.testRun) {
            var target = drivers.get(exports.env.options.drivers.target).driver;
            var metadata = results.get_metadata;
            target.putMeta(exports.env, metadata, function (err) {
                log.info("Mapping on target database is now ready");
                callback(err);
            });
        } else {
            log.info("Not storing meta data on target database because we're doing a test run.");
            callback();
        }
    }, callback);
};

exports.transfer_data = function (callback) {
    var processed = 0;
    var pointer = 0;
    var step = exports.env.options.run.step;
    var total = exports.env.statistics.docs.total;
    var sourceConcurrent = drivers.get(exports.env.options.drivers.source).threadsafe;
    var targetConcurrent = drivers.get(exports.env.options.drivers.target).threadsafe;
    var concurrency = sourceConcurrent && targetConcurrent ? exports.env.options.run.concurrency : 1;
    var pump = cluster.run(exports.env, concurrency);
    pump.onWorkDone(function(processedDocs) {
        processed += processedDocs;
        exports.env.statistics.docs.processed = processed;
        log.status('Processed %s of %s entries (%s%%)', processed, total, Math.round(processed / total * 100));
    });
    pump.onEnd(function() {
        exports.status = "done";
        log.info('Processed %s entries (100%%)', total);
        callback();
    });
    // TODO check if this really terminates or if the async.until() below will still run
    pump.onError(callback);

    exports.status = "running";
    log.info("Starting data export");

    async.until(function() {
        return pointer >= total;
    }, function(callback) {
        pump.work(pointer, step, callback);
        pointer += step;
    });
};

/**
 * This function ties everything together and performs all the operations from reading options to the actual export.
 *
 * @param callback will be called with an optional err message at the end of the export
 */
exports.run = function (callback) {
    async.auto({
        read_options: exports.read_options,
        verify_options: ["read_options", exports.verify_options],
        reset_source: ["verify_options", exports.reset_source],
        reset_target: ["verify_options", exports.verify_options],
        get_source_statistics: ["reset_source", exports.get_source_statistics],
        get_target_statistics: ["reset_target", exports.get_target_statistics],
        check_source_health: ["get_source_statistics", exports.check_source_health],
        check_target_health: ["get_target_statistics", exports.check_target_health],
        get_metadata: ["check_source_health", exports.get_metadata],
        store_metadata: ["check_target_health", "get_metadata", exports.store_metadata],
        transfer_data: ["check_source_health", "store_metadata", exports.transfer_data]
    }, callback);
};

if (require.main === module) {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    process.on('exit', function() {
        args.printSummary(exports.env.statistics);
    });
    exports.run(function(err) {
        if (err) {
            if (isNaN(err)) {
                log.error("The driver reported an error:", err);
                log.die(4);
            } else {
                log.die(err);
            }
        }
        process.exit(0);
    });
}