'use strict';

var http = require('http');
var https = require('https');
var util = require('util');

var async = require('async');

var log = require('./log.js');
var args = require('./args.js');
var options = require('./options.js');
var drivers = require('./drivers.js');
var cluster = require('./cluster.js');

/**
 * An empty callback that doesn't pass on anything.
 * @callback emptyCb
 */

/**
 * A callback with support for receiving errors.
 * @callback errorCb
 * @param {string|string[]|Error|*} [err] The error to pass on or null if no error
 */

/**
 * @callback dataCb
 * @param {String|String[]|Error} [err]
 * @param {*} [data]
 */

/**
 * @typedef {Object} Statistics
 * @property {SourceInfo} source     General information about the source service
 * @property {TargetInfo} target     General information about the target service
 * @property {Object} hits           Statistical information about documents
 * @property {number} hits.processed Statistical information about how many documents have been processed so far
 * @property {number} hits.total     Statistical information about how many documents there are in total
 * @property {Object} memory         Statistical information about memory usage
 * @property {number} memory.peak    Statistical information about maximum memory usage
 * @property {number} memory.ratio   Statistical information about memory usage ratio
 */

/**
 * The environment object that will be passed on the the drivers for all operations.
 * The properties available here are only a minimum set and can be extended by each driver however best suited.
 *
 * @constructor
 * @property {Object} options                   Options that will be used to determine the export behavior
 * @property {Object} options.log               Options for log related operations
 * @property {Statistics} statistics            Statistics that are collected throughout the export process
 */
function Environment() {
    this.options = {
        log: {}
    };
    this.statistics = {
        source: {
            version: "0.0",
            status: "Red",
            docs: {
                total: 0
            }
        },
        target: {
            version: "0.0",
            status: "Red"
        },
        hits: {
            processed: 0,
            total: 0
        },
        memory: {
            peak: 0,
            ratio: 0
        }
    };
}

/**
 * Stores the latest information about the systems memory usage. Is only used if memory.limit option is set.
 * @type {Object}
 */
exports.memUsage = null;

/**
 * Once the exporter starts this is where the global environment object is stored.
 * @type {Environment}
 */
exports.env = null;

/**
 * Status object that keeps track of where we are in the export process.
 * @type {string}
 */
exports.status = "ready";

/**
 * Queued up documents to be processed as soon as the target is ready.
 * @type {Data[]}
 */
exports.queue = [];

/**
 * A catch all exception handler that will try to print something useful before crashing.
 *
 * @param e
 */
exports.handleUncaughtExceptions = e => {
    log.error('Caught exception in Main process: %s'.bold, util.format(e));
    e instanceof Error && log.info(e.stack);
    log.die(2);
};

/**
 * Reads the options from either command line or file.
 *
 * @param {function} callback
 */
exports.readOptions = callback => {
    options.read(optionTree => {
        if (!optionTree) {
            callback('options have been returned empty');
        } else {
            callback(null, optionTree);
        }
    });
};

/**
 * Allows each driver to verify if the options supplied are sufficient.
 * Once verified, the options are available in the environment.
 *
 * @param {errorCb} callback
 * @param results   The option tree from readOptions()
 */
exports.verifyOptions = (results, callback) => {
    log.debug('Passing options to drivers for verification');
    options.verify(results.readOptions, err => {
        if (err && !err.length) {
            err = null;
        }
        exports.env = new Environment();
        exports.env.options = results.readOptions;
        if (exports.env.options.network && exports.env.options.network.sockets) {
            https.globalAgent.maxSockets = http.globalAgent.maxSockets = exports.env.options.network.sockets;
        }
        callback(err);
    });
};

/**
 * Calls the reset function on the source driver to get it ready for execution.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.resetSource = (results, callback) => {
    async.retry(exports.env.options.errors.retry, callback => {
        log.debug('Resetting source driver to begin operations');
        let source = drivers.get(exports.env.options.drivers.source).driver;
        source.reset(exports.env, callback);
    }, callback);
};

/**
 * Calls the reset function on the target driver to get it ready for execution.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.resetTarget = (results, callback) => {
    async.retry(exports.env.options.errors.retry, callback => {
        log.debug('Resetting target driver to begin operations');
        let target = drivers.get(exports.env.options.drivers.target).driver;
        target.reset(exports.env, callback);
    }, callback);
};

/**
 * Retrieve some basic statistics and status information from the source that allows to verify it's ready.
 * The response includes an information about how many documents will be exported in total.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.getSourceStatistics = (results, callback) => {
    async.retry(exports.env.options.errors.retry, callback => {
        log.debug('Fetching source statistics before starting run');
        let source = drivers.get(exports.env.options.drivers.source).driver;
        source.getSourceStats(exports.env, (err, sourceStats) => {
            exports.env.statistics.source = util._extend(exports.env.statistics.source, sourceStats);
            callback(err);
        });
    }, callback);
};

/**
 * Retrieve some basic statistics and status information from the target that allows to verify it's ready.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.getTargetStatistics = (results, callback) => {
    async.retry(exports.env.options.errors.retry, callback => {
        log.debug('Fetching target statistics before starting run');
        let target = drivers.get(exports.env.options.drivers.target).driver;
        target.getTargetStats(exports.env, (err, targetStats) => {
            exports.env.statistics.target = util._extend(exports.env.statistics.target, targetStats);
            callback(err);
        });
    }, callback);
};

/**
 * Checks for some basic information such as if the source driver has any documents to be exported.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.checkSourceHealth = (results, callback) => {
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

/**
 * Checks for some basic information of the target driver.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.checkTargetHealth = (results, callback) => {
    log.debug("Checking target database health");
    if (exports.env.statistics.target.status == "red") {
        callback("The target database is experiencing and error and cannot proceed");
    } else {
        callback(null);
    }
};

/**
 * Calls the source driver to retrieve the metadata.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.getMetadata = (results, callback) => {
    if (!exports.env.options.run.mapping) {
        return callback();
    }
    async.retry(exports.env.options.errors.retry, callback => {
        if (exports.env.options.mapping) {
            log.debug("Using mapping overridden through options");
            callback(null, exports.env.options.mapping);
        } else {
            log.debug("Fetching mapping from source database");
            let source = drivers.get(exports.env.options.drivers.source).driver;
            source.getMeta(exports.env, callback);
        }
    // TODO validate metadata format
    }, callback);
};

/**
 * Send the retrieved metadata to the target driver to be stored.
 *
 * @param {Object} results   Results object from async() that holds the getMetadata response
 * @param {Metadata} results.getMetadata    The metadata from the source driver
 * @param {errorCb} callback
 */
exports.storeMetadata = (results, callback) => {
    if (!exports.env.options.run.mapping) {
        return callback();
    }
    async.retry(exports.env.options.errors.retry, callback => {
        if (exports.env.options.run.test) {
            log.info("Not storing meta data on target database because we're doing a test run.");
            return callback();
        }

        let target = drivers.get(exports.env.options.drivers.target).driver;
        let metadata = results.getMetadata;
        target.putMeta(exports.env, metadata, err => {
            if (err) {
                log.error(err);
            } else {
                log.info("Mapping on target database is now ready");
            }
            callback(err);
        });
    }, callback);
};

/**
 * Performs the actual transfer of data once all other functions have returned without any errors.
 *
 * @param {Object} results  Ignored results object from async
 * @param {errorCb} callback
 */
exports.transferData = (results, callback) => {
    if (!exports.env.options.run.data) {
        return callback();
    }
    let processed = 0;
    let pointer = 0;
    let total = exports.env.statistics.hits.total = exports.env.statistics.source.docs.total || 0;
    let step = Math.min(exports.env.options.run.step, total);
    let sourceConcurrent = drivers.get(exports.env.options.drivers.source).info.threadsafe;
    let targetConcurrent = drivers.get(exports.env.options.drivers.target).info.threadsafe;
    let concurrency = sourceConcurrent && targetConcurrent ? exports.env.options.run.concurrency : 1;
    if (!sourceConcurrent || !targetConcurrent) {
        log.debug('Concurrency has been disabled because at least one of the drivers doesn\'t support it');
    }
    let pump = cluster.run(exports.env, concurrency);
    pump.onWorkDone(processedDocs => {
        processed += processedDocs;
        exports.env.statistics.hits.processed = processed || 0;
        log.status('Processed %s of %s entries (%s%%)', processed, total, Math.round(processed / total * 100));
    });
    pump.onEnd(() => {
        exports.status = "done";
        log.clearStatus();
        log.info('Processed %s entries (100%%)', total);
        callback();
    });
    pump.onError(err => {
        processed = total;
        callback(err);
    });

    exports.status = "running";
    log.info("Starting data export");

    async.until(() => pointer >= total, callback => {
        pump.work(pointer, step, () => {
            pointer += step;
            callback();
        });
    }, err => {
        err && log.error(err);
        log.debug('Worker loop finished with %s of %s entries processed (%s%%)', processed, total, Math.round(processed / total * 100));
    });
};

/**
 * This function ties everything together and performs all the operations from reading options to the actual export.
 *
 * @param {errorCb} callback
 */
exports.run = callback => {
    async.auto({
        readOptions: exports.readOptions,
        verifyOptions: ["readOptions", exports.verifyOptions],
        resetSource: ["verifyOptions", exports.resetSource],
        resetTarget: ["verifyOptions", exports.resetTarget],
        getSourceStatistics: ["resetSource", exports.getSourceStatistics],
        getTargetStatistics: ["resetTarget", exports.getTargetStatistics],
        checkSourceHealth: ["getSourceStatistics", exports.checkSourceHealth],
        checkTargetHealth: ["getTargetStatistics", exports.checkTargetHealth],
        getMetadata: ["checkSourceHealth", exports.getMetadata],
        storeMetadata: ["checkTargetHealth", "getMetadata", exports.storeMetadata],
        transferData: ["storeMetadata", exports.transferData]
    }, callback);
};

if (require.main === module) {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    process.on('exit', () => exports.env && exports.env.statistics && args.printSummary(exports.env.statistics));
    exports.run(err => {
        if (err) {
            if (isNaN(err)) {
                log.error("The driver reported an error:", err);
                log.die(4);
            } else {
                log.die(err);
            }
        }
    });
}
