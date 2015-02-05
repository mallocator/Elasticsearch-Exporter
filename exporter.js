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
    this.options = {
        log: {
            count: false
        }
    },
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

exports.printSummary = function() {
    if (!exports.env) {
        return;
    }
    log.info('Number of calls:\t%s', exports.env.statistics.numCalls);
    delete exports.env.statistics.numCalls;
    if (exports.opts && exports.env.statistics.source && exports.env.statistics.source.retries) {
        log.info('Retries to source:\t%s', exports.env.statistics.source.retries);
        delete exports.env.statistics.source.retries;
    }
    if (exports.opts && exports.env.statistics.target && exports.env.statistics.target.retries) {
        log.info('Retries to target:\t%s', exports.opts.target.retries);
        delete exports.opts.target.retries;
    }
    log.info('Fetched Entries:\t%s documents', exports.env.statistics.hits.fetched);
    delete exports.env.statistics.hits.fetched;
    log.info('Processed Entries:\t%s documents', exports.env.statistics.hits.processed);
    delete exports.env.statistics.hits.processed;
    log.info('Source DB Size:\t\t%s documents', exports.env.statistics.hits.total);
    delete exports.env.statistics.hits.total;
    if (exports.opts && exports.env.statistics.source && exports.env.statistics.source.count) {
        log.info('Unique Entries:\t\t%s documents', exports.env.statistics.source.count.uniques);
        delete exports.env.statistics.source.count.uniques;
        log.info('Duplicate Entries:\t%s documents', exports.env.statistics.source.count.duplicates);
        delete exports.env.statistics.source.count.duplicates;
    }
    if (exports.env.statistics.memory.peak) {
        log.info('Peak Memory Used:\t%s bytes (%s%%)', exports.env.statistics.memory.peak, Math.round(exports.memoryRatio * 100));
        delete exports.env.statistics.memory.peak;
        log.info('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
    }
    // TODO print remaining stats in general
    log.debug(exports.env.statistics);
};

/**
 * Returns the current used / available memory ratio.
 * Updates itself only every few milliseconds. Updates occur faster, when memory starts to run out.
 */
exports.getMemoryStats = function () {
    var nowObj = process.hrtime();
    var now = nowObj[0] * 1e9 + nowObj[1];
    var nextCheck = 0;
    if (exports.memUsage !== null) {
        nextCheck = Math.pow((exports.memUsage.heapTotal / exports.memUsage.heapUsed), 2) * 100000000;
    }
    if (exports.memUsage === null || exports.memUsage.lastUpdate + nextCheck < now) {
        exports.memUsage = process.memoryUsage();
        exports.memUsage.lastUpdate = now;
        exports.memUsage.ratio = exports.memUsage.heapUsed / exports.memUsage.heapTotal;
        if (exports.memUsage.heapUsed > exports.env.statistics.memory.peak) {
            exports.env.statistics.memory.peak = exports.memUsage.heapUsed;
            exports.env.statistics.memory.ratio = exports.memUsage.ratio;
        }
    }
    return exports.memUsage.ratio;
};

/**
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 *
 * @param {function} callback Function to be called as soon as memory is available again.
 */
exports.waitOnTargetDriver = function (callback, callback2) {
    if (global.gc && exports.getMemoryStats() > exports.env.options.memory.limit) {
        global.gc();
        setTimeout(function () {
            exports.waitOnTargetDriver(callback, callback2);
        }, 100);
    }
    else {
        callback(callback2);
    }
};

/**
 * Will take an array of hits, that are converted into an ElasticSearch Bulk request and then sent off to the target driver.
 * This function will not start running until the meta data has been stored successfully and hits will be queued up to be sent
 * to the target driver in one big bulk request, once the meta data is ready.
 *
 * @param {Object[]} hits Source data in the format ElasticSearch would return it to a search request.
 */
exports.storeData = function(hits) {
    if (exports.status != "running") {
        exports.queue = exports.queue.concat(hits);
    }

    if (exports.queue.length) {
        hits = hits.concat(exports.queue);
        exports.queue = [];
    }

    if (hits.length) {
        if (exports.env.options.log.count) {
            hits.forEach(function(hit) {
                var gid = hit._index + "_" + hit._type + "_" + hit._id;
                if (!exports.env.statistics.source.count) {
                    exports.env.statistics.source.count = {
                        duplicates: 0,
                        uniques: 0,
                        ids: {}
                    };
                }
                if (exports.env.statistics.source.count.ids[gid]) {
                    exports.env.statistics.source.count.duplicates++;
                    exports.env.statistics.source.count.uniques--;
                    exports.env.statistics.source.count.ids[gid]++;
                } else {
                    exports.env.statistics.source.count.ids[gid] = 1;
                    exports.env.statistics.source.count.uniques++;
                }
            });
        }

        var target = drivers.get(exports.env.options.drivers.target).driver;

        async.retry(exports.env.options.errors.retry, function(callback) {
            target.putData(exports.env, hits, function (err) {

                if (err) {
                    callback(err);
                    return;
                }
                exports.env.statistics.docs.processed += hits.length;
                var processed = exports.env.statistics.docs.processed;
                var total = exports.env.statistics.docs.total;
                log.status('Processed %s of %s entries (%s%%)', processed, total, Math.round(processed / total * 100));
                if (processed == total) {
                    log.info('Processed %s entries (100%%)', total);
                    if (target.end) {
                        target.end(exports.env);
                    }
                    else {
                        exports.status = "done";
                    }
                }
            });
        }, function(err) {
            if (err) {
                log.error(err);
                if (!exports.env.options.errors.ignore) {
                    exports.status = "done";
                } else {
                    exports.env.statistics.docs.processed += hits.length;
                }
            }
        });
    }
};

exports.testRun = function(hits) {
    exports.env.statistics.docs.processed += hits.length;
    var processed = exports.env.statistics.docs.processed;
    var total = exports.env.statistics.docs.total;
    log.status('Test run processed %s of %s entries (%s%%)', processed, total, Math.round(processed / total * 100));
    if (processed == total) {
        log.info('Processed %s entries (100%%)', total);
        var target = drivers.get(exports.env.options.drivers.target).driver;
        if (target.end) {
            target.end(exports.env);
        }
        else {
            exports.status = "done";
        }
    }
};

exports.main = {
    read_options: function (callback) {
        log.debug('Reading options');
        options.read(function (optionTree) {
            if (!optionTree) {
                callback('options have been returned empty');
            } else {
                callback(null, optionTree);
            }
        });
    }, verify_options: function (callback, results) {
        log.debug('Passing options to drivers for verification');
        options.verify(results.read_options, function (err) {
            exports.env = new Environment();
            exports.env.options = results.read_options;
            callback(err);
        });
    }, reset_source: function (callback) {
        async.retry(exports.env.options.errors.retry, function (callback) {
            log.debug('Resetting source driver to begin operations');
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.reset(exports.env, function () {
                callback(null, source);
            });
        }, callback);
    }, reset_target: function (callback) {
        async.retry(exports.env.options.errors.retry, function (callback) {
            log.debug('Resetting target driver to begin operations');
            var target = drivers.get(exports.env.options.drivers.target).driver;
            target.reset(exports.env, function () {
                callback(null, target);
            });
        }, callback);
    }, get_source_statistics: function (callback) {
        async.retry(exports.env.options.errors.retry, function (callback) {
            log.debug('Fetching source statistics before starting run');
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.getSourceStats(exports.env, function (err, sourceStats) {
                exports.env.statistics.source = util._extend(exports.env.statistics.source, sourceStats);
                callback(err);
            });
        }, callback);
    }, get_taget_statistics: function (callback) {
        async.retry(exports.env.options.errors.retry, function (callback) {
            log.debug('Fetching target statistics before starting run');
            var target = drivers.get(exports.env.options.drivers.target).driver;
            target.getTargetStats(exports.env, function (err, targetStats) {
                exports.env.statistics.target = util._extend(exports.env.statistics.target, targetStats);
                callback(err);
            });
        }, callback);
    }, check_source_health: function (callback) {
        log.debug("Checking source database health");
        if (exports.env.statistics.source.status == "red") {
            callback("The source database is experiencing and error and cannot proceed");
        }
        else if (exports.env.statistics.source.docs.total === 0) {
            callback("The source driver has not reported any documents that can be exported. Not exporting.");
        } else {
            callback(null);
        }
    }, check_target_health: function (callback) {
        log.debug("Checking target database health");
        if (exports.env.statistics.target.status == "red") {
            callback("The target database is experiencing and error and cannot proceed");
        } else {
            callback(null);
        }
    }, get_metadata: function (callback) {
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
    }, store_metadata: function (callback, results) {
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
    }, get_data: function (callback) {
        callback();

        function get(callback) {
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.getData(exports.env, function (err, data) {
                if (err) {
                    callback(err);
                    return;
                }
                if (exports.env.options.testRun) {
                    exports.testRun(data);
                } else {
                    exports.storeData(data);
                }
                callback();
            });
        }

        function whileWrapper() {
            async.retry(exports.env.options.errors.retry, function (callback) {
                exports.waitOnTargetDriver(get, callback);
            }, function (err) {
                if (err) {
                    log.error(err);
                    if (!exports.env.options.errors.ignore) {
                        exports.status = "done";
                    }
                }
            });
        }

        log.info("Starting data export");
        while (exports.status != "done") {
            whileWrapper();
        }
    },
    /**
     * This basically just flips the switch so that the exports.storeData function will not queue data anymore but
     * instead pass it on to the target driver.
     *
     * @param callback  will be called by async library
     */
    start_export: function (callback) {
        exports.status = "running";
        callback();
    },
    /**
     * This function ties everything together and performs all the operations from reading options to the actual export.
     *
     * @param finalCallback will be called with an optional err message at the end of the export
     */
    run: function (finalCallback) {
        async.auto({
            read_options: exports.main.read_options,
            verify_options: ["read_options", exports.main.verify_options],
            reset_source: ["verify_options", exports.main.reset_source],
            reset_target: ["verify_options", exports.main.verify_options],
            get_source_statistics: ["reset_source", exports.main.get_source_statistics],
            get_taget_statistics: ["reset_target", exports.main.get_taget_statistics],
            check_source_health: ["get_source_statistics", exports.main.check_source_health],
            check_target_health: ["get_taget_statistics", exports.main.check_target_health],
            get_metadata: ["check_source_health", exports.main.get_metadata],
            store_metadata: ["check_target_health", "get_metadata", exports.main.store_metadata],
            get_data: ["check_source_health", exports.main.get_data],
            start_export: ["store_metadata", exports.main.start_export]
        }, function (err) {
            if (err) {
                finalCallback(err);
                return;
            }
            finalCallback();
        });
    }
};

if (require.main === module) {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    process.on('exit', exports.printSummary);
    exports.main.run(function(err) {
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