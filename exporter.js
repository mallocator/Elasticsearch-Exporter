// Process exit signals:
// 0 - Operation successful
// 1 - No documents found to export
// 2 - Uncaught Exception
// 3 - invalid options specified
// 4 - source or target status = invalid / red / not ready
// 10 - driver interface is not implemented properly
// 11 - driver doesn't exist
// 12 - driver threw unknown error
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
            docs: {
                processed: 0,
                total: 0
            },
            retries: 0
        },
        target: {
            version: "0.0",
            status: "red",
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

exports.env = null;
exports.status = "ready";
exports.queue = [];

exports.handleUncaughtExceptions = function (e) {
    log.error('Caught exception in Main process: %s'.bold, e.toString());
    if (e instanceof Error) {
        log.info(e.stack);
    }
    process.exit(2);
};

exports.printSummary = function() {
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
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 *
 * @param {function} callback Function to be called as soon as memory is available again.
 */
exports.waitOnTargetDriver = function (callback) {
    if (global.gc && exports.getMemoryStats() > exports.env.options.memory.limit) {
        global.gc();
        setTimeout(function () {
            exports.waitOnTargetDriver(callback);
        }, 100);
    }
    else {
        callback();
    }
};


exports.convertToBulk = function (hit) {
    if (exports.env.options.log.count) {
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
    }
    var op = exports.env.options.target.overwrite ? 'index' : 'create';
    var metaData = {};
    metaData[op] = {
        _index: exports.env.options.target.index ? exports.env.options.target.index : hit._index,
        _type: exports.env.options.target.type ? exports.env.options.target.type : hit._type,
        _id: hit._id,
        _version: hit._version ? hit._version : null
    };
    if (hit.fields) {
        ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'].forEach(function (field) {
            if (hit.fields[field]) {
                metaData[op][field] = hit.fields[field];
            }
        });
    }
    return JSON.stringify(metaData) + '\n' + JSON.stringify(hit._source) + '\n';
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
        var data = '';
        hits.forEach(function (hit) {
            data += exports.convertToBulk(hit);
        });

        var target = drivers.get(exports.env.options.drivers.target).driver;

        target.putData(exports.env, data, function (err) {
            if (err) {
                log.error(err);
                if (!exports.env.options.errors.ignore) {
                    exports.status = "done";
                }
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

/**
 * This function ties everything together and performs all the operations from reading options to the actual export.
 */
exports.run = function (finalCallback) {
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
                exports.env = new Environment();
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
        reset_target: ["verify_options", function (callback) {
            log.debug('Resetting target driver to begin operations');
            var target = drivers.get(exports.env.options.drivers.target).driver;
            target.reset(function () {
                callback(null, target);
            });
        }],
        get_source_statistics: [ "reset_source", function(callback) {
            log.debug('Fetching source statistics before starting run');
            var source = drivers.get(exports.env.options.drivers.source).driver;
            source.getSourceStats(exports.env, function (err, sourceStats) {
                exports.env.statistics.source = util._extend(exports.env.statistics.source, sourceStats);
                callback(err);
            });
        }],
        get_taget_statistics: [ "reset_target", function(callback) {
            log.debug('Fetching target statistics before starting run');
            var target = drivers.get(exports.env.options.drivers.target).driver;
            target.getTargetStats(exports.env, function (err, targetStats) {
                exports.env.statistics.target = util._extend(exports.env.statistics.target, targetStats);
                callback(err);
            });
        }],
        check_source_health: [ "get_source_statistics", function(callback) {
            log.debug("Checking source database health");
            if (exports.env.statistics.source.status == "red") {
                callback("The source database is experiencing and error and cannot proceed");
            }
            else if (exports.env.statistics.source.docs.total === 0) {
                callback("The source driver has not reported any documents that can be exported. Not exporting.");
            } else {
                callback(null);
            }
        }],
        check_target_health: [ "get_taget_statistics", function(callback) {
            log.debug("Checking target database health");
            if (exports.env.statistics.target.status == "red") {
                callback("The target database is experiencing and error and cannot proceed");
            } else {
                callback(null);
            }
        }],
        get_metadata: [ "check_source_health", function(callback) {
            if (exports.env.options.mapping) {
                log.debug("Using mapping overridden through options");
                callback(null, exports.env.options.mapping);
            } else {
                log.debug("Fetching mapping from source database");
                var source = drivers.get(exports.env.options.drivers.source).driver;
                source.getMeta(exports.env, callback);
            }
        }],
        store_metadata: [ "check_target_health", "get_metadata", function(callback, results) {
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
        }],
        get_data: [ "check_source_health", function(callback) {
            callback();

            function get() {
                var source = drivers.get(exports.env.options.drivers.source).driver;
                source.getData(exports.env, function (err, data) {
                    if (err) {
                        log.error(err);
                        if (!exports.env.options.errors.ignore) {
                            exports.status = "done";
                        }
                    }
                    if (exports.env.options.testRun) {
                        exports.testRun(data);
                    } else {
                        exports.storeData(data);
                    }
                });
            }

            log.info("Starting data export");
            while (exports.status != "done") {
                exports.waitOnTargetDriver(get);
            }
        }],
        start_export: ["store_metadata", function(callback) {
            exports.status = "running";
            callback();
        }]
    }, function(err) {
        if (err) {
            finalCallback(err);
            return;
        }
        finalCallback(0);
    });
};

if (require.main === module) {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    process.on('exit', exports.printSummary);
    exports.run(function(err) {
        if (err) {
            log.error("The driver reported an error:", err);
            process.exit(4);
        }
        process.exit(0);
    });
}