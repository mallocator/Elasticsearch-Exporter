var async = require('async');
var drivers = require('./drivers.js');
var log = require('./log.js');

exports.memUsage = null;
exports.env = null;
exports.id = null;
exports.state = null;

process.on('message', function(m) {
    switch (m.type) {
        case 'Initialize':
            exports.id = m.id;
            exports.env = m.env;
            log.enabled.debug = exports.env.options.log.debug;
            log.enabled.info = exports.env.options.log.enabled;
            log.debug('Initializing worker %s', exports.id);
            async.each(exports.env.options.drivers.dir, function (dir, callback) {
                drivers.find(dir, callback);
            }, function () {
                exports.state = 'ready';
            });
            var source = drivers.get(exports.env.options.drivers.source).driver;
            if (source.prepareTransfer) {
                source.prepareTransfer(exports.env, true);
            }
            var target = drivers.get(exports.env.options.drivers.target).driver;
            if (target.prepareTransfer) {
                target.prepareTransfer(exports.env, false);
            }
            break;
        case 'Work':
            exports.work(m.from, m.size);
            break;
        case 'Done':
            log.debug('Terminating worker %s', exports.id);
            var target = drivers.get(exports.env.options.drivers.target).driver;
            if (target.end) {
                target.end(exports.env);
            }
            break;
    }
});

exports.send = {
    error: function(exception) {
        if (process.send) {
            process.send({
                id: exports.id,
                type: 'Error',
                message: exception
            });
        }
    },
    done: function(processed) {
        if (process.send) {
            process.send({
                id: exports.id,
                type: 'Done',
                processed: processed,
                memUsage: exports.memUsage
            });
        }
        exports.status = 'ready';
    }
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
    }
    return exports.memUsage.ratio;
};

/**
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 *
 * @param {function} callback Function to be called as soon as memory is available again.
 */
exports.waitOnTargetDriver = function (callback, callback2) {
    if (exports.state != 'ready') {
        setTimeout(function () {
            exports.waitOnTargetDriver(callback, callback2);
        }, 10);
    }
    else if (global.gc && exports.getMemoryStats() > exports.env.options.memory.limit) {
        global.gc();
        setTimeout(function () {
            exports.waitOnTargetDriver(callback, callback2);
        }, 100);
    }
    else {
        callback(callback2);
    }
};

exports.work = function(from, size) {
    function get(callback) {
        var source = drivers.get(exports.env.options.drivers.source).driver;
        source.getData(exports.env, function (err, data) {
            if (err) {
                callback(err);
                return;
            }
            // TODO validate data format
            if (exports.env.options.testRun) {
                exports.send.done(data.length);
            } else {
                exports.storeData(data);
            }
            callback();
        }, from, size);
    }

    async.retry(exports.env.options.errors.retry, function (callback) {
        exports.waitOnTargetDriver(get, callback);
    }, function (err) {
        if (err) {
            exports.send.error(err);
        }
    });
};

/**
 * Will take an array of hits, that are converted into an ElasticSearch Bulk request and then sent off to the target driver.
 * This function will not start running until the meta data has been stored successfully and hits will be queued up to be sent
 * to the target driver in one big bulk request, once the meta data is ready.
 *
 * @param {Object[]} hits Source data in the format ElasticSearch would return it to a search request.
 */
exports.storeData = function (hits) {
    if (!hits.length) {
        exports.send.done(hits.length);
        return;
    }

    var target = drivers.get(exports.env.options.drivers.target).driver;
    async.retry(exports.env.options.errors.retry, function (callback) {
        target.putData(exports.env, hits, function (err) {
            if (err) {
                callback(err);
                return;
            }
            exports.send.done(hits.length);
            callback();
        });
    }, function (err) {
        if (err) {
            exports.send.error(err);
        }
    });
};

process.on('uncaughtException', exports.send.error);