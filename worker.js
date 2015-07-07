var async = require('async');
var drivers = require('./drivers.js');
var log = require('./log.js');

exports.memUsage = null;
exports.env = null;
exports.id = null;
exports.state = null;

/**
 * React on messages being received from the master, all in one neat place so we can see what's happening, when ever
 * a new message comes in.
 *
 */
process.on('message', function(m) {
    switch (m.type) {
        case 'Initialize':
            exports.initialize(m.id, m.env);
            break;
        case 'Work':
            exports.work(m.from, m.size);
            break;
        case 'Done':
            exports.end();
            break;
    }
});

/**
 * A wrapper to make it easier to see all messages that are being sent to the master.
 *
 * @type {{error: Function, done: Function}}
 */
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
    },
    end: function() {
        if (process.send) {
            process.send({
                id: exports.id,
                type: 'End'
            });
        }
    }
};

/**
 * Set up the environment for the drivers to work. This is a miniature initialization of what goes on at the
 * beginning of the exporter.
 *
 * @param id
 * @param env
 */
exports.initialize = function(id, env) {
    exports.id = id;
    exports.env = env;
    log.enabled.debug = env.options.log.debug;
    log.enabled.info = env.options.log.enabled;
    log.debug('Initializing worker %s', id);
    async.each(env.options.drivers.dir, function (dir, callback) {
        drivers.find(dir, callback);
    }, function () {
        exports.state = 'ready';
    });
    var source = drivers.get(env.options.drivers.source).driver;
    if (source.prepareTransfer) {
        source.prepareTransfer(env, true);
    }
    var target = drivers.get(env.options.drivers.target).driver;
    if (target.prepareTransfer) {
        target.prepareTransfer(env, false);
    }
};

/**
 * Returns the current used / available memory ratio.
 * Updates itself only every few milliseconds. Updates occur faster, when memory starts to run out.
 *
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
 * @param {function} callback2 Parent callback to be passed on the first callback as paramter.
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

/**
 * Starts fetching data from the source driver and pass it on to the target driver once done.
 *
 * @param from
 * @param size
 */
exports.work = function(from, size) {
    var source = drivers.get(exports.env.options.drivers.source).driver;

    function get(callback) {
        source.getData(exports.env, function (err, data) {
            if (err) {
                callback(err);
                return;
            }
            // TODO validate data format
            // TODO validate that data.length == size
            if (!data.length) {
                exports.send.end();
            } else if (exports.env.options.run.test) {
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
            if (exports.env.options.errors.ignore) {
                exports.send.done(size);
            } else {
                exports.send.error(err);
            }
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
    // TODO check if hits is length of step or if we are at the end
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
        if (exports.env.options.errors.ignore) {
            exports.send.done(hits.length);
        } else {
            exports.send.error(err);
        }
    });
};

/**
 * Terminate the worker and call the end function of all the workers so they can shut down too.
 *
 */
exports.end = function() {
    log.debug('Terminating worker %s', exports.id);
    var source = drivers.get(exports.env.options.drivers.source).driver;
    if (source.end) {
        source.end(exports.env);
    }
    var target = drivers.get(exports.env.options.drivers.target).driver;
    if (target.end) {
        target.end(exports.env);
    }
};

process.on('uncaughtException', exports.send.error);