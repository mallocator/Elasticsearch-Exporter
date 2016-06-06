'use strict';

var async = require('async');

var drivers = require('./drivers.js');
var log = require('./log.js');
var encapsulator = require('./fn-encapsulate.js');


exports.memUsage = null;
exports.env = null;
exports.id = null;
exports.state = null;
exports.transform_function = null;

/**
 * React on messages being received from the master, all in one neat place so we can see what's happening, when ever
 * a new message comes in.
 */
process.on('message', m => {
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
 */
exports.send = {
    /**
     * Report an error to the main process.
     * @param {string} exception
     */
    error: exception => {
        if (process.send) {
            process.send({
                id: exports.id,
                type: 'Error',
                message: exception
            });
        }
    },
    /**
     * Report a number of documents as processed to the main process.
     * @param {number} processed
     */
    done: processed => {
        if (process.send) {
            process.send({
                id: exports.id,
                type: 'Done',
                processed,
                memUsage: exports.memUsage
            });
        }
        exports.status = 'ready';
    },
    /**
     * Report worker has finished to the main process.
     */
    end: () => {
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
 * @param {number} id       The process worker id
 * @param {Enviroment} env  A copy of the environment from the main process
 */
exports.initialize = (id, env) => {
    exports.id = id;
    exports.env = env;
    log.enabled.debug = env.options.log.debug;
    log.enabled.info = env.options.log.enabled;
    log.debug('Initializing worker %s', id);
    async.each(env.options.drivers.dir, (dir, callback) => drivers.find(dir, callback), err => {
        // TODO handle err
        exports.state = 'ready';
    });

    exports.initialize_transform();

    let source = drivers.get(env.options.drivers.source).driver;
    source.prepareTransfer && source.prepareTransfer(env, true);

    let target = drivers.get(env.options.drivers.target).driver;
    target.prepareTransfer && target.prepareTransfer(env, false);
};

/**
 * Set up any data transformation if needed
 *
 */
exports.initialize_transform = () => {
    if (exports.env.options.xform && exports.env.options.xform.file) {
        try {
            exports.transform_function = encapsulator(exports.env.options.xform.file, 'transform').transform;
            log.info(">> Data transform will be done during export using function file " + exports.env.options.xform.file);
        } catch (err) {
            log.debug("Could not read transform function: " + err);
            log.die(14, "Could not read transform function from file " + exports.env.options.xform.file);
        }
    } else {
        // Ensure transform function is null
        exports.transform_function = null;
        log.debug("Data is going to be transfered without any transformation");
    }
};

/**
 * Returns the current used / available memory ratio.
 * Updates itself only every few milliseconds. Updates occur faster, when memory starts to run out.
 *
 */
exports.getMemoryStats = () => {
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
 * @param {function} callback   Function to be called as soon as memory is available again.
 * @param {function} callback2  Parent callback to be passed on the first callback as parameter.
 */
exports.waitOnTargetDriver = (callback, callback2) => {
    if (exports.state != 'ready') {
        return setTimeout(() => exports.waitOnTargetDriver(callback, callback2), 10);
    }
    if (global.gc && exports.getMemoryStats() > exports.env.options.memory.limit) {
        global.gc();
        return setTimeout(() => exports.waitOnTargetDriver(callback, callback2), 100);
    }
    callback(callback2);
};

/**
 * Starts fetching data from the source driver and pass it on to the target driver once done.
 *
 * @param {number} from
 * @param {number} size
 */
exports.work = (from, size) => {
    let source = drivers.get(exports.env.options.drivers.source).driver;

    function get(callback) {
        source.getData(exports.env, (err, data) => {
            if (err) {
                return callback(err);
            }
            // TODO validate data format
            // TODO validate that data.length == size and throw a warning if not (does this work in a cluster?)
            if (!data || !data.length) {
                exports.send.end();
            } else if (exports.env.options.run.test) {
                exports.send.done(data.length);
            } else {
                exports.storeData(data);
            }
            callback();
        }, from, size);
    }

    async.retry(exports.env.options.errors.retry, callback => exports.waitOnTargetDriver(get, callback), (err) => {
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
 * Performs a transform function if specified in the options.
 * @param {Data[]} hits
 */
exports.transformHits = hits => {
    // Try/Catch once for all hits. When we implement smarter error handling this
    // will need to be changed to per-hit error handling
    try {
        for (let hit of hits) {
            hit._source = exports.transform_function(hit._source);
        }
    } catch (err) {
        log.die(14, "Error while performing transformation. Stopping. " + err);
    }
};

/**
 * Will take an array of hits, that are converted into an ElasticSearch Bulk request and then sent off to the target driver.
 * This function will not start running until the meta data has been stored successfully and hits will be queued up to be sent
 * to the target driver in one big bulk request, once the meta data is ready.
 *
 * @param {Data[]} hits Source data in the format ElasticSearch would return it to a search request.
 */
exports.storeData = hits => {
    // TODO check if hits is length of step or if we are at the end / might just be enough to check if no more data is coming
    if (!hits.length) {
        return exports.send.done(hits.length);
    }

    exports.transform_function && exports.transformHits(hits);

    let target = drivers.get(exports.env.options.drivers.target).driver;
    async.retry(exports.env.options.errors.retry, callback => {
        target.putData(exports.env, hits, err => {
            if (err) {
                return callback(err);
            }
            exports.send.done(hits.length);
            callback();
        });
    }, err => err && !exports.env.options.errors.ignore && exports.send.error(err));
};

/**
 * Terminate the worker and call the end function of all the workers so they can shut down too.
 *
 */
exports.end = () => {
    log.debug('Terminating worker %s', exports.id);
    let source = drivers.get(exports.env.options.drivers.source).driver;
    source.end && source.end(exports.env);
    let target = drivers.get(exports.env.options.drivers.target).driver;
    target.end && target.end(exports.env);
};

process.on('uncaughtException', exports.send.error);
