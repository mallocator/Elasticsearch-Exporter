var cp = require('child_process');
var util = require('util');
var drivers = require('./drivers.js');

/**
 * A helper class that wraps all communication with worker processes that do the actual import/export
 * @param env
 * @param numWorkers
 * @constructor
 */
var Cluster = function(env, numWorkers) {
    var that = this;
    this.workers = {};
    this.workListeners = [];
    this.errorListeners = [];
    this.endListeners = [];
    this.processed = 0;
    this.total = env.statistics.docs.total;
    this.messageReceiver = function(m) {
        var allDone = false;
        switch (m.type) {
            case 'Error':
                that.errorListeners.forEach(function (listener) {
                    listener(m.message);
                });
                break;
            case 'Done':
                that.workers[m.id].state = 'ready';
                that.processed += m.processed;
                that.workListeners.forEach(function (listener) {
                    listener(m.processed);
                });
                for (var id in that.workers) {
                    allDone = allDone || that.workers[id].state == 'ready';
                }
                if (m.memUsage.heapUsed > env.statistics.memory.peak) {
                    exports.env.statistics.memory.peak = m.memUsage.heapUsed;
                    exports.env.statistics.memory.ratio = m.memUsage.ratio;
                }
                break;
        }

        if (allDone && that.processed == that.total) {
            that.endListeners.forEach(function (listener) {
                listener();
            });
            for (var doneId in that.workers) {
                that.send.done(doneId);
            }
        }
    };
    this.send = {
        initialize: function (id, env) {
            that.workers[id].process.send({
                type: 'Initialize',
                id: id,
                env: env
            });
            that.workers[id].state = 'ready';
        },
        message: function (id, from, size) {
            that.workers[id].process.send({
                type: 'Work',
                from: from,
                size: size
            });
            that.workers[id].state = 'working';
        },
        done: function(id) {
            that.workers[id].process.send({
                type: 'Done'
            });
        }
    };

    for (var i = 0; i < numWorkers; i++) {
        var worker = cp.fork(exports.workerPath, ["--nouse-idle-notification", "--expose-gc", "--always_compact"]);
        worker.on('message', this.messageReceiver);
        this.workers[i] = {
            process: worker
        };
        this.send.initialize(i, env);
    }
};

/**
 * Sends a json object to an idle worker. If no worker is idle this will block until one becomes idle.
 * @param message
 * @param callback
 */
Cluster.prototype.work = function(from, size, callback) {
    for (var id in this.workers) {
        var worker = this.workers[id];
        if (worker.state == 'ready') {
            this.send.message(id, from, size);
            callback();
            return;
        }
    }
    var that = this;
    process.nextTick(function () {
        that.work(from, size, callback);
    });
};

/**
 * Add a listener here to receive messages from _workers whenever they send a message.
 * @param callback function(processedDocs) {}
 */
Cluster.prototype.onWorkDone = function (callback) {
    this.workListeners.push(callback);
};

/**
 * Add a listener here to receive messages from _workers whenever they throw an error.
 * @param callback function(error) {}
 */
Cluster.prototype.onError = function (callback) {
    this.errorListeners.push(callback);
};

/**
 * When all _workers are in idle mode (and no more messages are queued up) this listener will be fired.
 * @param callback
 */
Cluster.prototype.onEnd = function (callback) {
    this.endListeners.push(callback);
};


/**
 * An implementation of the cluster that is not a cluster, but instead calls the worker directly in the same process.
 * @param env
 * @constructor
 */
var NoCluster = function(env) {
    Cluster.call(this, env, 0);
    var that = this;
    this.worker = require(exports.workerPath);
    this.worker.env = env;

    var source = drivers.get(env.options.drivers.source).driver;
    if (source.prepareTransfer) {
        source.prepareTransfer(env, true);
    }
    var target = drivers.get(env.options.drivers.target).driver;
    if (target.prepareTransfer) {
        target.prepareTransfer(env, false);
    }

    this.worker.send.done = function (processed) {
        that.processed += processed;
        that.workListeners.forEach(function (listener) {
            listener(processed);
        });
        that.workDoneListener();
        if (that.processed == that.total) {
            that.endListeners.forEach(function (listener) {
                listener();
            });
        }
    };
    this.worker.send.error = function (exception) {
        that.errorListeners.forEach(function (listener) {
            listener(exception);
        });
    };
};

util.inherits(NoCluster, Cluster);

/**
 * Overrides the work function of the parent which would use process.send to communicate with the worker instead of
 * calling him directly.
 *
 * @param from
 * @param size
 * @param callback
 */
NoCluster.prototype.work = function(from, size, callback) {
    this.worker.work(from, size);
    this.workDoneListener = callback;
};

exports.workerPath = './worker.js';

/**
 * Create a new adapter for the data transfer which will be either a Clustered implementation (if numWorkers is
 * greater 1) or a direct calling implementation that will be executed in the same process.
 *
 * @param env
 * @param numWorkers
 * @returns {Cluster}
 */
exports.run = function(env, numWorkers) {
    return numWorkers < 2 ? new NoCluster(env) : new Cluster(env, numWorkers);
};