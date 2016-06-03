'use strict';

var cp = require('child_process');
var util = require('util');

var drivers = require('./drivers.js');


class Cluster {
    /**
     * A helper class that wraps all communication with worker processes that do the actual import/export
     * @param env
     * @param numWorkers
     * @constructor
     */
    constructor(env, numWorkers) {
        this.workers = {};
        this.workListeners = [];
        this.errorListeners = [];
        this.endListeners = [];
        this.processed = 0;
        this.total = env.statistics.source.docs.total;
        this.messageReceiver = m => {
            let allDone = false;
            switch (m.type) {
                case 'Error':
                    this.errorListeners.forEach(listener => listener(m.message));
                    break;
                case 'Done':
                    this.workers[m.id].state = 'ready';
                    this.processed += m.processed;
                    this.workListeners.forEach(function (listener) {
                        listener(m.processed);
                    });
                    for (let id in this.workers) {
                        allDone = allDone || this.workers[id].state == 'ready' || this.workers[id].state == 'end';
                    }
                    if (m.memUsage.heapUsed > env.statistics.memory.peak) {
                        exports.env.statistics.memory.peak = m.memUsage.heapUsed;
                        exports.env.statistics.memory.ratio = m.memUsage.ratio;
                    }
                    break;
                case 'End':
                    this.workers[m.id].state = 'end';
                    break;
            }

            if (allDone && this.processed == this.total) {
                this.endListeners.forEach(listener => listener());
                for (let doneId in this.workers) {
                    this.send.done(doneId);
                }
            }
        };
        this.send = {
            initialize: (id, env) => {
                this.workers[id].process.send({ type: 'Initialize', id, env });
                this.workers[id].state = 'ready';
            },
            message: (id, from, size) => {
                this.workers[id].process.send({ type: 'Work', from, size });
                this.workers[id].state = 'working';
            },
            done: id => this.workers[id].process.send({ type: 'Done' })
        };

        for (let i = 0; i < numWorkers; i++) {
            let worker = cp.fork(exports.workerPath, ["--nouse-idle-notification", "--expose-gc", "--always_compact"]);
            worker.on('message', this.messageReceiver);
            this.workers[i] = { process: worker };
            this.send.initialize(i, env);
        }
    }

    /**
     * Sends a json object to an idle worker. If no worker is idle this will block until one becomes idle.
     * @param from
     * @param size
     * @param callback
     */
    work(from, size, callback) {
        let allEnded = true;
        for (let id in this.workers) {
            let worker = this.workers[id];
            if (worker.state == 'ready') {
                this.send.message(id, from, size);
                callback();
                return;
            }
            if (worker.state != 'end') {
                allEnded = false;
            }
            if (allEnded && this.processed != this.total) {
                callback("The export has finished, but fewer documents than expected have beene exported.");
            }
        }

        process.nextTick(() => this.work(from, size, callback));
    }

    /**
     * Add a listener here to receive messages from _workers whenever they send a message.
     * @param callback function(processedDocs) {}
     */
    onWorkDone(callback) {
        this.workListeners.push(callback);
    }

    /**
     * Add a listener here to receive messages from _workers whenever they throw an error.
     * @param callback function(error) {}
     */
    onError(callback) {
        this.errorListeners.push(callback);
    }

    /**
     * When all _workers are in idle mode (and no more messages are queued up) this listener will be fired.
     * @param callback
     */
    onEnd(callback) {
        this.endListeners.push(callback);
    }
}

/**
 * An implementation of the cluster that is not a cluster, but instead calls the worker directly in the same process.
 * @param env
 * @constructor
 */
class NoCluster extends Cluster {
    constructor(env) {
        super(env, 0);
        var that = this;
        this.worker = require(exports.workerPath);
        this.worker.env = env;
        this.worker.id = 0;

        this.worker.initialize_transform();

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
            if (that.processed == that.total) {
                that.endListeners.forEach(function (listener) {
                    listener();
                });
                that.worker.end();
            }
            that.workDoneListener();
        };
        this.worker.send.error = function (exception) {
            that.errorListeners.forEach(function (listener) {
                listener(exception);
            });
        };
        this.worker.state = 'ready';
    };

    /**
     * Overrides the work function of the parent which would use process.send to communicate with the worker instead of
     * calling him directly.
     *
     * @param from
     * @param size
     * @param callback
     */
    work(from, size, callback) {
        this.workDoneListener = callback;
        this.worker.work(from, size);
    };
}

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
