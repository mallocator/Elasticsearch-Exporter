'use strict';

class MockCluster {
    constructor() {
        this.workListeners = [];
        this.workDoneListeners = [];
        this.endListeners = [];
        this.errorListeners = [];
        this.pointer = 0;
        this.steps = 0;
    }

    work(pointer, step, callback) {
        this.pointer = pointer;
        this.steps += step;
        this.workListeners.push(callback);
    }

    onWorkDone(listener) {
        this.workDoneListeners.push(listener);
    }

    onEnd(listener) {
        this.endListeners.push(listener);
    }

    onError(listener) {
        this.errorListeners.push(listener);
    }

    sendWorking() {
        this.workListeners.forEach(function(listener) {
            listener();
        });
    }

    sendWorkDone(processedDocs) {
        this.workDoneListeners.forEach(function (listener) {
            listener(processedDocs);
        });
    }

    sendEnd() {
        this.endListeners.forEach(function (listener) {
            listener();
        });
    }

    sendError(err) {
        this.errorListeners.forEach(function (listener) {
            listener(err);
        });
    }

    getSteps() {
        return this.steps;
    }

    getPointer() {
        return this.pointer;
    }
}

exports.getInstance = function() {
    return new MockCluster();
};
