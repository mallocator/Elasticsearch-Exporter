function MockCluster() {
    this.workListeners = [];
    this.workDoneListeners = [];
    this.endListeners = [];
    this.errorListeners = [];
    this.pointer = 0;
    this.steps = 0;
}

MockCluster.prototype.work = function (pointer, step, callback) {
    this.pointer = pointer;
    this.steps += step;
    this.workListeners.push(callback);
};

MockCluster.prototype.onWorkDone = function(listener) {
    this.workDoneListeners.push(listener);
};

MockCluster.prototype.onEnd = function(listener) {
    this.endListeners.push(listener);
};

MockCluster.prototype.onError = function(listener) {
    this.errorListeners.push(listener);
};

MockCluster.prototype.sendWorking = function() {
    this.workListeners.forEach(function(listener) {
        listener();
    });
};
MockCluster.prototype.sendWorkDone = function(processedDocs) {
    this.workDoneListeners.forEach(function (listener) {
        listener(processedDocs);
    });
};
MockCluster.prototype.sendEnd = function() {
    this.endListeners.forEach(function (listener) {
        listener();
    });
};
MockCluster.prototype.sendError = function(err) {
    this.errorListeners.forEach(function (listener) {
        listener(err);
    });
};
MockCluster.prototype.getSteps = function() {
    return this.steps;
};
MockCluster.prototype.getPointer = function() {
    return this.pointer;
};

exports.getInstance = function() {
    return new MockCluster();
};