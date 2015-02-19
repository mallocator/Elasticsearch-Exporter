/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate.
 */

function Driver() {
    this.reset();
}

Driver.prototype.reset = function (callback) {
    this.info = {
        id: 'mock',
        name: 'Mock Driver',
        version: '1.0',
        desciption: 'A mock implementation for testing purposes'
    };

    this.options = {
        source: {},
        target: {}
    };

    this.targetStats = {
        version: "1.0.0",
        cluster_status: "Green"
    };

    this.sourceStats = {
        version: "1.0.0",
        cluster_status: "Green",
        docs: {
            total: 1
        }
    };

    this.metadata = {
        mappings: {},
        settings: {}
    };

    this.data = [];
    if (callback) {
        callback();
    }
};

Driver.prototype.getInfo = function (callback) {
    callback(null, this.info, this.options);
};

Driver.prototype.verifyOptions = function (opts, callback) {
    callback();
};

Driver.prototype.getTargetStats = function (env, callback) {
    callback(null, this.targetStats);
};

Driver.prototype.getSourceStats = function (env, callback) {
    callback(null, this.sourceStats);
};

Driver.prototype.getMeta = function (env, callback) {
    callback(null, this.metadata);
};

Driver.prototype.putMeta = function (env, metadata, callback) {
    this.metadata = metadata;
    callback();
};

Driver.prototype.getData = function (env, callback) {
    callback(null, this.data);
};

Driver.prototype.putData = function (env, data, callback) {
    this.data = data;
    callback();
};



Driver.prototype.getDriver = function() {
    return new Driver();
};

Driver.prototype.setInfo = function(info) {
    this.info = info;
};

Driver.prototype.getInfoSync = function(threadsafe) {
    this.info.threadsafe = threadsafe;
    return this.info;
};

Driver.prototype.setOptions = function(options) {
    this.options = options;
};

Driver.prototype.getOptionsSync = function () {
    return this.options;
};

Driver.prototype.setTargetStats = function(targetStats) {
    this.targetStats = targetStats;
};

Driver.prototype.setSourceStats = function(sourceStats) {
    this.sourceStats = sourceStats;
};

Driver.prototype.addhit = function(hit) {
    this.data.push(hit);
};

Driver.prototype.addhits = function (hits) {
    this.data.concat(hits);
};

var util = require('util');
util._extend(exports, Driver.prototype);