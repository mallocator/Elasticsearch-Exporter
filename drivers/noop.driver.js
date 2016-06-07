'use strict';

var Driver = require('./driver.interface');
var log = require('../log.js');


class NoOp extends Driver {
    constructor() {
        super();
        this.id = 'noop';
    }

    getInfo(callback) {
        let driverInfo = {
            id: this.id,
            name: 'NoOp Driver',
            version: '1.0',
            description: 'An driver that does absolutely nothing'
        };

        let requiredOptions = {
            source: {},
            target: {}
        };

        callback(null, driverInfo, requiredOptions);
    }

    verifyOptions(opts, callback) {
        if (opts.drivers.source == this.id) {
            callback("You're using NoOp driver as source which makes no sense");
        } else {
            callback();
        }
    }

    getTargetStats(env, callback) {
        callback(null, { version: "1.0.0", cluster_status: "Green" });
    }

    getSourceStats(env, callback) {
        callback("You're using NoOp driver as source which makes no sense");
    }

    getMeta(env, callback) {
        callback("You're using NoOp driver as source which makes no sense");
    }

    putMeta(env, metadata, callback) {
        log.debug("Not writing any metadata anywhere (NoOp)");
        callback();
    }

    getData(env, callback, from, size) {
        callback("You're using NoOp driver as source which makes no sense");
    }

    putData(env, docs, callback) {
        log.debug("Not storing data anywhere (Noop)");
        callback();
    }
}

module.exports = new NoOp();
