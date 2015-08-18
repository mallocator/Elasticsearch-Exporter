var log = require('../log.js');

var id = 'noop';

exports.getInfo = function (callback) {
    var driverInfo = {
        id: id,
        name: 'NoOp Driver',
        version: '1.0',
        desciption: 'An driver that does absolutely nothing'
    };

    var requiredOptions = {
        source: {},
        target: {}
    };

    callback(null, driverInfo, requiredOptions);
};

exports.verifyOptions = function (opts, callback) {
    if (opts.drivers.source == id) {
        callback("You're using NoOp driver as source which makes no sense");
    } else {
        callback();
    }
};

exports.reset = function (env, callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback(null, {
        version: "1.0.0",
        cluster_status: "Green"
    });
};

exports.getSourceStats = function (env, callback) {
    callback("You're using NoOp driver as source which makes no sense");
};

exports.getMeta = function (env, callback) {
    callback("You're using NoOp driver as source which makes no sense");
};

exports.putMeta = function (env, metadata, callback) {
    log.debug("Not writing any metadata anywhere (NoOp)");
    callback();
};

exports.getData = function (env, callback, from, size) {
    callback("You're using NoOp driver as source which makes no sense");
};

exports.putData = function (env, docs, callback) {
    log.debug("Not storing data anywhere (Noop)");
    callback();
};