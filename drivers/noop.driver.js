var log = require('../log.js');


var id = 'noop';

exports.getInfo = (callback) => {
    let driverInfo = {
        id,
        name: 'NoOp Driver',
        version: '1.0',
        desciption: 'An driver that does absolutely nothing'
    };

    let requiredOptions = {
        source: {},
        target: {}
    };

    callback(null, driverInfo, requiredOptions);
};

exports.verifyOptions = (opts, callback) => {
    if (opts.drivers.source == id) {
        callback("You're using NoOp driver as source which makes no sense");
    } else {
        callback();
    }
};

exports.reset = (env, callback) => callback();

exports.getTargetStats = (env, callback) => callback(null, { version: "1.0.0", cluster_status: "Green" });

exports.getSourceStats = (env, callback) => callback("You're using NoOp driver as source which makes no sense");

exports.getMeta = (env, callback) => callback("You're using NoOp driver as source which makes no sense");

exports.putMeta = (env, metadata, callback) => {
    log.debug("Not writing any metadata anywhere (NoOp)");
    callback();
};

exports.getData = (env, callback, from, size) => callback("You're using NoOp driver as source which makes no sense");

exports.putData = (env, docs, callback) => {
    log.debug("Not storing data anywhere (Noop)");
    callback();
};
