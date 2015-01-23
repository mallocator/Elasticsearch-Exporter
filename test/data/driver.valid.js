exports.getInfo = function (callback) {
    callback(null, {id: 'test'}, null);
};

exports.verifyOptions = function (opts, callback) {};

exports.reset = function (env, callback) {};

exports.getTargetStats = function (env, callback) {};

exports.getSourceStats = function (env, callback) {};

exports.getMeta = function (env, callback) {};

exports.putMeta = function (env, metadata, callback) {};

exports.getData = function (env, callback) {};

exports.putData = function (env, data, callback) {};