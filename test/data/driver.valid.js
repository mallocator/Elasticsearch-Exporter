exports.getInfo = (callback) => {
    callback(null, {id: 'test'}, null);
};

exports.verifyOptions = (opts, callback) => {};

exports.reset = (env, callback) => {};

exports.getTargetStats = (env, callback) => {};

exports.getSourceStats = (env, callback) => {};

exports.getMeta = (env, callback) => {};

exports.putMeta = (env, metadata, callback) => {};

exports.getData = (env, callback) => {};

exports.putData = (env, docs, callback) => {};
