/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate.
 */


exports.getInfo = function (callback) {
    callback(null, {
        id: 'mock',
        name: 'Mock Driver',
        version: '1.0',
        desciption: 'A mock implementation for testing purposes'
    }, { source: {}, target: {}});
};

exports.verifyOptions = function (opts, callback) {
    callback();
};

exports.reset = function (callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback(null, {
        version: "1.0.0",
        cluster_status: "Green"
    });
};

exports.getSourceStats = function (env, callback) {
    callback(null, {
        version: "1.0.0",
        cluster_status: "Green",
        docs: {
            total: 1
        }
    });
};

exports.getMeta = function (env, callback) {
    callback(null, {
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    callback();
};

exports.getData = function (env, callback) {
    callback(null, []);
};

exports.putData = function (env, data, callback) {
    callback();
};
