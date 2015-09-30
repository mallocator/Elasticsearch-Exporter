var id = 'elasticsearch-query';

exports.getInfo = function (callback) {
    var errors, requiredOptions;
    callback(errors, {
        id: id,
        name: 'ElasticSearch Query Driver',
        version: '1.0',
        threadsafe: true,
        desciption: 'An Elasticsearch driver that makes use of the query API to read data'
    }, requiredOptions);
};

exports.verifyOptions = function (opts, callback) {
    callback();
};

exports.reset = function (env, callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    var errors = null;
    callback(errors, {
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getSourceStats = function (env, callback) {
    var errors = null;
    callback(errors, {
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        docs: {
            indices: {
                index1: 123,
                index2: 123,
                indexN: 123
            },
            total: 123
        },
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getMeta = function (env, callback) {
    var errors = null;
    callback(errors, {
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    callback();
};

exports.getData = function (env, callback, from, size) {
    var errors = null;
    callback(errors, [{
        _index: "indexName",
        _type: "typeName",
        _id: "1",
        _version: 1,
        found: true,
        _source: {}
    }]);
};

exports.putData = function (env, docs, callback) {
    callback();
};

exports.end = function (env) {
};