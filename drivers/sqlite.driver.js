var id = 'sqlite';

exports.getInfo = (callback) => {
    let errors, requiredOptions;
    callback(errors, {
        id: id,
        name: 'SQLite Driver',
        version: '0.0',
        desciption: '[N/A] A SQLite driver to import and export data'
    }, requiredOptions);
};

exports.verifyOptions = (opts, callback) => {
    callback();
};

exports.reset = (env, callback) => {
    callback();
};

exports.getTargetStats = (env, callback) => {
    let errors = null;
    callback(errors, {
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getSourceStats = (env, callback) => {
    let errors = null;
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

exports.getMeta = (env, callback) => {
    let errors = null;
    callback(errors, {
        mappings: {},
        settings: {}
    });
};

exports.putMeta = (env, metadata, callback) => {
    callback();
};

exports.getData = (env, callback) => {
    let errors = null;
    callback(errors, [{
        _index: "indexName",
        _type: "typeName",
        _id: "1",
        _version: 1,
        found: true,
        _source: {}
    }]);
};

exports.putData = (env, docs, callback) => {
    callback();
};

exports.end = (env) => {};
