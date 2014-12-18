exports.getInfo = function (callback) {
    var info = {
        id: 'file',
        name: 'Compressed File Driver',
        version: '1.0',
        desciption: 'A driver to read and store data in local files'
    };
    var options = {
        source: {
            file: {
                abbr: 'f',
                metavar: '<filebase>',
                help: 'The filename from which the data should be imported. The format depends on the compression flag (default = compressed)',
                required: true
            }
        },
        target: {
            file: {
                abbr: 'f',
                metavar: '<filebase>',
                help: 'The filename to which the data should be exported. The format depends on the compression flag (default = compressed)',
                required: true
            }, compression: {
                abbr: 'c',
                metavar: 'true|false',
                help: 'Set if compression should be used to write the data files',
                'default': true,
                choices: [true, false]
            }
        }
    };
    callback(info, options);
};

exports.verifyOptions = function (opts, callback) {
    callback([]);
};

exports.reset = function (callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback({});
};

exports.getSourceStats = function (env, callback) {
    callback({});
};

exports.getMeta = function (env, callback) {
    callback({
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    callback();
};

exports.getData = function (env, callback) {
    callback([], 0);
};

exports.putData = function (env, data, callback) {
    callback();
};