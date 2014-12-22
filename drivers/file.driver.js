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
                help: 'The filename from which the data should be imported. The format depends on the compression flag (default = compressed)',
                required: true
            }
        }, target: {
            file: {
                abbr: 'f',
                help: 'The filename to which the data should be exported. The format depends on the compression flag (default = compressed)',
                required: true
            }, compression: {
                abbr: 'c',
                help: 'Set if compression should be used to write the data files',
                preset: true,
                flag: true
            }
        }
    };
    callback(info, options);
};

exports.verifyOptions = function (opts, callback) {
    callback([]);
};

exports.reset = function (env, callback) {
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

/**
 * If a source file has been set then this will check if the file has been compressed by checking the file header.
 *
 * @param opts
 */
exports.detectCompression = function (opts) {
    if (!opts.sourceFile) return;
    var header = new Buffer(2);
    fs.readSync(fs.openSync(opts.sourceFile + '.data', 'r'), header, 0, 2);
    opts.sourceCompression = (header[0] == 0x1f && header[1] == 0x8b);
};

exports.getData = function (env, callback) {
    callback([], 0);
};

exports.putData = function (env, data, callback) {
    callback();
};