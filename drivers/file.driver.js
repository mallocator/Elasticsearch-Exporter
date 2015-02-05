var fs = require('fs');
var through = require('through');
var zlib = require('zlib');
var path = require('path');
var log = require('../log.js');

var id = 'file';

exports.getInfo = function (callback) {
    var info = {
        id: id,
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
    callback(null, info, options);
};

exports.verifyOptions = function (opts, callback) {
    if (opts.drivers.source == id) {
        var header = new Buffer(2);
        fs.readSync(fs.openSync(opts.source.file + '.data', 'r'), header, 0, 2);
        opts.source.compression = (header[0] == 0x1f && header[1] == 0x8b);
    }
    callback();
};

exports.reset = function (env, callback) {
    exports.targetStream = null;
    exports.lineCount = null;
    exports.buffer = '';
    exports.items = [];
    exports.fileReader = null;
    callback();
};

exports.getTargetStats = function (env, callback) {
    exports.getSourceStats({
        options: {
            source: {
                file: env.options.target.file
            }
        }
    }, callback);
};

exports.getSourceStats = function (env, callback) {
    fs.readFile(env.options.source.file + '.meta', {encoding: 'utf8'}, function (err, data) {
        if (err) {
            throw err;
        }
        data = JSON.parse(data);
        callback(null, {
            version: '1.0',
            cluster_status: 'green',
            docs: {
                total: data._docs
            },
            aliases: {}
        });
    });
};

exports.getMeta = function (env, callback) {
    log.info('Reading mapping from meta file ' + env.options.source.file + '.meta');
    fs.readFile(env.options.source.file + '.meta', {encoding: 'utf8'}, function (err, data) {
        if (err) {
            callback(err);
            return;
        }
        data = JSON.parse(data);
        if (data._index) {
            env.options.source.index = data._index;
            env.options.target.index = env.options.target.index ? env.options.target.index : env.options.source.index;
            delete data._index;
        }
        if (data._type) {
            env.options.source.type = data._type;
            env.options.target.type = env.options.target.type ? env.options.target.type : env.options.source.type;
            delete data._type;
        }
        delete data._scope;
        delete data._docs;
        callback(data);
    });
};

exports.putMeta = function (env, metadata, callback) {
    metadata._docs = env.statistics.docs.total;
    metadata._scope = 'all';
    if (env.options.source.index) {
        metadata._index = env.options.source.index;
        metadata._scope = 'index';
    }
    if (env.options.source.type) {
        metadata._type = env.options.source.type;
        metadata._scope = 'type';
    }
    log.info('Storing ' + metadata._scope + ' mapping in meta file ' + env.options.target.file + '.meta');

    var dir = '';
    path.dirname(env.options.target.file).split(path.sep).forEach(function (dirPart) {
        dir += dirPart + path.sep;
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir);
        }
    });

    fs.writeFile(env.options.target.file + '.meta', JSON.stringify(metadata, null, 2), {encoding: 'utf8'}, function (err) {
        if (err) {
            callback(err);
            return;
        }
        if (!env.options.target.compression) {
            fs.writeFile(env.options.target.file + '.data', '', function (err) {
                callback(err);
            });
        } else {
            exports.targetStream = through().pause();
            var out = fs.createWriteStream(env.options.target.file + '.data');
            exports.targetStream.pipe(zlib.createGzip()).pipe(out);
            callback();
        }
    });
};

function getNewlineMatches(buffer) {
    var matches = buffer.match(/\n/g);
    return matches !== null && buffer.match(/\n/g).length > 1;
}

function parseBuffer() {
    var nlIndex1 = exports.buffer.indexOf('\n');
    var nlIndex2 = exports.buffer.indexOf('\n', nlIndex1 + 1);
    var metaData = JSON.parse(exports.buffer.substr(0, nlIndex1));
    var data = JSON.parse(exports.buffer.substr(nlIndex1 + 1, nlIndex2 - nlIndex1));
    exports.buffer = exports.buffer.substr(nlIndex2 + 1);
    exports.items.push({
        _id: metaData.index._id,
        _index: metaData.index._index,
        _type: metaData.index._type,
        _version: metaData.index._version,
        fields: {
            _timestamp: metaData.index._timestamp,
            _percolate: metaData.index._percolate,
            _routing: metaData.index._routing,
            _parent: metaData.index._parent,
            _ttl: metaData.index._ttl
        },
        _source: data
    });
}

exports.getData = function (env, callback) {
    if (exports.fileReader === null) {
        exports.fileReader = fs.createReadStream(env.options.source.file + '.data');
        if (env.options.source.compression) {
            exports.fileReader = exports.fileReader.pipe(zlib.createGunzip());
        }
        exports.fileReader.on('data', function (chunk) {
            exports.fileReader.pause();
            exports.buffer += chunk;
            while (getNewlineMatches(exports.buffer)) {
                parseBuffer();
                if (exports.items.length >= 100) {
                    callback(null, exports.items);
                    exports.items = [];
                }
            }
            exports.fileReader.resume();
        });
        exports.fileReader.on('end', function () {
            if (exports.buffer.length) {
                exports.buffer += '\n';
                parseBuffer();
            }
            callback(null, exports.items);
        });
    }
};

exports.putData = function (env, docs, callback) {
    if (exports.targetStream) {
        exports.targetStream.queue(docs).resume();
        callback();
    } else {
        fs.appendFile(env.options.target.file + '.data', docs, {encoding: 'utf8'}, function (err) {
            callback(err);
        });
    }
};

exports.end = function() {
    if (exports.targetStream) {
        exports.targetStream.end();
    } else {
        process.exit(0);
    }
};