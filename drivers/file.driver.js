var fs = require('fs');
var path = require('path');
var async = require('async');
var log = require('../log.js');

var id = 'file';

exports.sourceStream = null;
exports.targetStream = null;
exports.targetArchive = null;

exports.getInfo = function (callback) {
    var info = {
        id: id,
        name: 'Multi File Driver',
        version: '1.0',
        desciption: 'A driver to read and store data in a local file. Contents are automatically zipped.'
    };
    var options = {
        source: {
            file: {
                abbr: 'f',
                help: 'The directory from which the data should be imported.',
                required: true
            }, index: {
                abbr: 'i',
                help: 'The index name from which to export data from. If no index is given, the entire file is exported'
            }, type: {
                abbr: 't',
                help: 'The type from which to export data from. If no type is given, the entire file is exported'
            }
        }, target: {
            file: {
                abbr: 'f',
                help: 'The directory to which the data should be exported.',
                required: true
            }
        }
    };
    callback(null, info, options);
};

exports.verifyOptions = function (opts, callback) {
    var err = [];
    if (opts.drivers.source == id) {
        if (!fs.existsSync(opts.source.file)) {
            err.push('The source file ' + opts.source.file + ' could not be found!');
        }
    }
     if (opts.drivers.target == id) {
         if (fs.existsSync(opts.target.file)) {
             log.info('Warning: ' + opts.target.file + ' already exists');
         }
     }
    callback(err);
};

exports.reset = function (env, callback) {
    exports.targetStream = null;
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback(null, {
        version: '1.0',
        cluster_status: 'green',
        indices: []
    });
};

exports.getSourceStats = function (env, callback) {
    callback(null, {
        version: '1.0',
        cluster_status: 'green',
        docs: {
            total: 2 // TODO
        },
        aliases: {}
    });
};

exports.archive = {
    files: {},
    createParentDir: function (location) {
        var dir = '';
        path.dirname(location).split(path.sep).forEach(function (dirPart) {
            dir += dirPart + path.sep;
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir);
            }
        });
    },
    write: function(env, index, type, name, data, callback) {
        var directory = env.options.target.file + path.sep + index + (type ? path.sep + type : '') + path.sep +name;
        if (!this.files[directory]) {
            this.createParentDir(directory);
            this.files[directory] = true;
            fs.writeFileSync(directory, data, {encoding: 'utf8'});
            callback();
            return;
        }
        fs.appendFile(directory, '\n' + data, {encoding: 'utf8'}, callback);
    },
    read: function(env, index, type, name, callback) {
        var directory = env.options.source.file + path.sep + index + (type ? path.sep + type : '') + path.sep + name;
        fs.readFile(directory, {encoding: 'utf8'}, function(err, data) {
            callback(err, JSON.parse(data));
        });
    }
};

exports.getMeta = function (env, callback) {
    var metadata = {
        mappings: {},
        settings: {}
    };
    var taskParams = [];

    var dir = fs.readdirSync(env.options.source.file);
    var indices = env.options.source.index ? env.options.source.index.split(',') : [];
    var types = env.options.source.type ? env.options.source.type.split(',') : [];
    for (var i in dir) {
        var index = dir[i];
        if (!indices.length || indices.indexOf(index) != -1) {
            if (fs.statSync(env.options.source.file + path.sep + index).isDirectory()) {
                metadata.mappings[index] = {};
                metadata.settings[index] = {};
                taskParams.push([index, null, 'settings']);
                var indexDir = fs.readdirSync(env.options.source.file + path.sep + index);
                for (var j in indexDir) {
                    var type = indexDir[j];
                    if (!types.length || types.indexOf(type) != -1) {
                        if (fs.statSync(env.options.source.file + path.sep + index + path.sep + type).isDirectory()) {
                            metadata.mappings[index][type] = {};
                            taskParams.push([index, type, 'mapping']);
                        }
                    }
                }
            }
        }
    }
    async.map(taskParams, function (item, callback) {
        log.debug('Reading %s for index [%s] type [%s]', item[2], item[0], item[1]);
        exports.archive.read(env, item[0], item[1], item[2], function(err, data) {
            if (item[2] == 'settings') {
                metadata.settings[item[0]] = data;
            } else {
                metadata.mappings[item[0]][item[1]] = data;
            }
            callback(err);
        });
    }, function(err) {
        callback(err, metadata);
    });
};

exports.putMeta = function (env, metadata, callback) {
    var taskParams = [];
    for (var index in metadata.mappings) {
        var types = metadata.mappings[index];
        for (var type in types) {
            var mapping = types[type];
            taskParams.push([index, type, 'mapping', JSON.stringify(mapping, null, 2)]);
        }
    }
    for (var index in metadata.settings) {
        var setting = metadata.settings[index];
        taskParams.push([index, null, 'settings', JSON.stringify(setting, null, 2)]);
    }
    async.map(taskParams, function(item, callback) {
        log.debug('Writing %s for index [%s] type [%s]', item[2], item[0], item[1]);
        exports.archive.write(env, item[0], item[1], item[2], item[3], callback);
    }, callback);
};

var dataFiles = [];

exports.prepareTransfer = function (env, isSource) {
    if (isSource) {
        var dir = fs.readdirSync(env.options.source.file);
        var indices = env.options.source.index ? env.options.source.index.split(',') : [];
        var types = env.options.source.type ? env.options.source.type.split(',') : [];
        for (var i in dir) {
            var index = dir[i];
            if (!indices.length || indices.indexOf(index) != -1) {
                if (fs.statSync(env.options.source.file + path.sep + index).isDirectory()) {
                    var indexDir = fs.readdirSync(env.options.source.file + path.sep + index);
                    for (var j in indexDir) {
                        var type = indexDir[j];
                        if (!types.length || types.indexOf(type) != -1) {
                            if (fs.statSync(env.options.source.file + path.sep + index + path.sep + type).isDirectory()) {
                                dataFiles.push(env.options.source.file + path.sep + index + path.sep + type + path.sep + 'data');
                            }
                        }
                    }
                }
            }
        }
    }
};

var stream = null;

exports.getData = function (env, callback) {
    if (dataFiles.length) {
        if (stream === null) {
            var file = dataFiles.pop();
            var buffer = '';
            var items = [];
            stream = fs.createReadStream(file);
            stream.on('data', function (chunk) {
                stream.pause();
                buffer += chunk;
                while (buffer.indexOf('\n') > 0) {
                    var endOfLine = buffer.indexOf('\n')
                    var line = buffer.substr(0, endOfLine);
                    buffer = buffer.substr(endOfLine);
                    items.push(JSON.parse(line));
                    if (items.length >= env.options.run.step) {
                        callback(null, items);
                        items = [];
                    }
                }
                stream.resume();
            });
            stream.on('end', function () {
                stream = null;
                while (buffer.indexOf('\n') > 0) {
                    var endOfLine = buffer.indexOf('\n')
                    var line = buffer.substr(0, endOfLine);
                    buffer = buffer.substr(endOfLine);
                    items.push(JSON.parse(line));
                }
                if (buffer.length) {
                    items.push(JSON.parse(buffer));
                }
                callback(null, items);
            });
        }
    } else {
        callback();
    }
};

exports.putData = function (env, docs, callback) {
    var taskParams = [];
    for (var i in docs) {
        var doc = docs[i];
        taskParams.push([doc._index, doc._type, JSON.stringify(doc)]);
    }
    async.map(taskParams, function (item, callback) {
        exports.archive.write(env, item[0], item[1], 'data', item[2], callback);
    }, callback);
};