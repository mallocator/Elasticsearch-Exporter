var fs = require('fs');
var path = require('path');

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line

var log = require('../log.js');


var id = 'file';

exports.sourceStream = null;
exports.targetStream = null;
exports.targetArchive = null;

exports.archive = {
    files: {},
    path: function(env, index, type, name) {
        return env.options.target.file + (index ? path.sep + index : '') + (type ? path.sep + type : '') + path.sep + name;
    },
    createParentDir: function (location) {
        var dir = '';
        path.dirname(location).split(path.sep).forEach(function (dirPart) {
            dir += dirPart + path.sep;
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir);
            }
        });
    },
    write: function (env, index, type, name, data, callback) {
        var directory = this.path(env, index, type, name);
        if (!this.files[directory]) {
            this.createParentDir(directory);
            this.files[directory] = true;
        }
        fs.appendFile(directory, '\n' + data, {encoding: 'utf8'}, callback);
    },
    read: function (env, index, type, name, callback) {
        var directory = this.path(env, index, type, name);
        fs.readFile(directory, {encoding: 'utf8'}, function (err, data) {
            try {
                data = JSON.parse(data);
            } catch (e) {}
            callback(err, data);
        });
    }
};

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
        if (opts.source.type && !opts.source.index) {
            err.push('Unable to read a type with an index being specified');
        }
    }
     if (opts.drivers.target == id) {
         if (fs.existsSync(opts.target.file)) {
             log.info('Warning: ' + opts.target.file + ' already exists, duplicate entries might occur');
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
    var stats = {
        version: '1.0',
        cluster_status: 'red',
        docs: {},
        aliases: {}
    };
    var indices = env.options.source.index ? env.options.source.index.split[','] : [];
    var types = env.options.source.type ? env.options.source.type.split[','] : [];
    if (!indices && !types) {
        exports.archive.read(env, null, null, 'count', function(err, data) {
            if (err) {
                callback(err, stats);
                return;
            }
            stats.docs.total = parseInt(data);
            stats.cluster_status = 'green';
        });
        return;
    }

    function readTask(index, type) {
        return function(callback) {
            exports.archive.read(env, index, type, 'count', callback);
        };
    }

    var readTasks = {};
    for (var i in indices) {
        var index = indices[i];
        if (!types.length) {
            readTasks.push(readTask(index));
        } else {
            for (var j in types) {
                var type = types[j];
                readTasks.push(readTask(index, type));
            }
        }
    }
    async.parallel(readTasks, function(err, result) {
        if (err) {
            callback(err, stats);
            return;
        }
        var sum = 0;
        for (var i in result) {
            sum += parseInt(result[i]);
        }
        stats.docs.total = sum;
        stats.cluster_status = 'green';
        callback(null, stats);
    });
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
                    var endOfLine = buffer.indexOf('\n');
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
                    var endOfLine = buffer.indexOf('\n');
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

exports.counts = {};

exports.putData = function (env, docs, callback) {
    var taskParams = [];
    for (var i in docs) {
        var doc = docs[i];
        if (!exports.counts[doc._index]) {
            exports.counts[doc._index] = {};
        }
        if (!exports.counts[doc._index][doc._type]) {
            exports.counts[doc._index][doc._type] = 0;
        }
        exports.counts[doc._index][doc._type]++;
        taskParams.push([doc._index, doc._type, JSON.stringify(doc)]);
    }
    async.map(taskParams, function (item, callback) {
        exports.archive.write(env, item[0], item[1], 'data', item[2], callback);
    }, callback);
};

exports.sumUpIndex = function(env, index, callback) {
    var readTasks = {};

    function readTask(type) {
        return function(callback) {
            exports.archive.read(env, index, type, 'count', callback);
        };
    }

    var directory = env.options.target.file + path.sep + index;
    fs.readdir(directory, function (err, files) {
        if (err) {
            callback(err);
            return;
        }
        for (var i in files) {
            var type = files[i];
            var typeDirectory = exports.archive.path(env, index, type, 'count');
            try {
                if (fs.statSync(typeDirectory).isDirectory) {
                    readTasks[type] = readTask(type);
                }
            } catch (e) {}
        }
        async.parallel(readTasks, function (err, result) {
            if (err) {
                callback(err);
                return;
            }
            var sum = 0;
            for (var type in result) {
                sum += parseInt(result[type]);
            }
            fs.writeFile(directory + path.sep + 'count', sum, function (err) {
                callback(err, sum);
            });
        });
    });
};

exports.end = function (env) {
    var indexTasks = {};

    function typeTask(index, type) {
        return function(callback) {
            exports.archive.read(env, index, type, 'count', function(err, data) {
                if (!err && !isNaN(data)) {
                    exports.counts[index][type] += parseInt(data);
                }
                var directory = exports.archive.path(env, index, type, 'count');
                fs.writeFile(directory, exports.counts[index][type], {encoding: 'utf8'}, callback);
            });
        };
    }

    function indexTask(index) {
        var typeTasks = {};
        return function(callback) {
            for (var type in exports.counts[index]) {
                typeTasks[type] = typeTask(index, type);
            }
            async.parallel(typeTasks, function(err) {
                if (err) {
                    callback(err);
                    return;
                }
                exports.sumUpIndex(env, index, callback);
            });
        };
    }

    for (var index in exports.counts) {
        indexTasks[index] = indexTask(index);
    }

    async.parallel(indexTasks, function (err, result) {
        if (err) {
            log.error(err);
            return;
        }
        var sum = 0;
        for (var index in result) {
            sum += parseInt(result[index]);
        }
        fs.writeFile(exports.archive.path(env, null, null, 'count'), sum, function(err) {
            if (err) {
                log.error(err);
            }
        });
    });
};