'use strict';

var fs = require('graceful-fs');
var path = require('path');

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line

var Driver = require('./driver.interface');
var log = require('../log.js');


class File extends Driver {
    constructor() {
        super();
        this.id = 'file';
        this.reset();
        this.archive = {
            files: {},
            path: (file, index, type, name) => file + (index ? path.sep + index : '') + (type ? path.sep + type : '') + path.sep + name,
            createParentDir: location => {
                let dir = '';
                path.dirname(location).split(path.sep).forEach(dirPart => {
                    dir += dirPart + path.sep;
                    if (!fs.existsSync(dir)) {
                        fs.mkdirSync(dir);
                    }
                });
            },
            write: (file, overwrite, index, type, name, data, callback) => {
                let directory = this.archive.path(file, index, type, name);
                if (!this.archive.files[directory]) {
                    this.archive.createParentDir(directory);
                    this.archive.files[directory] = true;
                }
                if (overwrite) {
                    fs.writeFile(directory, data, 'utf8', callback);
                } else {
                    fs.appendFile(directory, '\n' + data, 'utf8', callback);
                }
            },
            read: (file, index, type, name, callback) => {
                let directory = this.archive.path(file, index, type, name);
                fs.readFile(directory, 'utf8', (err, data) => {
                    try {
                        data = JSON.parse(data);
                    } catch (e) {}
                    callback(err, data);
                });
            }
        };
    }

    getInfo(callback) {
        let info = {
            id: this.id,
            name: 'Multi File Driver',
            version: '1.0',
            description: 'A driver to read and store data in a local file. Contents are automatically zipped.'
        };
        let options = {
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
    }

    verifyOptions(opts, callback) {
        let err = [];
        if (opts.drivers.source == this.id) {
            if (!fs.existsSync(opts.source.file)) {
                err.push('The source file ' + opts.source.file + ' could not be found!');
            }
            if (opts.source.type && !opts.source.index) {
                err.push('Unable to read a type with an index being specified');
            }
        }
        if (opts.drivers.target == this.id) {
            if (fs.existsSync(opts.target.file)) {
                log.info('Warning: ' + opts.target.file + ' already exists, duplicate entries might occur');
            }
        }
        callback(err);
    }

    reset(env, callback) {
        this.counts = {};
        this.dataFiles = [];
        this.stream = null;
        callback && callback();
    }

    getTargetStats(env, callback) {
        callback(null, {
            version: '1.0',
            cluster_status: 'green'
        });
    }

    getSourceStats(env, callback) {
        let stats = {
            version: '1.0',
            cluster_status: 'red',
            docs: {},
            aliases: {}
        };
        let indices = env.options.source.index ? env.options.source.index.split(',') : [];
        let types = env.options.source.type ? env.options.source.type.split(',') : [];
        if (!indices.length && !types.length) {
            this.archive.read(env.options.source.file, null, null, 'count', (err, data) => {
                if (err) {
                    return callback(err, stats);
                }
                stats.docs.total = parseInt(data);
                stats.cluster_status = 'green';
                callback(null, stats);
            });
            return;
        }

        function readTask(index, type) {
            return callback => this.archive.read(env.options.source.file, index, type, 'count', callback);
        }

        let readTasks = [];
        for (let index of indices) {
            if (!types.length) {
                readTasks.push(readTask(index));
            } else {
                for (let type of types) {
                    readTasks.push(readTask(index, type));
                }
            }
        }
        async.parallel(readTasks, (err, result) => {
            if (err) {
                return callback(err, stats);
            }
            let sum = 0;
            for (let i in result) {
                sum += parseInt(result[i]);
            }
            stats.docs.total = sum;
            stats.cluster_status = 'green';
            callback(null, stats);
        });
    }

    getMeta(env, callback) {
        let metadata = {
            mappings: {},
            settings: {}
        };
        let taskParams = [];

        let dir = fs.readdirSync(env.options.source.file);
        let indices = env.options.source.index ? env.options.source.index.split(',') : [];
        let types = env.options.source.type ? env.options.source.type.split(',') : [];
        for (let index of dir) {
            if (!indices.length || indices.indexOf(index) != -1) {
                if (fs.statSync(env.options.source.file + path.sep + index).isDirectory()) {
                    metadata.mappings[index] = {};
                    metadata.settings[index] = {};
                    taskParams.push([index, null, 'settings']);
                    let indexDir = fs.readdirSync(env.options.source.file + path.sep + index);
                    for (let type of indexDir) {
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
        async.map(taskParams, (item, callback) => {
            log.debug('Reading %s for index [%s] type [%s]', item[2], item[0], item[1]);
            this.archive.read(env.options.source.file, item[0], item[1], item[2], (err, data) => {
                if (item[2] == 'settings') {
                    metadata.settings[item[0]] = data;
                } else {
                    metadata.mappings[item[0]][item[1]] = data;
                }
                callback(err);
            });
        }, err => callback(err, metadata));
    }

    putMeta(env, metadata, callback) {
        let taskParams = [];
        for (let index in metadata.mappings) {
            let types = metadata.mappings[index];
            for (let type in types) {
                let mapping = types[type];
                taskParams.push([index, type, 'mapping', JSON.stringify(mapping, null, 2)]);
            }
        }
        for (let index in metadata.settings) {
            let setting = metadata.settings[index];
            taskParams.push([index, null, 'settings', JSON.stringify(setting, null, 2)]);
        }
        async.map(taskParams, (item, callback) => {
            log.debug('Writing %s for index [%s] type [%s]', item[2], item[0], item[1]);
            this.archive.write(env.options.target.file, true, item[0], item[1], item[2], item[3], callback);
        }, callback);
    }

    prepareTransfer(env, isSource) {
        if (isSource) {
            let dir = fs.readdirSync(env.options.source.file);
            let indices = env.options.source.index ? env.options.source.index.split(',') : [];
            let types = env.options.source.type ? env.options.source.type.split(',') : [];
            for (let index of dir) {
                if (!indices.length || indices.indexOf(index) != -1) {
                    if (fs.statSync(env.options.source.file + path.sep + index).isDirectory()) {
                        let indexDir = fs.readdirSync(env.options.source.file + path.sep + index);
                        for (let type of indexDir) {
                            if (!types.length || types.indexOf(type) != -1) {
                                if (fs.statSync(env.options.source.file + path.sep + index + path.sep + type).isDirectory()) {
                                    this.dataFiles.push(env.options.source.file + path.sep + index + path.sep + type + path.sep + 'data');
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    getData(env, callback) {
        if (!this.dataFiles.length) {
            return callback();
        }
        if (this.stream === null) {
            let file = this.dataFiles.pop();
            let buffer = '';
            let items = [];
            this.stream = fs.createReadStream(file);
            this.stream.on('data', chunk => {
                this.stream.pause();
                buffer += chunk;
                while (buffer.indexOf('\n') > 0) {
                    let endOfLine = buffer.indexOf('\n');
                    let line = buffer.substr(0, endOfLine);
                    buffer = buffer.substr(endOfLine);
                    try {
                        items.push(JSON.parse(line));
                        if (items.length >= env.options.run.step) {
                            callback(null, items);
                            items = [];
                        }
                    } catch (e) {
                        if (e.text != '\n') {
                            log.debug("File driver couldn't read JSON line from source " + e.text);
                        }
                    }
                }
                this.stream.resume();
            });
            this.stream.on('end', () => {
                this.stream = null;
                while (buffer.indexOf('\n') >= 0) {
                    let endOfLine = buffer.indexOf('\n');
                    if (endOfLine == 0) {
                        buffer = buffer.substr(1);
                        continue;
                    }
                    let line = buffer.substr(0, endOfLine);
                    buffer = buffer.substr(endOfLine);
                    items.push(JSON.parse(line));
                }
                if (buffer.length) {
                    items.push(JSON.parse(buffer));
                }
                callback(null, items);
            });
        }
        // TODO check if fallthrough for callback is expected
    }

    putData(env, docs, callback) {
        let taskParams = [];
        for (let doc of docs) {
            if (!this.counts[doc._index]) {
                this.counts[doc._index] = {};
            }
            if (!this.counts[doc._index][doc._type]) {
                this.counts[doc._index][doc._type] = 0;
            }
            this.counts[doc._index][doc._type]++;
            taskParams.push([doc._index, doc._type, JSON.stringify(doc)]);
        }
        async.map(taskParams, (item, callback) => {
            this.archive.write(env.options.target.file, false, item[0], item[1], 'data', item[2], callback);
        }, callback);
    }

    _sumUpIndex(env, index, callback) {
        let readTasks = {};

        let directory = env.options.target.file + path.sep + index;
        fs.readdir(directory, (err, files) => {
            if (err) {
                return callback(err);
            }
            for (let type of files) {
                let typeDirectory = this.archive.path(env.options.target.file, index, type, 'count');
                try {
                    if (fs.statSync(typeDirectory).isDirectory) {
                        readTasks[type] = callback => this.archive.read(env.options.target.file, index, type, 'count', callback);
                    }
                } catch (e) {}
            }
            async.parallel(readTasks, (err, result) => {
                if (err) {
                    return callback(err);
                }
                let sum = 0;
                for (let type in result) {
                    sum += parseInt(result[type]);
                }
                fs.writeFile(directory + path.sep + 'count', sum, err => callback(err, sum));
            });
        });
    }

    end(env) {
        if (!env.options.target.file) {
            return;
        }

        let indexTasks = {};
        for (let index in this.counts) {
            indexTasks[index] = callback => {
                let typeTasks = {};
                for (let type in this.counts[index]) {
                    typeTasks[type] = callback => {
                        this.archive.read(env.options.target.file, index, type, 'count', (err, data) => {
                            if (!err && !isNaN(data)) {
                                this.counts[index][type] += parseInt(data);
                            }
                            let directory = this.archive.path(env.options.target.file, index, type, 'count');
                            fs.writeFile(directory, this.counts[index][type], {encoding: 'utf8'}, callback);
                        });
                    };
                }
                async.parallel(typeTasks, err => {
                    if (err) {
                        return callback(err);
                    }
                    this._sumUpIndex(env, index, callback);
                });
            };
        }

        async.parallel(indexTasks, (err, result) => {
            if (err) {
                return log.error(err);
            }
            let sum = 0;
            for (let index in result) {
                sum += parseInt(result[index]);
            }
            fs.writeFile(this.archive.path(env.options.target.file, null, null, 'count'), sum, err => err && log.error(err));
        });
    }
}

module.exports = new File();
