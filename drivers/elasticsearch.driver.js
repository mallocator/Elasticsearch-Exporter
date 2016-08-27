'use strict';

var http = require('http');

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line

var Driver = require('./driver.interface');
var log = require('../log.js');
var request = require('../request');
var SemVer = require('../semver');

class Elasticsearch extends Driver {
    constructor() {
        super();
        this.id = 'elasticsearch';
    }

    getInfo(callback) {
        let info = {
            id: this.id,
            name: 'ElasticSearch Scroll Driver',
            version: '1.0',
            description: 'An Elasticsearch driver that makes use of the scrolling API to read data'
        };
        let options = {
            source: {
                host: {
                    abbr: 'h',
                    preset: 'localhost',
                    help: 'The host from which data is to be exported from'
                }, port: {
                    abbr: 'p',
                    preset: 9200,
                    help: 'The port of the source host to talk to',
                    min: 0,
                    max: 65535
                }, index: {
                    abbr: 'i',
                    help: 'The index name from which to export data from. If no index is given, the entire database is exported. Multiple indices can be specified separated by comma.'
                }, type: {
                    abbr: 't',
                    help: 'The type from which to export data from. If no type is given, the entire index is exported. Multiple types can be specified separated by comma.'
                }, query: {
                    abbr: 'q',
                    help: 'Define a query that limits what kind of documents are exporter from the source',
                    preset: '{"match_all":{}}'
                }, auth: {
                    abbr: 'a',
                    help: 'Set authentication parameters for reaching the source Elasticsearch cluster'
                }, proxy: {
                    abbr: 'P',
                    help: 'Set an http proxy to use for all source requests.'
                }, useSSL: {
                    abbr: 'u',
                    help: 'Will attempt to connect to the source driver using https',
                    flag: true
                }, insecure: {
                    abbr: 'x',
                    help: 'Allow connections to SSL site without certs or with incorrect certs.',
                    flag: true
                }, size: {
                    abbr: 'z',
                    help: 'The maximum number of results to be returned per query.',
                    preset: 100,
                    min: 1
                }, cpuLimit: {
                    abbr: 'c',
                    help: 'Set the max cpu load that the source server should reach (on any node) before the exporter starts waiting',
                    preset: 100,
                    min: 1,
                    max: 100
                }
            }, target: {
                host: {
                    abbr: 'h',
                    help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                    preset: 'localhost'
                }, port: {
                    abbr: 'p',
                    preset: 9200,
                    help: 'The port of the target host to talk to',
                    min: 0,
                    max: 65535
                }, index: {
                    abbr: 'i',
                    help: 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
                }, type: {
                    abbr: 't',
                    help: 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
                }, auth: {
                    abbr: 'a',
                    help: 'Set authentication parameters for reaching the target Elasticsearch cluster'
                }, overwrite: {
                    abbr: 'o',
                    help: 'Allows to preserve already imported docs in the target database, so that changes are not overwritten',
                    preset: true,
                    flag: true
                }, proxy: {
                    abbr: 'P',
                    help: 'Set an http proxy to use for all target requests.'
                }, useSSL: {
                    abbr: 'u',
                    metavar: 'true|false',
                    help: 'Will attempt to connect to the target driver using https',
                    flag: true
                }, insecure: {
                    abbr: 'x',
                    help: 'Allow connections to SSL site without certs or with incorrect certs.',
                    flag: true
                }, replicas: {
                    abbr: 'r',
                    help: 'Sets the number of replicas the target index should be initialized with (only works with new indices).'
                }, cpuLimit: {
                    abbr: 'c',
                    help: 'Set the max cpu load that the target server should reach (on any node) before the exporter starts waiting',
                    preset: 100,
                    min: 1,
                    max: 100
                }
            }
        };
        callback(null, info, options);
    }

    verifyOptions(opts, callback) {
        if (opts.source.query) {
            try {
                opts.source.query = JSON.parse(opts.source.query);
            } catch(e) {}
        }
        if (opts.drivers.source == this.id && opts.drivers.target == this.id) {
            opts.target.host = opts.target.host || opts.source.host;
            opts.target.port = opts.target.port || opts.source.port;
            opts.target.index = opts.target.index || opts.source.index;
            opts.target.type = opts.target.type || opts.source.type;
            opts.source.proxy = opts.source.proxy || process.env.HTTP_PROXY || process.env.http_proxy;
            if (opts.source.host != opts.target.host) { return callback(); }
            if (opts.source.port != opts.target.port) { return callback();}
            if (opts.source.index != opts.target.index) { return callback(); }
            if (opts.source.type != opts.target.type && opts.source.index) { return callback(); }
        } else {
            let optSet = opts.drivers.source == this.id ? opts.source : opts.target;
            if (optSet.host) { return callback(); }
        }
        callback('Not enough information has been given to be able to perform an export. Please review the OPTIONS and examples again.');
    }

    reset(env, callback) {
        if (env.options.drivers.source == this.id) {
            this.scrollId = null;
            if (env.options.source.insecure) {
                process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
            }
        }
        if (env.options.drivers.target == this.id) {
            if (env.options.target.maxSockets) {
                http.globalAgent.maxSockets = env.options.target.maxSockets;
            }
            if (env.options.target.insecure) {
                process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
            }
        }
        callback && callback();
    }

    getTargetStats(env, callback) {
        let stats = {
            aliases: {},
            indices: []
        };

        async.parallel([
            subCallback => {
                request.target.get(env, '/', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.version = new SemVer(data.version.number);
                    subCallback();
                });
            },
            subCallback => {
                request.target.get(env, '/_cluster/health', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.status = data.status;
                    subCallback();
                });
            },
            subCallback => {
                request.target.get(env, '/_alias', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let index in data) {
                        stats.indices.push(index);
                        for (let alias in data[index].aliases) {
                            stats.aliases[alias] = index;
                        }
                    }
                    subCallback();
                });
            }
        ], err => {
            log.debug('ElasticSearch target version: ', stats.version);
            // TODO print information about number of nodes of target
            callback(err, stats);
        });
    }

    getSourceStats(env, callback) {
        let stats = {
            aliases: {},
            indices: [],
            docs: {}
        };

        async.parallel([
            subCallback => {
                request.source.get(env, '/', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.version = new SemVer(data.version.number);
                    subCallback();
                });
            },
            subCallback => {
                request.source.get(env, '/_cluster/health', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.status = data.status;
                    subCallback();
                });
            },
            subCallback => {
                request.source.get(env, '/_alias', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let index in data) {
                        stats.indices.push(index);
                        for (let alias in data[index].aliases) {
                            stats.aliases[alias] = index;
                        }
                    }
                    subCallback();
                });
            },
            subCallback => {
                let uri = '/';
                if (env.options.source.index) {
                    uri += encodeURIComponent(env.options.source.index) + '/';
                }
                if (env.options.source.type) {
                    uri += encodeURIComponent(env.options.source.type)+ '/';
                }
                request.source.get(env, uri + '_count', {query: env.options.source.query}, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.docs.total = data.count;
                    subCallback();
                });
            }
        ], err => {
            log.debug('ElasticSearch source version: ', stats.version);
            // TODO print information about number of nodes of source
            callback(err, stats);
        });
    }

    getMeta(env, callback) {
        let settingsUri = '/';
        if (env.options.source.index) {
            settingsUri += encodeURIComponent(env.options.source.index) + '/';
        }
        let mappingsUri = settingsUri;
        if (env.options.source.type) {
            mappingsUri += encodeURIComponent(env.options.source.type) + '/';
        }

        let metadata = {
            mappings: {},
            settings: {}
        };

        async.parallel([
            subCallback => {
                request.source.get(env, mappingsUri + '_mapping', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let index in data) {
                        // TODO move this into the exporter so that it's not up to the driver to do transformations
                        let newIndex = index;
                        if (env.options.target.index && env.options.target.index != env.options.source.index && index == env.options.source.index) {
                            newIndex = env.options.target.index;
                        }
                        let mappings = data[index].mappings ? data[index].mappings : data[index];
                        metadata.mappings[newIndex] = {};
                        for (let type in mappings) {
                            let newType = type;
                            if (env.options.target.type&& env.options.target.type != env.options.source.type && type == env.options.source.type) {
                                newType = env.options.target.type;
                            }
                            metadata.mappings[newIndex][newType] = mappings[type];
                        }
                    }
                    subCallback();
                });
            },
            subCallback => {
                if (env.options.source.type) {
                    return subCallback();
                }
                request.source.get(env, settingsUri + '_settings', (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let index in data) {
                        // TODO move this into the exporter so that it's not up to the driver to do transformations
                        let newIndex = index;
                        if (env.options.target.index && env.options.target.index != env.options.source.index && index == env.options.source.index) {
                            index = env.options.target.index;
                        }
                        metadata.settings[newIndex] = data[index];
                    }
                    subCallback();
                });
            }
        ], err => callback(err, metadata));
    }

    putMeta(env, metadata, callback) {
        function createIndexTask(index) {
            return callback => {
                let body = {settings: metadata.settings[index] ? metadata.settings[index] : {}};
                if (env.options.target.replicas) {
                    body.settings.number_of_replicas = env.options.target.replicas;
                }
                request.target.put(env, '/' + encodeURIComponent(index), body, err => {
                    if (err) {
                        return callback(err);
                    }
                    env.statistics.target.indices.push(index);
                    log.debug('Created index ' + index + ' on target ElasticSearch instance');
                    callback();
                });
            };
        }

        function createTypeTask(index, type) {
            return callback => {
                let uri;
                if (env.statistics.target.version.le(0.9)) {
                    uri = '/' + encodeURIComponent(index) + '/' + encodeURIComponent(type) + '/_mapping';
                } else {
                    uri = '/' + encodeURIComponent(index)+ '/_mapping/' + encodeURIComponent(type);
                }
                let mapping = {};
                mapping[type] = metadata.mappings[index][type];
                request.target.put(env, uri, mapping, err => {
                    if (err) {
                        return callback(err);
                    }
                    log.debug('Created type ' + type + ' in target ElasticSearch instance on existing index ' + index);
                    callback();
                });
            };
        }

        let indexTasks = [], typeTasks = [];
        for (let index in metadata.mappings) {
            if (env.statistics.target.indices.indexOf(index) == -1) {
                indexTasks.push(createIndexTask(index));
            } else {
                log.debug('Not creating index ' + index + ' on target ElasticSearch instance because it already exists');
            }
            for (let type in metadata.mappings[index]) {
                typeTasks.push(createTypeTask(index, type));
            }
        }

        async.series([
            callback => {
                if (indexTasks.length > 0) {
                    log.debug('Creating indices in target ElasticSearch instance');
                    return async.parallel(indexTasks, callback);
                }
                callback();
            },
            callback => {
                log.debug('Creating types in target ElasticSearch instance');
                async.parallel(typeTasks, callback);
            }
        ], callback);
    }

    _getQuery(env) {
        var fields = [ '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl' ];
        var size = env.options.source.size;
        var query = env.options.source.query;
        if (env.options.source.index) {
            var indices = env.options.source.index.split(',');
            var indexQuery = { indices: { indices, query, no_match_query: 'none' }};
            if (env.options.source.type) {
                if (env.statistics.source.version.lt(2.0)) {
                    return { fields, size, query: indexQuery, filter: { type: { value: env.options.source.type }} };
                }
                var payload = { fields, size, query: { bool: { must: [ indexQuery], should: [], minimum_should_match: 1}}};
                for (let type of env.options.source.type.split(',')) {
                    payload.query.bool.should.push({ type: { value: type }});
                }
                return payload;
            }
            return { fields, size, query: indexQuery};
        }
        return { fields, size, query };
    }

    /**
     * Fetches data from ElasticSearch via a scroll/scan request.
     *
     * @param {Environment} env
     * @param {Driver~getDataCallback} callback Callback which is called when data has been received with the first
     *                                          argument as an array of hits, and the second the number of total hits.
     * @param {number} [from]                   Ignored since this driver does not support concurrency
     * @param {number} [size]                   Ignored since this driver does not support concurrency
     */
    getData(env, callback, from, size) {
        let query = this._getQuery(env);

        if (this.scrollId) {
            request.source.post(env, '/_search/scroll?scroll=60m', this.scrollId, (err, data) => {
                if (err) {
                    return callback(err);
                }
                this.scrollId = data._scroll_id;
                callback(null, data.hits ? data.hits.hits : []);
            });
        } else {
            request.source.post(env, '/_search?search_type=scan&scroll=60m', query, (err, data) => {
                if (err) {
                    return callback(err);
                }
                this.scrollId = data._scroll_id;
                this.getData(env, callback);
            });
        }
    }

    /**
     * Stores data using a bulk request.
     *
     * @param env
     * @param docs The data to transmit in ready to use bulk format.
     * @param callback Callback function that is called without any arguments when the data has been stored unless there was an error.
     */
    putData(env, docs, callback) {
        let op = env.options.target.overwrite ? 'index' : 'create';
        let data = '';
        docs.forEach(doc => {
            let metaData = {};
            metaData[op] = {
                _index: env.options.target.index ? env.options.target.index : doc._index,
                _type: env.options.target.type ? env.options.target.type : doc._type,
                _id: doc._id,
                _version: doc._version ? doc._version : null
            };
            if (doc._fields) {
                ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'].forEach(field => {
                    if (doc._fields[field]) {
                        metaData[op][field] = doc._fields[field];
                    }
                });
            }
            data += JSON.stringify(metaData) + '\n' + JSON.stringify(doc._source) + '\n';
        });
        request.target.post(env, '/_bulk', data, (err, data) => {
            if (err) {
                return callback(err);
            }
            if (data.errors) {
                for (let item of data.items) {
                    if (!item.index || item.index.status / 100 != 2) {
                        callback(JSON.stringify(item));
                        break;
                    }
                }
            } else {
                callback();
            }
        });
    }
}

module.exports = new Elasticsearch();
exports.request = request;
