const http = require('http');

const async = require('async');
const es = require('elasticsearch');
const JSON = require('json-bigint');

const Driver = require('./driver.interface');
const log = require('../log.js');
const SemVer = require('../semver');


class Elasticsearch extends Driver {
    constructor() {
        super();
        this.id = 'elasticsearch-client';
    }

    getInfo(callback) {
        let info = {
            id: this.id,
            name: 'ElasticSearch Client Driver',
            version: '1.0',
            description: 'An Elasticsearch driver that makes use of the scrolling API of the official elasticsearch.js client'
        };
        let options = {
            source: {
                version: {
                    abbr: 'v',
                    preset: '5.0',
                    help: 'The api version of the elasticsearch.js client to use'
                }, host: {
                    abbr: 'h',
                    preset: 'http://localhost:9200',
                    help: 'The host from which data is to be exported from'
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
                    help: 'Specifies the default http auth as a string with username and password separated by a colon (eg. user:pass)'
                }, ssl: {
                    abbr: 'u',
                    help: 'See https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html#config-ssl for details',
                }, size: {
                    abbr: 'z',
                    help: 'The maximum number of results to be returned per query.',
                    preset: 100,
                    min: 1
                }
            }, target: {
                version: {
                    abbr: 'v',
                    preset: '5.0',
                    help: 'The api version of the elasticsearch.js client to use'
                }, host: {
                    abbr: 'h',
                    help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                    preset: 'http://localhost:9200'
                }, index: {
                    abbr: 'i',
                    help: 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
                }, type: {
                    abbr: 't',
                    help: 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
                }, auth: {
                    abbr: 'a',
                    help: 'Specifies the default http auth as a string with username and password separated by a colon (eg. user:pass)'
                }, overwrite: {
                    abbr: 'o',
                    help: 'Allows to preserve already imported docs in the target database, so that changes are not overwritten',
                    preset: true,
                    flag: true
                }, ssl: {
                    abbr: 'u',
                    help: 'See https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html#config-ssl for details',
                }, replicas: {
                    abbr: 'r',
                    help: 'Sets the number of replicas the target index should be initialized with (only works with new indices).'
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
            opts.target.index = opts.target.index || opts.source.index;
            opts.target.type = opts.target.type || opts.source.type;
            if (opts.source.host != opts.target.host) { return callback(); }
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
            this.source = new es.Client({
                host: env.options.source.host,
                httpAuth: env.options.source.auth,
                apiVersion: env.options.source.version,
                maxSockets: env.options.source.maxSockets,
                ssl: env.options.source.ssl
            });
        }
        if (env.options.drivers.target == this.id) {
            this.target = new es.Client({
                host: env.options.target.host,
                httpAuth: env.options.target.auth,
                apiVersion: env.options.target.version,
                maxSockets: env.options.target.maxSockets,
                ssl: env.options.target.ssl
            });
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
                this.target.info({}, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.version = new SemVer(data.version.number);
                    subCallback();
                });
            },
            subCallback => {
                this.target.cluster.health({}, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    console.log(data)
                    stats.status = data.status;
                    subCallback();
                });
            },
            subCallback => {
                this.target.cat.aliases({ format: 'json' }, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let entry of data) {
                        stats.indices.push(entry.index);
                        stats.aliases[entry.alias] = entry.index;
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
                this.source.info({}, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.version = new SemVer(data.version.number);
                    subCallback();
                });
            },
            subCallback => {
                this.source.cluster.health({}, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    stats.status = data.status;
                    subCallback();
                });
            },
            subCallback => {
                this.source.cat.aliases({format: 'json' }, (err, data) => {
                    if (err) {
                        return subCallback(err);
                    }
                    for (let entry of data) {
                        stats.indices.push(entry.index);
                        stats.aliases[entry.alias] = entry.index;
                    }
                    subCallback();
                });
            },
            subCallback => {
                this.source.count({
                    index: env.options.source.index,
                    type: env.options.source.type,
                    body: env.options.source.query
                }, (err, data) => {
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
                this.source.indices.getMapping({
                    index: env.options.source.index,
                    type: env.options.source.type
                }, (err, data) => {
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
                this.source.indices.getSettings({
                    index: env.options.source.index,
                    type: env.options.source.type
                }, (err, data) => {
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
        let that = this;
        function createIndexTask(index) {
            return callback => {
                let body = {settings: metadata.settings[index] ? metadata.settings[index] : {}};
                if (env.options.target.replicas) {
                    body.settings.number_of_replicas = env.options.target.replicas;
                }
                that.target.indices.create({ index, body }, err => {
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
                let mapping = {};
                mapping[type] = metadata.mappings[index][type];
                that.target.indices.putMapping({ index, type, body: mapping }, err => {
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
        let stored_fields = [ '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl' ];
        let size = env.options.source.size;
        let query = env.options.source.query;
        if (env.options.source.index) {
            let indices = env.options.source.index.split(',');
            let indexQuery = { indices: { indices, query, no_match_query: 'none' }};
            if (env.options.source.type) {
                if (env.statistics.source.version.lt(2.0)) {
                    return { stored_fields, size, query: indexQuery, filter: { type: { value: env.options.source.type }} };
                }
                let payload = { stored_fields, size, query: { bool: { must: [ indexQuery], should: [], minimum_should_match: 1}}};
                for (let type of env.options.source.type.split(',')) {
                    payload.query.bool.should.push({ type: { value: type }});
                }
                return payload;
            }
            return { stored_fields, size, query: indexQuery};
        }
        return { stored_fields, size, query };
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
            this.source.scroll({
                scroll: '5m',
                scrollId: this.scrollId
            }, (err, data) => {
                if (err) {
                    return callback(err);
                }
                this.scrollId = data._scroll_id;
                callback(null, data.hits ? data.hits.hits : []);
            });
        } else {
            this.source.search({
                index: env.options.source.index,
                type: env.options.source.type,
                scroll: '5m',
                sort: '_doc',
                body: query
            }, (err, data) => {
                if (err) {
                    return callback(err);
                }
                this.scrollId = data._scroll_id;
                data.hits && callback(null, data.hits.hits);
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
        this.target.bulk({ body: data }, (err, data) => {
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
