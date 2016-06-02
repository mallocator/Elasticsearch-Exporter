var http = require('http');
var https = require('https');
var url = require('url');

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line

var log = require('../log.js');


var id = 'elasticsearch';

exports.getInfo = (callback) => {
    let info = {
        id: id,
        name: 'ElasticSearch Scroll Driver',
        version: '1.0',
        desciption: 'An Elasticsearch driver that makes use of the scrolling API to read data'
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
                help: 'The index name from which to export data from. If no index is given, the entire database is exported'
            }, type: {
                abbr: 't',
                help: 'The type from which to export data from. If no type is given, the entire index is exported'
            }, query: {
                abbr: 'q',
                help: 'Define a query that limits what kind of documents are exporter from the source',
                preset: '{"match_all":{}}'
            }, auth: {
                abbr: 'a',
                help: 'Set authentication parameters for reaching the source Elasticsearch cluster'
            }, maxSockets: {
                abbr: 'm',
                help: 'Sets the maximum number of concurrent sockets for the global http agent',
                preset: 30,
                min: 1,
                max: 65535
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
            }, maxSockets: {
                abbr: 'm',
                help: 'Sets the maximum number of concurrent sockets for the global http agent',
                preset: 30,
                min: 1
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
};

exports.verifyOptions = (opts, callback) => {
    if (opts.source.query) {
        try {
            opts.source.query = JSON.parse(opts.source.query);
        } catch(e) {}
    }
    if (opts.drivers.source == id && opts.drivers.target == id) {
        opts.target.host = opts.target.host || opts.source.host;
        opts.target.port = opts.target.port || opts.source.port;
        opts.target.index = opts.target.index || opts.source.index;
        opts.target.type = opts.target.type || opts.source.type;
        if ((process.env.HTTP_PROXY || process.env.http_proxy) && !opts.source.proxy) {
            if (process.env.HTTP_PROXY) {
                opts.source.proxy = process.env.HTTP_PROXY;
            } else if (process.env.http_proxy) {
                opts.source.proxy = process.env.http_proxy;
            }
        }

        if (opts.source.host != opts.target.host) { return callback(); }
        if (opts.source.port != opts.target.port) { return callback();}
        if (opts.source.index != opts.target.index) { return callback(); }
        if (opts.source.type != opts.target.type && opts.source.index) { return callback(); }
    } else {
        let optSet = opts.drivers.source == id ? opts.source : opts.target;
        if (optSet.host) { return callback(); }
    }
    callback('Not enough information has been given to be able to perform an export. Please review the OPTIONS and examples again.');
};

var request = {
    buffer_concat: (buffers, nread) => {
        let buffer = null;
        switch (buffers.length) {
            case 0:
                buffer = new Buffer(0);
                break;
            case 1:
                buffer = buffers[0];
                break;
            default:
                buffer = new Buffer(nread);
                for (let i = 0, pos = 0, l = buffers.length; i < l; i++) {
                    let chunk = buffers[i];
                    chunk.copy(buffer, pos);
                    pos += chunk.length;
                }
                break;
        }
        let data = buffer.toString();
        try {
            return JSON.parse(data);
        } catch(e) {
            throw new Error("There was an error trying to parse a json response from the server. Server response:\n" + data);
        }
    },
    create: (httpProxy, ssl, host, port, auth, path, method, data, callback, errCallback) => {
        let protocol = ssl ? https : http;
        let buffer = null, err = null;
        let reqOpts = {
            host: host,
            port: port,
            path: path,
            auth: auth,
            headers: {},
            method: method
        };
        if (httpProxy) {
            let httpUrl = url.parse(httpProxy);
            reqOpts.host = httpUrl.hostname;
            reqOpts.port = httpUrl.port;
            reqOpts.path = 'http://' + host + ':' + port + path;
            reqOpts.headers.Host = host;
        }
        if (data) {
            if (typeof data == 'object') {
                data = JSON.stringify(data);
            }
            buffer = new Buffer(data, 'utf8');
            reqOpts.headers['Content-Length'] = buffer.length;
        }
        let req = protocol.request(reqOpts, res => {
            let buffers = [];
            let nread = 0;
            res.on('data', chunk => {
                buffers.push(chunk);
                nread += chunk.length;
            });
            res.on('end', () => !err && callback(request.buffer_concat(buffers, nread)));
        });
        req.on('error', e => {
            err = true;
            // TODO pretty print errors, such as "can't connect"
            switch (e.code) {
                case 'ECONNREFUSED':
                    errCallback('Unable to connect to host ' + host + ' on port ' + port);
                    break;
                default: errCallback(e);
            }
        });
        req.end(buffer);
    },
    wait: (env, cpuLimit, proxy, ssl, host, port, auth, callback, errCallback, timeout) => {
        if (cpuLimit>=100) {
            return callback();
        }
        timeout = timeout ? Math.min(timeout + 1, 30) : 1;
        let destination = (host == env.options.source.host) ? 'source' : 'target';
        request.create(proxy, ssl, host, port, auth, '/_nodes/stats/process', 'GET', null, nodesData => {
            for (let nodeName in nodesData.nodes) {
                let nodeCpu = nodesData.nodes[nodeName].process.cpu.percent;
                if (nodeCpu > cpuLimit) {
                    log.status('Waiting %s seconds for %s cpu to cool down. Current load is %s%%', timeout, destination, nodeCpu);
                    return setTimeout(request.wait, timeout * 1000, env, cpuLimit, proxy, ssl, host, port, auth, callback, errCallback, timeout);
                }
            }
            callback();
        }, errCallback);

    },
    source: {
        get: (env, path, data, callback, errCallback) => {
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            let s = env.options.source;
            request.wait(env, s.cpuLimit, s.proxy, s.useSSL, s.host, s.port, s.auth, () => {
                request.create(s.proxy, s.useSSL, s.host, s.port, s.auth, path, 'GET', data, callback, errCallback);
            }, errCallback);
        },
        post: (env, path, data, callback, errCallback) => {
            let s = env.options.source;
            request.wait(env, s.cpuLimit, s.proxy, s.useSSL, s.host, s.port, s.auth, () => {
                request.create(s.proxy, s.useSSL, s.host, s.port, s.auth, path, 'POST', data, callback, errCallback);
            }, errCallback);
        }
    },
    target: {
        get: (env, path, data, callback, errCallback) => {
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            let t = env.options.target;
            request.wait(env, t.cpuLimit, t.proxy, t.useSSL, t.host, t.port, t.auth, () => {
                request.create(t.proxy, t.useSSL, t.host, t.port, t.auth, path, 'GET', data, callback, errCallback);
            }, errCallback);
        },
        post: (env, path, data, callback, errCallback) => {
            let t = env.options.target;
            request.wait(env, t.cpuLimit, t.proxy, t.useSSL, t.host, t.port, t.auth, () => {
                request.create(t.proxy, t.useSSL, t.host, t.port, t.auth, path, 'POST', data, callback, errCallback);
            }, errCallback);
        },
        put: (env, path, data, callback, errCallback) => {
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            let t = env.options.target;
            request.wait(env, t.cpuLimit, t.proxy, t.useSSL, t.host, t.port, t.auth, () => {
                request.create(t.proxy, t.useSSL, t.host, t.port, t.auth, path, 'PUT', data, callback, errCallback);
            }, errCallback);
        }
    }
};
exports.request = request;

exports.reset = (env, callback) => {
    if (env.options.drivers.source == id) {
        exports.scrollId = null;
        if (env.options.source.maxSockets) {
            http.globalAgent.maxSockets = env.options.source.maxSockets;
        }
        if (env.options.source.insecure) {
            process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
        }
    }
    if (env.options.drivers.target == id) {
        if (env.options.target.maxSockets) {
            http.globalAgent.maxSockets = env.options.target.maxSockets;
        }
        if (env.options.target.insecure) {
            process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
        }
    }
    callback();
};

exports.getTargetStats = (env, callback) => {
    let stats = {
        aliases: {},
        indices: []
    };

    async.parallel([
        subCallback => {
            request.target.get(env, '/', data => {
                stats.version = data.version.number;
                subCallback();
            }, subCallback);
        },
        subCallback => {
            request.target.get(env, '/_cluster/health', data => {
                stats.status = data.status;
                subCallback();
            }, subCallback);
        },
        subCallback => {
            request.target.get(env, '/_cluster/state', data => {
                for (let index in data.metadata.indices) {
                    stats.indices.push(index);
                    if (data.metadata.indices[index].aliases.length) {
                        data.metadata.indices[index].aliases.forEach(alias => {
                            stats.aliases[alias] = index;
                        });
                    }
                }
                subCallback();
            }, subCallback);
        }
    ], err => {
        log.debug('ElasticSearch target version: ', stats.version);
        // TODO print information about number of nodes of target
        callback(err, stats);
    });
};

exports.getSourceStats = (env, callback) => {
    let stats = {
        aliases: {},
        indices: [],
        docs: {}
    };

    async.parallel([
        subCallback => {
            request.source.get(env, '/', data => {
                stats.version = data.version.number;
                subCallback();
            }, subCallback);
        },
        subCallback => {
            request.source.get(env, '/_cluster/health', data => {
                stats.status = data.status;
                subCallback();
            }, subCallback);
        },
        subCallback => {
            request.source.get(env, '/_cluster/state', data => {
                for (let index in data.metadata.indices) {
                    stats.indices.push(index);
                    if (data.metadata.indices[index].aliases.length) {
                        data.metadata.indices[index].aliases.forEach(alias => {
                            stats.aliases[alias] = index;
                        });
                    }
                }
                subCallback();
            }, subCallback);
        },
        subCallback => {
            let uri = '/';
            if (env.options.source.index) {
                uri += encodeURIComponent(env.options.source.index) + '/';
            }
            if (env.options.source.type) {
                uri += encodeURIComponent(env.options.source.type)+ '/';
            }
            request.source.get(env, uri + '_count', {query: env.options.source.query}, data => {
                stats.docs.total = data.count;
                subCallback();
            }, subCallback);
        }
    ], err => {
        log.debug('ElasticSearch source version: ', stats.version);
        // TODO print information about number of nodes of source
        callback(err, stats);
    });
};

exports.getMeta = (env, callback) => {
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
            request.source.get(env, mappingsUri + '_mapping', data => {
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
            }, subCallback);
        },
        subCallback => {
            if (env.options.source.type) {
                return subCallback();
            }
            request.source.get(env, settingsUri + '_settings', data => {
                for (let index in data) {
                    // TODO move this into the exporter so that it's not up to the driver to do transformations
                    let newIndex = index;
                    if (env.options.target.index && env.options.target.index != env.options.source.index && index == env.options.source.index) {
                        index = env.options.target.index;
                    }
                    metadata.settings[newIndex] = data[index];
                }
                subCallback();
            }, subCallback);
        }
    ], err => {
        callback(err, metadata);
    });
};

exports.putMeta = (env, metadata, callback) => {
    function createIndexTask(index) {
        return callback => {
            let body = {settings: metadata.settings[index] ? metadata.settings[index] : {}};
            if (env.options.target.replicas) {
                body.settings.number_of_replicas = env.options.target.replicas;
            }
            request.target.put(env, '/' + encodeURIComponent(index), body, () => {
                env.statistics.target.indices.push(index);
                log.debug('Created index ' + index + ' on target ElasticSearch instance');
                callback();
            }, callback);
        };
    }

    function createTypeTask(index, type) {
        return callback => {
            let uri;
            if (env.statistics.target.version.substring(0, 3) == '0.9') {
                uri = '/' + encodeURIComponent(index) + '/' + encodeURIComponent(type) + '/_mapping';
            } else {
                uri = '/' + encodeURIComponent(index)+ '/_mapping/' + encodeURIComponent(type);
            }
            let mapping = {};
            mapping[type] = metadata.mappings[index][type];
            request.target.put(env, uri, mapping, () => {
                log.debug('Created type ' + type + ' in target ElasticSearch instance on existing index ' + index);
                callback();
            }, callback);
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
};

exports.getQuery = env => {
    let query = {
        fields: [
            '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
        ],
        size: env.options.source.size,
        query: env.options.source.query
    };
    if (env.options.source.index) {
        query = {
            fields: [
                '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
            ],
            size: env.options.source.size,
            query: {
                indices: {
                    indices: [
                        env.options.source.index
                    ],
                    query: env.options.source.query,
                    no_match_query: 'none'
                }
            }
        };
    }
    if (env.options.source.type) {
        query = {
            fields: [
                '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
            ],
            size: env.options.source.size,
            query: {
                indices: {
                    indices: [
                        env.options.source.index
                    ],
                    query: env.options.source.query,
                    no_match_query: 'none'
                }
            },
            filter: {
                type: {
                    value: env.options.source.type
                }
            }
        };
    }
    return query;
};

/**
 * Fetches data from ElasticSearch via a scroll/scan request.
 *
 * @param env
 * @param callback Callback which is called when data has been received with the first argument as an array of hits,
 *        and the second the number of total hits.
 */
exports.getData = (env, callback) => {
    let query = exports.getQuery(env);

    function handleResult(data) {
        exports.scrollId = data._scroll_id;
        callback(null, data.hits ? data.hits.hits : []);
    }

    if (exports.scrollId !== null) {
        request.source.post(env, '/_search/scroll?scroll=60m', exports.scrollId, handleResult, callback);
    } else {
        request.source.post(env, '/_search?search_type=scan&scroll=60m', query, data => {
            exports.scrollId = data._scroll_id;
            exports.getData(env, callback);
        }, callback);
    }
};

/**
 * Stores data using a bulk request.
 *
 * @param env
 * @param docs The data to transmit in ready to use bulk format.
 * @param callback Callback function that is called without any arguments when the data has been stored unless there was an error.
 */
exports.putData = (env, docs, callback) => {
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
        if (doc.fields) {
            ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'].forEach(field => {
                if (doc.fields[field]) {
                    metaData[op][field] = doc.fields[field];
                }
            });
        }
        data += JSON.stringify(metaData) + '\n' + JSON.stringify(doc._source) + '\n';
    });
    request.target.post(env, '/_bulk', data, data => {
        if (data.errors) {
            for (let i in data.items) {
                let item = data.items[i];
                if (!item.index || item.index.status / 100 != 2) {
                    callback(JSON.stringify(item));
                    break;
                }
            }
        } else {
            callback();
        }
    }, callback);
};
