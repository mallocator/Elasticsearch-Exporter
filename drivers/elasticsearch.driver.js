var http = require('http');
var https = require('https');
var url = require('url');

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line

var log = require('../log.js');


var id = 'elasticsearch';

exports.getInfo = function (callback) {
    var info = {
        id: id,
        name: 'ElasticSearch Scroll Driver',
        version: '1.0',
        desciption: 'An Elasticsearch driver that makes use of the scrolling API to read data'
    };
    var options = {
        source: {
            host: {
                abbr: 'h',
                preset: 'localhost',
                help: 'The host from which data is to be exported from'
            }, port: {
                abbr: 'p',
                preset: 9200,
                help: 'The port of the source host to talk to'
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
                preset: 30
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
                preset: 100
            }, throttleTimeout: {
                abbr: 'TT',
                help: 'The milliseconds between two request for getting data when any of the ES nodes has high CPU',
                preset: 1000
            }, throttleCPULimit: {
                abbr: 'TC',
                help: 'The maximum number of percents of the CPU load on any of the ES nodes after which the requests for getting data will be throttled',
                preset: 200
            }
        }, target: {
            host: {
                abbr: 'h',
                help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                preset: 'localhost'
            }, port: {
                abbr: 'p',
                preset: 9200,
                help: 'The port of the target host to talk to'
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
                preset: 30
            }, replicas: {
                abbr: 'r',
                help: 'Sets the number of replicas the target index should be initialized with (only works with new indices).'
            }, throttleTimeout: {
                abbr: 'TT',
                help: 'The milliseconds between two request for putting data when any of the ES nodes has high CPU',
                preset: 1000
            }, throttleCPULimit: {
                abbr: 'TC',
                help: 'The maximum number of percents of the CPU load on any of the ES nodes after which the requests for putting data will be throttled',
                preset: 200
            }
        }
    };
    callback(null, info, options);
};

exports.verifyOptions = function(opts, callback) {
    if (opts.source.query) {
        try {
            opts.source.query = JSON.parse(opts.source.query);
        } catch(e) {}
    }
    if (opts.drivers.source == id && opts.drivers.target == id) {
        if (!opts.target.host) {
            opts.target.host = opts.source.host;
        }
        if (!opts.target.port) {
            opts.target.port = opts.source.port;
        }
        if (opts.source.index && !opts.target.index) {
            opts.target.index = opts.source.index;
        }
        if (opts.source.type && !opts.target.type) {
            opts.target.type = opts.source.type;
        }
        if ((process.env.HTTP_PROXY || process.env.http_proxy) && !opts.source.proxy) {
            if (process.env.HTTP_PROXY) {
                opts.source.proxy = process.env.HTTP_PROXY;
            } else if (process.env.http_proxy) {
                opts.source.proxy = process.env.http_proxy;
            }
        }

        if (opts.source.host != opts.target.host) { callback(); return; }
        if (opts.source.port != opts.target.port) { callback(); return; }
        if (opts.source.index != opts.target.index) { callback(); return; }
        if (opts.source.type != opts.target.type && opts.source.index) { callback(); return; }
    } else {
        var optSet = opts.drivers.source == id ? opts.source : opts.target;
        if (optSet.host) { callback(); return; }
    }
    callback('Not enough information has been given to be able to perform an export. Please review the OPTIONS and examples again.');
};

var request = {
    buffer_concat: function(buffers, nread) {
        var buffer = null;
        switch (buffers.length) {
            case 0:
                buffer = new Buffer(0);
                break;
            case 1:
                buffer = buffers[0];
                break;
            default:
                buffer = new Buffer(nread);
                for (var i = 0, pos = 0, l = buffers.length; i < l; i++) {
                    var chunk = buffers[i];
                    chunk.copy(buffer, pos);
                    pos += chunk.length;
                }
                break;
        }
        var data = buffer.toString();
        try {
            return JSON.parse(data);
        } catch(e) {
            throw new Error("There was an error trying to parse a json response from the server. Server response:\n" + data);
        }
    },
    create: function(httpProxy, ssl, host, port, auth, path, method, data, callback, errCallback) {
        var protocol = ssl ? https : http;
        var buffer = null, err = null;
        var reqOpts = {
            host: host,
            port: port,
            path: path,
            auth: auth,
            headers: {},
            method: method
        };
        if (httpProxy) {
            reqOpts.host = url.parse(httpProxy);
            reqOpts.path = 'http://' + host + ':' + port + path;
            reqOpts.headers.Host = httpProxy;
        }
        if (data) {
            if (typeof data == 'object') {
                data = JSON.stringify(data);
            }
            buffer = new Buffer(data, 'utf8');
            reqOpts.headers['Content-Length'] = buffer.length;
        }
        var req = protocol.request(reqOpts, function (res) {
            var buffers = [];
            var nread = 0;
            res.on('data', function (chunk) {
                buffers.push(chunk);
                nread += chunk.length;
            });
            res.on('end', function () {
                if (!err) {
                    callback(request.buffer_concat(buffers, nread));
                }
            });
        });
        req.on('error', function(e) {
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
    source: {
        get: function (env, path, data, callback, errCallback) {
            var source = env.options.source;
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            request.create(source.proxy, source.useSSL, source.host, source.port, source.auth, path, 'GET', data, callback, errCallback);
        },
        post: function (env, path, data, callback, errCallback) {
            var source = env.options.source;
            request.create(source.proxy, source.useSSL, source.host, source.port, source.auth, path, 'POST', data, callback, errCallback);
        }
    },
    target: {
        get: function (env, path, data, callback, errCallback) {
            var source = env.options.source;
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            request.create(source.proxy, source.useSSL, source.host, source.port, source.auth, path, 'GET', data, callback, errCallback);
        },
        post: function (env, path, data, callback, errCallback) {
            var target = env.options.target;
            request.create(target.proxy, target.useSSL, target.host, target.port, target.auth, path, 'POST', data, callback, errCallback);
        },
        put: function (env, path, data, callback, errCallback) {
            if (typeof data == 'function') {
                errCallback = callback;
                callback = data;
                data = null;
            }
            var target = env.options.target;
            request.create(target.proxy, target.useSSL, target.host, target.port, target.auth, path, 'PUT', data, callback, errCallback);
        }
    }
};

exports.reset = function (env, callback) {
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

exports.getTargetStats = function (env, callback) {
    var stats = {
        aliases: {},
        indices: []
    };

    async.parallel([
        function (subCallback) {
            request.source.get(env, '/', function (data) {
                stats.version = data.version.number;
                subCallback();
            }, subCallback);
        },
        function (subCallback) {
            request.source.get(env, '/_cluster/health', function (data) {
                stats.status = data.status;
                subCallback();
            }, subCallback);
        }, function (subCallback) {
            request.source.get(env, '/_cluster/state', function (data) {
                for (var index in data.metadata.indices) {
                    stats.indices.push(index);
                    if (data.metadata.indices[index].aliases.length) {
                        data.metadata.indices[index].aliases.forEach(function (alias) {
                            stats.aliases[alias] = index;
                        });
                    }
                }
                subCallback();
            }, subCallback);
        }
    ], function (err) {
        log.debug('ElasticSearch target version: ', stats.version);
        // TODO print information about number of nodes of target
        callback(err, stats);
    });
};

exports.getSourceStats = function (env, callback) {
    var stats = {
        aliases: {},
        indices: [],
        docs: {}
    };

    async.parallel([
        function(subCallback) {
            request.source.get(env, '/', function(data) {
                stats.version = data.version.number;
                subCallback();
            }, subCallback);
        },
        function(subCallback) {
            request.source.get(env, '/_cluster/health', function(data) {
                stats.status = data.status;
                subCallback();
            }, subCallback);
        }, function(subCallback) {
            request.source.get(env, '/_cluster/state', function(data) {
                for (var index in data.metadata.indices) {
                    stats.indices.push(index);
                    if (data.metadata.indices[index].aliases.length) {
                        data.metadata.indices[index].aliases.forEach(function (alias) {
                            stats.aliases[alias] = index;
                        });
                    }
                }
                subCallback();
            }, subCallback);
        }, function(subCallback) {
            var uri = '/';
            if (env.options.source.index) {
                uri += env.options.source.index + '/';
            }
            if (env.options.source.type) {
                uri += env.options.source.type + '/';
            }
            request.source.get(env, uri + '_count', {query: env.options.source.query}, function(data) {
                stats.docs.total = data.count;
                subCallback();
            }, subCallback);
        }
    ], function(err) {
        log.debug('ElasticSearch source version: ', stats.version);
        // TODO print information about number of nodes of source
        callback(err, stats);
    });
};

exports.getMeta = function (env, callback) {
    var settingsUri = '/';
    if (env.options.source.index) {
        settingsUri += env.options.source.index + '/';
    }
    var mappingsUri = settingsUri;
    if (env.options.source.type) {
        mappingsUri += env.options.source.type + '/';
    }

    var metadata = {
        mappings: {},
        settings: {}
    };

    async.parallel([
        function(subCallback) {
            request.source.get(env, mappingsUri + '_mapping', function (data) {
                for (var index in data) {
                    // TODO move this into the exporter so that it's not up to the driver to do transformations
                    var newIndex = index;
                    if (env.options.target.index && env.options.target.index != env.options.source.index && index == env.options.source.index) {
                        newIndex = env.options.target.index;
                    }
                    var mappings = data[index].mappings ? data[index].mappings : data[index];
                    metadata.mappings[newIndex] = {};
                    for (var type in mappings) {
                        var newType = type;
                        if (env.options.target.type&& env.options.target.type != env.options.source.type && type == env.options.source.type) {
                            newType = env.options.target.type;
                        }
                        metadata.mappings[newIndex][newType] = mappings[type];
                    }
                }
                subCallback();
            }, subCallback);
        }, function(subCallback) {
            if (env.options.source.type) {
                subCallback();
                return;
            }
            request.source.get(env, settingsUri + '_settings', function (data) {
                for (var index in data) {
                    // TODO move this into the exporter so that it's not up to the driver to do transformations
                    var newIndex = index;
                    if (env.options.target.index && env.options.target.index != env.options.source.index && index == env.options.source.index) {
                        index = env.options.target.index;
                    }
                    metadata.settings[newIndex] = data[index];
                }
                subCallback();
            }, subCallback);
        }
    ], function(err) {
        callback(err, metadata);
    });
};

exports.putMeta = function (env, metadata, callback) {
    function createIndexTask(index) {
        return function (callback) {
            var body = {settings: metadata.settings[index] ? metadata.settings[index] : {}};
            if (env.options.target.replicas) {
                body.settings.number_of_replicas = env.options.target.replicas;
            }
            request.target.put(env, '/' + index, body, function() {
                env.statistics.target.indices.push(index);
                log.debug('Created index ' + index + ' on target ElasticSearch instance');
                callback();
            }, callback);
        };
    }

    function createTypeTask(index, type) {
        return function(callback) {
            var uri;
            if (env.statistics.target.version.substring(0, 3) == '0.9') {
                uri = '/' + index + '/' + type + '/_mapping';
            } else {
                uri = '/' + index+ '/_mapping/' + type;
            }
            var mapping = {};
            mapping[type] = metadata.mappings[index][type];
            request.target.put(env, uri, mapping, function() {
                log.debug('Created type ' + type + ' in target ElasticSearch instance on existing index ' + index);
                callback();
            }, callback);
        };
    }

    var indexTasks = [], typeTasks = [];
    for (var index in metadata.mappings) {
        if (env.statistics.target.indices.indexOf(index) == -1) {
            indexTasks.push(createIndexTask(index));
        } else {
            log.debug('Not creating index ' + index + ' on target ElasticSearch instance because it already exists');
        }
        for (var type in metadata.mappings[index]) {
            typeTasks.push(createTypeTask(index, type));
        }
    }

    async.series([
        function(callback) {
            if (indexTasks.length > 0) {
                log.debug('Creating indices in target ElasticSearch instance');
                async.parallel(indexTasks, callback);
            } else {
                callback();
            }
        },
        function (callback) {
            log.debug('Creating types in target ElasticSearch instance');
            async.parallel(typeTasks, callback);
        }
    ], callback);
};

/**
 * Fetches data from ElasticSearch via a scroll/scan request.
 *
 * @param env
 * @param callback Callback which is called when data has been received with the first argument as an array of hits,
 *        and the second the number of total hits.
 */
exports.getData = function (env, callback) {
    var query = {
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

    function handleResult(data) {
        exports.scrollId = data._scroll_id;
        callback(null, data.hits ? data.hits.hits : []);
    }

    if (exports.scrollId !== null) {
        var search = function search() {
            request.source.get(env, '/_nodes/stats/process', function(data) {
                for (var nodeName in data.nodes) {
                    if(data.nodes[nodeName].process.cpu.percent >= env.options.source.throttleCPULimit) {
                        log.status('Wait some time to free the CPU resource. Current CPU load is %s...', data.nodes[nodeName].process.cpu.percent);
                        return setTimeout(search, env.options.source.throttleTimeout);
                    }
                }

                request.source.post(env, '/_search/scroll?scroll=60m', exports.scrollId, handleResult, callback);
            }, callback)
        };
        search();
    } else {
        request.source.post(env, '/_search?search_type=scan&scroll=60m', query, function(data) {
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
exports.putData = function (env, docs, callback) {
    var op = env.options.target.overwrite ? 'index' : 'create';
    var data = '';
    docs.forEach(function (doc) {
        var metaData = {};
        metaData[op] = {
            _index: env.options.target.index ? env.options.target.index : doc._index,
            _type: env.options.target.type ? env.options.target.type : doc._type,
            _id: doc._id,
            _version: doc._version ? doc._version : null
        };
        if (doc.fields) {
            ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'].forEach(function (field) {
                if (doc.fields[field]) {
                    metaData[op][field] = doc.fields[field];
                }
            });
        }
        data += JSON.stringify(metaData) + '\n' + JSON.stringify(doc._source) + '\n';
    });

    var write = function write() {
        request.target.get(env, '/_nodes/stats/process', function(data) {
            for (var nodeName in data.nodes) {
                if(data.nodes[nodeName].process.cpu.percent >= env.options.target.throttleCPULimit) {
                    log.status('Wait some time to free the CPU resource. Current CPU load is %s...', data.nodes[nodeName].process.cpu.percent);
                    return setTimeout(write, env.options.target.throttleTimeout);
                }
            }

            request.target.post(env, '/_bulk', data, function (data) {
                if (data.errors) {
                    for (var i in data.items) {
                        var item = data.items[i];
                        if (!item.index || item.index.status / 100 != 2) {
                            callback(JSON.stringify(item));
                            break;
                        }
                    }
                } else {
                    callback();
                }
            }, callback);
        }, callback)
    };
    write();
};