var http = require('http');
var https = require('https');
var url = require('url');
var async = require('async');
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
                preset: '{match_all: {}}'
            }, auth: {
                abbr: 'a',
                help: 'Set authentication parameters for reaching the source Elasticsearch cluster'
            }, skipData: {
                abbr: 's',
                help: 'Do not copy data, just the mappings',
                flag: true
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
                preset: 10
            }
        }, target: {
            host: {
                abbr: 'h',
                help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                required: true
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
            }
        }
    };
    callback(null, info, options);
};

exports.verifyOptions = function(opts, callback) {
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
    create: function(httpProxy, ssl, host, port, auth, path, method, headers, callback) {
        var reqOpts = {
            host: host,
            port: port,
            path: path,
            auth: auth,
            headers: headers,
            method: method
        };
        if (httpProxy) {
            reqOpts.host = url.parse(httpProxy);
            reqOpts.path = 'http://' + host + ':' + port + path;
            headers.headers.Host = httpProxy;
        }
        var protocol = ssl ? https : https;
        protocol.request(reqOpts,  function (res) {
            // TODO add error handling in extra callback parameter
            var buffers = [];
            var nread = 0;
            res.on('data', function (chunk) {
                buffers.push(chunk);
                nread += chunk.length;
            });
            res.on('end', function () {
                callback(this.buffer_concat(buffers, nread));
            });
        });
    },
    source: {
        get: function (env, path, callback) {
            var source = env.options.source;
            return this.create(source.proxy, source.useSSL, source.host, source.port, source.auth, path, 'GET', {}, callback);
        },
        post: function (env, path, headers, callback) {
            var source = env.options.source;
            return this.create(source.proxy, source.useSSL, source.host, source.port, source.auth, path, 'POST', headers, callback);
        }
    },
    target: {
        post: function (env, path, headers, callback) {
            var target = env.options.target;
            return this.create(target.proxy, target.useSSL, target.host, target.port, target.auth, path, 'POST', headers, callback);
        },
        put: function (env, path, headers, callback) {
            var target = env.options.target;
            return this.create(target.proxy, target.useSSL, target.host, target.port, target.auth, path, 'PUT', headers, callback);
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
        exports.scrollId = null;
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
    // TODO this needs to change because we're not doing a count on the target database
    var tmpEnv = {
        options: {
            source: {
                host: env.options.target.host,
                useSSL: env.options.target.useSSL,
                port: env.options.target.port,
                auth: env.options.target.auth,
                proxy: env.options.target.proxy
            }
        }
    };
    exports.getSourceStats(tmpEnv, callback);
};

exports.getSourceStats = function (env, callback) {
    var stats = {};

    async.parallel([
        function(subCallback) {
            var serverStatusReq = request.source.get(env, '/', function(data) {
                stats.version = data.version.number;
                subCallback();
            });
            serverStatusReq.on('error', subCallback);
            serverStatusReq.end();
        },
        function(subCallback) {
            var clusterHealthReq = request.source.get(env, '/_cluster/health', function(data) {
                stats.status = data.status;
                subCallback();
            });
            clusterHealthReq.on('error', subCallback);
            clusterHealthReq.end();
        }, function(subCallback) {
            var clusterStateReq = request.source.get(env, '/_cluster/state', function(data) {
                var aliases = {};
                for (var index in data.metadata.indices) {
                    if (data.metadata.indices[index].aliases.length) {
                        data.metadata.indices[index].aliases.forEach(function(alias) {
                            aliases[alias] = index;
                        });
                    }
                }
                stats.aliases = aliases;
                subCallback();
            });
            clusterStateReq.on('error', subCallback);
            clusterStateReq.end();
        }, function(subCallback) {
            var uri = '/';
            if (env.options.source.index) {
                uri += env.options.source.index + '/';
            }
            if (env.options.source.type) {
                uri += env.options.source.type + '/';
            }
            var countReq = request.source.get(env, uri + '_count', function(data) {
                stats.docs.total = data.count;
                subCallback();
            });
            countReq.on('error', subCallback);
            countReq.end(new Buffer(JSON.stringify({query: env.options.source.query}), 'utf8'));
        }
    ], function(err) {
        callback(err, stats);
    });
};

exports.getMeta = function (env, callback) {
    var uri = '/';
    if (env.options.source.index) {
        uri += env.options.source.index + '/';
    }
    if (env.options.source.type) {
        uri += env.options.source.type + '/';
    }

    var metadata = {
        mappings: {},
        settings: {}
    };

    async.parallel([
        function(subCallback) {
            var req = request.source.get(env, uri + '_mapping', function (data) {
                if (env.options.source.type) {
                    metadata.mappings = data;
                } else if (env.options.source.index) {
                    metadata.mappings = data[env.options.source.index];
                } else {
                    for (var index in data) {
                        metadata.mappings[index] = data[index].mappings ? data[index].mappings : data[index];
                    }
                }
                subCallback();
            });
            req.on('error', subCallback);
            req.end();
        }, function(subCallback) {
            if (env.options.source.type) {
                callback();
                return;
            }
            // Get settings for either 'index' or 'all' scope
            var req = request.source.get(env, uri + '_settings', function (data) {
                if (env.options.source.index) {
                    metadata.settings = data[env.options.source.index].settings;
                } else {
                    metadata.settings = data.settings;
                }
                subCallback();
            });
            req.on('error', subCallback);
            req.end();
        }
    ], function(err) {
        callback(err, metadata);
    });
};

exports.putMeta = function (env, metadata, callback) {
    if (env.options.source.type) {
        storeTypeMeta(env, metadata, callback);
    } else if (env.options.source.index) {
        storeIndexMeta(env, metadata, callback);
    } else {
        storeAllMeta(env, metadata, callback);
    }
    callback();
};

/**
 * Does the actual store operation for a type metadata. When storing types, only mapping data is stored.
 * This is different then the index or all scope, as it uses the put mapping request.
 *
 * @param env
 * @param metadata
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeTypeMeta(env, metadata, callback) {
    log.debug('Creating type mapping in target ElasticSearch instance');
    var createIndexReq = request.target.put(env, '/' + env.options.target.index, {"Content-Length": 0}, function () {
        var uri, buffer = new Buffer(JSON.stringify(metadata), 'utf8');

        if (env.statistics.target.version.substring(0, 4) == '0.9.') {
            uri = '/' + env.options.target.index + '/' + env.options.target.type + '/_mapping';
        } else {
            uri = '/' + env.options.target.index + '/_mapping/' + env.options.target.type + '/';
        }
        var typeMapOptions = request.target.put(env, uri, { "Content-Length": buffer.length });
        var protocol = env.options.target.useSSL ? https : http;
        var typeMapReq = protocol.request(typeMapOptions, callback);
        typeMapReq.on('error', callback);
        typeMapReq.end(buffer);
    });
    createIndexReq.on('error', callback);
    createIndexReq.end();
}

/**
 * Stores the index mappings and settings data via a create index call.
 *
 * @param env
 * @param metadata
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeIndexMeta(env, metadata, callback) {
    log.debug('Creating index mapping in target ElasticSearch instance');
    var buffer = new Buffer(JSON.stringify(metadata), 'utf8');
    var createIndexReq = request.target.put(env, '/' + env.options.target.index, {"Content-Length": buffer.length}, callback);
    createIndexReq.on('error', callback);
    createIndexReq.end(buffer);
}

/**
 * Stores the mappings and settings of all passed in indices via several create index calls.
 *
 * @param env
 * @param metadata The meta data object, how it was retrieved from the #getMeta() function.
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeAllMeta(env, metadata, callback) {
    function createIndexTask(index) {
        return function (callback) {
            var buffer = new Buffer(JSON.stringify(metadata[index]), 'utf8');
            var createIndexReq = request.target.put(env, '/' + index, {"Content-Length": buffer.length}, callback);
            createIndexReq.on('error', callback);
            createIndexReq.end(buffer);
        };
    }

    log.debug('Creating entire mapping in target ElasticSearch instance');
    var tasks = [];
    for (var index in metadata) {
        tasks.push(createIndexTask(index));
    }
    async.parallel(tasks, callback);
}

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
        var scrollBuffer = new Buffer(exports.scrollId, 'utf8');
        var scrollReq = request.source.post(env, '/_search/scroll?scroll=60m', {"Content-Length": scrollBuffer.length}, handleResult);
        scrollReq.on('error', callback);
        scrollReq.end(scrollBuffer);
    } else {
        var firstBuffer = new Buffer(JSON.stringify(query), 'utf8');
        var firstReq = request.source.post(env, '/_search?search_type=scan&scroll=60m', {"Content-Length": firstBuffer.length}, handleResult);
        firstReq.on('error', callback);
        firstReq.end(firstBuffer);
    }
};

/**
 * Stores data using a bulk request.
 *
 * @param env
 * @param data The data to transmit in ready to use bulk format.
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
        data += JSON.stringify(metaData) + '\n' + JSON.stringify(hit._source) + '\n';
    });
    var buffer = new Buffer(data, 'utf8');
    var putReq = request.target.post(env, '/_bulk', {"Content-Length": buffer.length}, function (data) {
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
    });
    putReq.on('error', callback);
    putReq.end(buffer);
};