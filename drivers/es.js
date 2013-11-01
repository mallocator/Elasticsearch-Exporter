if (global.GENTLY) require = global.GENTLY.hijack(require);
var http = require('http');
http.globalAgent.maxSockets = 30;

exports.getMeta = function(opts, callback) {
    console.log('Reading mapping from ElasticSearch');
    var source = '/';
    if (opts.sourceIndex) {
        source += opts.sourceIndex + '/';
    }
    if (opts.sourceType) {
        source += opts.sourceType + '/';
    }
    var options = { host: opts.sourceHost, port: opts.sourcePort, path: source + '_mapping' };
    http.get(options, function(res) {
        var data = '';
        res.on('data', function(chunk) {
            data += chunk;
        });
        res.on('end', function() {
            data = JSON.parse(data);
            if (opts.sourceType) {
                getSettings(opts, data, callback);
            } else if (opts.sourceIndex) {
                getSettings(opts, { mappings: data[opts.sourceIndex] }, callback);
            } else {
                var metadata = {};
                for (var index in data) {
                    metadata[index] = {
                        mappings: data[index]
                    };
                }
                getSettings(opts, metadata, callback);
            }
        });
    }).on('error', console.log);
};

function getSettings(opts, metadata, callback) {
    var source = '/';
    if (opts.sourceIndex) {
        source += opts.sourceIndex + '/';
    }
    if (opts.sourceType) {
        callback(metadata);
        return;
    }
    // Get settings for either 'index' or 'all' scope
    var options = { host: opts.sourceHost, port: opts.sourcePort, path: source + '_settings' };
    http.get(options,function (res) {
        var data = '';
        res.on('data', function (chunk) {
            data += chunk;
        });
        res.on('end', function () {
            data = JSON.parse(data);
            if (opts.sourceIndex) {
                metadata.settings = data[opts.sourceIndex].settings;
            } else {
                for (var index in data) {
                    metadata[index].settings = data[index].settings;
                }
            }
            callback(metadata);
        });
    }).on('error', console.log);
}

exports.createMeta = function(opts, metadata, callback) {
    if (opts.sourceType) {
        createTypeMeta(opts, metadata, callback);
    } else if (opts.sourceIndex) {
        createIndexMeta(opts, metadata, callback);
    } else {
        createAllMeta(opts, metadata, callback);
    }
};

function createTypeMeta(opts, metadata, callback) {
    console.log('Creating type mapping in target ElasticSearch instance');
    var createIndexReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '/' + opts.targetIndex,
		method : 'PUT'
	}, function() {
		var typeMapReq = http.request({
			host : opts.targetHost,
			port : opts.targetPort,
			path : '/' + opts.targetIndex + '/' + opts.targetType + '/' + '_mapping',
			method : 'PUT'
		}, callback);
		typeMapReq.on('error', console.log);
		typeMapReq.end(JSON.stringify(metadata));
	});
	createIndexReq.on('error', console.log);
	createIndexReq.end();
}

function createIndexMeta(opts, metadata, callback) {
    console.log('Creating index mapping in target ElasticSearch instance');
	var createIndexReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '/' + opts.targetIndex,
		method : 'PUT'
	}, callback);
	createIndexReq.on('error', console.log);
	createIndexReq.end(JSON.stringify(metadata));
}

function createAllMeta(opts, metadata, callback) {
    console.log('Creating entire mapping in target ElasticSearch instance');
	var numIndices = 0;
	var indicesDone = 0;
    function done() {
        indicesDone++;
        if (numIndices == indicesDone) {
            callback();
        }
    }
	for (var index in metadata) {
		numIndices++;
		var createIndexReq = http.request({
			host : opts.targetHost,
			port : opts.targetPort,
			path : '/' + index,
			method : 'PUT'
		}, done);
		createIndexReq.on('error', console.log);
		createIndexReq.end(JSON.stringify(metadata[index]));
	}
}

var scrollId = null;
exports.getData = function(opts, callback, retries) {
    if (!retries) {
        retries = 0;
    } else {
        if (retries == opts.errorsAllowed) {
            console.log('Maximum number of retries for fetching data reached. Aborting!');
            process.exit(1);
        }
        retries++;
    }

    var query = {
        fields : [
            '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
        ],
        size : opts.sourceSize,
        query : opts.sourceQuery
    };
    if (opts.sourceIndex) {
        query = {
            fields : [
                '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
            ],
            size : opts.sourceSize,
            query : {
                indices : {
                    indices : [
                        opts.sourceIndex
                    ],
                    query : opts.sourceQuery,
                    no_match_query : 'none'
                }
            }
        };
    }
    if (opts.sourceType) {
        query = {
            fields : [
                '_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
            ],
            size : opts.sourceSize,
            query : {
                indices : {
                    indices : [
                        opts.sourceIndex
                    ],
                    query : opts.sourceQuery,
                    no_match_query : 'none'
                }
            },
            filter : {
                type : {
                    value : opts.sourceType
                }
            }
        };
    }

    function handleResult(result) {
        var data = '';
        result.on('data', function(chunk) {
            data += chunk;
        });
        result.on('end', function() {
            try {
                data = JSON.parse(data);
            } catch (e) {}
            scrollId = data._scroll_id;
            callback(data.hits ? data.hits.hits : [], data.hits.total);
        });
    }

    if (scrollId !== null) {
        var scrollReq = http.request({
            host : opts.sourceHost,
            port : opts.sourcePort,
            path : '/_search/scroll?scroll=5m',
            method : 'POST'
        }, handleResult);
        scrollReq.on('error', function(err) {
            console.log(err);
            setTimeout(function() {
                exports.getData(opts, callback, retries);
            }, 1000);
        });
        scrollReq.end(scrollId);
    } else {
        var firstReq = http.request({
            host : opts.sourceHost,
            port : opts.sourcePort,
            path : '/_search?search_type=scan&scroll=5m',
            method : 'POST'
        }, handleResult);
        firstReq.on('error', function (err) {
            console.log(err);
            setTimeout(function () {
                exports.getData(opts, callback, retries);
            }, 1000);
        });
        firstReq.end(JSON.stringify(query));
    }
};

exports.storeHits = function(opts, data, callback, retries) {
    if (!retries) {
        retries = 0;
    } else {
        if (retries == opts.errorsAllowed) {
            console.log('Maximum number of retries for writing data reached. Aborting!');
            process.exit(1);
        }
        retries++;
    }

    var putReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '_bulk',
		method : 'POST'
	}, function(res) {
		//Data must be fetched, otherwise socket won't be set to free
		res.on('data', function () {});
		callback();
	});
    putReq.on('error', function (err) {
        console.log(err);
        setTimeout(function () {
            exports.storeHits(opts, data, callback, retries);
        }, 1000);
    });
	putReq.end(data);
};
