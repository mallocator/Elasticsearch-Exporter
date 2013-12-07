var http = require('http');
http.globalAgent.maxSockets = 30;

exports.reset = function() {
    exports.scrollId = null;
};

/**
 * Returns both settings and mappings depending on which scope is set in opts.
 * The resulting object is as close to the format that is sent to ES as possible.
 * For the all scope the returned object holds the proper requests stored under the name of each index.
 * @param opts
 * @param callback Callback function that receives the meta data object as first parameter
 */
exports.getMeta = function(opts, callback) {
    if (opts.logEnabled) {
        console.log('Reading mapping from ElasticSearch');
    }
    var source = '/';
    if (opts.sourceIndex) {
        source += opts.sourceIndex + '/';
    }
    if (opts.sourceType) {
        source += opts.sourceType + '/';
    }
    var options = { host: opts.sourceHost, port: opts.sourcePort, path: source + '_mapping', auth: opts.sourceAuth };
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

/**
 * Does the settings request after the mapping has been fetched. Settings are not fetched for a type request,
 * as the index that is created when a type does not exists is only there to ensure that data can be copied.
 * To ensure the full copy including mappings, at least an index export should be done.
 *
 * @param opts
 * @param metadata
 * @param callback Callback function that receives the meta data object as first parameter
 */
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
    var options = { host: opts.sourceHost, port: opts.sourcePort, path: source + '_settings', auth: opts.sourceAuth };
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

/**
 * Stores the meta data according to the scope that is set through the opts.
 *
 * @param opts
 * @param metadata
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
exports.storeMeta = function(opts, metadata, callback) {
    if (opts.sourceType) {
        storeTypeMeta(opts, metadata, callback);
    } else if (opts.sourceIndex) {
        storeIndexMeta(opts, metadata, callback);
    } else {
        storeAllMeta(opts, metadata, callback);
    }
};

/**
 * Does the actual store operation for a type metadata. When storing types, only mapping data is stored.
 * This is different then the index or all scope, as it uses the put mapping request.
 *
 * @param opts
 * @param metadata
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeTypeMeta(opts, metadata, callback) {
    if (opts.logEnabled) {
        console.log('Creating type mapping in target ElasticSearch instance');
    }
    var createIndexReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '/' + opts.targetIndex,
		method : 'PUT',
        auth: opts.targetAuth
	}, function() {
		var typeMapReq = http.request({
			host : opts.targetHost,
			port : opts.targetPort,
			path : '/' + opts.targetIndex + '/' + opts.targetType + '/' + '_mapping',
			method : 'PUT',
            auth: opts.targetAuth
		}, callback);
		typeMapReq.on('error', console.log);
		typeMapReq.end(JSON.stringify(metadata));
	});
	createIndexReq.on('error', console.log);
	createIndexReq.end();
}

/**
 * Stores the index mappings and settings data via a create index call.
 *
 * @param opts
 * @param metadata
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeIndexMeta(opts, metadata, callback) {
    if (opts.logEnabled) {
        console.log('Creating index mapping in target ElasticSearch instance');
    }
	var createIndexReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '/' + opts.targetIndex,
		method : 'PUT',
        auth: opts.targetAuth
	}, callback);
	createIndexReq.on('error', console.log);
	createIndexReq.end(JSON.stringify(metadata));
}

/**
 * Stores the mappings and settings of all passed in indices via several create index calls.
 *
 * @param opts
 * @param metadata The meta data object, how it was retrieved from the #getMeta() function.
 * @param callback Callback method that will called once the meta data has been stored. No significant data is passed via arguments.
 */
function storeAllMeta(opts, metadata, callback) {
    if (opts.logEnabled) {
        console.log('Creating entire mapping in target ElasticSearch instance');
    }
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
			method : 'PUT',
            auth: opts.targetAuth
		}, done);
		createIndexReq.on('error', console.log);
		createIndexReq.end(JSON.stringify(metadata[index]));
	}
}

/**
 * Fetches data from ElasticSearch via a scroll/scan request.
 *
 * @param opts
 * @param callback Callback which is called when data has been received with the first argument as an array of hits,
 *        and the second the number of total hits.
 * @param retries Should not be set from the calling method, as this is increase through recursion whenever a call fails
 */
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
        if (result.statusCode < 200 && result.statusCode > 299) {
            setTimeout(function () {
                exports.getData(opts, callback, retries);
            }, 1000);
        }
        var data = '';
        result.on('data', function(chunk) {
            data += chunk;
        });
        result.on('end', function() {
            try {
                data = JSON.parse(data);
            } catch (e) {}
            exports.scrollId = data._scroll_id;
            callback(data.hits.hits, data.hits.total);
        });
    }

    if (exports.scrollId !== null) {
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
        scrollReq.end(exports.scrollId);
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

/**
 * Stores data using a bulk request.
 *
 * @param opts
 * @param data The data to transmit in ready to use bulk format.
 * @param callback Callback function that is called without any arguments when the data has been stored.
 * @param retries Should not be set from the calling method, as this is increase through recursion whenever a call fails
 */
exports.storeData = function(opts, data, callback, retries) {
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
		method : 'POST',
        auth: opts.targetAuth
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
