var util = require('util');
var http = require('http');
var colors = require('colors');

process.on('uncaughtException', function(e) {
	console.log('Caught exception in Main process: %s'.bold, e.toString().red);
	if (e instanceof Error) {
		console.log(e.stack);
	}
	process.exit(1);
});

var nomnom = require('nomnom').script('exporter').options({
	sourceHost : {
		abbr : 'a',
		'default' : 'localhost',
		metavar : '<hostname>',
		help : 'The host from which data is to be exported from (default is localhost)'
	},
	targetHost : {
		abbr : 'b',
		metavar : '<hostname>',
		help : 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given'
	},
	sourcePort : {
		abbr : 'p',
		'default' : 9200,
		metavar : '<port>',
		help : 'The port of the source host to talk to (default is 9200)'
	},
	targetPort : {
		abbr : 'q',
		metavar : '<port>',
		help : 'The port of the target host to talk to (default is 9200)'
	},
	sourceIndex : {
		abbr : 'i',
		metavar : '<index>',
		help : 'The index name from which to export data from. If no index is given, the entire database is exported'
	},
	targetIndex : {
		abbr : 'j',
		metavar : '<index>',
		help : 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
	},
	sourceType : {
		abbr : 't',
		metavar : '<type>',
		help : 'The type from which to export data from. If no type is given, the entire index is exported'
	},
	targetType : {
		abbr : 'u',
		metavar : '<type>',
		help : 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
	}
}).colors();
var opts = nomnom.parse();

function verify() {
	if (!opts.targetHost) {
		opts.targetHost = opts.sourceHost;
	}
	if (!opts.targetPort) {
		opts.targetPort = opts.sourcePort;
	}
	if (opts.sourceIndex && !opts.targetIndex) {
		opts.targetIndex = opts.sourceIndex;
	}
	if (opts.sourceType && !opts.targetType) {
		opts.targetType = opts.sourceType;
	}
	if (opts.sourceHost != opts.targetHost) { return; }
	if (opts.sourcePort != opts.targetPort) { return; }
	if (opts.sourceIndex != opts.targetIndex) { return; }
	if (opts.sourceType != opts.targetType && opts.sourceIndex) { return; }
	console.log(nomnom.getUsage());
	process.exit(1);
}
verify();

var numCalls = 0;
var totalHits = null;
var fetchedHits = 0;
var processedHits = 0;

process.on('exit', function() {
	console.log('Number of calls:\t%s', numCalls);
	console.log('Fetched Entries:\t%s', fetchedHits);
	console.log('Processed Entries:\t%s', processedHits);
	console.log('Source DB Size:\t\t%s', totalHits);
});

var source = '/';
var target = '/';
if (opts.sourceIndex) {
	source += opts.sourceIndex + '/';
	target += opts.targetIndex + '/';
}
if (opts.sourceType) {
	source += opts.sourceType + '/';
	target += opts.targetType + '/';
}

var firstCall = true;
function handleScrollResult(result) {
	var data = '';
	result.on('data', function(chunk) {
		data += chunk;
	});
	result.on('end', function() {
		try {
			data = JSON.parse(data);
		} catch (e) {
			console.log(e);
			console.log(data);
			process.exit(1);
		}
		if (totalHits == null) {
			totalHits = data.hits.total;
		}
		if (data.hits.hits.length) {
			storeHits(data.hits.hits);
		}
		if (firstCall || data.hits.hits.length) {
			firstCall = false;
			fetchedHits += data.hits.hits.length;
			numCalls++;
			var req = http.request({
				host : opts.sourceHost,
				port : opts.sourcePort,
				path : '/_search/scroll?scroll=5m',
				method : 'POST'
			}, handleScrollResult);
			req.on('error', console.log);
			req.end(data._scroll_id);
		}
	});
}

var query = {
	fields : [
			'_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
	],
	query : {
		match_all : {}
	}
};
if (opts.sourceIndex) {
	query = {
		fields : [
				'_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'
		],
		query : {
			indices : {
				indices : [
					opts.sourceIndex
				],
				query : {
					match_all : {}
				},
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
		query : {
			indices : {
				indices : [
					opts.sourceIndex
				],
				query : {
					match_all : {}
				},
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

var req = http.request({
	host : opts.sourceHost,
	port : opts.sourcePort,
	path : '/_search?search_type=scan&scroll=5m',
	method : 'POST'
}, handleScrollResult);
req.on('error', console.log);
req.end(JSON.stringify(query));

var mappingReady = false;
var hitQueue = [];

var options = { host: opts.sourceHost, port: opts.sourcePort, path: source + '_mapping' };
var req = http.get(options, function(res) {
	var data = '';
	res.on('data', function(chunk) {
		data += chunk;
	});
	res.on('end', function() {
		if (opts.sourceType != null) {
			console.log('create index on target host');
			var createIndexReq = http.request({
				host : opts.targetHost,
				port : opts.targetPort,
				path : '/' + opts.targetIndex,
				method : 'PUT'
			}, function(res) {
				console.log('create type mapping on target host');
				var typeMapReq = http.request({
					host : opts.targetHost,
					port : opts.targetPort,
					path : target + '_mapping',
					method : 'PUT'
				}, function(res) {
					mappingReady = true;
					if (hitQueue.length) {
						storeHits([]);
					}
				});
				typeMapReq.on('error', console.log);
				typeMapReq.end(data);
			});
			createIndexReq.on('error', console.log);
			createIndexReq.end();
		} else if (opts.sourceIndex != null) {
			console.log('create index mapping target host');
			var mapping = JSON.parse(data);
			var createIndexReq = http.request({
				host : opts.targetHost,
				port : opts.targetPort,
				path : '/' + opts.targetIndex,
				method : 'PUT'
			}, function(res) {
				mappingReady = true;
				if (hitQueue.length) {
					storeHits([]);
				}
			});
			createIndexReq.on('error', console.log);
			createIndexReq.end(JSON.stringify({
				mappings : mapping[opts.sourceIndex]
			}));
		} else {
			console.log('create all index mappings on target host');
			var mapping = JSON.parse(data);
			var numIndices = 0;
			var indicesDone = 0;
			for ( var index in mapping) {
				numIndices++;
				var createIndexReq = http.request({
					host : opts.targetHost,
					port : opts.targetPort,
					path : '/' + index,
					method : 'PUT'
				}, function(res) {
					indicesDone++;
					if (numIndices == indicesDone) {
						mappingReady = true;
						if (hitQueue.length) {
							storeHits([]);
						}
					}
				});
				createIndexReq.on('error', console.log);
				createIndexReq.end(JSON.stringify({
					mappings : mapping[index]
				}));
			}
		}
	});
});

function storeHits(hits) {
	if (!mappingReady) {
		hitQueue = hitQueue.concat(hits);
		console.log('Waiting for mapping on target host to be ready, queue length %s', hitQueue.length);
		return;
	}
	hits = hits.concat(hitQueue);
	hitQueue = [];
	var data = '';
	hits.forEach(function(hit) {
		processedHits++;
		var metaData = {
			index : {
				_index : opts.targetIndex ? opts.targetIndex : hit._index,
				_type : opts.targetType ? opts.targetType : hit._type,
				_id : hit._id
			}
		};
		if (hit.fields) {
			if (hit.fields._timestamp) {
				metaData.index._timestamp = hit.fields._timestamp;
			}
			if (hit.fields._version) {
				metaData.index._version = hit.fields._version;
			}
			if (hit.fields._routing) {
				metaData.index._routing = hit.fields._routing;
			}
			if (hit.fields._percolate) {
				metaData.index._percolate = hit.fields._percolate;
			}
			if (hit.fields._parent) {
				metaData.index._parent = hit.fields._parent;
			}
			if (hit.fields._ttl) {
				metaData.index._ttl = hit.fields._ttl;
			}
		}
		data += JSON.stringify(metaData) + '\n' + JSON.stringify(hit._source) + '\n';
		if (processedHits % 100 == 0) {
			console.log('Processed %s of %s entries (%s%%)', processedHits, totalHits, Math.round(processedHits / totalHits * 100));
		}
	});
	var putReq = http.request({
		host : opts.targetHost,
		port : opts.targetPort,
		path : '_bulk',
		method : 'POST'
	});
	putReq.end(data);
}