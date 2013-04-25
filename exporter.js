var numCalls = 0, totalHits = 0, fetchedHits = 0, processedHits = 0;

process.on('uncaughtException', function(e) {
	console.log('Caught exception in Main process: %s'.bold, e.toString());
	if (e instanceof Error) {
		console.log(e.stack);
	}
	process.exit(1);
});

process.on('exit', function() {
	console.log('Number of calls:\t%s', numCalls);
	console.log('Fetched Entries:\t%s', fetchedHits);
	console.log('Processed Entries:\t%s', processedHits);
	console.log('Source DB Size:\t\t%s', totalHits);
});

var opts = require('./options.js').opts;
var sourceDriver = require(opts.sourceFile ? './drivers/file.js' : './drivers/es.js');
var targetDriver = require(opts.targetFile ? './drivers/file.js' : './drivers/es.js');
//var sourceDriver = require('./drivers/test.js');
//var targetDriver = require('./drivers/test.js');

var mappingReady = false;
var firstRun = true;
var hitQueue = [];

function handleMetaResult(data) {
    if (opts.testRun) {
        return;
    }
    function done() {
        console.log("Mapping is now ready. Starting with " + hitQueue.length + " queued hits.");
        mappingReady = true;
        if (hitQueue.length) {
            storeHits([]);
        }
    }
    if (opts.sourceType) {
        targetDriver.createTypeMeta(opts, data, done);
	} else if (opts.sourceIndex) {
        targetDriver.createIndexMeta(opts, data, done);
	} else {
        targetDriver.createAllMeta(opts, data, done);
	}
}

function handleDataResult(data, total) {
    totalHits = total;
    if (opts.testRun) {
        console.log("Stopping further execution, since this is only a test run. No operations have been executed on the target database.");
        process.exit(0);
    }
    if (data.length) {
		storeHits(data);
	}
    if (firstRun || data.length) {
        firstRun = false;
		fetchedHits += data.length;
		numCalls++;
        sourceDriver.getData(opts, handleDataResult);
	}
}

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
            ['_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'].forEach(function(field){
                if (hit.fields[field]) {
                    metaData.index[field] = hit.fields[field];
                }
            });
		}
		data += JSON.stringify(metaData) + '\n' + JSON.stringify(hit._source) + '\n';
		if (processedHits % 100 === 0) {
			console.log('Processed %s of %s entries (%s%%)', processedHits, totalHits, Math.round(processedHits / totalHits * 100));
		}
	});
    targetDriver.storeHits(opts, data);
}

sourceDriver.getMeta(opts, handleMetaResult);
sourceDriver.getData(opts, handleDataResult);