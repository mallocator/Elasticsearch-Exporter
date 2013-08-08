var numCalls = 0, totalHits = 0, fetchedHits = 0, processedHits = 0, peakMemory = 0, memoryRatio = 0;

process.on('uncaughtException', function(e) {
	console.log('Caught exception in Main process: %s'.bold, e.toString());
	if (e instanceof Error) {
		console.log(e.stack);
	}
	process.exit(1);
});

process.on('exit', function() {
	console.log('Number of calls:\t%s', numCalls);
	console.log('Fetched Entries:\t%s documents', fetchedHits);
	console.log('Processed Entries:\t%s documents', processedHits);
	console.log('Source DB Size:\t\t%s documents', totalHits);
    console.log('Peak Memory Used:\t%s bytes (%s%%)', peakMemory, Math.round(memoryRatio * 100));
    console.log('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
});

var opts = require('./options.js').opts;
var sourceDriver = require(opts.sourceFile ? './drivers/file.js' : './drivers/es.js');
var targetDriver = require(opts.targetFile ? './drivers/file.js' : './drivers/es.js');
//var sourceDriver = require('./drivers/test.js');
//var targetDriver = require('./drivers/test.js');

var mappingReady = false;
var firstRun = true;
var hitQueue = [];
var memUsage = null;

/**
 * Returns the current used / available memory ratio.
 * Updates itself only every few milliseconds. Updates occur faster, when memory starts to run out.
 */
function getMemoryStats() {
    var nowObj = process.hrtime();
    var now = nowObj[0] * 1e9 + nowObj[1];
    var nextCheck = 0;
    if (memUsage !== null) {
        nextCheck = Math.pow((memUsage.heapTotal / memUsage.heapUsed), 2) * 100000000;
    }
    if (memUsage===null || memUsage.lastUpdate + nextCheck < now ) {
        memUsage = process.memoryUsage();
        memUsage.lastUpdate = now;
        memUsage.ratio = memUsage.heapUsed / memUsage.heapTotal;
        if (memUsage.heapUsed > peakMemory) {
            peakMemory = memUsage.heapUsed;
            memoryRatio = memUsage.ratio;
        }
    }
    return memUsage.ratio;
}

/**
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 * 
 * @param {function} callback Function to be called as soon as memory is available again.
 */
function waitOnTargetDriver(callback) {
    if (global.gc && getMemoryStats() > opts.memoryLimit) {
        global.gc();
        setTimeout(function() {
            waitOnTargetDriver(callback);   
        }, 100);
    }
    else {
        callback();
    }
}

/**
 * The response handler for fetching the meta data definition on the source driver. This will trigger the creation of 
 * meta data on the target driver and notify the storeHits function that hits are ready to be stored. What kind of meta data
 * will be stored actually depends on the settings in the opts object.
 * 
 * @param {Object} data Meta data object in form ElasticSearch understands it.
 */
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

/**
 * The response handler for fetching data from thr source driver. Will pass on the data to the storeHits function as soon
 * as some statistical data has been measured.
 * 
 * @param {Object[]} data Source data in the format ElasticSearch would return it to a search request.
 * @param {number} total Total number of hits to expect from the source driver
 */
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
        waitOnTargetDriver(function() {
            sourceDriver.getData(opts, handleDataResult);
        });
	}
}

/**
 * Will take an array of hits, that are converted into an ElasticSearch Bulk request and then sent off to the target driver.
 * This function will not start running until the meta data has been stored successfully and hits will be queued up to be sent
 * to the target driver in one big bulk request, once the meta data is ready.
 *
 * @param {Object[]} hits Source data in the format ElasticSearch would return it to a search request.
 */
function storeHits(hits) {
	if (!mappingReady) {
		hitQueue = hitQueue.concat(hits);
		console.log('Waiting for mapping on target host to be ready, queue length %s', hitQueue.length);
		return;
	}
	hits = hits.concat(hitQueue);
	hitQueue = [];
    if (!hits.length) {
        return;
    }
	var data = '';
	hits.forEach(function(hit) {
        if (!hit) {
            return;
        }
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
	});
    if (data.length) {
        targetDriver.storeHits(opts, data, function() {
            processedHits += hits.length;
            if (processedHits % 100 === 0) {
                console.log('Processed %s of %s entries (%s%%)', processedHits, totalHits, Math.round(processedHits / totalHits * 100));
            }
            if (processedHits == totalHits) {
            	console.log(processedHits, totalHits)
                process.exit(0);
            }
        });
    }
}

sourceDriver.getMeta(opts, handleMetaResult);
sourceDriver.getData(opts, handleDataResult);
