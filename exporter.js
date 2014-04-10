// Process exit signals:
// 0 - Operation successful / No documents found to export
// 1 - invalid options
// 2 - source or target databse cluster health = red
// 99 - Uncaught Exception

exports.opts = null;
exports.sourceDriver = null;
exports.targetDriver = null;
exports.mappingReady = false;
exports.firstRun = true;
exports.hitQueue = [];
exports.memUsage = null;
exports.numCalls = 0;
exports.totalHits = 0;
exports.fetchedHits = 0;
exports.processedHits = 0;
exports.peakMemory = 0;
exports.memoryRatio = 0;

exports.handleUncaughtExceptions = function(e) {
    console.log('Caught exception in Main process: %s'.bold, e.toString());
    if (e instanceof Error) {
        console.log(e.stack);
    }
    process.exit(99);
};

exports.printSummary = function() {
    if (exports.opts.logEnabled) {
        console.log('Number of calls:\t%s', exports.numCalls);
        if (exports.opts && exports.opts.sourceStats && exports.opts.sourceStats.retries) {
            console.log('Retries to source:\t%s', exports.opts.sourceStats.retries);
        }
        if (exports.opts && exports.opts.targetStats && exports.opts.targetStats.retries) {
            console.log('Retries to target:\t%s', exports.opts.targetStats.retries);
        }
        console.log('Fetched Entries:\t%s documents', exports.fetchedHits);
        console.log('Processed Entries:\t%s documents', exports.processedHits);
        console.log('Source DB Size:\t\t%s documents', exports.totalHits);
        if (exports.opts && exports.opts.sourceStats && exports.opts.sourceStats.count) {
            console.log('Unique Entries:\t\t%s documents', exports.opts.sourceStats.count.uniques);
            console.log('Duplicate Entries:\t%s documents', exports.opts.sourceStats.count.duplicates);
        }
        if (exports.peakMemory) {
            console.log('Peak Memory Used:\t%s bytes (%s%%)', exports.peakMemory, Math.round(exports.memoryRatio * 100));
            console.log('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
        }
    }
};

/**
 * Returns the current used / available memory ratio.
 * Updates itself only every few milliseconds. Updates occur faster, when memory starts to run out.
 */
exports.getMemoryStats = function() {
    var nowObj = process.hrtime();
    var now = nowObj[0] * 1e9 + nowObj[1];
    var nextCheck = 0;
    if (exports.memUsage !== null) {
        nextCheck = Math.pow((exports.memUsage.heapTotal / exports.memUsage.heapUsed), 2) * 100000000;
    }
    if (exports.memUsage===null || exports.memUsage.lastUpdate + nextCheck < now ) {
        exports.memUsage = process.memoryUsage();
        exports.memUsage.lastUpdate = now;
        exports.memUsage.ratio = exports.memUsage.heapUsed / exports.memUsage.heapTotal;
        if (exports.memUsage.heapUsed > exports.peakMemory) {
            exports.peakMemory = exports.memUsage.heapUsed;
            exports.memoryRatio = exports.memUsage.ratio;
        }
    }
    return exports.memUsage.ratio;
}

/**
 * If more than 90% of the memory is used up, this method will use setTimeout to wait until there is memory available again.
 *
 * @param {function} callback Function to be called as soon as memory is available again.
 */
exports.waitOnTargetDriver = function(callback) {
    if (global.gc && exports.getMemoryStats() > exports.opts.memoryLimit) {
        global.gc();
        setTimeout(function() {
            exports.waitOnTargetDriver(callback);
        }, 100);
    }
    else {
        callback();
    }
}

/**
 * Waits for sourceStats and targetStats to come back and checks if the cluster health is not red. Only when both stat
 * objects are available will the check be performed and the callback executed.
 * @param callback
 */
exports.checkHealth = function(callback) {
    if (exports.opts.sourceStats && exports.opts.targetStats) {
        var error = false;
        if (exports.opts.sourceStats.cluster_status == 'red') {
            console.log("The source cluster health status is 'red'! Aborting export.".bold)
            error = true;
        }
        if (exports.opts.targetStats.cluster_status == 'red') {
            console.log("The target cluster health status is 'red'! Aborting export.".bold)
            error = true;
        }
        if (error) {
            process.exit(2);
        }
        if (exports.opts.sourceStats.docs.total === 0) {
            console.log("The source driver has not reported any documents that can be exported. Not exporting.".bold)
            process.exit(0);
        }
        if (exports.opts.sourceStats.aliases[exports.opts.sourceIndex]) {
            exports.opts.sourceIndex = exports.opts.sourceStats.aliases[exports.opts.sourceIndex];
        }
        if (exports.opts.targetStats.aliases[exports.opts.targetIndex]) {
            exports.opts.targetIndex = exports.opts.targetStats.aliases[exports.opts.targetIndex];
        }
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
exports.handleMetaResult = function(data) {
    if (exports.opts.testRun) {
        return;
    }
    function done(err) {
        if (err) console.log(err);
        if(exports.opts.logEnabled) {
            console.log("Mapping is now ready. Starting with " + exports.hitQueue.length + " queued hits.");
        }
        exports.mappingReady = true;
        if (exports.hitQueue.length) {
            exports.storeHits([]);
        }
    }
    exports.targetDriver.storeMeta(exports.opts, data, done);
}

/**
 * The response handler for fetching data from thr source driver. Will pass on the data to the storeHits function as soon
 * as some statistical data has been measured.
 *
 * @param {Object[]} data Source data in the format ElasticSearch would return it to a search request.
 * @param {number} total Total number of hits to expect from the source driver
 */
exports.handleDataResult = function(data, total) {
    exports.totalHits = total;
    if (exports.opts.testRun) {
        console.log("Stopping further execution, since this is only a test run. No operations have been executed on the target database.");
        process.exit(0);
    }
    if (data.length) {
		exports.storeHits(data);
	}
    if (exports.firstRun || data.length) {
        exports.firstRun = false;
		exports.fetchedHits += data.length;
		exports.numCalls++;
        exports.waitOnTargetDriver(function() {
            exports.sourceDriver.getData(exports.opts, exports.handleDataResult);
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
exports.storeHits = function(hits) {
	if (!exports.mappingReady) {
		exports.hitQueue = exports.hitQueue.concat(hits);
        if (exports.opts.logEnabled) {
		    console.log('Waiting for mapping on target host to be ready, queue length %s', exports.hitQueue.length);
        }
		return;
	}
	hits = hits.concat(exports.hitQueue);
	exports.hitQueue = [];
    if (!hits.length) {
        return;
    }
	var data = '';
	hits.forEach(function(hit) {
        if (!hit) {
            return;
        }
        if (exports.opts.count) {
            var gid = hit._index + "_" + hit._type + "_" + hit._id;
            if (!exports.opts.sourceStats.count) {
                exports.opts.sourceStats.count = {
                    duplicates: 0,
                    uniques: 0
                }
            }
            if (exports.opts.sourceStats.count.ids[gid]) {
                exports.opts.sourceStats.count.duplicates++;
            } else {
                exports.opts.sourceStats.count.uniques++;
            }
        }
        var op = exports.opts.overwrite ? 'index' : 'create';
        var metaData = {};
        metaData[op] =  {
            _index: exports.opts.targetIndex ? exports.opts.targetIndex : hit._index,
            _type: exports.opts.targetType ? exports.opts.targetType : hit._type,
            _id: hit._id,
            _version: hit._version ? hit._version : null
        }
		if (hit.fields) {
            ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'].forEach(function(field){
                if (hit.fields[field]) {
                    metaData[op][field] = hit.fields[field];
                }
            });
		}
		data += JSON.stringify(metaData) + '\n' + JSON.stringify(hit._source) + '\n';
	});
    if (data.length) {
        exports.targetDriver.storeData(exports.opts, data, function(err) {
            if (err) console.log(err);
            exports.processedHits += hits.length;
            if (exports.processedHits % 100 === 0 && exports.opts.logEnabled) {
                console.log('Processed %s of %s entries (%s%%)', exports.processedHits, exports.totalHits, Math.round(exports.processedHits / exports.totalHits * 100));
            }
            if (exports.processedHits == exports.totalHits) {
                if (exports.targetDriver.end) {
                    exports.targetDriver.end();
                }
                else {
                    process.exit(0);
                }
            }
        });
    }
}

if (require.main === module) {
    process.on('uncaughtException', exports.handleUncaughtExceptions);
    exports.opts = require('./options.js').opts();
    process.on('exit', exports.printSummary);

    exports.sourceDriver = require(exports.opts.sourceFile ? './drivers/file.js' : './drivers/es.js');
    exports.targetDriver = require(exports.opts.targetFile ? './drivers/file.js' : './drivers/es.js');

    exports.sourceDriver.reset();
    exports.targetDriver.reset();

    function startExport() {
        if (exports.opts.mapping) {
            exports.handleMetaResult(exports.opts.mapping)
        } else {
            exports.sourceDriver.getMeta(exports.opts, exports.handleMetaResult);
        }
        exports.sourceDriver.getData(exports.opts, exports.handleDataResult);
    }

    exports.sourceDriver.getSourceStats(exports.opts, function() {
        exports.checkHealth(startExport)
    });
    exports.targetDriver.getTargetStats(exports.opts, function () {
        exports.checkHealth(startExport)
    });
}
