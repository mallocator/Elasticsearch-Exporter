var fs = require('fs');

exports.createTypeMeta = function(opts, metadata, callback) {
    console.log('Storing type mapping in meta file ' + opts.targetFile + '.meta');
    createMeta(opts, {
        type : opts.sourceType,
        index : opts.sourceIndex,
        metadata: metadata
    }, callback);
};

exports.createIndexMeta = function(opts, metadata,  callback) {
    console.log('Storing index mapping in meta file ' + opts.targetFile + '.meta');
    createMeta(opts, {
        index : opts.sourceIndex,
        metadata: metadata
    }, callback);
};

exports.createAllMeta = function(opts, metadata, callback) {
    console.log('Storing entire index mapping in meta file ' + opts.targetFile + '.meta');
    createMeta(opts, {
        metadata: metadata
    }, callback);
};

function createMeta(opts, data, callback) {
    fs.writeFile(opts.targetFile + '.meta', JSON.stringify(data, null, 2), { encoding:'utf8' }, function (err) {
        if (err) throw err;
        fs.writeFile(opts.targetFile + '.data', '', function() {
            if (err) throw err;
            callback();
        });
    });
}

exports.getMeta = function(opts, callback) {
    console.log('Reading mapping from meta file ' + opts.sourceFile + '.meta');
    fs.readFile(opts.sourceFile + '.meta', { encoding:'utf8' }, function (err, data) {
        if (err) throw err;
        data = JSON.parse(data);
        if (data.type) {
            opts.sourceType = data.type;
            if (!opts.targetType) {
                opts.targetType = opts.sourceType;
            }
        }
        if (data.index) {
            opts.sourceIndex = data.index;
            if (!opts.targetIndex) {
                opts.targetIndex = opts.sourceIndex;
            }
        }
        callback(data.metadata);
    });
};

var fileReader = null;
var readable = true;
var end = false;
var lineProgress = 0;
var lineCount = null;
var buffer = '';
var items = [];
var end = false;

function getLineCount(file, callback) {
    if (lineCount !== null) {
        callback(lineCount);
        return;
    }
    var count = 0;
    var stream = fs.createReadStream(file);
    stream.on('readable', function(chunk) {
    	try {
        	count += (''+ stream.read()).match(/\n/g).length;
    	} catch (e) {}
    });
    stream.on('end', function() {
        lineCount = Math.ceil(count/2);
        callback(lineCount);
    });
}

function getNewlineMatches(buffer) {
	var matches = buffer.match(/\n/g);
	return matches != null && buffer.match(/\n/g).length > 1
}

function parseBuffer() {
	var nlIndex1 = buffer.indexOf('\n');
	var nlIndex2 = buffer.indexOf('\n', nlIndex1 + 1);
	var metaData = JSON.parse(buffer.substr(0, nlIndex1));
	var data = JSON.parse(buffer.substr(nlIndex1 + 1, nlIndex2 - nlIndex1));
	buffer = buffer.substr(nlIndex2 + 1);
	items.push({
		_id : metaData.index._id,
		_index : metaData.index._index,
		_type : metaData.index._type,
		fields : {
			_timestamp : metaData.index._timestamp,
			_version : metaData.index._version,
			_percolate : metaData.index._percolate,
			_routing : metaData.index._routing,
			_parent : metaData.index._parent,
			_ttl : metaData.index._ttl
		},
		_source : data
	});
}

exports.getData = function(opts, callback) {
    if (fileReader === null) {
        getLineCount(opts.sourceFile + '.data', function(lineCount) {
            fileReader = fs.createReadStream(opts.sourceFile + '.data', { encoding:'utf8' });
            fileReader.on('data', function(chunk) {
            	fileReader.pause();
            	buffer += chunk;
            	while (getNewlineMatches(buffer)) {
					parseBuffer();
					if (items.length >= 100) {
						callback(items, lineCount);
						items = [];
					}
            	}
            	fileReader.resume();
            });
            fileReader.on('end', function() {
            	end = true;
            	if (buffer.length) {
            		buffer += '\n';
            		parseBuffer();
            	}
                callback(items, lineCount);
            });
        });
    }
};

exports.storeHits = function(opts, data, callback) {
    fs.appendFile(opts.targetFile + '.data', data, { encoding:'utf8' }, function (err) {
        if (err) throw err;
        callback();
    });
};
