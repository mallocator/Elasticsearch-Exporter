if (global.GENTLY) require = global.GENTLY.hijack(require);
var fs = require('fs');
var through = require('through');
var zlib = require('zlib');
var path = require('path');

exports.storeMeta = function (opts, metadata, callback) {
    if (opts.sourceType) {
        console.log('Storing type mapping in meta file ' + opts.targetFile + '.meta');
        createMetaFile(opts, {
            _type: opts.sourceType,
            _index: opts.sourceIndex,
            mapping: metadata
        }, callback);
    } else if (opts.sourceIndex) {
        console.log('Storing index mapping in meta file ' + opts.targetFile + '.meta');
        createMetaFile(opts, {
            _index: opts.sourceIndex,
            metadata: metadata
        }, callback);
    } else {
        console.log('Storing entire index mapping in meta file ' + opts.targetFile + '.meta');
        createMetaFile(opts, {
            metadata: metadata
        }, callback);
    }
};

function createParentDir(opts) {
	var dir = '';
    path.dirname(opts.targetFile).split(path.sep).forEach(function(dirPart){
        dir += dirPart + path.sep;
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir);
        }
    });
}

var targetStream = null;

function createMetaFile(opts, data, callback) {
    createParentDir(opts);
    fs.writeFile(opts.targetFile + '.meta', JSON.stringify(data, null, 2), { encoding:'utf8' }, function (err) {
        if (err) throw err;
        if (!opts.targetCompression) {
            fs.writeFile(opts.targetFile + '.data', '', function() {
                if (err) throw err;
                callback();
            });
        } else {
            targetStream = through().pause();
            var out = fs.createWriteStream(opts.targetFile + '.data');
            targetStream.pipe(zlib.createGzip()).pipe(out);
            callback();
        }
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

var lineCount = null;

function getLineCount(opts, callback) {
    if (lineCount !== null) {
        callback(lineCount);
        return;
    }
    var count = 0;
    var file = opts.sourceFile + '.data';
    var stream = opts.sourceCompression ? fs.createReadStream(file).pipe(zlib.createGunzip()) : fs.createReadStream(file);

    stream.on('readable', function() {
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
	return matches !== null && buffer.match(/\n/g).length > 1;
}

var buffer = '';
var items = [];

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

var fileReader = null;
var end = false;

exports.getData = function(opts, callback) {
    if (fileReader === null) {
        getLineCount(opts, function(lineCount) {
            if (opts.sourceCompression) {
                fileReader = fs.createReadStream(opts.sourceFile + '.data').pipe(zlib.createGunzip());
            } else {
                fileReader = fs.createReadStream(opts.sourceFile + '.data');
            }
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

exports.storeData = function(opts, data, callback) {
    if (targetStream) {
        targetStream.queue(data).resume();
        callback();
    } else {
        fs.appendFile(opts.targetFile + '.data', data, { encoding: 'utf8' }, function (err) {
            if (err) throw err;
            callback();
        });
    }
};

exports.end = function() {
    if (targetStream) {
        targetStream.end();
    } else {
        process.exit(0);
    }
};