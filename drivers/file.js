if (global.GENTLY) require = global.GENTLY.hijack(require);
var fs = require('fs');
var through = require('through');
var zlib = require('zlib');
var path = require('path');

exports.reset = function() {
    exports.targetStream = null;
    exports.lineCount = null;
    exports.buffer = '';
    exports.items = [];
    exports.fileReader = null;
};

exports.storeMeta = function (opts, metadata, callback) {
    metadata._scope = 'all';
    if (opts.sourceIndex) {
        metadata._index = opts.sourceIndex;
        metadata._scope = 'index';
    }
    if (opts.sourceType) {
        metadata._type = opts.sourceType;
        metadata._scope = 'type';
    }
    if (opts.logEnabled) {
        console.log('Storing ' + metadata._scope + ' mapping in meta file ' + opts.targetFile + '.meta');
    }
    createMetaFile(opts, metadata, callback);
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
            exports.targetStream = through().pause();
            var out = fs.createWriteStream(opts.targetFile + '.data');
            exports.targetStream.pipe(zlib.createGzip()).pipe(out);
            callback();
        }
    });
}

exports.getMeta = function(opts, callback) {
    if (opts.logEnabled) {
        console.log('Reading mapping from meta file ' + opts.sourceFile + '.meta');
    }
    fs.readFile(opts.sourceFile + '.meta', { encoding:'utf8' }, function (err, data) {
        if (err) throw err;
        data = JSON.parse(data);
        if (data._index) {
            opts.sourceIndex = data._index;
            opts.targetIndex = opts.targetIndex? opts.targetIndex : opts.sourceIndex;
            delete data._index;
        }
        if (data._type) {
            opts.sourceType = data._type;
            opts.targetType = opts.targetType? opts.targetType : opts.sourceType;
            delete data._type;
        }
        delete data._scope;
        callback(data);
    });
};


function getLineCount(opts, callback) {
    if (exports.lineCount !== null) {
        callback(exports.lineCount);
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
        exports.lineCount = Math.ceil(count/2);
        callback(exports.lineCount);
    });
}

function getNewlineMatches(buffer) {
	var matches = buffer.match(/\n/g);
	return matches !== null && buffer.match(/\n/g).length > 1;
}


function parseBuffer() {
	var nlIndex1 = exports.buffer.indexOf('\n');
	var nlIndex2 = exports.buffer.indexOf('\n', nlIndex1 + 1);
	var metaData = JSON.parse(exports.buffer.substr(0, nlIndex1));
	var data = JSON.parse(exports.buffer.substr(nlIndex1 + 1, nlIndex2 - nlIndex1));
    exports.buffer = exports.buffer.substr(nlIndex2 + 1);
    exports.items.push({
		_id : metaData.index._id,
		_index : metaData.index._index,
		_type : metaData.index._type,
        _version: metaData.index._version,
		fields : {
			_timestamp : metaData.index._timestamp,
			_percolate : metaData.index._percolate,
			_routing : metaData.index._routing,
			_parent : metaData.index._parent,
			_ttl : metaData.index._ttl
		},
		_source : data
	});
}


exports.getData = function(opts, callback) {
    if (exports.fileReader === null) {
        getLineCount(opts, function(lineCount) {
            if (opts.sourceCompression) {
                exports.fileReader = fs.createReadStream(opts.sourceFile + '.data').pipe(zlib.createGunzip());
            } else {
                exports.fileReader = fs.createReadStream(opts.sourceFile + '.data');
            }
            exports.fileReader.on('data', function(chunk) {
                exports.fileReader.pause();
                exports.buffer += chunk;
                while (getNewlineMatches(exports.buffer)) {
					parseBuffer();
					if (exports.items.length >= 100) {
						callback(exports.items, lineCount);
                        exports.items = [];
					}
                }
                exports.fileReader.resume();
            });
            exports.fileReader.on('end', function() {
                if (exports.buffer.length) {
                    exports.buffer += '\n';
                    parseBuffer();
                }
                callback(exports.items, lineCount);
            });
        });
    }
};

exports.storeData = function(opts, data, callback) {
    if (exports.targetStream) {
        exports.targetStream.queue(data).resume();
        callback();
    } else {
        fs.appendFile(opts.targetFile + '.data', data, { encoding: 'utf8' }, function (err) {
            if (err) throw err;
            callback();
        });
    }
};

exports.end = function() {
    if (exports.targetStream) {
        exports.targetStream.end();
    } else {
        process.exit(0);
    }
};