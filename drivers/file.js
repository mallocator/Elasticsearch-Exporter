var fs = require('fs');

exports.createTypeMeta = function(opts, metadata, callback) {
    console.log('Storing type mapping in meta file ' + opts.sourceFile + '.meta');
    createMeta(opts, {
        type : opts.sourceType,
        index : opts.sourceIndex,
        metadata: metadata
    }, callback);
};

exports.createIndexMeta = function(opts, metadata,  callback) {
    console.log('Storing index mapping in meta file ' + opts.sourceFile + '.meta');
    createMeta(opts, {
        index : opts.sourceIndex,
        metadata: metadata
    }, callback);
};

exports.createAllMeta = function(opts, metadata, callback) {
    console.log('Storing entire index mapping in meta file ' + opts.sourceFile + '.meta');
    createMeta(opts, {
        metadata: metadata
    }, callback);
};

function createMeta(opts, data, callback) {
    fs.writeFile(opts.targetFile + '.meta', JSON.stringify(data, null, 2), { encoding:'utf8' }, function (err) {
        if (err) throw err;
        fs.writeFile(opts.targetFile + '.data', null, function() {
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
var buffer = '';
var end = false;
var lineCount = null;

function getLine() {
    var linePosition = buffer.indexOf('\n');
    if (linePosition == -1) {
        return buffer;
    }
    var line = buffer.substr(0, linePosition);
    buffer = buffer.substr(linePosition + 1);
    return JSON.parse(line);
}

function getLineCount(file, callback) {
    if (lineCount !== null) callback(lineCount);
    var count = 0;
    var stream = fs.createReadStream(file);
    stream.on('readable', function(chunk) {
        count += (''+ stream.read()).match(/\n/g).length;
    });
    stream.on('end', function() {
        lineCount = count/2;
        callback(lineCount);
    });
}

exports.getData = function(opts, callback) {
    if (end) {
        getLineCount(opts.sourceFile + '.data', function(count) {
            callback([], count);    
        });
    }
    if (fileReader === null) {
        fileReader = fs.createReadStream(opts.sourceFile + '.data', { encoding:'utf8' });
    }
    fileReader.on('readable', function() {
        var items = [];
        buffer += fileReader.read();
        while (buffer.length && items.length < 100) {
            var metaData = getLine();
            var data = getLine();
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
        getLineCount(opts.sourceFile + '.data', function(count) {
            callback(items, count);    
        });
    });
    fileReader.on('end', function() {
        end = true;
    });
};

exports.storeHits = function(opts, data) {
    fs.appendFile(opts.targetFile + '.data', data, { encoding:'utf8' }, function (err) {
        if (err) throw err;
    });
};