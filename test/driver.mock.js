exports.reset = function() {
    exports.sourceIndex = null;
    exports.sourceType = null;
    exports.metadata = null;
    exports.data = null;
    exports.log = false;
    exports.failMethod = null;
    exports.runs = 0;
    exports.maxruns = 2;
};

exports.storeMeta = function (opts, metadata, callback) {
    if (exports.failMethod) {
        exports.failMethod();
        return;
    }
    exports.metadata = metadata;
    if (exports.log) {
        if (opts.sourceType) {
                console.log('(Not) Creating meta data on type level in test sink:');
        } else if (opts.sourceIndex) {
            console.log('(Not) Creating meta data on index level in test sink:');
        } else {
            console.log('(Not) Creating all meta data on root level in test sink:');
        }
        console.log('--- options:\n', opts, '\n--- metadata:\n', metadata, '\n');
    }
    callback();
};

exports.getMeta = function(opts, callback) {
    if (exports.failMethod) {
        exports.failMethod();
        return;
    }
    if (exports.log) {
        console.log('Returning test metadata on index level');
    }
    if (exports.sourceIndex) {
        opts.sourceIndex = exports.sourceIndex;
    }
    if (exports.sourceType) {
        opts.sourceType = exports.sourceType;
    }
    callback(exports.metadata);
};

exports.getData = function(opts, callback) {
    if (exports.failMethod) {
        exports.failMethod();
        return;
    }
    if (exports.runs == exports.maxruns) {
        callback([], exports.data.length);
        return;
    }
    exports.runs++;
    if (exports.log) {
        console.log('Returning test data');
    }
    var slice = [];
    for (var i = 0; i < exports.data.length/(exports.maxruns-exports.runs); i++) {
        slice.push(exports.data.pop());
    }
    callback(slice, exports.data.length);
};

exports.storeData = function(opts, data, callback) {
    if (exports.failMethod) {
        exports.failMethod();
        return;
    }
    exports.data = data;
    if (exports.log) {
        console.log('(Not) Storing data in test sink:');
        console.log('--- options:\n', opts, '\n--- data:\n', data, '\n');
    }
    callback();
};

exports.end = function() {
    if (exports.failMethod) {
        exports.failMethod();
        return;
    }
};