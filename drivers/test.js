exports.createTypeMeta = function(opts, metadata, callback) {
    console.log('(Not) Creating meta data on type level in test sink:');
    console.log('--- options:\n', opts, '\n--- metadata:\n', metadata, '\n');
    callback();
};

exports.createIndexMeta = function(opts, metadata,  callback) {
    console.log('(Not) Creating meta data on index level in test sink:');
    console.log('--- options:\n', opts, '\n--- metadata:\n', metadata, '\n');
    callback();
};

exports.createAllMeta = function(opts, metadata, callback) {
    console.log('(Not) Creating all meta data on root level in test sink:');
    console.log('--- options:\n', opts, '\n--- metadata:\n', metadata, '\n');
    callback();
};

exports.getMeta = function(opts, callback) {
    console.log('Returning test metadata on index level');
    opts.sourceIndex = "my.test.index";
    if (!opts.targetIndex) {
        opts.targetIndex = "my.test.index";
    }
    callback({
        "my.test.index": {
            mytype: {
                properties: {
                    prop1:  {
                        type: "string"
                    },
                    prop2:  {
                        type: "string"
                    }
                }
            }
        }
    });
};

var id = 1;
var runs = 0;
var maxruns = 2;
exports.getData = function(opts, callback) {
    if (runs==maxruns) {
        callback([], maxruns * 2);
        return;
    }
    runs++;
    console.log('Returning test data');
    callback([{
            _id : id++,
            _index : 'my.test.index',
            _type : 'mytype',
            _source : {
                prop1 : 'first test object',
                prop2 : 'what is?'
            }
        },{
            _id : id++,
            _index : 'my.test.index',
            _type : 'mytype',
            _source : {
                prop1 : 'second test object',
                prop2 : '42... obviously!'
            }
        }
    ], maxruns * 2);
};

exports.storeHits = function(opts, data, callback) {
    console.log('(Not) Storing data in test sink:');
    console.log('--- options:\n', opts, '\n--- data:\n', data, '\n');
    callback();
};