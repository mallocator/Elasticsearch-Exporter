/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate.
 */


exports.getInfo = function(callback) {
    console.log('Returns the name, version and other information about this plugin');
    var driverInfo = {
        id: 'uniqueIdentifier',
        name: 'Interface Definition',
        version: '1.0',
        desciption: 'An non functional driver implementation that show cases which methods exist and how to properly implement them'
    };

    console.log("Returns a list of required OPTIONS in the nomnom format.");
    var requiredOptions = {
        source: {},
        target: {}
    };

    callback(driverInfo, requiredOptions);
};

exports.verifyOptions = function (opts, callback) {
    callback([
        'This function should either return an array of error messages',
        'or it should be empty/null to signal everything is okay',
        'Either way, this can be used to fill some additional options after they have been parsed'
    ]);
};

exports.reset = function(callback) {
    console.log('Reset the state of this driver so that it can be used again');
    callback();
};

exports.getTargetStats = function(env, callback) {
    console.log('Return some information about the the database if it used as a target');
    callback({
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        docs: {
            indices: {
                index1: 123,
                index2: 123,
                indexN: 123
            },
            total: 123
        },
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getSourceStats = function(env, callback) {
    console.log('Return some information about the the database if it used as a source');
    callback({
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        docs: {
            indices: {
                index1: 123,
                index2: 123,
                indexN: 123
            },
            total: 123
        },
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getMeta = function (env, callback) {
    console.log("Returns information about the meta data of the source database. The format must be valid ElasticSearch 1.x format to work properly");
    callback({
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    console.log("Uses the metadata from #getMeta() and stores it in the target database");
    callback();
};

exports.getData = function (env, callback) {
    console.log('Returns the data from the source database in standard ElasticSearch format');
    var data = [{
        _index: "indexName",
        _type: "typeName",
        _id: "1",
        _version: 1,
        found: true,
        _source: {}
    }];
    callback(data, 1000);
};

exports.putData = function (env, data, callback) {
    console.log("Stores the data in the target database");
    callback();
};