/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate. In case the driver should call a process.exit() at any point, please use a status
 * code above 130 when exiting.
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

    var errors = null;

    callback(errors, driverInfo, requiredOptions);
};

exports.verifyOptions = function (opts, callback) {
    // This option is called if the driver is either the target or the source. To check if it is either look up the id of
    // opts.drivers.source or opts.drivers.target
    callback([
        'This function should either return an array of error messages',
        'or it should be empty/null to signal everything is okay',
        'Either way, this can be used to fill some additional options after they have been parsed'
    ]);
};

exports.reset = function(env, callback) {
    console.log('Reset the state of this driver so that it can be used again');
    var errors = null;
    callback(errors);
};

exports.getTargetStats = function(env, callback) {
    console.log('Return some information about the the database if it used as a target');
    var errors = null;
    callback(errors, {
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red",
        aliases: ["list", "of", "aliases", "or", false]
    });
};

exports.getSourceStats = function(env, callback) {
    console.log('Return some information about the the database if it used as a source');
    var errors = null;
    callback(errors, {
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

/**
 * This method fetches the meta data from the source data base. It is convention that the source driver has to override
 * the index and type, if they have been set to be different than the target database (env.options.target.index,
 * env.options.target.type).
 * @param env
 * @param callback
 */
exports.getMeta = function (env, callback) {
    console.log("Returns information about the meta data of the source database. The format must be valid ElasticSearch 1.x format to work properly.");
    var errors = null;
    callback(errors, {
        mappings: {
            index1: {
                type1: {}
            }
        },
        settings: {
            index1: {}
        }
    });
};

exports.putMeta = function (env, metadata, callback) {
    console.log("Uses the metadata from #getMeta() and stores it in the target database");
    var errors = null;
    callback(errors);
};

/**
 * This is an additional convenience method that will be called right before the import with getData() is started and
 * again before putData() is called. The implementation is optional and will not be validated.
 * @param env
 * @param isSource is set to true if called right before getData() otherwise it's being called right before putData()
 */
exports.prepareTransfer = function(env, isSource) {

};

/**
 * This as well as the putData function will be called in a separate process so that stateful values are reset. If the
 * driver does not support concurrency it will be in the same process. Also note that it is convention for the source
 * driver to override the type and index, if they have been set differently (env.options.target.index,
 * env.options.target.type).
 * @param env
 * @param callback
 * @param from
 * @param size
 */
exports.getData = function (env, callback, from, size) {
    console.log('Returns the data from the source database in standard ElasticSearch format. The "from" and "size" parameters are both optional and will not be verified, but passed in.');
    var errors = null;
    var data = [{
        _id: "1",
        _index: "indexName",
        _type: "typeName",
        _source: {}
    }];
    callback(errors, data);
};

exports.putData = function (env, docs, callback) {
    console.log("Stores the data in the target database. Make sure that you generate an id for each element of none is given.");
    callback();
};

exports.end = function(env) {
    console.log("An optional finalizer method on the target driver that gets called after all documents have been exported. Allows the driver to do some clean up.");
}