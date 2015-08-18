var MongoClient = require('mongodb').MongoClient;
var async = require('async');
var log = require('../log.js');

var id = 'mongodb';

exports.getInfo = function (callback) {
    var info = {
        id: id,
        name: 'Mongo DB Driver',
        version: '1.0',
        desciption: 'A Mongo DB driver to import and export data'
    };
    var options = {
        source: {
            host: {
                abbr: 'h',
                preset: 'localhost',
                help: 'The host from which data is to be exported from.'
            }, port: {
                abbr: 'p',
                preset: 27017,
                help: 'The port of the source host to talk to.'
            }, database: {
                abbr: 'd',
                help: 'The database name from which to export data from. If no database is given the entire database is exported.'
            }, collection: {
                abbr: 'c',
                help: 'The collection from which to export data from. If no collection is given the entire database is exported.'
            }, query: {
                abbr: 'q',
                help: 'A query that allows to filter the source data. The standard MongoDB find() format is used.'
            }, user: {
                abbr: 'u',
                help: 'The user to authenticate with (if necessary)'
            }, secret: {
                abbr: 's',
                help: 'The users secret/passsword used to authenticate (if necessary)'
            }
        },
        target: {
            host: {
                abbr: 'h',
                preset: 'localhost',
                help: 'The host from which data is to be exported from.'
            }, port: {
                abbr: 'p',
                preset: 27017,
                help: 'The port of the source host to talk to.'
            }, database: {
                abbr: 'd',
                help: 'The database name to which to import data to. If no database is given, the source project name is used.'
            }, collection: {
                abbr: 'c',
                help: 'The collection to which to import data to. If no collection is given the source collection name is used.'
            }, user: {
                abbr: 'u',
                help: 'The user to authenticate with (if necessary)'
            }, secret: {
                abbr: 's',
                help: 'The users secret/passsword used to authenticate (if necessary)'
            }
        }
    };
    callback(null, info, options);
};

exports.verifyOptions = function (opts, callback) {
    var err = [];
    if (opts.drivers.source == id && opts.drivers.target == id) {
        if (opts.source.host != opts.target.host) { callback(); return; }
        if (opts.source.port != opts.target.port) { callback(); return; }
        if (opts.target.project && opts.source.project != opts.target.project) { callback(); return; }
        if (opts.target.dataset && opts.source.dataset != opts.target.dataset) { callback(); return; }
        err.push("The source and target location are the same. Not moving any data!");
    }
    if (opts.drivers.source == id) {
        if (opts.source.user && !opts.source.secret) {
            err.push("The source driver has a username specified but not secret/password");
        }
        if (!opts.source.user && opts.source.secret) {
            err.push("The source driver has a secret/password specified but no user");
        }
        if (opts.source.query) {
            try {
                opts.source.query = JSON.parse(opts.source.query);
            } catch(e) {}
        }
    }
    if (opts.drivers.target == id) {
        if (opts.target.user && !opts.target.secret) {
            err.push("The target driver has a username specified but not secret/password");
        }
        if (!opts.target.user && opts.target.secret) {
            err.push("The target driver has a secret/password specified but no user");
        }
    }
    callback(err);
};

exports.target = {
    _cli: null,
    _db: null,
    connect: function(env, callback) {
        if (!this._cli) {
            var url = 'mongodb://' + env.options.target.host + ':' + env.options.target.port;
            MongoClient.connect(url, function(err, db) {
                if (err){
                    callback(err);
                    return;
                }
                exports.target._db = db;
                callback();
            });
        }
        callback();
    },
    disconnect: function() {
        this._db.close();
    }
};

exports.source = {
    _db: null,
    getDb: function(sopts, callback) {
        if (!exports.source._db) {
            var url = 'mongodb://' + sopts.host + ':' + sopts.port;
            MongoClient.connect(url, function (err, db) {
                if (err) {
                    callback(err);
                    return;
                }
                exports.source._db = db;
                callback(null, db);
            });
        }
        else {
            callback(null, exports.source._db);
        }
    },

    listDatabases: function (env, callback) {
        var filter = env.options.source.database ? env.options.source.database.split(',') : [];
        exports.source.getDb(env.options.source, function (err) {
            if (err) {
                callback(err);
                return;
            }
            exports.source._db.admin().listDatabases(function(err, result) {
                if (err) {
                    callback(err);
                    return;
                }
                var response = [];
                for (var i in result.databases) {
                    if (!filter.length || filter.indexOf(result.databases[i].name) != -1) {
                        response.push(result.databases[i].name);
                    }
                }
                callback(null, response);
            });
        });
    },

    disconnect: function () {
        if (this._db) {
            this._db.close();
        }
    }
};

exports.reset = function (env, callback) {
    exports.source._cli = null;
    exports.source._db = null;
    callback();
};

exports.getTargetStats = function (env, callback) {
    // TODO
    var errors = null;
    callback(errors, {
        version: "1.0.0 or something",
        cluster_status: "Green, Yellow or Red"
    });
};

exports.getSourceStats = function (env, callback) {
    var stats = {
        cluster_status: 'Red',
        docs: {
            total: 0
        },
        databases: {}
    };

    async.parallel([
        function(callback) {
            exports.source.getDb(env.options.source, function(err, db) {
                if (err) {
                    callback(err);
                    return;
                }
                db.admin().serverStatus(function (err, info) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    stats.version = info.version;
                    stats.cluster_status = 'Green';
                    callback();
                });
            });
        }, function(callback) {
            function countCollectionTask(database, collectionName) {
                return function(callback) {
                    database.collection(collectionName, function(err, collection) {
                        if (err) {
                            callback(err);
                            return;
                        }
                        log.debug("Counting items in %s/%s", database.databaseName, collectionName);
                        collection.count(env.options.source.query, function(err, count) {
                            if (err) {
                                callback(err);
                                return;
                            }
                            stats.databases[database.databaseName][collectionName] = count;
                            callback();
                        });
                    });
                };
            }

            function listCollectionsTask(databaseName) {
                var filter = env.options.source.collection ? env.options.source.collection.split(',') : [];
                return function (callback) {
                    var database = exports.source._db.db(databaseName);
                    database.listCollections().toArray(function(err, collections) {
                        if (err) {
                            callback(err);
                            return;
                        }
                        var countTasks = [];
                        for (var i in collections) {
                            if (!filter.length || filter.indexOf(collections[i].name) != -1) {
                                log.debug("Adding task to count collection %s/%s", databaseName, collections[i].name);
                                countTasks.push(countCollectionTask(database, collections[i].name));
                            }
                        }
                        async.parallel(countTasks, callback);
                    });
                };
            }

            exports.source.getDb(env.options.source, function (err) {
                if (err) {
                    callback(err);
                    return;
                }
                exports.source.listDatabases(env, function (err, databases) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    var tasks = [];
                    log.debug("Found %d databases on source MongoDB", databases.length);
                    for (var i in databases) {
                        var database = databases[i];
                        log.debug("Adding task for database %s to fetch colections", database);
                        tasks.push(listCollectionsTask(database));
                        stats.databases[database] = {};
                    }
                    async.parallel(tasks, function (err) {
                        if (err) {
                            callback(err);
                            return;
                        }
                        for (var i in stats.databases) {
                            for (var j in stats.databases[i]) {
                                stats.docs.total += stats.databases[i][j];
                            }
                        }
                        callback();
                    });
                });
            });
        }
    ], function(err) {
        console.log(stats)
        callback(err, stats);
    });
};

exports.getMeta = function (env, callback) {
    console.log(env.statistics.source)
    process.exit();
    var errors = null;
    callback(errors, {
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    callback();
};

exports.getData = function (env, callback) {
    var errors = null;
    callback(errors, [{
        _index: "indexName",
        _type: "typeName",
        _id: "1",
        _version: 1,
        found: true,
        _source: {}
    }]);
};

exports.putData = function (env, docs, callback) {
    callback();
};

exports.end = function (env) {
    exports.source.disconnect();
};