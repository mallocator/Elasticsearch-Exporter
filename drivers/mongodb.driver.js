'use strict';

var async = require('async');
var JSON = require('json-bigint'); // jshint ignore:line
var MongoClient = require('mongodb').MongoClient;

var Driver = require('./driver.interface');
var log = require('../log.js');


class MongoDB extends Driver {
    constructor() {
        super();
        this.id = 'mongodb';
        this.source = {
            _db: null,
            getDb: (sopts, callback) => {
                if (!this.source._db) {
                    let url = 'mongodb://' + sopts.host + ':' + sopts.port;
                    return MongoClient.connect(url, (err, db) => {
                        if (err) {
                            return callback(err);
                        }
                        this.source._db = db;
                        callback(null, db);
                    });
                }
                callback(null, this.source._db);
            },
            listDatabases: (env, callback) => {
                let filter = env.options.source.database ? env.options.source.database.split(',') : [];
                this.source.getDb(env.options.source, err => {
                    if (err) {
                        return callback(err);
                    }
                    this.source._db.admin().listDatabases((err, result) => {
                        if (err) {
                            return callback(err);
                        }
                        let response = [];
                        for (let i in result.databases) {
                            if (!filter.length || filter.indexOf(result.databases[i].name) != -1) {
                                response.push(result.databases[i].name);
                            }
                        }
                        callback(null, response);
                    });
                });
            },
            disconnect: () => this.source._db && this.source.close()
        };
        this.target = {
            _cli: null,
            _db: null,
            connect: (env, callback) => {
                if (!this.target._cli) {
                    let url = 'mongodb://' + env.options.target.host + ':' + env.options.target.port;
                    MongoClient.connect(url, (err, db) => {
                        if (err){
                            return callback(err);
                        }
                        this.target._db = db;
                        callback();
                    });
                }
                callback();
            },
            disconnect: () => this.target._db && this.target._db.close()
        };
    }

    getInfo(callback) {
        let info = {
            id: this.id,
            name: 'Mongo DB Driver',
            version: '0.0',
            description: '[N/A] A Mongo DB driver to import and export data'
        };
        let options = {
            source: {
                host: {
                    abbr: 'h',
                    preset: 'localhost',
                    help: 'The host from which data is to be exported from.'
                }, port: {
                    abbr: 'p',
                    preset: 27017,
                    help: 'The port of the source host to talk to.',
                    min: 0,
                    max: 65535
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
                    help: 'The port of the source host to talk to.',
                    min: 0,
                    max: 65535
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
    }

    verifyOptions(opts, callback) {
        let err = [];
        if (opts.drivers.source == this.id && opts.drivers.target == this.id) {
            if (opts.source.host != opts.target.host) { return callback(); }
            if (opts.source.port != opts.target.port) { return callback(); }
            if (opts.target.project && opts.source.project != opts.target.project) { return callback(); }
            if (opts.target.dataset && opts.source.dataset != opts.target.dataset) { return callback(); }
            err.push("The source and target location are the same. Not moving any data!");
        }
        if (opts.drivers.source == this.id) {
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
        if (opts.drivers.target == this.id) {
            if (opts.target.user && !opts.target.secret) {
                err.push("The target driver has a username specified but not secret/password");
            }
            if (!opts.target.user && opts.target.secret) {
                err.push("The target driver has a secret/password specified but no user");
            }
        }
        callback(err);
    }

    reset(env, callback) {
        this.source._cli = null;
        this.source._db = null;
        callback();
    }

    getTargetStats(env, callback) {
        // TODO
        let errors = null;
        callback(errors, {
            version: "1.0.0 or something",
            cluster_status: "Green, Yellow or Red"
        });
    }

    getSourceStats(env, callback) {
        let stats = {
            cluster_status: 'Red',
            docs: {
                total: 0
            },
            databases: {}
        };

        async.parallel([
            callback => {
                this.source.getDb(env.options.source, (err, db) => {
                    if (err) {
                        return callback(err);
                    }
                    db.admin().serverStatus((err, info) => {
                        if (err) {
                            return callback(err);
                        }
                        stats.version = info.version;
                        stats.cluster_status = 'Green';
                        callback();
                    });
                });
            },
            callback => {
                function countCollectionTask(database, collectionName) {
                    return callback => {
                        database.collection(collectionName, (err, collection) => {
                            if (err) {
                                return callback(err);
                            }
                            log.debug("Counting items in %s/%s", database.databaseName, collectionName);
                            collection.count(env.options.source.query, (err, count) => {
                                if (err) {
                                    return callback(err);
                                }
                                stats.databases[database.databaseName][collectionName] = count;
                                callback();
                            });
                        });
                    };
                }

                function listCollectionsTask(databaseName) {
                    let filter = env.options.source.collection ? env.options.source.collection.split(',') : [];
                    return callback => {
                        let database = this.source._db.db(databaseName);
                        database.listCollections().toArray((err, collections) => {
                            if (err) {
                                return callback(err);
                            }
                            let countTasks = [];
                            for (let i in collections) {
                                if (!filter.length || filter.indexOf(collections[i].name) != -1) {
                                    log.debug("Adding task to count collection %s/%s", databaseName, collections[i].name);
                                    countTasks.push(countCollectionTask(database, collections[i].name));
                                }
                            }
                            async.parallel(countTasks, callback);
                        });
                    };
                }

                this.source.getDb(env.options.source, err => {
                    if (err) {
                        return callback(err);
                    }
                    this.source.listDatabases(env, (err, databases) => {
                        if (err) {
                            return callback(err);
                        }
                        let tasks = [];
                        log.debug("Found %d databases on source MongoDB", databases.length);
                        for (let i in databases) {
                            let database = databases[i];
                            log.debug("Adding task for database %s to fetch colections", database);
                            tasks.push(listCollectionsTask(database));
                            stats.databases[database] = {};
                        }
                        async.parallel(tasks, err => {
                            if (err) {
                                return callback(err);
                            }
                            for (let i in stats.databases) {
                                for (let j in stats.databases[i]) {
                                    stats.docs.total += stats.databases[i][j];
                                }
                            }
                            callback();
                        });
                    });
                });
            }
        ], err => callback(err, stats));
    }

    getMeta(env, callback) {
        process.exit();
        let errors = null;
        callback(errors, {
            mappings: {},
            settings: {}
        });
    }

    getData(env, callback) {
        let errors = null;
        callback(errors, [{
            _index: "indexName",
            _type: "typeName",
            _id: "1",
            _version: 1,
            found: true,
            _source: {}
        }]);
    }

    end() {
        this.source.disconnect();
    }
}

module.exports = new MongoDB();
