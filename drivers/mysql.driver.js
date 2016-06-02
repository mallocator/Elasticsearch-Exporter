var mysql = require('mysql');
var async = require('async');
var log = require('../log.js');

var id = 'mysql';

var connection = {
    fields: null,
    source: null,
    target: null,
    error: null
};

var queue = [];

function connect(connectionType, opts, callback) {
    if (connection[connectionType]) {
        callback(null);
    }
    let ssl;
    try {
        ssl = JSON.parse(opts.UseSSL);
    } catch(e) {
        ssl = opts.UseSSL;
    }
    connection[connectionType] = mysql.createConnection({
        host: opts.host,
        user: opts.user,
        password: opts.password,
        socketPath: opts.socketPath,
        charset: opts.charset,
        timezone: opts.timezone,
        connectTimeout: opts.connectTimeout,
        insecureAuth: opts.insecureAuth,
        supportBigNumbers: opts.supportBigNumbers,
        bigNumberStrings: opts.bigNumberStrings,
        dateStrings: opts.dateStrings,
        multipleStatements: true,
        flags: opts.flags,
        ssl: ssl
    });

    connection[connectionType].on('error', err => {
        log.error('The MySQL server has repsonded with an error: %s', err);
        connection[connectionType].end();
        connection[connectionType] = null;
    });

    connection[connectionType].connect(err => callback(err) );
}

exports.getInfo = (callback) => {
    let info = {
        id: id,
        name: 'MySQL Driver',
        version: '0.0',
        desciption: '[N/A] A driver to read and store data via MySQL. For more details check https://github.com/felixge/node-mysql'
    };
    let options = {
        source: {
            host: {
                abbr: 'h',
                preset: 'localhost',
                help: 'The host from which data is to be exported from'
            }, port: {
                abbr: 'P',
                preset: 3306,
                help: 'The port of the source host to talk to'
            }, user: {
                abbr: 'u',
                help: 'Username to use to connect to the database'
            }, password: {
                abbr: 'p',
                help: 'Password to use to connect to the database'
            }, socketPath: {
                abbr: 's',
                help: 'The path to a unix domain socket to connect to. When used host and port are ignored'
            }, charset: {
                abbr: 'c',
                help: 'The charset for the connection. This is called "collation" in the SQL-level of MySQL'
            }, connectTimeout: {
                abbr: 'T',
                help: 'The milliseconds before a timeout occurs during the initial connection to the MySQL server'
            }, insecureAuth: {
                abbr: 'i',
                help: 'Allow connecting to MySQL instances that ask for the old (insecure) authentication method',
                flag: true
            }, supportBigNumbers: {
                abbr: 'b',
                help: 'When dealing with big numbers (BIGINT and DECIMAL columns) in the database, you should enable this option',
                flag: true
            }, bigNumberStrings: {
                abbr: 'B',
                help: 'Enabling both supportBigNumbers and bigNumberStrings forces big numbers (BIGINT and DECIMAL columns) to be always returned as JavaScript String objects',
                flag: true
            }, dateStrings: {
                abbr: 'S',
                help: 'Force date types (TIMESTAMP, DATETIME, DATE) to be returned as strings rather then inflated into JavaScript Date objects'
            }, flags: {
                abbr: 'f',
                help: 'List of connection flags to use other than the default ones'
            }, useSSL: {
                abbr: 'u',
                help: 'object with ssl parameters or a string containing name of ssl profile'
            }, database: {
                abbr: 'd',
                help: 'The database to connect to. The database will be used as a stand in for the index on the target driver if set',
                required: true
            }, table: {
                abbr: 't',
                help: 'The table to read from. The table will be used as a stand in for the type on the target driver if set',
                required: true
            }, query: {
                abbr: 'q',
                help: 'Either a query string or the path to a .js file that will return a query when called via require().query(). Defaults to SELECT * FROM <table>'
            }, countQuery: {
                abbr: 'Q',
                help: 'The query that will be used to determine the number of results. If left empty the standard query will be wrapped in SELECT COUNT(*) FROM (<query>). Can also be a .js file which will be called via require().countQuery()'
            }, queueSize: {
                abbr: 'n',
                preset: 100,
                help: 'The number of rows to be cached before waiting for the target driver to catch up.'
            }, primaryKey: {
                abbr: 'k',
                help: 'Set the primary key in the result that will be used as the the document id. If no id is set the target database will most likely generate a random id for you.'
            }
        }, target: {
            host: {
                abbr: 'h',
                preset: 'localhost',
                help: 'The host from which data is to be exported from'
            }, port: {
                abbr: 'P',
                preset: 3306,
                help: 'The port of the source host to talk to'
            }, user: {
                abbr: 'u',
                help: 'Username to use to connect to the database'
            }, password: {
                abbr: 'p',
                help: 'Password to use to connect to the database'
            }, socketPath: {
                abbr: 's',
                help: 'The path to a unix domain socket to connect to. When used host and port are ignored'
            }, charset: {
                abbr: 'c',
                help: 'The charset for the connection. This is called "collation" in the SQL-level of MySQL'
            }, connectTimeout: {
                abbr: 'T',
                help: 'The milliseconds before a timeout occurs during the initial connection to the MySQL server'
            }, insecureAuth: {
                abbr: 'i',
                help: 'Allow connecting to MySQL instances that ask for the old (insecure) authentication method'
            }, flags: {
                abbr: 'f',
                help: 'List of connection flags to use other than the default ones'
            }, useSSL: {
                abbr: 'u',
                help: 'object with ssl parameters or a string containing name of ssl profile'
            }, database: {
                abbr: 'd',
                help: 'The database to connect to. If left empty the source index will be used to determine the database. This option is should match createTableQuery if set'
            }, table: {
                abbr: 't',
                help: 'The table that should be used for the default create table query. Defaults to the type from the source. This option is should match createTableQuery if set'
            }, timezone: {
                abbr: 't',
                help: 'The timezone used to store local dates'
            }, createTableQuery: {
                abbr: 'q',
                help: 'Allows to set a custom query to create the table where data will be imported into. If left empty the exporter will attempt to autmatically generate a table based on the known ElasticSearch types. Can also be a .js file that will be called via require().createTableQuery()'
            }
        }
    };
    callback(null, info, options);
};

exports.verifyOptions = (opts, callback) => {
    if (opts.drivers.source == id && opts.drivers.target == id) {
        if (!opts.target.host) {
            opts.target.host = opts.source.host;
        }
        if (!opts.target.port) {
            opts.target.port = opts.source.port;
        }
        if (opts.source.database && !opts.target.database) {
            opts.target.database = opts.source.database;
        }

        if (opts.source.host != opts.target.host) { return callback(); }
        if (opts.source.port != opts.target.port) { return callback(); }
        if (opts.source.database != opts.target.database) { return callback(); }
    } else {
        let optSet = opts.drivers.source == id ? opts.source : opts.target;
        if (optSet.host && optSet.port) { return callback(); }
    }
    callback('Not enough information has been given to be able to perform an export. Please review the OPTIONS and examples again.');
};

exports.reset = (env, callback) => {
    if (connection.source) {
        connection.source.end();
        connection.source = null;
    }
    if (connection.target) {
        connection.target.end();
        connection.target = null;
    }
    if (callback) {
        callback();
    }
};

exports.getSourceStats = (env, callback) => {
    connect('source', env.options.source, err => {
        if (err) {
            return callback('Unable to establish connection with the source MySQL database');
        }

        let stats = {
            cluster_status: "Green"
        };

        async.parallel([
            subCallback => {
                connection.source.query('SHOW VARIABLES LIKE "version";', (err, rows) => {
                    stats.version = rows[0].Value;
                    subCallback(err);
                });
            },
            subCallback => {
                let q = 'SELECT COUNT(*) as count FROM (' + env.options.source.query + ')';
                if (env.options.source.countQuery) {
                    if (/\.js$$/.test(env.options.source.countQuery)) {
                        try {
                            q = require(env.options.source.countQuery).countQuery();
                        } catch (e) {
                            subCallback('Unable to read count query from given .js file: ' + e);
                            return;
                        }
                    } else {
                        q = env.options.source.countQuery;
                    }
                }
                connection.source.query(q, (err, rows) => {
                    stats.docs = {
                        total: rows[0].count
                    };
                    subCallback(err);
                });
            }
        ], err => callback(err, stats));
    });
};

exports.getTargetStats = (env, callback) => {
    connect('target', env.options.target, err => {
        if (err) {
            return callback('Unable to establish connection with the target MySQL database');
        }

        connection.target.query('SHOW VARIABLES LIKE "version";', (err, rows) => {
            callback(err, {
                version: rows[0].Value,
                cluster_status: err ? "Red" : "Green"
            });
        });
    });
};

exports.getMeta = (env, callback) => {
    connect('source', env.options.source, err => {
        if (err) {
            return callback(err);
        }

        let q = 'SELECT * FROM ' + env.options.source.table;
        if (env.options.source.query) {
            q = env.options.source.query;
        }
        let query = connection.source.query(q);
        query.on('error', err => {
            connection.error = err;
            // TODO this error is nowhere handled
        }).on('fields', fields => {
            // Maybe us a metadata field instead to store additional data
            connection.fields = fields;
            // TODO write metadata (with additional field for mysql information) and use callback
            // TODO find the primary key
            callback(null, {
                mappings: {},
                settings: {}
            });
        }).on('result', row => {
            row && queue.push(row);
            queue.length >= env.options.source.queueSize && connection.source.pause();
        }).on('end', () => log.debug('All documents have been read from source MySQL database'));
    });
};

exports.putMeta = (env, metadata, callback) => {
    connect('target', env.options.target, err => {
        if (err) {
            return callback(err);
        }

        // TODO use target table query or generate query based on given types (or additional mysql field if available)
        // TODO create table if it doesn't exists yet
    });
};

exports.getData = (env, callback, from, size) => {
    connect('source', env.options.source, err => {
        if (err) {
            return callback(err);
        }

        // TODO get the actual data (and check if it can be done thread safe)

        let data = [];
        for (let row of queue) {
            // TODO convert row into ES document
            data.push({
                _index: "indexName",
                _type: "typeName",
                _id: "1",
                _version: 1,
                found: true,
                _source: {}
            });
        }
        // TODO this might throw an error if the connection has not been paused
        connection.source.resume();
        callback(null, data);
    });
};

exports.putData = (env, docs, callback) => {
    connect('target', env.options.target, err => {
        if (err) {
            return callback(err);
        }

        let inserts = "";

        for (let i = 0; i<data.length; i = i+2) {
            let meta = data[i];
            let doc = data[i+1];
            // TODO convert data to insert statements by flattening the document tree
        }

        connection.target.query(inserts, (err, result) => {
            // TODO check if affectedRows == data.length/2
            callback(err);
        });
    });
};

exports.end = (env) => exports.reset(env);
