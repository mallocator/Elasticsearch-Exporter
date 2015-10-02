var fs = require('fs');
var log = require('../log.js');

var id = 'bigquery';

//node exporter.js -s bigquery -sp motorola.com:psylocke -su 317035392657-879qufufgbp6842l630tn3dgdlqdbf2t@developer.gserviceaccount.com -sk key.pem -sq SELECT * FORM dev_context_eng_logging.2015_05_28_0 -t file -tf sample

exports.getInfo = function (callback) {
    var info = {
        id: id,
        name: 'BigQuery Driver',
        version: '0.0',
        desciption: '[N/A] A Google BigQuery driver to import and export data using the service account'
    };
    var options = {
        source: {
            project: {
                abbr: 'p',
                help: 'The google project name to connect to',
                required: true
            },
            user: {
                abbr: 'u',
                help: 'The google service account user id (email format) used to authenticate',
                required: true
            },
            key: {
                abbr: 'k',
                help: 'The key (pem format) of the service account used to authenticate',
                required: true
            },
            query: {
                abbr: 'q',
                help: 'The query to be sent to BigQuery',
                required: true
            }, transform: {
                abbr: 't',
                help: 'This allows you to create a nested document from a bigquery row. Refer to the documentation for more info.'
            }
        },
        target: {
            project: {
                abbr: 'p',
                help: 'The google project name to connect to',
                required: true
            },
            user: {
                abbr: 'u',
                help: 'The google service account user id (email format) used to authenticate',
                required: true
            },
            key: {
                abbr: 'k',
                help: 'The key (pem format) of the service account used to authenticate',
                required: true
            },
            dataset: {
                abbr: 'd',
                help: 'The dataset in which to insert data',
                required: true
            },
            table: {
                abbr: 't',
                help: 'The table in which to insert the data',
                required: true
            }
        }
    };

    callback(null, info, options);
};

exports.verifyOptions = function (opts, callback) {
    var errors = [];
    if (opts.drivers.source == id) {
        if (!fs.existsSync(opts.source.key)) {
            errors.push("Can't open key file " + opts.source.key);
        }
    }
    if (opts.drivers.source == id && opts.drivers.target == id) {
        if (!opts.target.project) {
            opts.target.project = opts.source.project;
        }
        if (!opts.target.user) {
            opts.target.user = opts.source.user;
        }
        if (!opts.target.key) {
            opts.target.key = opts.source.key;
        }
    }
    if (opts.drivers.target == id) {
        if (!fs.existsSync(opts.target.key)) {
            errors.push("Can't open key file " + opts.target.key);
        }
    }
    callback(errors);
};

exports.reset = function (env, callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    // TODO connect to database to see if status is green
    var errors = null;
    callback(errors, {
        version: "-.0.0",
        cluster_status: "Green",
        aliases: []
    });
};

exports.bq = {
    client: null,
    getClient: function(env) {
        if (this.client) {
            return this.client;
        }
        this.client = require('google-bigquery')({
            "iss": env.options.source.user,
            "key": fs.readFileSync(env.options.source.key, 'utf8')
        });
        return this.client();
    },
    query: function(env, query, callback) {
        this.getClient().jobs.query({ projId: env.options.source.project, query: query }, callback);
    }
};

exports.getSourceStats = function (env, callback) {
    // TODO connect to database to see if status is green
    // TODO wrap in count query to get the number of documents
    var errors = null;
    callback(errors, {
        version: "1.0.0",
        cluster_status: "Green",
        docs: {
            total: 100
        }
    });
};

exports.getMeta = function (env, callback) {
    // TODO check if any data is needed, otherwise just don't provide any metadata to store
    callback(null, {
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    // TODO map metadata to bigquery types
    callback();
};

exports.prepareTransfer = function (env, isSource) {

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
};