
exports.getInfo = function (callback) {
    var info = {
        id: 'elasticsearch',
        name: 'ElasticSearch Scroll Driver',
        version: '1.0',
        desciption: 'An Elasticsearch driver that makes use of the scrolling API to read data'
    };
    var options = {
        source: {
            host: {
                abbr: 'h',
                'default': 'localhost',
                metavar: '<hostname>',
                help: 'The host from which data is to be exported from'
            }, port: {
                abbr: 'p',
                'default': 9200,
                metavar: '<port>',
                help: 'The port of the source host to talk to'
            }, index: {
                abbr: 'i',
                metavar: '<index>',
                help: 'The index name from which to export data from. If no index is given, the entire database is exported'
            }, type: {
                abbr: 't',
                metavar: '<type>',
                help: 'The type from which to export data from. If no type is given, the entire index is exported'
            }, query: {
                abbr: 'q',
                metavar: '<query>',
                help: 'Define a query that limits what kind of documents are exporter from the source',
                'default': {
                    match_all: {}
                }
            }, auth: {
                abbr: 'a',
                metavar: '<username:password>',
                help: 'Set authentication parameters for reaching the source Elasticsearch cluster'
            }, skipData: {
                abbr: 's',
                metaVar: 'true|false',
                help: 'Do not copy data, just the mappings',
                'default': false,
                choices: [true, false]
            }, count: {
                abbr: 'c',
                metavar: 'true|false',
                help: 'Keep track of individual documents fetched from the source driver. Warning: might take up lots of memory',
                'default': false,
                choices: [true, false]
            }, maxSockets: {
                abbr: 'm',
                metavar: '<number>',
                help: 'Sets the maximum number of concurrent sockets for the global http agent',
                'default': 30
            }, proxy: {
                abbr: 'p',
                metavar: '<host>',
                help: 'Set an http proxy to use for all source requests.'
            }, UseSSL: {
                abbr: 'u',
                metavar: 'true|false',
                help: 'Will attempt to connect to the source driver using https',
                'default': false,
                choices: [true, false]
            }, insecure: {
                abbr: 'x',
                metavar: 'true|false',
                help: 'Allow connections to SSL site without certs or with incorrect certs.',
                'default': false,
                choices: [true, false]
            }
        },
        target: {
            host: {
                abbr: 'h',
                metavar: '<hostname>',
                help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                required: true
            }, port: {
                abbr: 'p',
                'default': 9220,
                metavar: '<port>',
                help: 'The port of the target host to talk to'
            }, index: {
                abbr: 'i',
                metavar: '<index>',
                help: 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
            }, type: {
                abbr: 't',
                metavar: '<type>',
                help: 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
            }, auth: {
                abbr: 'a',
                metavar: '<username:password>',
                help: 'Set authentication parameters for reaching the target Elasticsearch cluster'
            }, mapping: {
                abbr: 'm',
                metavar: '<mapping/setting>',
                help: 'Override the settings/mappings of the source with the given settings/mappings string (needs to be proper format for ElasticSearch)'
            }, overwrite: {
                abbr: 'o',
                metavar: 'true|false',
                help: 'Allows to preserve already imported docs in the target database, so that changes are not overwritten',
                'default': true,
                choices: [true, false]
            }, proxy: {
                abbr: 'p',
                metavar: '<host>',
                help: 'Set an http proxy to use for all target requests.'
            }, useSSL: {
                abbr: 'u',
                metavar: 'true|false',
                help: 'Will attempt to connect to the target driver using https',
                'default': false,
                choices: [true, false]
            }
        }
    };
    callback(info, options);
};

exports.verifyOptions = function(opts, callback) {
    callback([]);
};

exports.reset = function (callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback({});
};

exports.getSourceStats = function (env, callback) {
    callback({});
};

exports.getMeta = function (env, callback) {
    callback({
        mappings: {},
        settings: {}
    });
};

exports.putMeta = function (env, metadata, callback) {
    callback();
};

exports.getData = function (env, callback) {
    callback([], 0);
};

exports.putData = function (env, data, callback) {
    callback();
};