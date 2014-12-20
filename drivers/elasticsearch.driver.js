
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
                preset: 'localhost',
                help: 'The host from which data is to be exported from'
            }, port: {
                abbr: 'p',
                preset: 9200,
                help: 'The port of the source host to talk to'
            }, index: {
                abbr: 'i',
                help: 'The index name from which to export data from. If no index is given, the entire database is exported'
            }, type: {
                abbr: 't',
                help: 'The type from which to export data from. If no type is given, the entire index is exported'
            }, query: {
                abbr: 'q',
                help: 'Define a query that limits what kind of documents are exporter from the source',
                preset: {
                    match_all: {}
                }
            }, auth: {
                abbr: 'a',
                help: 'Set authentication parameters for reaching the source Elasticsearch cluster'
            }, skipData: {
                abbr: 's',
                help: 'Do not copy data, just the mappings',
                flag: true
            }, count: {
                abbr: 'c',
                help: 'Keep track of individual documents fetched from the source driver. Warning: might take up lots of memory',
                flag: true
            }, maxSockets: {
                abbr: 'm',
                help: 'Sets the maximum number of concurrent sockets for the global http agent',
                preset: 30
            }, proxy: {
                abbr: 'p',
                help: 'Set an http proxy to use for all source requests.'
            }, UseSSL: {
                abbr: 'u',
                help: 'Will attempt to connect to the source driver using https',
                flag: true
            }, insecure: {
                abbr: 'x',
                help: 'Allow connections to SSL site without certs or with incorrect certs.',
                flag: true
            }
        }, target: {
            host: {
                abbr: 'h',
                help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given',
                required: true
            }, port: {
                abbr: 'p',
                preset: 9200,
                help: 'The port of the target host to talk to'
            }, index: {
                abbr: 'i',
                help: 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
            }, type: {
                abbr: 't',
                help: 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
            }, auth: {
                abbr: 'a',
                help: 'Set authentication parameters for reaching the target Elasticsearch cluster'
            }, mapping: {
                abbr: 'm',
                help: 'Override the settings/mappings of the source with the given settings/mappings string (needs to be proper format for ElasticSearch)'
            }, overwrite: {
                abbr: 'o',
                help: 'Allows to preserve already imported docs in the target database, so that changes are not overwritten',
                preset: true,
                flag: true
            }, proxy: {
                abbr: 'p',
                help: 'Set an http proxy to use for all target requests.'
            }, useSSL: {
                abbr: 'u',
                metavar: 'true|false',
                help: 'Will attempt to connect to the target driver using https',
                flag: true
            }
        }
    };
    callback(info, options);
};

exports.verifyOptions = function(opts, callback) {
    if (opts.drivers.source == 'elasticsearch' && opts.drivers.target == 'elasticsearcg') {
        if (!opts.target.host) {
            opts.target.host = opts.source.host;
        }
        if (!opts.target.port) {
            opts.target.port = opts.source.port;
        }
        if (opts.source.index && !opts.target.index) {
            opts.target.index = opts.source.index;
        }
        if (opts.source.type && !opts.target.type) {
            opts.target.type = opts.source.type;
        }
        if ((process.env.HTTP_PROXY || process.env.http_proxy) && !opts.source.proxy) {
            if (process.env.HTTP_PROXY) {
                opts.source.proxy = process.env.HTTP_PROXY;
            } else if (process.env.http_proxy) {
                opts.source.proxy = process.env.http_proxy;
            }
        }

        if (opts.source.host != opts.target.host) callback([]); return;
        if (opts.source.port != opts.target.port) callback([]); return;
        if (opts.source.index != opts.targetIndex) callback([]); return;
        if (opts.source.type != opts.targetType && opts.sourceIndex) callback([]); return;
    } else {
        var optSet = opts.drivers.source == 'elasticsearch' ? opts.source : opts.target;
        if (optSet.host) callback([]); return;
    }
    callback('Not enough information has been given to be able to perform an export. Please review the OPTIONS and examples again.');

};

exports.reset = function (callback) {
    callback();
};

exports.getTargetStats = function (env, callback) {
    callback({
    });
};

exports.getSourceStats = function (env, callback) {
    callback({
    });
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