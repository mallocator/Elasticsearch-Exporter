var parent = require('./elasticsearch.driver.js');


var id = 'elasticsearch-query';

exports.getInfo = function (callback) {
    parent.getInfo(function(err, info, options) {
        callback(err, {
            id: id,
            name: 'ElasticSearch Query Driver',
            version: '1.0',
            threadsafe: true,
            desciption: 'An Elasticsearch driver that makes use of the query API to read data'
        }, options);
    });

};

exports.getData = function (env, callback, from, size) {
    var query = parent.getQuery(env);
    query.sort = [{
        '_id': { order: "asc"}
    }];
    var url = '/_search?search_type&size=' + size + '&from=' + from;
    exports.request.source.get(env, url, query, function (data) {
        callback(null, data.hits ? data.hits.hits : []);
    }, callback);
};

exports.verifyOptions = parent.verifyOptions;
exports.reset = parent.reset;
exports.getTargetStats = parent.getTargetStats;
exports.getSourceStats = parent.getSourceStats;
exports.getMeta = parent.getMeta;
exports.putMeta = parent.putMeta;
exports.putData = parent.putData;
exports.end = parent.end;