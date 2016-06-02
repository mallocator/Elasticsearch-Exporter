var parent = require('./elasticsearch.driver.js');


var id = 'elasticsearch-query';

exports.getInfo = (callback) => {
    parent.getInfo((err, info, options) => {
        callback(err, {
            id: id,
            name: 'ElasticSearch Query Driver',
            version: '1.0',
            threadsafe: true,
            desciption: 'An Elasticsearch driver that makes use of the query API to read data'
        }, options);
    });

};

exports.getData = (env, callback, from, size) => {
    let query = parent.getQuery(env);
    query.sort = [{
        '_id': { order: "asc"}
    }];
    let url = '/_search?search_type&size=' + size + '&from=' + from;
    exports.request.source.get(env, url, query, data => {
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
