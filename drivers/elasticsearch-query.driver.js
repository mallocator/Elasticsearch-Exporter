'use strict';

var parent = require('./elasticsearch.driver.js');
var request = require('../request');

var id = 'elasticsearch-query';

class ElasticsearchQuery extends parent.constructor {
    constructor() {
        super();
    }

    getInfo(callback) {
        super.getInfo((err, info, options) => {
            callback(err, {
                id,
                name: 'ElasticSearch Query Driver',
                version: '1.0',
                threadsafe: true,
                description: 'An Elasticsearch driver that makes use of the query API to read data'
            }, options);
        });
    }

    getData(env, callback, from, size) {
        let query = super.getQuery(env);
        query.sort = [{
            '_id': { order: "asc"}
        }];
        let url = '/_search?search_type&size=' + size + '&from=' + from;
        request.source.get(env, url, query, (err, data) => callback(err, data.hits ? data.hits.hits : []));
    }
}

module.exports = new ElasticsearchQuery();
