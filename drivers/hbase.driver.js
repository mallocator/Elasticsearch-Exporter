'use strict';

var Driver = require('./driver.interface');


class HBase extends Driver {
    constructor() {
        super();
        this.id = 'hbase';
    }

    getInfo(callback) {
        let errors, requiredOptions;
        callback(errors, {
            id: this.id,
            name: 'HBase Driver',
            version: '0.0',
            description: '[N/A] An Hbase DB driver to import and export data'
        }, requiredOptions);
    }

    getTargetStats(env, callback) {
        let errors = null;
        callback(errors, {
            version: "1.0.0 or something",
            cluster_status: "Green, Yellow or Red",
            aliases: ["list", "of", "aliases", "or", false]
        });
    }

    getSourceStats(env, callback) {
        let errors = null;
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
    }

    getMeta(env, callback) {
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
}

module.exports = new HBase();
