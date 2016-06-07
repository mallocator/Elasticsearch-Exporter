'use strict';

var Driver = require('./driver.interface');


class Datastore extends Driver {
    constructor() {
        super();
        this.id = 'datastore';
    }

    getInfo(callback) {
        let errors, requiredOptions;
        callback(errors, {
            id: this.id,
            name: 'Datastore Driver',
            version: '0.0',
            description: '[N/A] A Google App Engine Datastore driver to import and export data'
        }, requiredOptions);
    }

    verifyOptions(opts, callback) {
        callback();
    }

    reset(env, callback) {
        callback();
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

    putMeta(env, metadata, callback) {
        callback();
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

    putData(env, docs, callback) {
        callback();
    }
}

module.exports = new Datastore();
