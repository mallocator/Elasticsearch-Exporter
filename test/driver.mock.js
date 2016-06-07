/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate.
 */

'use strict';

var Driver = require('../drivers/driver.interface');

class MockDriver extends Driver {
    constructor() {
        super();
        this.reset();
    }

    reset(env, callback) {
        this.info = {
            id: 'mock',
            name: 'Mock Driver',
            version: '1.0',
            description: 'A mock implementation for testing purposes'
        };

        this.options = {
            source: {},
            target: {}
        };

        this.targetStats = {
            version: "1.0.0",
            cluster_status: "Green"
        };

        this.sourceStats = {
            version: "1.0.0",
            cluster_status: "Green",
            docs: {
                total: 1
            }
        };

        this.metadata = {
            mappings: {},
            settings: {}
        };

        this.data = [];
        if (callback) {
            callback();
        }
    }

    getInfo(callback) {
        callback(null, this.info, this.options);
    }

    verifyOptions(opts, callback) {
        callback();
    }

    getTargetStats(env, callback) {
        callback(null, this.targetStats);
    }

    getSourceStats(env, callback) {
        callback(null, this.sourceStats);
    }

    getMeta(env, callback) {
        callback(null, this.metadata);
    }

    putMeta(env, metadata, callback) {
        this.metadata = metadata;
        callback();
    }

    getData(env, callback, from, size) {
        callback(null, this.data);
    }

    putData(env, data, callback) {
        this.data = data;
        callback();
    }

    /**
     *
     * @param {DriverInfo} info
     */
    setInfo(info) {
        this.info = info;
    }

    /**
     *
     * @param threadsafe
     * @returns {DriverInfo}
     */
    getInfoSync(threadsafe) {
        this.info.threadsafe = threadsafe;
        return this.info;
    }

    /**
     *
     * @param {Object.<string, OptionDef>} options
     */
    setOptions(options) {
        this.options = options;
    }

    /**
     *
     * @returns {Object.<string, OptionDef>}
     */
    getOptionsSync() {
        return this.options;
    }

    /**
     *
     * @param {TargetInfo} targetStats
     */
    setTargetStats(targetStats) {
        this.targetStats = targetStats;
    }

    /**
     *
     * @param {SourceInfo} sourceStats
     */
    setSourceStats(sourceStats) {
        this.sourceStats = sourceStats;
    }

    /**
     *
     * @param {Data} hit
     */
    addhit(hit) {
        this.data.push(hit);
    }

    /**
     *
     * @param {Data[]} hits
     */
    addhits(hits) {
        this.data.concat(hits);
    }
}


module.exports = MockDriver;
module.exports.getDriver = () => new MockDriver();
