'use strict';

/******
 * This file is used to describe the interface that is used to interact with plugins for the exporter.
 * Included is information about what each method does and what callbacks are expected.
 *
 * Each method receives an option object where all configuration and stats (once set) are available.
 * Each method also receives a callback method that should be called whenever an operation is complete.
 * The only case when no callback should be called is when an error occurred and instead the program
 * should terminate. In case the driver should call a process.exit() at any point, please use a status
 * code above 130 when exiting.
 * @interface
 */
class Driver {
    /**
     * An object that holds various information about a driver.
     * @typedef {Object} DriverInfo
     * @property {string} id           A unique identifier for this driver
     * @property {string} name         A readable name for the driver
     * @property {string} version      The semver version of this driver
     * @property {string} description  A longer description about the driver
     */

    /**
     * Object that holds general information about a target service.
     * @typedef {Object} TargetInfo
     * @property {string} version                   The semver version of the target service
     * @property {string} cluster_status            The status of the target service ('Green', 'Yellow' or 'Red')
     * @property {Object.<string, string>} aliases   A map of aliases (key) to actual indices (values)
     */

    /**
     * Object that holds general information about a target service
     * @typedef {Object} SourceInfo
     * @extends TargetInfo
     * @property {Object} docs                             An object with information about how many docs are in each index
     * @property {Object.<string, number>} docs.indices    A map of indices to number of docs
     * @property {number} docs.total                       The total number of documents to be exported
     */

    /**
     * For details on how to define the mappings, refer to {@link https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html}.
     * Internally the exporter is using this mapping to map all other services to types.
     * @typedef {Object} Mapping
     */

    /**
     * A settings object that allows to store arbitrary settings for individual services. The standard format is the one
     * used by elasticsearch which is explained in detail here: {@link https://www.elastic.co/guide/en/elasticsearch/reference/2.3/index-modules.html#index-modules-settings}.
     * @typedef {Object} Setting
     */

    /**
     * @typedef {Object} Metadata
     * @property {Object.<string, Object.<string, Mapping>>} mappings   The mapping data per index and type
     * @property {Object.<string, Setting>} meta.settings               The settings per index
     */

    /**
     * The actual data to be exported.
     * @typedef {Object} Data
     * @property {string} _id               The id of the document to be exported
     * @property {string} _index            The index the document belongs to
     * @property {string} _type             The type the document belongs to
     * @property {Object} _source           The actual document to be exported
     * @property {Object} _fields           Special property fields for this entry
     * @property {number|string} _timestamp The timestamp this entry was created/modified
     * @property {string} _routing          Hash used to determine routing path in cluster
     * @property {number} _version          The modification version of the entry
     * @property {Object} _percolate        ES percolate options for this entry
     * @property {string} _parent           The id of the parent entry
     * @property {number|string} _ttl       The life expectancy of this document
     */

    /**
     * Returns the name, version and other information about this plugin.
     * @param {Driver~getInfoCallback} callback
     * @abstract
     */
    getInfo(callback) {
        throw new Error('Not Implemented');
    }
    /**
     * @callback Driver~getInfoCallback
     * @param {String|String[]} errors              Pass on any errors using this parameter if they occur
     * @param {DriverInfo} info                     An object with common info about the driver
     * @param {Object.<string, OptionDef>} options  The options available for this driver
     */

    /**
     * This option is called if the driver is either the target or the source. To check if it is either look up the id of
     * opts.drivers.source or opts.drivers.target.
     * @param {Object} opts
     * @param {errorCb} callback
     * @abstract
     */
    verifyOptions(opts, callback) {
        callback();
    }

    /**
     * Reset the state of this driver so that it can be used again.
     * @param {Environment} env
     * @param {errorCb} callback
     * @abstract
     */
    reset(env, callback) {
        callback();
    }

    /**
     * Return some information about the the database if it used as a target.
     * @param {Environment} env
     * @param {Driver~getTargetStatsCallback} callback
     * @abstract
     */
    getTargetStats(env, callback) {
        throw new Error('Not implemented');
    }
    /**
     * @callback Driver~getTargetStatsCallback
     * @param {String|String[]} errors  Pass on any errors using this parameter if they occur.
     * @param {TargetInfo} info         Object that holds general information about a target service
     */


    /**
     * Return some information about the the database if it used as a source.
     * @param {Environment} env
     * @param {Driver~getSourceStatsCallback} callback
     * @abstract
     */
    getSourceStats(env, callback) {
        throw new Error('Not Implemented');
    }
    /**
     * @callback Driver~getSourceStatsCallback
     * @param {String|String[]} errors  Pass on any errors using this parameter if they occur.
     * @param {SourceInfo} info         Object that holds general information about a target service
     */


    /**
     * This method fetches the meta data from the source data base. It is convention that the source driver has to override
     * the index and type, if they have been set to be different than the target database (env.options.target.index,
     * env.options.target.type).
     * @param {Environment} env
     * @param {Driver~getMetaCallback} callback
     * @abstract
     */
    getMeta(env, callback) {
        throw new Error('Not Implemented');
    }
    /**
     * @callback Driver~getMetaCallback
     * @param {String|String[]} errors  Pass on any errors using this parameter if they occur.
     * @param {Metadata} meta           The meta data used to created tables/indices etc.
     */


    /**
     * Uses the metadata from #getMeta() and stores it in the target database
     * @param {Environment} env
     * @param {Metadata} metadata
     * @param {errorCb} callback
     * @abstract
     */
    putMeta(env, metadata, callback) {
        callback();
    }


    /**
     * This is an additional convenience method that will be called right before the import with getData() is started and
     * again before putData() is called. The implementation is optional and will not be validated. Code executed needs to
     * be synchronized to make sure it's not executed after the a getData call.
     * @param {Environment} env
     * @param {boolean} isSource    Is set to true if called right before getData() otherwise it's being called right before putData()
     */
    prepareTransfer(env, isSource) {}

    /**
     * This as well as the putData function will be called in a separate process so that stateful values are reset. If the
     * driver does not support concurrency it will be in the same process. Also note that it is convention for the source
     * driver to override the type and index, if they have been set differently (env.options.target.index,
     * env.options.target.type).
     * @param {Environment} env
     * @param {Driver~getDataCallback} callback
     * @param {number} [from]                   This value is important if the driver support multi threaded exports,
     *                                          otherwise it can be ignored
     * @param {number} [size]                   This value is important if the driver support multi threaded exports,
     *                                          otherwise it can be ignored
     * @abstract
     */
    getData(env, callback, from, size) {
        throw new Error('Not implemented');
    }
    /**
     * @callback Driver~getDataCallback
     * @param {String|String[]} errors  Pass on any errors using this parameter if they occur.
     * @param {Data[]} data
     */


    /**
     * Stores the data in the target database. Make sure that you generate an id for each element of none is given.
     * @param {Environment} env
     * @param {Data[]} docs
     * @param {errorCb} callback
     * @abstract
     */
    putData(env, docs, callback) {
        callback();
    }


    /**
     * An optional finalizer method on the target driver that gets called after all documents have been exported. Allows the driver to do some clean up.
     * @param {Environment} env
     */
    end(env) {}
}

module.exports = Driver;
