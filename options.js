'use strict';

var fs = require('fs');
var util = require('util');

var async = require('async');
var JSON = require('json-bigint');

var args = require('./args.js');
var log = require('./log.js');
var drivers = require('./drivers.js');

/**
 * This object defines an option that can be passed into the script.
 * @typedef {Object} OptionDef
 * @property {string} abbr          The abbreviation with which we can call this options
 * @property {*} [preset]           The value for this options if it has not been set
 * @property {string} help          An explanatory description about what this option controls
 * @property {boolean} [list=false] Whether this option can be set multiple times
 * @property {boolean} [flag=false] Whether this option needs any values passed in
 * @property {number} [min]         An upper limit for numeric option values that will be accepted
 * @property {number} [max]         A lower limit for numeric option values that will be accepted
 * @property {*} [value]            Used internally to store the value that we parsed
 * @property {boolean} [required]   Specifies that the script can't execute unless this options has been set. Settings this
 *                                  option only makes sense if there's no preset
 */

/**
 *
 * @type {Object.<string, OptionDef|Object.<string, OptionDef>> } OptionDefs
 * @property {Object.<string, OptionDef>} drivers   Driver options such as specifying the source and target driver to use
 * @property {Object.<string, OptionDef>} run       Options specifying performance values
 * @property {Object.<string, OptionDef>} xform     Options for specifying transform functions
 * @property {Object.<string, OptionDef>} memory    Options for memory limits
 * @property {Object.<string, OptionDef>} errors    Options for error handling
 * @property {Object.<string, OptionDef>} log       Options for reporting information
 * @property {OptionDef} optionsfile                Allows to specify a file to use in addition to command line options
 * @property {OptionDef} mapping                    Allows to overwrite the mapping received from the source driver
 * @property {OptionDef} help                       Will print the help of the exporter instead of processing anything
 * @property {Object.<string, OptionDef>} source    Options set by the source driver through {@link Driver#getInfo}
 * @property {Object.<string, OptionDef>} target    Options set by the target driver through {@link Driver#getInfo}
 */
var OPTIONS = {
    drivers: {
        source: {
            abbr: 's',
            preset: 'elasticsearch',
            help: 'The id of the driver to use to export data from'
        }, target: {
            abbr: 't',
            preset: 'elasticsearch',
            help: 'The id of the driver to use to import data into'
        }, dir: {
            abbr: 'd',
            preset: ['./drivers'],
            list: true,
            help: 'Additional directories that the script should look for drivers in (can be used multiple times)'
        }, list: {
            abbr: 'l',
            flag: true,
            help: 'List all the drivers the script has found'
        }, longlist: {
            abbr: 'll',
            flag: true,
            help: 'List all the drivers the script has found with extended information abtout the drivers including version numbers'
        }
    }, run: {
        test: {
            abbr: 'rt',
            help: 'Run only the source driver, not storing anything at the target driver',
            flag: true
        },
        step: {
            abbr: 'rs',
            help: 'How many documents should be fetched with each request (= step size)',
            preset: 100,
            min: 1
        },
        concurrency: {
            abbr: 'rc',
            help: 'How many processes should be spawned to do work in parallel (only used if both drivers support concurrent read/write)',
            preset: 4,
            min: 1
        },
        mapping: {
            abbr: 'rm',
            flag: true,
            help: 'Flag to enable copying mapping. If set to false all mapping operations are skipped.',
            preset: true
        },
        data: {
            abbr: 'rd',
            flag: true,
            help: 'Flag to enable copying data. If set to false all data copy operations are skipped.',
            preset: true
        }
    }, xform: {
        file: {
            abbr: 'xf',
            help: 'Filename for transform function which gets an object and returns the transformed object'
        }
        // TODO Control smarter error handling of transforms
    }, "memory.limit": {
        abbr: 'ml',
        help: 'Set how much of the available memory the process should use for caching data to be written to the target driver. Should be a float value between 0 and 1 (make sure to pass --nouse-idle-notification --expose-gc as node OPTIONS to make this work)',
        preset: 0.9,
        min: 0,
        max: 1
    }, errors: {
        retry: {
            abbr: 'er',
            help: 'If a connection error occurs this will set how many time the script will try to connect. This is for both reading and writing data',
            preset: 3,
            min: 1
        }, ignore: {
            abbr: 'ei',
            help: 'Allows the script to continue if the script has reached the retry limit by simply skipping this request.',
            flag: true
        }
    }, log: {
        debug: {
            abbr: 'v',
            help: 'Enable debug messages to be printed out to console',
            flag: true
        }, enabled: {
            abbr: 'le',
            help: 'Set logging to console to be enable or disabled. Errors will still be printed, no matter what.',
            preset: true,
            flag: true
        }, timestamps: {
            abbr: 'lt',
            help: 'Print timestamps before each log message',
            flag: true
        }
    }, optionsfile: {
        abbr: 'o',
        help: 'Read OPTIONS from a given file. Options from command line will override these values'
    }, mapping: {
        abbr: 'm',
        help: 'Override the settings/mappings of the source with the given settings/mappings string (needs to be proper format for ElasticSearch)'
    }, help: {
        abbr: 'h',
        help: 'Print these options. Use with a driver to get driver specific options.',
        flag: true
    }, "network.sockets": {
        abbr: 'ns',
        help: 'Sets the maximum number of concurrent sockets for the global http agent',
        preset: 30,
        min: 1,
        max: 65535
    }
};

/**
 * Flattens the options structure so that complex objects like { prop1: { opt1: val1 }} are converted to { prop1.opt1: val1 }.
 * This format makes it easier to find matching options in a tree by using a simple map look up.
 * The method also modifies the abbreviated form of the option by pre-pending the first character of the parent property.
 *
 * @param options   the option tree to convert
 * @param type      the parent property of the tree to flatten, others are ignored
 * @returns {{}}
 */
exports.deflate = (options, type) => {
    let driverOptions = {};
    let abbrPrefix = type.charAt(0);
    for (let option in options[type]) {
        driverOptions[type + "." + option] = options[type][option];
        driverOptions[type + "." + option].abbr = abbrPrefix + driverOptions[type + "." + option].abbr;
    }
    return driverOptions;
};

/**
 * Expands a flat options structure into an options tree so that { prop1.opt1: val1 } is converted to { prop1: { opt1: val1 }}.
 * The abbreviated forms are left untouched in this process.
 *
 * @param {Object} options   the option map to convert
 * @returns {Object}
 */
exports.inflate = options => {
    let expandedOpts = {};
    for (let prop in options) {
        let separator = prop.indexOf(".");
        if (separator > -1) {
            let group = prop.substr(0, separator);
            let value = prop.substr(separator + 1);
            if (!expandedOpts[group]) {
                expandedOpts[group] = {};
            }
            expandedOpts[group][value] = options[prop];
        } else {
            expandedOpts[prop] = options[prop];
        }
    }
    return expandedOpts;
};

/**
 * Flattens an options tree the way it is stored in the options file (which is the way it's used for the rest of the process).
 *
 * @param {Object} options   the options tree to flatten
 * @param {string} prefix    used for recursive operation, set as empty ''
 * @returns {Object}
 */
exports.deflateFile = (options, prefix) => {
    let driverOptions = {};
    for (let key in options) {
        let option = options[key];
        let newKey = prefix ? prefix + "." + key : key;
        if (typeof option == 'object') {
            driverOptions = util._extend(driverOptions, exports.deflateFile(option, newKey));
        } else {
            driverOptions[newKey] = option;
        }
    }
    return driverOptions;
};

/**
 * Read the contents of the options file and set appropriate values in the existing options structure.
 * Note that this is called after script options have been parsed, but before source and target options are
 * parsed.
 *
 * @param {Object} scriptOptions
 * @param {Object} sourceOptions
 * @param {Object} targetOptions
 */
exports.readFile = (scriptOptions, sourceOptions, targetOptions) => {
    if (!fs.existsSync(scriptOptions.optionsfile.value)) {
        log.error('The given option file could not be found!');
        log.die(3);
    }
    let fileContent = String(fs.readFileSync(scriptOptions.optionsfile.value));
    let fileOpts = exports.deflateFile(JSON.parse(fileContent));
    for (let prop in scriptOptions) {
        if (!scriptOptions[prop].value && fileOpts[prop]) {
            scriptOptions[prop].value = fileOpts[prop];
        }
    }
    for (let prop in sourceOptions) {
        if (fileOpts[prop]) {
            sourceOptions[prop].preset = fileOpts[prop];
            sourceOptions[prop].required = false;
        }
    }
    for (let prop in targetOptions) {
        if (fileOpts[prop]) {
            targetOptions[prop].preset = fileOpts[prop];
            targetOptions[prop].required = false;
        }
    }
};

/**
 * The main entry point to read options from either command line or the options file.
 *
 * @param {ReadOptionsCb} callback
 */
exports.read = callback => {
    let scriptOptions = args.parse(OPTIONS);

    log.enabled.debug = scriptOptions['log.debug'];
    log.enabled.info = scriptOptions['log.enabled'];
    log.enabled.timestamps = scriptOptions['log.timestamps'];

    args.printVersion();
    log.debug('Reading options');

    async.each(scriptOptions["drivers.dir"], (dir, callback) => drivers.find(dir, callback), () => {
        if (scriptOptions['drivers.list']) {
            drivers.describe(false);
            process.exit(0);
        }
        if (scriptOptions['drivers.longlist']) {
            drivers.describe(true);
            process.exit(0);
        }

        let sourceOptions = exports.deflate(drivers.get(scriptOptions['drivers.source']).options, 'source');
        let targetOptions = exports.deflate(drivers.get(scriptOptions['drivers.target']).options, 'target');

        if (scriptOptions.optionsfile) {
            exports.readFile(scriptOptions, sourceOptions, targetOptions);
        }

        let driverOptions = {};
        for (let prop in OPTIONS) {
            driverOptions[prop] = OPTIONS[prop];
        }
        for (let prop in sourceOptions) {
            driverOptions[prop] = sourceOptions[prop];
        }
        for (let prop in targetOptions) {
            driverOptions[prop] = targetOptions[prop];
        }
        let parsedDriverOptions = args.parse(driverOptions, true);
        let options = exports.inflate(parsedDriverOptions);
        callback(options);
    });
};
/**
 * @callback ReadOptionsCb
 * @property {Object} options
 */

/**
 * A helper method that will pass the parsed options to the selected drivers for verification.
 *
 * @param {Object} options
 * @param {errorCb} callback
 */
exports.verify = (options, callback) => {
    if (options.drivers.source == options.drivers.target) {
        let driver = drivers.get(options.drivers.source);
        log.debug('%s is verifying options', driver.info.name);
        return driver.driver.verifyOptions(options, callback);
    }
    async.map([options.drivers.source, options.drivers.target], (driverId, callback) => {
        let driver = drivers.get(driverId);
        log.debug('%s is verifying options', driver.info.name);
        driver.driver.verifyOptions(options, callback);
    }, callback);
};
