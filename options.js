var fs = require('fs');
var util = require('util');
var async = require('async');
var log = require('./log.js');
var drivers = require('./drivers.js');
var args = require('./args.js');


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
        }
    },
    testrun: {
        abbr: 'r',
        help: 'Run only the source driver, not storing anything at the target driver',
        flag: true
    },
    "memory.limit": {
        abbr: 'm',
        help: 'Set how much of the available memory the process should use for caching data to be written to the target driver. Should be a float value between 0 and 1 (make sure to pass --nouse-idle-notification --expose-gc as node OPTIONS to make this work)',
        preset: 0.9
    },
    "errors.allowed": {
        abbr: 'x',
        help: 'If a connection error occurs this will set how often the script will retry to connect. This is for both reading and writing data.',
        preset: 3
    },
    log: {
        debug: {
            abbr: 'v',
            help: 'Enable debug messages to be printed out to console',
            flag: true
        }, enabled: {
            abbr: 'l',
            help: 'Set logging to console to be enable or disabled. Errors will still be printed, no matter what.',
            preset: true,
            flag: true
        }
    },
    optionsfile: {
        abbr: 'o',
        help: 'Read OPTIONS from a given file. Options from command line will override these values'
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
exports.deflate = function(options, type) {
    var driverOptions = {};
    var abbrPrefix = type.charAt(0);
    for (var option in options[type]) {
        driverOptions[type + "." + option] = options[type][option];
        driverOptions[type + "." + option].abbr = abbrPrefix + driverOptions[type + "." + option].abbr;
    }
    return driverOptions;
};

/**
 * Expands a flat options structure into an options tree so that { prop1.opt1: val1 } is converted to { prop1: { opt1: val1 }}.
 * The abbreviated forms are left untouched in this process.
 *
 * @param options   the option map to convert
 * @returns {{}}
 */
exports.inflate = function(options) {
    var expandedOpts = {};
    for (var prop in options) {
        var separator = prop.indexOf(".");
        if (separator > -1) {
            var group = prop.substr(0, separator);
            var value = prop.substr(separator + 1);
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
 * @param options   the options tree to flatten
 * @param prefix    used for recursive operation, set as empty ''
 * @returns {{}}
 */
exports.deflateFile = function (options, prefix) {
    var driverOptions = {};
    for (var key in options) {
        var option = options[key];
        var newKey = prefix ? prefix + "." + key : key;
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
 * @param scriptOptions
 * @param sourceOptions
 * @param targetOptions
 */
exports.readFile = function(scriptOptions, sourceOptions, targetOptions) {
    if (!fs.existsSync(scriptOptions.optionsfile)) {
        log.error('The given option file could not be found!');
        process.exit(3);
    }
    var fileOpts = exports.deflateFile(JSON.parse(fs.readFileSync(scriptOptions.optionsfile)), '');
    for (var prop in scriptOptions) {
        if (!scriptOptions[prop].value && fileOpts[prop]) {
            scriptOptions[prop].value = fileOpts[prop];
        }
    }
    for (var prop in sourceOptions) {
        if (fileOpts[prop]) {
            sourceOptions[prop].preset = fileOpts[prop];
            sourceOptions[prop].required = false;
        }
    }
    for (var prop in targetOptions) {
        if (fileOpts[prop]) {
            targetOptions[prop].preset = fileOpts[prop];
            targetOptions[prop].required = false;
        }
    }
};

/**
 * The main entry point to read options from either command line or the options file.
 *
 * @param callback
 */
exports.read = function(callback) {
    var scriptOptions = args.parse(OPTIONS);

    log.enabled.debug = scriptOptions['log.debug'];
    log.enabled.info = scriptOptions['log.enabled'];
    args.printVersion();

    async.each(scriptOptions["drivers.dir"], function(dir, callback) {
        drivers.find(dir, callback);
    }, function() {
        if (scriptOptions['drivers.list']) {
            drivers.describe();
            process.exit(0);
        }

        var sourceOptions = exports.deflate(drivers.get(scriptOptions['drivers.source']).options, 'source');
        var targetOptions = exports.deflate(drivers.get(scriptOptions['drivers.target']).options, 'target');

        if (scriptOptions.optionsfile) {
            exports.readFile(scriptOptions, sourceOptions, targetOptions);
        }

        var driverOptions = {};
        for (var prop in OPTIONS) {
            driverOptions[prop] = OPTIONS[prop];
        }
        for (var prop in sourceOptions) {
            driverOptions[prop] = sourceOptions[prop];
        }
        for (var prop in targetOptions) {
            driverOptions[prop] = targetOptions[prop];
        }
        var parsedDriverOptions = args.parse(driverOptions);
        var options = exports.inflate(parsedDriverOptions);
        callback(options);
    });
};

/**
 * A helper method that will pass the parsed options to the selected drivers for verification.
 *
 * @param options
 * @param callback
 */
exports.verify = function(options, callback) {
    function errorHandler(errors) {
        var error;
        for (var i in errors) {
            error = true;
            log.info(errors[i]);
        }
        if (error) {
            log.error("The program could not validate all options and will terminate");
            process.exit(3);
        }
        callback();
    }
    if (options.drivers.source == options.drivers.target) {
        var driver = drivers.get(options.drivers.source);
        log.debug('%s is verifying options', driver.info.name);
        driver.driver.verifyOptions(options, errorHandler);
    } else {
        async.map([options.drivers.source, options.drivers.target], function (driverId, callback) {
            var driver = drivers.get(driverId);
            log.debug('%s is verifying options', driver.info.name);
            driver.driver.verifyOptions(options, callback);
        }, errorHandler);
    }
};