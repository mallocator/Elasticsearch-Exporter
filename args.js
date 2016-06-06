'use strict';

var JSON = require('json-bigint');

var log = require('./log.js');


exports.args = process.argv;
exports.args.splice(0, 2);

/**
 * uses the given options to build a map of process arguments that match up with the given configurations.
 * This map is later used to find the configuration for a specific option and to store any values found.
 *
 * @param {Object} options
 * @param {string} prefix
 * @param {Object} map
 * @returns {Object}
 */
exports.buildOptionMap = (options, prefix, map = {}) => {
    for (let key in options) {
        let option = options[key];
        if (option.abbr) {
            if (map["-" + option.abbr]) {
                log.error("Warning: driver is overwriting an existing abbreviated option: %s! (current: %s, previous: %s)", "-" + option.abbr, "--" + prefix + key, map["-" + option.abbr].alt);
            }
            map["-" + option.abbr] = {
                alt: "--" + prefix + key,
                list: option.list !== undefined && option.list,
                value: option.flag ? option.preset === true : option.preset,
                required: option.preset !== undefined || option.required === true,
                found: option.preset !== undefined,
                flag: option.flag === true,
                help: option.help,
                min: option.min,
                max: option.max
            };
            if (map["--" + prefix + key]) {
                log.error("Warning: driver is overwriting an existing option: %s!", "--" + prefix + key);
            }
            map["--" + prefix + key] = {
                alt: "-" + option.abbr,
                list: option.list !== undefined && option.list,
                value: option.flag ? option.preset === true : option.preset,
                required: option.preset !== undefined || option.required === true,
                found: option.preset !== undefined,
                flag: option.flag === true,
                help: option.help,
                min: option.min,
                max: option.max
            };
        } else {
            try {
                exports.buildOptionMap(option, prefix + key + ".", map);
            } catch (e) {
                if (e.message == "Maximum call stack size exceeded") {
                    log.die(13, "The option map passed in by the driver contained an error (most likely no abbr property found)");
                } else {
                    log.die(13, e.message);
                }
            }
        }
    }
    return map;
};

/**
 * Tries to cast the value to various types or just returns the original.
 * @param {*} value
 * @param {string} arg
 * @param {Object} option
 * @returns typed value
 */
exports.cast = (value, arg, option) => {
    if (value === null || value === '') {
        return value;
    }
    if (!isNaN(value)) {
        let float = parseFloat(value);
        if (option.min !== undefined && float < option.min) {
            if (arg.substr(0, 2) == '--') {
                log.die(5, arg.substr(2) + " is below constraint (min:" + option.min + ")");
            } else {
                log.die(5, option.alt.substr(2) + " is below constraint (min: " + option.min + ")");
            }
        }
        if (option.max !== undefined && float > option.max) {
            if (arg.substr(0, 2) == '--') {
                log.die(5, arg.substr(2) + " is above constraint (min:" + option.max + ")");
            } else {
                log.die(5, option.alt.substr(2) + " is above constraint (min: " + option.max + ")");
            }
        }
        return float;
    }
    if (value === 'false' || value === 'FALSE') {
        return false;
    }
    if (value === 'true' || value === 'TRUE') {
        return true;
    }
    try {
        return JSON.parse(value);
    } catch (e) {}
    return value;
};

/**
 * Takes the arguments and matches them against the option map that has all the configuration information.
 * The result is a flat option map.
 *
 * @param {Object} options
 * @param {boolean} [complete] Indicates whether all options have been loaded
 * @returns {Object}
 */
exports.parse = (options, complete) => {
    let optionMap = exports.buildOptionMap(options, '');
    let lastArg;
    for (let arg of exports.args) {
        if (optionMap[arg]) {
            lastArg = arg;
            if (optionMap[lastArg].flag) {
                optionMap[lastArg].found = true;
                optionMap[lastArg].value = true;
            }
        }
        else if (lastArg) {
            if (optionMap[lastArg].list) {
                if (!Array.isArray(optionMap[lastArg].value)) {
                    optionMap[lastArg].value = [];
                }
                optionMap[lastArg].value.push(exports.cast(arg, lastArg, optionMap[lastArg]));
            } else {
                if (optionMap[lastArg].parsed) {
                    log.die(5, 'An option that is not a list has been defined twice: ' + lastArg);
                }
                optionMap[lastArg].value = exports.cast(arg, lastArg, optionMap[lastArg]);
            }
            optionMap[lastArg].found = true;
            optionMap[lastArg].parsed = true;
            lastArg = null;
        }
    }

    for (let prop in optionMap) {
        if (optionMap[prop].required && !optionMap[prop].found && !optionMap[optionMap[prop].alt].found) {
            exports.printHelp(prop, optionMap);
        }
    }

    if (complete) {
        if (optionMap['--help'] && optionMap['--help'].found || optionMap['-h'] && optionMap['-h'].found) {
            exports.printHelp(null, optionMap);
            log.die(0);
        }
    }

    let parsed = {};
    for (var option in optionMap) {
        if (optionMap[option].value !== undefined) {
            var optionName = option.substr(2);
            if (option.substr(0, 2) != "--") {
                optionName = optionMap[option].alt.substr(2);
            }
            if (optionMap[option].parsed || parsed[optionName] === undefined) {
                parsed[optionName] = optionMap[option].value;
            }
        }
    }
    return parsed;
};

/**
 * Prints a simple version information about the script, as well as passed in command line arguments
 *
 */
exports.printVersion = () => {
    log.info("Elasticsearch Exporter - Version %s", require('./package.json').version);
    log.debug("Arguments:", exports.args);
};

/**
 * Prints the help for all the given options. If a missing option/property is specified the script will exit with an
 * error message.
 *
 * @param {string} missingProp
 * @param {Object} optionMap
 */
exports.printHelp = (missingProp, optionMap) => {
    function fill(string = '', width) {
        if (typeof string == 'object') {
            string = JSON.stringify(string);
        }
        string = '' + string;
        while (string.length < width) {
            string += ' ';
        }
        return string;
    }

    console.log();
    console.log('Usage Examples:'.bold);
    console.log('  ex.sh -th somehost');
    console.log('  ex.sh -th somehost -si myindex');
    console.log('  ex.sh -th somehost -si myindex -ti renamedindex');
    console.log('  ex.sh -t file -tf path/to/file');
    console.log('  ex.sh -s file -sf path/to/file -th localhost');
    console.log('  ex.sh -s file -sf path/to/file -t mysql -th somehost -tu username -tp password');
    console.log('  ex.sh -o path/to/optionsfile');
    console.log();
    console.log('Global Options:'.bold);
    var inSource, inTarget;
    for (let prop in optionMap) {
        if (prop.substr(0,2) == "--") {
            if (!inSource && prop.indexOf('--source') != -1) {
                console.log();
                console.log('Source Options:'.bold);
                inSource = true;
            }
            if (!inTarget && prop.indexOf('--target') != -1) {
                console.log();
                console.log('Target Options:'.bold);
                inTarget = true;
            }
            var option = optionMap[prop];
            console.log("  " + fill(option.alt, 6) + fill(prop, 25) + fill(option.value, 20) + option.help.grey);
        }
    }
    console.log();
    console.log('More driver specific options can be found when defining a source or target driver'.bold);
    console.log();
    if (missingProp) {
        let missingName;
        if (missingProp.substr(0, 2) == "--") {
            missingName = missingProp.substr(2);
        } else {
            missingName = optionMap[missingProp].alt.substr(2);
        }
        console.log();
        console.log('A required argument is missing: --' + missingName.red);
        console.log();
        log.die(3);
    }
};

/**
 * Prints a summary of all statistics collected throughout the last run.
 *
 * @param {Statistics} statistics
 */
exports.printSummary = statistics => {
    if (statistics.source && statistics.source.retries) {
        log.info('Retries to source:\t%s', statistics.source.retries);
        delete statistics.source.retries;
    }
    if (statistics.target && statistics.target.retries) {
        log.info('Retries to target:\t%s', statistics.target.retries);
        delete statistics.target.retries;
    }
    let error = statistics.hits.processed < statistics.hits.total;
    let none = statistics.hits.total === 0;
    log.info('Processed Entries:\t%s documents', statistics.hits.processed);
    delete statistics.hits.processed;
    log.info('Source DB Size:\t\t%s documents', statistics.hits.total);
    delete statistics.hits.total;
    delete statistics.hits;
    error && log.error('Not all documents have been exported!');
    none && log.error('No documents have been found in the source database!');
    if (statistics.source && statistics.source.count) {
        log.info('Unique Entries:\t\t%s documents', statistics.source.count.uniques);
        delete statistics.source.count.uniques;
        log.info('Duplicate Entries:\t%s documents', statistics.source.count.duplicates);
        delete statistics.source.count.duplicates;
    }
    if (statistics.memory.peak) {
        let ratio = Math.round(statistics.memory.peak / process.memoryUsage().heapTotal  * 100);
        log.info('Peak Memory Used:\t%s bytes (%s%%)', statistics.memory.peak, ratio);
        log.info('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
    }
    delete statistics.memory;
};
