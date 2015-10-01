var JSON = require('json-bigint'); // jshint ignore:line

var log = require('./log.js');


exports.args = process.argv;
exports.args.splice(0, 2);

/**
 * uses the given options to build a map of process arguments that match up with the given configurations.
 * This map is later used to find the configuration for a specific option and to store any values found.
 *
 * @param options
 * @param prefix
 * @param map
 * @returns {{}}
 */
exports.buildOptionMap = function(options, prefix, map) {
    if (!map) {
        map = {};
    }
    for (var key in options) {
        var option = options[key];
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
                help: option.help
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
                help: option.help
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
 * Takes the arguments and matches them against the option map that has all the configuration information.
 * The result is a flat option map.
 *
 * @param options
 * @param complete Indicates whether all options have been loaded
 * @returns {{}}
 */
exports.parse = function (options, complete) {
    var optionMap = exports.buildOptionMap(options, '');

    var lastArg;
    for (var i in exports.args) {
        var arg = exports.args[i];
        if (optionMap[arg]) {
            lastArg = arg;
            if (!optionMap[lastArg].value) {
                optionMap[lastArg].value = true;
            }
            if (optionMap[lastArg].flag) {
                optionMap[lastArg].found = true;
            }
        }
        else if (lastArg) {
            if (optionMap[lastArg].list) {
                if (!Array.isArray(optionMap[lastArg].value)) {
                    optionMap[lastArg].value = [];
                }
                optionMap[lastArg].value.push(arg);
            } else {
                if (optionMap[lastArg].parsed) {
                    log.die(5, 'An option that is not a list has been defined twice: ' + lastArg);
                }
                optionMap[lastArg].value = arg;
            }
            optionMap[lastArg].found = true;
            optionMap[lastArg].parsed = true;
            lastArg = null;
        }
    }

    for (var prop in optionMap) {
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

    var parsed = {};
    for (var option in optionMap) {
        if (optionMap[option].value) {
            if (option.substr(0, 2) == "--") {
                if (!parsed[option.substr(2)] || option.parsed) {
                    parsed[option.substr(2)] = optionMap[option].value;
                }
            } else {
                if (!parsed[optionMap[option].alt.substr(2)] || optionMap[option].parsed) {
                    parsed[optionMap[option].alt.substr(2)] = optionMap[option].value;
                }
            }
        }
    }
    return parsed;
};

/**
 * Prints a simple version information about the script, as well as passed in command line arguments
 *
 */
exports.printVersion = function() {
    log.info("Elasticsearch Exporter - Version %s", require('./package.json').version);
    log.debug("Arguments:", exports.args);
};

/**
 * Prints the help for all the given options. If a missing option/property is specified the script will exit with an
 * error message.
 *
 * @param missingProp
 * @param optionMap
 */
exports.printHelp = function(missingProp, optionMap) {
    function fill(string, width) {
        if (string === undefined) {
            string = '';
        }
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
    for (var prop in optionMap) {
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
        var missingName;
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
 * @param statistics
 */
exports.printSummary = function (statistics) {
    if (statistics.source && statistics.source.retries) {
        log.info('Retries to source:\t%s', statistics.source.retries);
        delete statistics.source.retries;
    }
    if (statistics.target && statistics.target.retries) {
        log.info('Retries to target:\t%s', statistics.target.retries);
        delete statistics.target.retries;
    }
    log.info('Fetched Entries:\t%s documents', statistics.hits.fetched);
    delete statistics.hits.fetched;
    log.info('Processed Entries:\t%s documents', statistics.hits.processed);
    delete statistics.hits.processed;
    log.info('Source DB Size:\t\t%s documents', statistics.hits.total);
    delete statistics.hits.total;
    if (statistics.source && statistics.source.count) {
        log.info('Unique Entries:\t\t%s documents', statistics.source.count.uniques);
        delete statistics.source.count.uniques;
        log.info('Duplicate Entries:\t%s documents', statistics.source.count.duplicates);
        delete statistics.source.count.duplicates;
    }
    if (statistics.memory.peak) {
        var ratio = Math.round(statistics.memory.peak / process.memoryUsage().heapTotal  * 100);
        log.info('Peak Memory Used:\t%s bytes (%s%%)', statistics.memory.peak, ratio);
        delete statistics.memory.peak;
        log.info('Total Memory:\t\t%s bytes', process.memoryUsage().heapTotal);
    }
    // TODO print remaining stats in general
    log.debug(statistics);
};