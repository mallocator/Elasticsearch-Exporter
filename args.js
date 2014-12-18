var util = require('util');
var log = require('./log.js');

exports.args = process.argv;
exports.args.splice(0, 2);

/**
 * uses the given options to build a map of process arguments that match up with the given configurations.
 * This map is later used to find the configuration for a specific option and to store any values found.
 * @param options
 * @param prefix
 * @returns {{}}
 */
exports.buildOptionMap = function(options, prefix) {
    var map = {};
    for (var key in options) {
        var option = options[key];
        if (option.abbr) {
            map["-" + option.abbr] = {
                alt: "--" + prefix + key,
                list: option.list,
                value: option.flag ? option.preset === true : option.preset,
                required: option.preset !== undefined || option.required === true,
                found: option.preset !== undefined,
                help: option.help
            };
            map["--" + prefix + key] = {
                alt: "-" + option.abbr,
                list: option.list,
                value: option.flag ? option.preset === true : option.preset,
                required: option.preset !== undefined || option.required === true,
                found: option.preset !== undefined,
                help: option.help
            };
        } else {
            map = util._extend(map, exports.buildOptionMap(option, key + "."));
        }
    }
    return map;
};

/**
 * Prints a simple version information about the script, as well as passed in command line arguments
 */
exports.printVersion = function() {
    log.info("Elasticsearch Exporter - Version %s", require('./package.json').version);
    log.debug("Arguments:", exports.args);
};

/**
 * Prints the help for all the given options. If a missing option/property is specified the script will exit with an error message.
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
        string = '' + string
        while (string.length < width) {
            string += ' ';
        }
        return string;
    }

    console.log();
    for (var prop in optionMap) {
        if (prop.substr(0,2) == "--") {
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
        process.exit(3);
    }
};

/**
 * Takes the arguments and matches them against the option map that has all the configuration information.
 * The result is a flat option map.
 *
 * @param options
 * @returns {{}}
 */
exports.parse = function(options) {
    var optionMap = exports.buildOptionMap(options, '');

    var lastArg;
    for (var i in exports.args) {
        var arg = exports.args[i];
        if (optionMap[arg]) {
            lastArg = arg;
            optionMap[lastArg].value = true;
        }
        else if (lastArg) {
            if (optionMap[lastArg].list) {
                if (!Array.isArray(optionMap[lastArg].value)) {
                    optionMap[lastArg].value = [];
                }
                optionMap[lastArg].value.push(arg);
            } else {
                optionMap[lastArg].value = arg;
            }
            optionMap[lastArg].found = true;
            lastArg = null;
        }
    }

    for (var prop in optionMap) {
        if (optionMap[prop].required && !optionMap[prop].found && !optionMap[optionMap[prop].alt].found) {
            exports.printHelp(prop, optionMap);
        }
    }

    var parsed = {};
    for (var option in optionMap) {
        if (optionMap[option].value) {
            if (option.substr(0,2) == "--") {
                parsed[option.substr(2)] = optionMap[option].value;
            } else {
                parsed[optionMap[option].alt.substr(2)] = optionMap[option].value;
            }
        }
    }
    return parsed;
};