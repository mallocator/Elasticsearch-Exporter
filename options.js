// Process exit signals:
// 0 - Operation successful
// 1 - No documents found to export
// 2 - Uncaught Exception
// 3 - invalid options specified
// 4 - source or target status = invalid / red / not ready
// 10 - driver interface is not implemented properly
// 11 - driver doesn't exist
// 131-254 - reserved for driver specific problems


require('colors');
var fs = require('fs');
var util = require('util');
var async = require('async');
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
        help: 'Make a connection with the database, but don\'t actually export anything',
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
    "log.enabled": {
        abbr: 'e',
        help: 'Set logging to console to be enable or disabled. Errors will still be printed, no matter what.',
        preset: true,
        flag: true
    },
    optionsfile: {
        abbr: 'o',
        help: 'Read OPTIONS from a given file. Options from command line will override these values'
    }
};

exports.deflate = function(options, type) {
    var driverOptions = {};
    var abbrPrefix = type.charAt(0);
    for (var option in options[type]) {
        driverOptions[type + "." + option] = options[type][option];
        driverOptions[type + "." + option].abbr = abbrPrefix + driverOptions[type + "." + option].abbr;
    }
    return driverOptions;
};

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

exports.readFile = function(scriptOptions, sourceOptions, targetOptions) {
    if (!fs.existsSync(scriptOptions.optionsfile)) {
        console.log('The given option file could not be found!'.red);
        process.exit(2);
    }
    var fileOpts = exports.deflateFile(JSON.parse(fs.readFileSync(scriptOptions.optionsfile)), '');
    console.log(fileOpts)
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

exports.read = function(callback) {
    var scriptOptions = args.parse(OPTIONS);

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

exports.verify = function(options, callback) {
    async.map([options.drivers.source, options.drivers.target], function(driver, callback){
        drivers.get(driver).driver.verifyOptions(options, callback)
    }, function(errors){
        var error;
        for (var i in errors) {
            error = true;
            console.log(errors[i]);
        }
        if (error) {
            var message  = "The program could not validate all options and will terminate";
            console.log(message.red);
            process.exit(3);
        }
        callback();
    });
};

exports.read(function(options){
    console.log(options);
    exports.verify(options, function(){});
});


// TODO debug level logging & no logging options

// TODO print out all passed in OPTIONS