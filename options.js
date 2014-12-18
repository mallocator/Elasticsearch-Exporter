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
var drivers = require('./drivers.js');
var nomnom = require('nomnom');
var async = require('async');

var OPTIONS = {
    "driver.source": {
        abbr: 's',
        'default': 'elasticsearch',
        metavar: '<driver id>',
        help: 'The id of the driver to use to export data from'
    },
    "driver.target": {
        abbr: 't',
        'default': 'elasticsearch',
        metavar: '<driver id>',
        help: 'The id of the driver to use to import data into'
    },
    "drivers.dir": {
        abbr: 'd',
        'default': [],
        metavar: '<directory>',
        list: true,
        help: 'Additional directories that the script should look for drivers in (can be used multiple times)'
    },
    "drivers.list": {
        abbr: 'l',
        flag: true,
        help: 'List all the drivers the script has found'
    },
    testrun: {
        abbr: 'r',
        metavar: 'true|false',
        help: 'Make a connection with the database, but don\'t actually export anything',
        'default': false,
        choices: [true, false]
    },
    "memory.limit": {
        abbr: 'm',
        metavar: '<fraction>',
        help: 'Set how much of the available memory the process should use for caching data to be written to the target driver. Should be a float value between 0 and 1 (make sure to pass --nouse-idle-notification --expose-gc as node OPTIONS to make this work)',
        'default': 0.9
    },
    "errors.allowed": {
        abbr: 'x',
        metavar: '<count>',
        help: 'If a connection error occurs this will set how often the script will retry to connect. This is for both reading and writing data.',
        'default': 3
    },
    "log.enabled": {
        abbr: 'e',
        metavar: 'true|false',
        help: 'Set logging to console to be enable or disabled. Errors will still be printed, no matter what.',
        'default': true,
        choices: [true, false]
    },
    optionsfile: {
        abbr: 'o',
        metavar: '<file.json>',
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

exports.read = function(callback) {
    var scriptOptions = nomnom.script('exporter').options(OPTIONS).help("Further driver specific options are available when you specify them").parse();
    scriptOptions["drivers.dir"].push('./drivers')
    async.each(scriptOptions["drivers.dir"], function(dir, callback) {
        drivers.find(dir, callback);
    }, function() {
        if (scriptOptions['drivers.list']) {
            drivers.describe();
            process.exit(0);
        }
        var sourceOptions = exports.deflate(drivers.get(scriptOptions['driver.source']).options, 'source');
        var targetOptions = exports.deflate(drivers.get(scriptOptions['driver.target']).options, 'target');
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
        var parsedDriverOptions = nomnom.script('drivers').options(driverOptions).parse();
        var options = exports.inflate(parsedDriverOptions);
        callback(options);
    });
};

exports.verify = function(options, callback) {
    async.map([options.driver.source, options.driver.target], function(driver, callback){
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
    exports.verify(options, function(){});
});


// TODO debug level logging & no logging options

// TODO print out all passed in OPTIONS