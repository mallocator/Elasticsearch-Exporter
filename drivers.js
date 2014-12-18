require('colors');
var fs = require('fs');
var util = require('util');
var async = require('async');

var STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg;
var ARGUMENT_NAMES = /([^\s,]+)/g;
var REQUIRED_METHODS = {
    getInfo: ['callback'],
    verifyOptions: ['opts', 'callback'],
    getTargetStats: ['env', 'callback'],
    getSourceStats: ['env', 'callback'],
    getMeta: ['env', 'callback'],
    putMeta: ['env', 'metadata', 'callback'],
    getData: ['env', 'callback'],
    putData: ['env', 'data', 'callback'],
    reset: ['callback']
};

exports.drivers = {};

exports.params = {
    get: function (func) {
        var fnStr = func.toString().replace(STRIP_COMMENTS, '');
        var result = fnStr.slice(fnStr.indexOf('(') + 1, fnStr.indexOf(')')).match(ARGUMENT_NAMES);
        if (result === null) {
            result = [];
        }
        return result;
    },
    verify: function (func, b) {
        var a = this.get(func);
        if (a === b) {
            return true;
        }
        for (var i in b) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
};

exports.verify = function (driver) {
    var requiredMethods = util._extend({}, REQUIRED_METHODS);
    for (var property in driver) {
        if (typeof driver[property] == "function" && requiredMethods[property]) {
            if (exports.params.verify(driver[property], requiredMethods[property])) {
                delete requiredMethods[property];
            } else {
                var missingParamsMessage = "The selected driver is missing parameters on a function: " + property;
                console.log(missingParamsMessage.red);
            }
        }
    }
    if (!Object.keys(requiredMethods).length) {
        return true;
    }
    for (var missingMethod in requiredMethods) {
        var missingMethodMessage = "The selected driver is missing a required function: " + missingMethod;
        console.log(missingMethodMessage.red);
    }
    return false;
};

exports.register = function (driver, callback) {
    if (!exports.verify(driver)) {
        process.exit(10);
    }

    driver.getInfo(function (info, options) {
        exports.drivers[info.id] = {
            info: info,
            options: options,
            driver: driver
        };
        // TODO only show for debug
        console.log("Loaded [" + info.name + "] version: " + info.version);
        callback();
    });
};

exports.find = function (dir, callback) {
    var files = fs.readdirSync(dir);
    async.each(files, function (file, callback) {
        if (file.indexOf(".driver.js") > -1) {
            exports.register(require(dir + '/' + file), callback);
        }
        else {
            callback();
        }
    }, callback);
};

exports.get = function(id) {
    if (!exports.drivers[id]) {
        var message = "Tried to load driver [" + id + "] that doesnt exist!";
        console.log(message.red);
        process.exit(11);
    }
    return exports.drivers[id];
}

exports.describe = function() {
    for (var i in exports.drivers) {
        var driver = exports.drivers[i].info;
        console.log("ID: [" + driver.id.blue + "] Version: [" + driver.version + "]" + (" - " + driver.name + ": " + driver.desciption).grey);
    }
}