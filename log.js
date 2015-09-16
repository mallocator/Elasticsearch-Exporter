var util = require('util');

require('colors');


var logs = [];

function capture(level, args) {
    if (exports.capture) {
        logs.push(level + ": " + util.format.apply(null, args));
        return true;
    }
    return false;
}

exports.enabled = {
    debug: false,
    info: true
};

/**
 * Enabling capture will disable sending logs to console and instead store them in memory so that they can be polled.
 *
 * @type {boolean}
 */
exports.capture = false;

/**
 * Empty the currently queued up logs and return them in an array. All log messages will have the error level prefixed.
 *
 * @returns {Array}
 */
exports.pollCapturedLogs = function() {
    var chunk = logs;
    logs = [];
    return chunk;
};

/**
 * All arguments are passed on to util format to log the message in red.
 *
 */
exports.error = function() {
    if (!capture("ERROR", arguments)) {
        console.log(util.format.apply(null, arguments).red);
    }
};

/**
 * All arguments are passed on to util format to log the message.
 *
 */
exports.info = function() {
    if (!capture("INFO", arguments) && exports.enabled.info) {
        console.log(util.format.apply(null, arguments));
    }
};

/**
 * All arguments are passed on to util format to log the message in grey.
 *
 */
exports.debug = function() {
    if (!capture("DEBUG", arguments) && exports.enabled.debug) {
        console.log(util.format.apply(null, arguments).grey);
    }
};

exports.returnCtrl = /^win/.test(process.platform) ? "\033[0G" : "\r";

/**
 * Prints a line without a trailing new line character end returns the carriage to the beginning so that this message
 * can be overwritten by the next output.
 *
 */
exports.status = function() {
    if (!capture("STATUS", arguments) && exports.enabled.info) {
        process.stdout.write(util.format.apply(null, arguments) + exports.returnCtrl);
    }
};

/**
 * End the process with a given status code and spit out one last message. Some of the messages are predefined
 * depending on the given status code.
 *
 * @param status
 * @param message
 */
exports.die = function(status, message) {
    if (exports.capture) {
        if (message) {
            logs.push("ERROR: " + message);
        }
        throw new Error(message);
    }

    if (!message) {
        switch(status) {
            case 1: message = "No documents found to export"; break;
            case 2: message = "Uncaught Exception"; break;
            case 4: message = "driver is passing on an error"; break;
            case 10: message = "driver interface is not implemented properly"; break;
            case 11: message = "driver doesn't exist"; break;
            case 12: message = "driver threw unknown error"; break;
            case 13: message = "driver option map is invalid"; break;
        }
    }
    if (message) {
        console.log(("Exit code " + status + ": " + message).red);
    }
    process.exit(status);
};