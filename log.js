'use strict';

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

function timestamp() {
    if (exports.enabled.timestamps) {
        return ('[' + new Date().toISOString() + '] ').grey.bold;
    } else {
        return '';
    }
}

exports.enabled = {
    debug: false,
    info: true,
    timestamps: false
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
exports.pollCapturedLogs = () => {
    var chunk = logs;
    logs = [];
    return chunk;
};

/**
 * All arguments are passed on to util format to log the message in red.
 *
 */
exports.error = (...args) => !capture("ERROR", args) && console.log(timestamp() + util.format(...args).red);

/**
 * All arguments are passed on to util format to log the message.
 *
 */
exports.info = (...args) => !capture("INFO", args) && exports.enabled.info && console.log(timestamp() + util.format(...args));

/**
 * All arguments are passed on to util format to log the message in grey.
 *
 */
exports.debug = (...args) => !capture("DEBUG", args) && exports.enabled.debug && console.log(timestamp() + util.format(...args).grey);

exports.statusMaxLength = 0;

exports.returnCtrl = /^win/.test(process.platform) ? "\x1B[0G" : "\r";

/**
 * Prints a line without a trailing new line character end returns the carriage to the beginning so that this message
 * can be overwritten by the next output.
 */
exports.status = (...args) => {
    if (!capture("STATUS", args) && exports.enabled.info) {
        let message = timestamp() + util.format(...args);
        exports.statusMaxLength = Math.max(exports.statusMaxLength, message.length);
        process.stdout.write(message + exports.returnCtrl);
    }
};

/**
 * Removes all characters that have been left over by any previous status calls.
 */
exports.clearStatus = () => {
    var message = new Array(exports.statusMaxLength + 1).join(' ');
    process.stdout.write(message + exports.returnCtrl);
};

/**
 * End the process with a given status code and spit out one last message. Some of the messages are predefined
 * depending on the given status code and will be printed if no message has been given with the respective status code.
 *
 * @param {number} [status]     The status code with which the process will exit
 * @param {string} [message]    The message that will be printed
 */
exports.die = (status, message) => {
    if (exports.capture) {
        message && logs.push("ERROR: " + message);
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
    message && console.log(timestamp() + ("Exit code " + status + ": " + message).red);
    process.exit(status);
};
