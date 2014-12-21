require('colors');
var util = require('util');

exports.enabled = {
    debug: false,
    info: true
};

/**
 * All arguments are passed on to util format to log the message in red.
 *
 */
exports.error = function() {
    console.log(util.format.apply(null, arguments).red);
};

/**
 * All arguments are passed on to util format to log the message.
 *
 */
exports.info = function() {
    if (exports.enabled.info) {
        console.log(util.format.apply(null, arguments));
    }
};

/**
 * All arguments are passed on to util format to log the message in grey.
 *
 */
exports.debug = function() {
    if (exports.enabled.debug) {
        console.log(util.format.apply(null, arguments).grey);
    }
};

/**
 * Prints a line without a trailing new line character end returns the carriage to the beginning so that this message
 * can be overwritten by the next output.
 */
exports.status = function() {
    if (exports.enabled.info) {
        util.print(util.format.apply(null, arguments) + "\r");
    }
}