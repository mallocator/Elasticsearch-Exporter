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