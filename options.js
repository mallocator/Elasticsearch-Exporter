var fs = require('fs');
require('colors');

/**
 * Holds the nomnom object with all the options and settings.
 */
exports.nomnom = null;

/**
 * Sets up nomnom with all available command line options and returns the parsed options object.
 *
 * @returns {Object}
 */
exports.initialize = function() {
    exports.nomnom = require('nomnom').script('exporter').options({
        sourceHost: {
            abbr: 'a',
            'default': 'localhost',
            metavar: '<hostname>',
            help: 'The host from which data is to be exported from'
        },
        targetHost: {
            abbr: 'b',
            metavar: '<hostname>',
            help: 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given'
        },
        sourcePort: {
            abbr: 'p',
            'default': 9200,
            metavar: '<port>',
            help: 'The port of the source host to talk to'
        },
        targetPort: {
            abbr: 'q',
            metavar: '<port>',
            help: 'The port of the target host to talk to'
        },
        sourceIndex: {
            abbr: 'i',
            metavar: '<index>',
            help: 'The index name from which to export data from. If no index is given, the entire database is exported'
        },
        targetIndex : {
            abbr : 'j',
            metavar : '<index>',
            help : 'The index name to which to import the data to. Will only be used and is required if a source index has been specified'
        },
        sourceType : {
            abbr : 't',
            metavar : '<type>',
            help : 'The type from which to export data from. If no type is given, the entire index is exported'
        },
        targetType: {
            abbr: 'u',
            metavar: '<type>',
            help: 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
        },
        sourceQuery: {
            abbr: 's',
            metavar: '<query>',
            help: 'Define a query that limits what kind of documents are exporter from the source',
            'default': {
                match_all:{}
            }
        },
        sourceSize: {
            abbr: 'z',
            metavar: '<size>',
            help: 'The maximum number of results to be returned per query.',
            'default': 10
        },
        sourceFile: {
            abbr: 'f',
            metavar: '<filebase>',
            help: 'The filename from which the data should be imported. The format depends on the compression flag (default = compressed)'
        },
        targetFile: {
            abbr: 'g',
            metavar: '<filebase>',
            help: 'The filename to which the data should be exported. The format depends on the compression flag (default = compressed)'
        },
        testRun: {
            abbr: 'r',
            metavar: 'true|false',
            help: 'Make a connection with the database, but don\'t actually export anything',
            'default': false,
            choices: [ true, false ]
        },
        memoryLimit: {
            abbr: 'm',
            metavar: '<fraction>',
            help: 'Set how much of the available memory the process should use for caching data to be written to the target driver. Should be a float value between 0 and 1 (make sure to pass --nouse-idle-notification --expose-gc as node options to make this work)',
            'default' : 0.9
        },
        sourceCompression: {
            abbr: 'c',
            metavar: 'true|false',
            help: 'Override if compression should be used to read the data files (default is to auto detect)',
            choices: [ true, false ]
        },
        targetCompression: {
            abbr: 'd',
            metavar: 'true|false',
            help: 'Set if compression should be used to write the data files',
            'default': true,
            choices: [ true, false ]
        },
        errorsAllowed: {
            abbr: 'e',
            metavar: '<count>',
            help: 'If a connection error occurs this will set how often the script will retry to connect (-1 would be infinite retries). This is for both reading and writing data.',
            'default': 3
        },
        logEnabled: {
            abbr: 'l',
            metavar: 'true|false',
            help: 'Set logging to console to be enable or disabled. Errors will still be printed, no matter what.',
            'default': true,
            choices: [ true, false ]
        }
    });
    return exports.nomnom.parse();
};

/**
 * If a source file has been set then this will check if the file has been compressed by checking the file header.
 * This check is circumvented if the sourceCompression flag has been set (which forces to read un-/compressed).
 *
 * @param opts
 */
exports.detectCompression = function(opts) {
    if (!opts.sourceFile || opts.sourceCompression !== undefined) return;
    var header = new Buffer(2);
    fs.readSync(fs.openSync(opts.sourceFile + '.data', 'r'), header, 0, 2);
    opts.sourceCompression = (header[0] == 0x1f && header[1] == 0x8b);
};

/**
 * A lot of settings that are needed later can be set automatically to make the life of the user easier.
 * This function performs this task.
 * @param opts
 */
exports.autoFillOptions = function(opts) {
    if (!opts.targetHost && !opts.targetFile) {
        opts.targetHost = opts.sourceHost;
    }
    if (!opts.targetPort && !opts.targetFile) {
        opts.targetPort = opts.sourcePort;
    }
    if (opts.sourceIndex && !opts.targetIndex) {
        opts.targetIndex = opts.sourceIndex;
    }
    if (opts.sourceType && !opts.targetType) {
        opts.targetType = opts.sourceType;
    }
};

/**
 * This function will attempt to filter out any combinations of options that are not valid.
 *
 * @param opts
 * @returns {String} An error message if any or null
 */
exports.validateOptions = function(opts) {
    if (opts.sourceFile) {
        if (!fs.existsSync(opts.sourceFile + '.meta')) {
            return 'Source File "' + opts.sourceFile + '.meta" doesn\'t exist';
        }
        if (!fs.existsSync(opts.sourceFile + '.data')) {
            return 'Source File "' + opts.sourceFile + '.data" doesn\'t exist';
        }
    }
	if (opts.sourceHost != opts.targetHost) return;
	if (opts.sourcePort != opts.targetPort) return;
	if (opts.sourceIndex != opts.targetIndex) return;
	if (opts.sourceType != opts.targetType && opts.sourceIndex) return;
    if (opts.sourceFile && opts.targetHost) return;
    if (opts.targetHost && opts.sourceFile) return;
    return 'Not enough information has been given to be able to perform an export. Please review the options and examples again.';
};

/**
 * This function will run the initialization and all validity checks available before returning the resulting options object.
 * @returns {Object}
 */
exports.opts = function() {
    var opts = exports.initialize();
    exports.detectCompression(opts);
    exports.autoFillOptions(opts);
    var error = exports.validateOptions(opts);
    if (error) {
        if (opts.logEnabled) {
            console.log(error.red);
            console.log(exports.nomnom.getUsage());
        }
        process.exit(1);
    }
    return opts;
};
