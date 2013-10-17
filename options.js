var fs = require('fs');
require('colors');

var nomnom = require('nomnom').script('exporter').options({
    sourceHost : {
		abbr : 'a',
		'default' : 'localhost',
		metavar : '<hostname>',
		help : 'The host from which data is to be exported from'
	},
	targetHost : {
		abbr : 'b',
		metavar : '<hostname>',
		help : 'The host to which to import the data to. Needs to be a different host than the source host, if no index is given'
	},
	sourcePort : {
		abbr : 'p',
		'default' : 9200,
		metavar : '<port>',
		help : 'The port of the source host to talk to'
	},
	targetPort : {
		abbr : 'q',
		metavar : '<port>',
		help : 'The port of the target host to talk to'
	},
	sourceIndex : {
		abbr : 'i',
		metavar : '<index>',
		help : 'The index name from which to export data from. If no index is given, the entire database is exported'
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
	targetType : {
		abbr : 'u',
		metavar : '<type>',
		help : 'The type name to which to import the data to. Will only be used and is required if were importing to the same'
	},
    sourceQuery : {
        abbr : 's',
        metavar: '<query>',
        help: 'Define a query that limits what kind of documents are exporter from the source',
        'default' : {
            match_all:{}
        }
    },
    sourceSize : {
        abbr : 'z',
        metavar: '<size>',
        help: 'The maximum number of results to be returned per query.',
        'default' : 10
    },
    sourceFile : {
        abbr : 'f',
        metavar : '<filebase>',
        help : 'The filename from which the data should be imported. The format depends on the compression flag (default = compressed)'
    },
    targetFile : {
        abbr : 'g',
        metavar : '<filebase>',
        help : 'The filename to which the data should be exported. The format depends on the compression flag (default = compressed)'
    },
    testRun: {
        abbr : 'r',
        metavar: 'true|false',
        help: 'Make a connection with the database, but don\'t actually export anything',
        'default': false,
        choices: [ true, false ]
    },
    memoryLimit: {
        abbr : 'm',
        metavar : '<fraction>',
        help: 'Set how much of the available memory the process should use for caching data to be written to the target driver. Should be a float value between 0 and 1 (make sure to pass --nouse-idle-notification --expose-gc as node options to make this work)',
        'default' : 0.9
    },
    sourceCompression: {
        abbr: 'c',
        metavar: 'true|false',
        help: 'Set if compression should be used to read the data files (ignored if not using sourceFile option)',
        'default': true,
        choices: [ true, false ]
    },
    targetCompression: {
        abbr: 'd',
        metavar: 'true|false',
        help: 'Set compression should be used to write the data files (ignored if not using targetFile option)',
        'default': true,
        choices: [ true, false ]
    }
});
var opts = nomnom.parse();

(function validateOptions() {
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
    if (opts.sourceFile) {
        if (!fs.existsSync(opts.sourceFile + '.meta')) {
            console.log(('Source File "' + opts.sourceFile + '.meta" doesn\'t exist').red);
            process.exit(1);
        }
        if (!fs.existsSync(opts.sourceFile + '.data')) {
            console.log(('Source File "' + opts.sourceFile + '.data" doesn\'t exist').red);
            process.exit(1);
        }
    } else if (opts.sourceCompression) {
        console.log('Warning: compression has been set for source file, but no source file is being used!'.red);
        process.exit(1);
    }
    if (!opts.targetFile && opts.targetCompression) {
        console.log('Warning: compression has been set for target file, but no target file is being used!'.red);
        process.exit(1);
    }
	if (opts.sourceHost != opts.targetHost) { return; }
	if (opts.sourcePort != opts.targetPort) { return; }
	if (opts.sourceIndex != opts.targetIndex) { return; }
	if (opts.sourceType != opts.targetType && opts.sourceIndex) { return; }
    if (opts.sourceFile && opts.targetHost) { return; }
    if (opts.targetHost && opts.sourceFile) { return; }
	console.log(nomnom.getUsage());
	process.exit(1);
})();

exports.opts = opts;
