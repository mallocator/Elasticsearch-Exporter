'use strict';

var fs = require('fs');

var Driver = require('./driver.interface');
var log = require('../log.js');


class CSV extends Driver {
    constructor() {
        super();
        this.id = 'csv';
        this.indexCounter = 0;
        this.propertyMap = {};
    }

    getInfo(callback) {
        let info = {
            id: this.id,
            name: 'CSV Driver',
            version: '1.0',
            description: 'A CSV driver to export data that maps all fields to columns'
        };
        let options = {
            target: {
                noheader: {
                    abbr: 'h',
                    help: 'Whether to include a header row or not',
                    flag: true,
                    preset: false
                }, separator: {
                    abbr: 's',
                    help: 'The separator to use between columns',
                    preset: ','
                }, quoteEverything: {
                    abbr: 'q',
                    help: 'Whether to force to have all values encapsulated in quotes',
                    flag: true,
                    preset: false
                }, trimData: {
                    abbr: 't',
                    help: 'Whether to trim data in columns and strip wihtespace from beginning/end',
                    flag: true,
                    preset: false
                }, unixQuotes: {
                    abbr: 'c',
                    help: 'Whether to use "" (standard) to escape quotes or \\"',
                    flag: true,
                    preset: false
                }, file: {
                    abbr: 'f',
                    help: 'The file to which the data should be exported.',
                    required: true
                }, append: {
                    abbr: 'a',
                    help: 'If a file exists append or overwrite existing data',
                    flag: true,
                    preset: false
                }
            }
        };
        callback(null, info, options);
    }

    verifyOptions(opts, callback) {
        let err = [];
        opts.drivers.source == this.id && err.push("The CSV driver doesn't support import operations.");
        if (opts.drivers.target == this.id) {
            if (fs.existsSync(opts.target.file)) {
                log.info('Warning: ' + opts.target.file + ' already exists, duplicate entries might occur');
            }
            if (opts.target.separator.trim === '') {
                err.push('Seperator is empty, resulting file will not be a CSV');
            }
        }
        callback(err);
    }

    reset(env, callback) {
        this.propertyMap = {};
        this.indexCounter = 0;
        callback();
    }

    getTargetStats(env, callback) {
        callback(null, {
            version: "1.0.0",
            cluster_status: "Green"
        });
    }

    getSourceStats(env, callback) {
        throw new Error("CSV driver doesn't support import operations");
    }

    getMeta(env, callback) {
        throw new Error("CSV driver doesn't support import operations");
    }

    putMeta(env, metadata, callback) {
        if (!env.options.target.append) {
            fs.writeFileSync(env.options.target.file, '', {encoding: 'utf8'});
        }
        let separator = env.options.target.separator;
        let header;
        if (env.options.target.quoteEverything) {
            header = '"index"' + separator + '"type"';
        } else {
            header = 'index' + separator + 'type';
        }
        for (let mapping in metadata.mappings) {
            for (let index in metadata.mappings[mapping]) {
                for (let type in metadata.mappings[mapping][index]) {
                    for (let property in metadata.mappings[mapping][index][type]) {
                        if (!this.propertyMap[property]) {
                            this.propertyMap[property] = this.indexCounter++;
                            header += separator + this._escape(env, property);
                        }
                    }
                }
            }

        }
        if (!env.options.target.noheader) {
            if (!fs.existsSync(env.options.target.file) || fs.statSync(env.options.target.file).size === 0) {
                fs.writeFileSync(env.options.target.file, header + '\n', {encoding: 'utf8'});
            }
        }
        callback();
    }

    getData(env, callback) {
        throw new Error("CSV driver doesn't support import operations");
    }

    putData(env, docs, callback) {
        for (let doc of docs) {
            let line = [this.propertyMap.length];
            for (let property in doc._source) {
                line[this.propertyMap[property]] = this._escape(env, doc._source[property]);
            }
            line.unshift(doc._type);
            line.unshift(doc._index);
            fs.appendFile(env.options.target.file, line.join(env.options.target.separator) + '\n', {encoding: 'utf8'});
        }
        callback();
    }

    _escape(env, data) {
        if (!data) {
            return '';
        }
        if (!isNaN(data)) {
            return data;
        }
        if (typeof data === 'object') {
            data = JSON.stringify(data);
        }
        if (env.options.target.trimData) {
            data = data.trim();
        }
        if (env.options.target.quoteEverything
            || data.indexOf('\n') !== -1
            || data.indexOf('"') !== -1
            || data.indexOf(env.options.target.separator) !== -1) {
            if (env.options.target.unixQuotes) {
                data = data.replace('"', '\\"');
            } else {
                data = data.replace('"', '""');
            }
            data =  "\"" + data + "\"";
        }
        return data;
    }
}

module.exports = new CSV();
