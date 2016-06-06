#!/usr/bin/env node --nouse-idle-notification --expose-gc --always_compact --max_old_space_size=1024

var exporter = require('../exporter.js');
var args = require('../args.js');
var log = require('../log.js');

process.on('uncaughtException', exporter.handleUncaughtExceptions);
process.on('exit', () => exporter.env && exporter.env.statistics && args.printSummary(exporter.env.statistics));
exporter.run(err => {
    if (err) {
        if (isNaN(err)) {
            log.error("The driver reported an error:", err);
            log.die(4);
        } else {
            log.die(err);
        }
    }
});
