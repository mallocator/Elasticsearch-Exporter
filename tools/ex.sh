#!/usr/bin/env node --nouse-idle-notification --expose-gc --always_compact --max_old_space_size=1024

var exporter = require( '../exporter.js' );

process.on('uncaughtException', exporter.handleUncaughtExceptions);
process.on('exit', exporter.printSummary);
exporter.main.run(function(err) {
    if (err) {
        if (isNaN(err)) {
            console.log("ERROR: The driver reported an error:", err);
            process.exit(4);
        } else {
            process.exit(err);
        }
    }
    process.exit(0);
});