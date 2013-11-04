var fs = require('fs');
var expect = require('chai').expect;
var gently = new (require('gently'));
var mockDriver = require('./driver.mock.js');
var exporter = require('../exporter.js');


exporter.sourceDriver = mockDriver;
exporter.targetDriver = mockDriver;
exporter.opts = {
    logEnabled: false
};

describe('exporter', function () {
    afterEach(function () {
        gently.verify();
    });

    describe('#getMemoryStats()', function () {
        it("should have a memory ratio between 0 and 1", function (done) {
            var ratio = exporter.getMemoryStats();
            expect(ratio).to.be.within(0, 1);
            done();
        });

        it("should cache memory requests for a time", function (done) {
            var ratio1 = exporter.getMemoryStats();
            var ratio2 = exporter.getMemoryStats();
            expect(ratio1).to.be.equal(ratio2);
            setTimeout(function () {
                var ratio3 = exporter.getMemoryStats();
                expect(ratio1).not.to.be.equal(ratio3);
                done();
            }, 1000);
        });
    });

    describe('#waitOnTargetDriver()', function () {
        it("should not be trying to do a gc and just keep going", function (done) {
            gently.expect(exporter, 'getMemoryStats', function () {
                return 0.5;
            });

            global.gc = true;
            exporter.opts.memoryLimit = 0.8;
            exporter.waitOnTargetDriver(done);
        });

        it("should try gc once and then continue", function (done) {
            gently.expect(exporter, 'getMemoryStats', function () {
                return 0.9;
            });
            gently.expect(global, 'gc');

            exporter.opts.memoryLimit = 0.8;
            exporter.waitOnTargetDriver(done);
        });

        it("should not do anything other than call the callback", function (done) {
            global.gc = false;
            exporter.opts.memoryLimit = 0.9;
            exporter.waitOnTargetDriver(done);
        });
    });

    describe('#handleMetaResult()', function () {
        it("should call targetDriver#storeMeta() and not exporter#storeHits()", function (done) {
            var metadata = require('./data/mem.all.json');
            gently.expect(exporter.targetDriver, 'storeMeta', function (opts, data, callback) {
                expect(opts).to.be.deep.equal({logEnabled: false});
                expect(data).to.be.deep.equal(metadata);
                expect(callback).to.be.a('function');
                callback();
                setTimeout(function () {
                    expect(exporter.mappingReady).to.be.ok;
                    done();
                }, 100);
            });

            exporter.hitQueue = [];
            exporter.opts = {logEnabled: false};
            exporter.handleMetaResult(metadata);
        });

        it("should call exporter#storeHits() if hits have been queued", function (done) {
            var metadata = require('./data/mem.all.json');
            gently.expect(exporter.targetDriver, 'storeMeta', function (opts, data, callback) {
                callback();
            });
            gently.expect(exporter, "storeHits", function(hits){
                expect(hits).to.be.empty;
                done();
            });

            exporter.hitQueue = [{}];
            exporter.opts = {logEnabled: false};
            exporter.handleMetaResult(metadata);
        });
    });

    describe('#handleDataResult()', function () {
        it("should call targetDriver#getData() on the first run without data", function (done) {
            gently.expect(exporter, 'waitOnTargetDriver', function (callback) {
                callback();
            });
            gently.expect(exporter.sourceDriver, 'getData', function(opts, callback) {
                expect(opts).to.be.deep.equal({logEnabled: false});
                expect(callback).to.be.a('function');
                expect(exporter.firstRun).to.be.false;
                done();
            });

            exporter.sourceDriver.failMethod = null;
            exporter.firstRun = true;
            exporter.opts = {logEnabled: false};
            exporter.handleDataResult([], 10);
        });


        it("should call exporter#storeHits() and targetDriver#getData()", function (done) {
            gently.expect(exporter,'storeHits', function(data) {
                expect(data).to.be.deep.equal([{},{},{}]);
            });
            gently.expect(exporter, 'waitOnTargetDriver', function (callback) {
                callback();
            });
            gently.expect(exporter.sourceDriver, 'getData', function (opts, callback) {
                expect(opts).to.be.deep.equal({logEnabled: false});
                expect(callback).to.be.a('function');
                done();
            });

            exporter.sourceDriver.failMethod = null;
            exporter.opts = {logEnabled: false};
            exporter.handleDataResult([{},{},{}], 3);
        });

        it("should not do anything after the first run, when there is no more data", function () {
            exporter.sourceDriver.failMethod = function() {
                throw new Error("Method should not have been called");
            };

            exporter.firstRun = false;
            exporter.opts = {logEnabled: false};
            exporter.handleDataResult([], 10);
        });
    });

    describe('#storeHits()', function () {
        it("should not do anything as long the mapping isn't ready", function () {
            exporter.targetDriver.failMethod = function() {
                throw new Error("Method should not have been called");
            };

            exporter.mappingReady = false;
            exporter.hitQueue = [];
            exporter.storeHits([{},{},{}]);
            expect(exporter.hitQueue).to.be.deep.equal([{},{},{}]);
        });

        it("should not do anything if there is no data being passed in", function () {
            exporter.targetDriver.failMethod = function () {
                throw new Error("Method should not have been called");
            };

            exporter.mappingReady = true;
            exporter.hitQueue = [];
            exporter.storeHits([]);
        });

        it("should call targetDriver#storeData() when data is being passed in", function (done) {
            var input = require('./data/mem.data.json');
            var output = fs.readFileSync(__dirname + '/data/put.data.njson', { encoding: 'UTF-8'});
            gently.expect(exporter.targetDriver, 'storeData', function(opts, data, callback){
                expect(opts).to.be.deep.equal({logEnabled: false});
                expect(data).to.be.equal(output);
                expect(callback).to.be.a('function');
                done();
            });

            exporter.opts = {logEnabled: false};
            exporter.mappingReady = true;
            exporter.hitQueue = [];
            exporter.storeHits(input);
        });

        it("should call targetDriver#end() when all data has been processed", function (done) {
            gently.expect(exporter.targetDriver, 'storeData', function (opts, data, callback) {
                callback();
            });
            gently.expect(exporter.targetDriver, 'end', function() {
                done();
            });
            exporter.opts = {logEnabled: false};
            exporter.mappingReady = true;
            exporter.hitQueue = [{}];
            exporter.processedHits = 1;
            exporter.totalHits = 5;
            exporter.storeHits([{},{},{}]);
        });
    });
});