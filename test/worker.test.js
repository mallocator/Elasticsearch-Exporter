var expect = require('chai').expect;
var gently = new (require('gently'))();
var mockDriver = require('./driver.mock.js');
var worker = require('../worker.js');
var drivers = require('../drivers.js');

describe("worker", function () {
    describe('#getMemoryStats()', function () {
        it("should have a memory ratio between 0 and 1", function (done) {
            var ratio = worker.getMemoryStats();
            expect(ratio).to.be.within(0, 1);
            done();
        });

        it("should cache memory requests for a time", function (done) {
            var ratio1 = worker.getMemoryStats();
            var ratio2 = worker.getMemoryStats();
            expect(ratio1).to.be.equal(ratio2);
            setTimeout(function () {
                var ratio3 = worker.getMemoryStats();
                expect(ratio1).not.to.be.equal(ratio3);
                done();
            }, 1000);
        });
    });

    describe("#waitOnTargetDriver()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should not be trying to do a gc and just keep going", function (done) {
            gently.expect(worker, 'getMemoryStats', function () {
                return 0.5;
            });

            global.gc = true;
            worker.env = {
                options: {
                    memory: {
                        limit: 0.8
                    }
                }
            };
            worker.state = 'ready';
            worker.waitOnTargetDriver(done);
        });

        it("should try gc once and then continue", function (done) {
            gently.expect(worker, 'getMemoryStats', function () {
                return 0.9;
            });
            gently.expect(global, 'gc');
            worker.env = {
                options: {
                    memory: {
                        limit: 0.8
                    }
                }
            };

            worker.state = 'ready';
            worker.waitOnTargetDriver(done);
        });

        it("should not do anything other than call the callback", function (done) {
            global.gc = false;
            worker.env = {
                options: {
                    memory: {
                        limit: 0.9
                    }
                }
            };
            worker.state = 'ready';
            worker.waitOnTargetDriver(done);
        });
    });

    describe("#storeData()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should send all hits to the driver", function (done) {
            worker.env = {
                options: {
                    log: {
                        count: false
                    },
                    errors: {
                        retry: 0,
                        ignore: 0
                    },
                    drivers: {
                        target: 'mock'
                    }
                }
            };

            var hits = [{
                _id: '1',
                _index: 'mock',
                _type: 'test',
                _source: {
                    some: 'thing'
                }
            }, {
                _id: '2',
                _index: 'mock',
                _type: 'test',
                _source: {
                    other: 'thing'
                }
            }];
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'putData', function (env, driverHits, callback) {
                expect(driverHits).to.be.deep.equal(hits);
                callback();
            });

            gently.expect(worker.send, 'done', function(numHits){
                expect(numHits).to.be.equal(hits.length);
                done();
            });

            worker.storeData(hits);
        });

        it("should retry a call when it returned an error the first time", function (done) {
            worker.env = {
                options: {
                    log: {
                        count: false
                    },
                    errors: {
                        retry: 2,
                        ignore: 0
                    },
                    drivers: {
                        target: 'mock'
                    }
                }
            };

            var hits = [{}, {}];
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'putData', 2, function (env, driverHits, callback) {
                expect(driverHits).to.be.deep.equal(hits);
                callback("error");
            });

            gently.expect(worker.send, 'error', function (error) {
                expect(error).to.be.ok();
                done();
            });

            worker.storeData(hits);
        });
    });

    describe('#work()', function () {
        afterEach(function () {
            gently.verify();
        });

        it("should fetch data and then call store data once it received it", function (done) {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    }
                }
            };

            var mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', function(env, callback) {
                callback(null, [{},{},{},{},{}]);
            });

            gently.expect(worker, 'storeData', function(hits) {
                expect(hits.length).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });

        it("should retry fetching data if an error has been reported", function (done) {
            worker.env = {
                options: {
                    errors: {
                        retry: 2,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    }
                }
            };

            var mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', function (env, callback) {
                callback("Connection not Ready");
            });

            gently.expect(mock, 'getData', function (env, callback) {
                callback(null, [{}, {}, {}, {}, {}]);
            });

            gently.expect(worker, 'storeData', function (hits) {
                expect(hits.length).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });

        it("should send an Exception to the master if too many errors have been thrown", function (done) {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    }
                }
            };

            var mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', function (env, callback) {
                callback("Error");
            });

            gently.expect(worker.send, 'error', function (error) {
                expect(error).to.be.equal("Error");
                done();
            });

            worker.work(10, 5);
        });

        it("should should continue execution if ignoe errors is set, after too many errors have been thrown", function (done) {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: true
                    },
                    drivers: {
                        source: 'mock'
                    }
                }
            };

            var mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', function (env, callback) {
                callback("Error");
            });

            gently.expect(worker.send, 'done', function (processed) {
                expect(processed).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });
    });
});