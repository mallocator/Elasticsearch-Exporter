/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var gently = new (require('gently'))();
var mockDriver = require('./driver.mock.js');
var worker = require('../worker.js');
var drivers = require('../drivers.js');
var fs = require('fs');

describe("worker", () => {
    describe('#getMemoryStats()', () => {
        it("should have a memory ratio between 0 and 1", done => {
            let ratio = worker.getMemoryStats();
            expect(ratio).to.be.within(0, 1);
            done();
        });

        it("should cache memory requests for a time", done => {
            let ratio1 = worker.getMemoryStats();
            let ratio2 = worker.getMemoryStats();
            expect(ratio1).to.be.equal(ratio2);
            setTimeout(() => {
                let ratio3 = worker.getMemoryStats();
                expect(ratio1).not.to.be.equal(ratio3);
                done();
            }, 1000);
        });
    });

    describe("#waitOnTargetDriver()", () => {
        afterEach(() => gently.verify());

        it("should not be trying to do a gc and just keep going", done => {
            gently.expect(worker, 'getMemoryStats', () => 0.5);

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

        it("should try gc once and then continue", done => {
            gently.expect(worker, 'getMemoryStats', () => 0.9);
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

        it("should not do anything other than call the callback", done => {
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

    describe('#transformData', () => {
        afterEach(() => gently.verify());

        it("should send all hits to the transform function",done => {
            let myEnv = {
                options: {
                    log: {
                        count: false
                    },
                    errors: {
                        retry: 0,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock',
                        target: 'mock'
                    },
                    xform: {
                        file: 'dummyFile'
                    }
                }
            };

            let hits = [
                { _id: '1' , _index: 'mock', _type:'test', _source : { some: 'thing' } },
                { _id: '2' , _index: 'mock', _type:'test', _source : { other: 'thing' } }];

            let mock = mockDriver.getDriver();

            gently.expect(fs,'readFileSync',(filename, encoding) => {
                expect(filename).to.be.equal('dummyFile');
                // Return an extended object, with the original inside
                return 'function transform(obj) { return { "_original" : obj , "dummyKey" : "dummyValue" }; }';
            });

            gently.expect(drivers, 'get', 3, id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'putData', (env, driverHits, callback) => {
                let transformedHits = [
                    { _id: '1' , _index: 'mock', _type:'test', _source : { _original : { some : "thing" } , "dummyKey" : "dummyValue" } },
                    { _id: '2' , _index: 'mock', _type:'test', _source : { _original : { other : "thing" } , "dummyKey" : "dummyValue" } }];
                expect(driverHits).to.be.deep.equal(transformedHits);
                callback();
            });

            gently.expect(worker.send, 'done', numHits => {
                expect(numHits).to.be.equal(hits.length);
                done();
            });

            worker.initialize(0,myEnv);
            worker.storeData(hits);
        });

    });

    describe("#storeData()", () => {
        afterEach(() => gently.verify());

        it("should send all hits to the driver", done => {
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

            let hits = [{
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
            let mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'putData', (env, driverHits, callback) => {
                expect(driverHits).to.be.deep.equal(hits);
                callback();
            });

            gently.expect(worker.send, 'done', numHits => {
                expect(numHits).to.be.equal(hits.length);
                done();
            });

            worker.storeData(hits);
        });

        it("should retry a call when it returned an error the first time", done => {
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

            let hits = [{}, {}];
            let mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'putData', 2, (env, driverHits, callback) => {
                expect(driverHits).to.be.deep.equal(hits);
                callback("error");
            });

            gently.expect(worker.send, 'error', error => {
                expect(error).to.be.ok;
                done();
            });

            worker.storeData(hits);
        });
    });

    describe('#work()', () => {
        afterEach(() => gently.verify());

        it("should fetch data and then call store data once it received it", done => {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    },
                    run: {}
                }
            };

            let mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', (env, callback) => callback(null, [{},{},{},{},{}]));

            gently.expect(worker, 'storeData', hits => {
                expect(hits.length).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });

        it("should retry fetching data if an error has been reported", done => {
            worker.env = {
                options: {
                    errors: {
                        retry: 2,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    },
                    run: {}
                }
            };

            let mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', (env, callback) => callback("Connection not Ready"));

            gently.expect(mock, 'getData', (env, callback) => callback(null, [{}, {}, {}, {}, {}]));

            gently.expect(worker, 'storeData', hits => {
                expect(hits.length).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });

        it("should send an Exception to the master if too many errors have been thrown", done => {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: 0
                    },
                    drivers: {
                        source: 'mock'
                    },
                    run: {}
                }
            };

            let mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', (env, callback) => callback("Error"));

            gently.expect(worker.send, 'error', error => {
                expect(error).to.be.equal("Error");
                done();
            });

            worker.work(10, 5);
        });

        it("should should continue execution if ignoe errors is set, after too many errors have been thrown", done => {
            worker.env = {
                options: {
                    errors: {
                        retry: 1,
                        ignore: true
                    },
                    drivers: {
                        source: 'mock'
                    },
                    run: {}
                }
            };

            let mock = mockDriver.getDriver();
            gently.expect(drivers, 'get', id => {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'getData', (env, callback) => {
                callback("Error");
            });

            gently.expect(worker.send, 'done', processed => {
                expect(processed).to.be.equal(5);
                done();
            });

            worker.work(10, 5);
        });
    });
});
