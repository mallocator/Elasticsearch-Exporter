/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var gently = new (require('gently'))();
var mockDriver = require('./driver.mock.js');
var mockCluster = require('./cluster.mock.js');
var exporter = require('../exporter.js');
var options = require('../options.js');
var drivers = require('../drivers.js');
var cluster = require('../cluster.js');
var log = require('../log.js');

log.capture = true;

describe("exporter", () => {
    describe("#handleUncaughtExceptions()", () => {
        beforeEach(log.pollCapturedLogs);

        it("should print the exception if one is passed in", () => {
            try {
                exporter.handleUncaughtExceptions(new Error("Test Error"));
            } catch (e) {}
            let logs = log.pollCapturedLogs();
            expect(logs[0]).to.contain('Test Error');
        });

        it("should print the message if one is passed in", () => {
            try {
                exporter.handleUncaughtExceptions("Test Message");
            } catch (e) {}
            let logs1 = log.pollCapturedLogs();
            expect(logs1[0]).to.contain('Test Message');
        });

        it("should print a generic message if nothing is passed in", () => {
            try {
                exporter.handleUncaughtExceptions("Test Message");
            } catch (e) {}
            let logs1 = log.pollCapturedLogs();
            expect(logs1[0]).to.not.be.empty;
        });
    });

    function setUpMockDriver(calls, notThreadsafe) {
        if (!calls) {
            calls = 1;
        }
        exporter.env = {
            options: {
                drivers: {
                    target: 'mock',
                    source: 'mock'
                },
                errors: {
                    retry: 1
                },
                run: {
                    concurrency: 1,
                    step: 5,
                    mapping: true,
                    data: true
                }
            },
            statistics: {
                source: {
                    docs: {
                        total: 20
                    }
                },
                hits: {},
                target: {}
            }
        };
        let mock = mockDriver.getDriver();
        gently.expect(drivers, 'get', calls, id => {
            expect(id).to.be.equal('mock');
            return {
                info: mock.getInfoSync(!notThreadsafe),
                options: mock.getOptionsSync(),
                driver: mock
            };
        });
        return mock;
    }

    describe("#readOptions()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the callback when an option tree has been returned", done => {
            gently.expect(options, 'read', callback => {
                callback({
                    option: 'test'
                });
            });
            exporter.readOptions((err, options) => {
                expect(err).to.be.null;
                expect(options).to.be.deep.equal({
                    option: 'test'
                });
                done();
            });
        });

        it("should throw an error if nothing is returned", done => {
            gently.expect(options, 'read', callback => {
                callback();
            });
            exporter.readOptions(err => {
                expect(err).to.not.be.null;
                done();
            });
        });
    });

    describe("#verifyOptions()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the callback when a verification has completed successfully", done => {
            gently.expect(options, 'verify', (options, callback) => {
                expect(options).to.be.deep.equal({
                    options: 'test'
                });
                callback();
            });
            exporter.verifyOptions({
                readOptions: {
                    options: 'test'
                }
            }, err => {
                expect(err).to.not.be.ok;
                done();
            });
        });

        it("should throw an error if the verification has not worked as expected", done => {
            gently.expect(options, 'verify', (options, callback) => {
                expect(options).to.be.deep.equal({
                    options: 'test'
                });
                callback(['There has been an error']);
            });
            exporter.verifyOptions({
                readOptions: {
                    options: 'test'
                }
            }, err => {
                expect(err).to.be.ok;
                done();
            });
        });
    });

    describe("#resetSource()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the reset function of the source driver", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'reset', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env);
                callback();
            });
            exporter.resetSource(null, err => {
                expect(err).to.not.be.ok;
                done();
            });
        });
    });

    describe("#resetTarget()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the reset function of the target driver", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'reset', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env);
                callback();
            });
            exporter.resetTarget(null, () => {
                done();
            });
        });
    });

    describe("#getSourceStatistics()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the getSourceStats function of the source driver", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getSourceStats', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env, callback);
                callback(null, {
                    sourceStat: 0
                });
            });
            exporter.getSourceStatistics(null, err => {
                expect(err).to.not.be.ok;
                expect(exporter.env.statistics.source).to.be.deep.equal({
                    docs: {
                        total: 20
                    },
                    sourceStat: 0
                });
                done();
            });
        });

        it("should continue without errors if the source driver returns nothing", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getSourceStats', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env, callback);
                callback();
            });
            exporter.getSourceStatistics(null, err => {
                expect(err).to.not.be.ok;
                expect(exporter.env.statistics.source).to.be.deep.equal({
                    docs: {
                        total: 20
                    }
                });
                done();
            });
        });
    });

    describe("#getTargetStatistics()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the getSourceStats function of the source driver", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getTargetStats', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env, callback);
                callback(null, {
                    targetStat: 0
                });
            });
            exporter.getTargetStatistics(null, err => {
                expect(err).to.not.be.ok;
                expect(exporter.env.statistics.target).to.be.deep.equal({
                    targetStat: 0
                });
                done();
            });
        });

        it("should continue without errors if the source driver returns nothing", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getTargetStats', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env, callback);
                callback();
            });
            exporter.getTargetStatistics(null, err => {
                expect(err).to.not.be.ok;
                expect(exporter.env.statistics.target).to.be.deep.equal({});
                done();
            });
        });
    });

    describe("#checkSourceHealth()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should check if the source is connected and docs are available", done => {
            exporter.env = {
                statistics: {
                    source: {
                        status: 'green',
                        docs: {
                            total: 1
                        }
                    }
                }
            };
            exporter.checkSourceHealth(null, err => {
                expect(err).to.be.not.ok;
               done();
            });
        });

        it("should throw an error if no docs can be exported", done => {
            exporter.env = {
                statistics: {
                    source: {
                        status: 'green',
                        docs: {
                            total: 0
                        }
                    }
                }
            };
            exporter.checkSourceHealth(null, err => {
                expect(err.length).to.be.at.least(1);
                done();
            });
        });

        it("should throw an error if source is not ready", done => {
            exporter.env = {
                statistics: {
                    source: {
                        status: 'red'
                    }
                }
            };
            exporter.checkSourceHealth(null, err => {
                expect(err.length).to.be.at.least(1);
                done();
            });
        });
    });

    describe("#checkTargetHealth()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should check if the source is ready", done => {
            exporter.env = {
                statistics: {
                    target: {
                        status: 'green'
                    }
                }
            };
            exporter.checkTargetHealth(null, err => {
                expect(err).to.be.not.ok;
                done();
            });
        });

        it("should throw an error if target is not ready", done => {
            exporter.env = {
                statistics: {
                    target: {
                        status: 'red'
                    }
                }
            };
            exporter.checkTargetHealth(null, err => {
                expect(err.length).to.be.at.least(1);
                done();
            });
        });
    });

    describe("#getMetadata()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should call the source driver getMeta function", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getMeta', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env);
                callback(null, {
                    source: 'metadata'
                });
            });
            exporter.getMetadata(null, (err, metadata) => {
                expect(err).to.not.be.ok;
                expect(metadata).to.be.deep.equal({
                    source: 'metadata'
                });
                done();
            });
        });

        it("should use the mapping from the options instead of calling the source driver", done => {
            exporter.env = {
                options: {
                    errors: {
                        retry: 0
                    },
                    mapping: {
                        test: 'mapping'
                    },
                    run: {
                        mapping: true
                    }
                }
            };

            exporter.getMetadata(null, (err, metadata) => {
                expect(err).to.be.not.ok;
                expect(metadata).to.be.deep.equal({
                    test: 'mapping'
                });
                done();
            });
        });

        it("should pass on an error if the source returns an error", done => {
            let mock = setUpMockDriver();
            gently.expect(mock, 'getMeta', (env, callback) => {
                expect(env).to.be.deep.equal(exporter.env);
                callback("Error");
            });
            exporter.getMetadata(null, err => {
                expect(err).to.be.equal("Error");
                done();
            });
        });
    });

    describe("#storeMetadata()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should not do anything when testRun is active", done => {
            exporter.env = {
                options: {
                    errors: {
                        retry: 0
                    },
                    run: {
                        test: true
                    }
                }
            };

            exporter.storeMetadata({
                getMetadata: {}
            }, err => {
                expect(err).to.not.exist;
                done();
            });
        });

        it("should call putMeta on the target driver", done => {
            let metadata = {
                _mapping: {},
                _settings: {}
            };

            let mock = setUpMockDriver();
            gently.expect(mock, 'putMeta', (env, md, callback) => {
                expect(env).to.be.deep.equal(exporter.env);
                expect(md).to.be.deep.equal(metadata);
                callback();
            });

            exporter.storeMetadata({
                getMetadata: metadata
            }, err => {
                expect(err).to.not.exist;
                done();
            });
        });
    });

    // TODO figure out why this breaks on travis
    describe.skip("#transferData()", () => {
        afterEach(() => {
            gently.verify();
        });

        it("should keep telling the cluster to run until all files have been processed", done => {
            setUpMockDriver(2);

            let testCluster = mockCluster.getInstance();

            gently.expect(cluster, 'run', (env, concurrency) => {
                expect(env).to.be.deep.equal(exporter.env);
                expect(concurrency).to.be.equal(exporter.env.options.run.concurrency);
                return testCluster;
            });

            exporter.transferData(null, err => {
                expect(err).to.not.exist;
                expect(testCluster.getPointer()).to.be.equal(15);
                expect(testCluster.getSteps()).to.be.equal(20);
                done();
            });

            testCluster.sendWorking();
            testCluster.sendWorkDone(5);
            testCluster.sendWorking();
            testCluster.sendWorkDone(5);
            testCluster.sendWorking();
            testCluster.sendWorkDone(5);
            testCluster.sendWorking();
            testCluster.sendWorkDone(5);
            testCluster.sendEnd();
        });

        it("should stop exporting if the cluster reported an error", done => {
            setUpMockDriver(2);

            let testCluster = mockCluster.getInstance();

            gently.expect(cluster, 'run', (env, concurrency) => {
                expect(env).to.be.deep.equal(exporter.env);
                expect(concurrency).to.be.equal(exporter.env.options.run.concurrency);
                return testCluster;
            });

            exporter.transferData(null, err => {
                expect(err).to.be.equal("Error");
                expect(testCluster.getPointer()).to.be.equal(5);
                expect(testCluster.getSteps()).to.be.equal(10);
                done();
            });

            testCluster.sendWorking();
            testCluster.sendError("Error");
        });

        it("should set concurrency to 1 of one of the drivers does not support it", done => {
            setUpMockDriver(2, true);
            exporter.env.options.run.concurrency = 4;

            let testCluster = mockCluster.getInstance();

            gently.expect(cluster, 'run', (env, concurrency) => {
                expect(concurrency).to.be.equal(1);
                return testCluster;
            });

            exporter.transferData(null, err => {
                expect(err).to.not.exist;
                done();
            });

            testCluster.sendEnd();
        });

        it("should set concurrency to the option value of both support it", done => {
            setUpMockDriver(2);
            exporter.env.options.run.concurrency = 4;

            let testCluster = mockCluster.getInstance();

            gently.expect(cluster, 'run', (env, concurrency) => {
                expect(concurrency).to.be.equal(exporter.env.options.run.concurrency);
                return testCluster;
            });

            exporter.transferData(null, err => {
                expect(err).to.not.exist;
                done();
            });

            testCluster.sendEnd();
        });
    });
});
