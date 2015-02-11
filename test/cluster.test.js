var expect = require('chai').expect;
var gently = new (require('gently'))();
var cluster = require('../cluster.js');
var drivers = require('../drivers.js');
var mockDriver = require('./driver.mock.js');

describe("cluster", function() {
    describe("#run()", function() {
        afterEach(function () {
            gently.verify();
        });

        it("should load the non-cluster implementation if only 1 worker is specified", function() {
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', 2, function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            var instance = cluster.run({
                statistics: {
                    docs: {}
                },
                options: {
                    drivers: {
                        source: 'mock',
                        target: 'mock'
                    }
                }
            }, 1);

            expect(instance.work).to.be.a('function');
            expect(instance.onEnd).to.be.a('function');
            expect(instance.workListeners).to.be.an('Array');
            expect(instance.processed).to.be.equal(0);
        });

        it("should load the cluster implementation if more than 1 worker is specified", function() {
            var instance = cluster.run({statistics: {docs: {}}}, 2);
            expect(instance.work).to.be.a('function');
            expect(instance.onEnd).to.be.a('function');
            expect(instance.workListeners).to.be.an('Array');
            expect(instance.total).to.be.undefined();
        });
    });

    describe("new Cluster()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should send an initialization request to the worker and then do some work", function(done) {
            cluster.workerPath = './test/worker.mock.js';

            var instance = cluster.run({
                statistics: {
                    memory: {
                        heapUsed: 0,
                        ratio: 0
                    },
                    docs: {
                        total: 10
                    }
                }
            }, 2);
            instance.onEnd(function() {
                expect(instance.workers['0'].state).to.be.equal('ready');
                expect(instance.workers['1'].state).to.be.equal('ready');
                done();
            });

            expect(instance.workers['0'].state).to.be.equal('ready');
            expect(instance.workers['1'].state).to.be.equal('ready');

            instance.work(10, 5, function () {});
            expect(instance.workers['0'].state).to.be.equal('working');

            instance.work(15, 5, function () {});
            expect(instance.workers['1'].state).to.be.equal('working');

            instance.workers['0'].process.on('message', function(m) {
                if (m.type == 'initializationTest') {
                    expect(m).to.be.deep.equal({
                        id: '0',
                        type: 'initializationTest',
                        messages: [
                            {
                                type: 'Initialize',
                                id: '0',
                                env: {
                                    statistics: {
                                        memory: {
                                            heapUsed: 0,
                                            ratio: 0
                                        },
                                        docs: {
                                            total: 10
                                        }
                                    }
                                }
                            }, {
                                "type": "Work",
                                "from": 10,
                                "id": "0",
                                "size": 5
                            }
                        ]
                    });
                }
            });

            instance.workers['0'].process.send({
                id: 0,
                type: 'getMessages',
                responseType: 'initializationTest'
            });

            instance.workers['0'].process.send({
                id: 0,
                type: 'sendDone',
                processed: 5,
                memUsage: {
                    heapUsed: 100,
                    ratio: 0.5
                }
            });

            instance.workers['1'].process.send({
                id: 1,
                type: 'sendDone',
                processed: 5,
                memUsage: {
                    heapUsed: 100,
                    ratio: 0.5
                }
            });
        });
    });

    describe("new NoCluster()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should load the NoCluster implementation if only 1 worker has been specified", function(done) {
            cluster.workerPath = './test/worker.mock.js';

            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', 2, function (id) {
                expect(id).to.be.equal('mock');
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            var instance = cluster.run({
                statistics: {
                    memory: {
                        heapUsed: 0,
                        ratio: 0
                    },
                    docs: {
                        total: 10
                    }
                },
                options: {
                    drivers: {
                        source: 'mock',
                        target: 'mock'
                    }
                }
            }, 1);

            instance.onEnd(function () {
                done();
            });

            instance.work(10,5, function() {
                instance.work(15, 5, function () {});
            });
        });
    });
});