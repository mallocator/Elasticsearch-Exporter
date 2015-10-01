var expect = require('chai').expect;
var gently = new (require('gently'))();
var nock = require('nock');
var es = require('../drivers/elasticsearch.driver.js');
var log = require('../log.js');

log.capture = true;

describe("drivers/elasticsearch", function() {
    describe("#getInfo()", function() {
        it("should return two objects with both the info and the options of the driver", function(done) {
            es.getInfo(function (error, info, options) {
                expect(error).to.be.not.ok;
                expect(info.id).to.exist;
                expect(info.name).to.exist;
                expect(info.version).to.exist;
                expect(info.desciption).to.exist;
                expect(options.source).to.exist;
                expect(options.target).to.exist;
                done();
            });
        });
    });

    describe("#verifyOptions()", function () {
        it("should set the target host with source and target being es, if a source host is set and no target host", function (done) {
            var opts = {
                drivers: {
                    source: 'elasticsearch',
                    target: 'elasticsearch'
                },
                source: {
                    host: 'host1',
                    index: 'index1'
                },
                target: {
                    index: 'index2'
                }
            };
            es.verifyOptions(opts, function(err) {
                expect(err).to.not.exist;
                expect(opts.target.host).to.be.equal('host1');
                done();
            });
        });

        it("should set the target port, if a source port is set and no target port", function (done) {
            var opts = {
                drivers: {
                    source: 'elasticsearch',
                    target: 'elasticsearch'
                },
                source: {
                    port: 9200
                },
                target: {
                    host: 'host2'
                }
            };
            es.verifyOptions(opts, function (err) {
                expect(err).to.not.exist;
                expect(opts.target.port).to.be.equal(9200);
                done();
            });
        });

        it("should set the target index, if a source index is set and no target index", function (done) {
            var opts = {
                drivers: {
                    source: 'elasticsearch',
                    target: 'elasticsearch'
                },
                source: {
                    index: 'index1'
                },
                target: {
                    host: 'host2'
                }
            };
            es.verifyOptions(opts, function (err) {
                expect(err).to.not.exist;
                expect(opts.target.index).to.be.equal('index1');
                done();
            });
        });

        it("should set the target type, if a source type is set and no target type", function (done) {
            var opts = {
                drivers: {
                    source: 'elasticsearch',
                    target: 'elasticsearch'
                },
                source: {
                    index: 'index1',
                    type: 'type1'
                },
                target: {
                    host: 'host2'
                }
            };
            es.verifyOptions(opts, function () {
                expect(opts.target.type).to.be.equal('type1');
                done();
            });
        });
    });

    describe("#reset()", function () {
        it("should reset the scroll id if the driver source id is elasticsearch", function(done) {
            es.scrollId = '1';
            var env = {
                options: {
                    drivers: {
                        source: 'elasticsearch'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, function() {
                expect(es.scrollId).to.be.null;
                done();
            });
        });

        it("should not reset the scroll id if the source driver id is not elasticsearch", function (done) {
            es.scrollId = '1';
            var env = {
                options: {
                    drivers: {
                        source: 'other'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, function () {
                expect(es.scrollId).to.be.equal('1');
                done();
            });
        });

        it("should not reset the scroll id if the target driver id is elasticsearch", function (done) {
            es.scrollId = '1';
            var env = {
                options: {
                    drivers: {
                        target: 'elasticsearch'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, function () {
                expect(es.scrollId).to.be.equal('1');
                done();
            });
        });
    });

    describe("#getSourceStats()", function () {
        it("should return a proper stats object from 0.x source", function (done) {
            nock('http://host:9200').get('/').reply(200, require('./data/get.elasticsearch.json'));
            nock('http://host:9200').get('/_cluster/health').reply(200, require('./data/get.cluster.health.json'));
            nock('http://host:9200').get('/_cluster/state').reply(200, require('./data/get.cluster.state.json'));
            nock('http://host:9200').get('/_count').reply(200, require('./data/get.count.json'));
            var env = {
                options: {
                    source: {
                        host: 'host',
                        port: 9200
                    }
                }
            };
            es.getSourceStats(env, function (err, stats) {
                expect(err).to.not.exist;
                expect(stats).to.be.a('object');
                expect(stats).to.be.deep.equal(require('./data/mem.stats.json'));
                done();
            });
        });

        it("should return a proper stats object from 1.x source", function (done) {
            nock('http://host:9200').get('/').reply(200, require('./data/get.elasticsearch.1x.json'));
            nock('http://host:9200').get('/_cluster/health').reply(200, require('./data/get.cluster.health.1x.json'));
            nock('http://host:9200').get('/_cluster/state').reply(200, require('./data/get.cluster.state.1x.json'));
            nock('http://host:9200').get('/_status').reply(200, require('./data/get.count.1x.json'));
            var opts = {
                sourceHost: 'host',
                sourcePort: 9200
            };
            es.getSourceStats(opts, function () {
                expect(opts.sourceStats).to.be.a('object');
                expect(opts.sourceStats).to.be.deep.equal(require('./data/mem.stats.json'));
                done();
            });
        });
    });

    describe("#getTargetStats()", function () {

    });

    describe("#getMeta()", function () {

    });

    describe("#putMeta()", function () {

    });

    describe("#getData()", function () {

    });

    describe("#putData()", function () {

    });
});