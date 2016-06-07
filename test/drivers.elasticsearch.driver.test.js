/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var gently = new (require('gently'))();
var nock = require('nock');
var es = require('../drivers/elasticsearch.driver.js');
var log = require('../log.js');


log.capture = true;

describe("drivers/elasticsearch", () => {
    describe("#getInfo()", () => {
        it("should return two objects with both the info and the options of the driver", done => {
            es.getInfo((error, info, options) => {
                expect(error).to.be.not.ok;
                expect(info.id).to.exist;
                expect(info.name).to.exist;
                expect(info.version).to.exist;
                expect(info.description).to.exist;
                expect(options.source).to.exist;
                expect(options.target).to.exist;
                done();
            });
        });
    });

    describe("#verifyOptions()", () => {
        it("should set the target host with source and target being es, if a source host is set and no target host", done => {
            let opts = {
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
            es.verifyOptions(opts, err => {
                expect(err).to.not.exist;
                expect(opts.target.host).to.be.equal('host1');
                done();
            });
        });

        it("should set the target port, if a source port is set and no target port", done => {
            let opts = {
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
            es.verifyOptions(opts, err => {
                expect(err).to.not.exist;
                expect(opts.target.port).to.be.equal(9200);
                done();
            });
        });

        it("should set the target index, if a source index is set and no target index", done => {
            let opts = {
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
            es.verifyOptions(opts, err => {
                expect(err).to.not.exist;
                expect(opts.target.index).to.be.equal('index1');
                done();
            });
        });

        it("should set the target type, if a source type is set and no target type", done => {
            let opts = {
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
            es.verifyOptions(opts, () => {
                expect(opts.target.type).to.be.equal('type1');
                done();
            });
        });
    });

    describe("#reset()", () => {
        it("should reset the scroll id if the driver source id is elasticsearch", done => {
            es.scrollId = '1';
            let env = {
                options: {
                    drivers: {
                        source: 'elasticsearch'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, () => {
                expect(es.scrollId).to.be.null;
                done();
            });
        });

        it("should not reset the scroll id if the source driver id is not elasticsearch", done => {
            es.scrollId = '1';
            let env = {
                options: {
                    drivers: {
                        source: 'other'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, () => {
                expect(es.scrollId).to.be.equal('1');
                done();
            });
        });

        it("should not reset the scroll id if the target driver id is elasticsearch", done => {
            es.scrollId = '1';
            let env = {
                options: {
                    drivers: {
                        target: 'elasticsearch'
                    },
                    source: {},
                    target: {}
                }
            };
            es.reset(env, () => {
                expect(es.scrollId).to.be.equal('1');
                done();
            });
        });
    });

    describe("#getSourceStats()", () => {
        it("should return a proper stats object from 1.x source", done => {
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./data/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/').reply(200, require('./data/get.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./data/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_cluster/health').reply(200, require('./data/get.cluster.health.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./data/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_cluster/state').reply(200, require('./data/get.cluster.state.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./data/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_count').reply(200, require('./data/get.count.json'));
            let env = {
                options: {
                    drivers: {
                        source: 'elasticsearch'
                    },
                    source: {
                        host: 'host',
                        port: 9200
                    },
                    target: {}
                }
            };
            es.getSourceStats(env, (err, stats) => {
                expect(err).to.be.not.ok;
                expect(stats).to.be.a('object');
                expect(stats).to.be.deep.equal(require('./data/mem.statistics.json'));
                done();
            });
        });
    });

    describe("#getTargetStats()", () => {

    });

    describe("#getMeta()", () => {

    });

    describe("#putMeta()", () => {

    });

    describe("#getData()", () => {

    });

    describe("#putData()", () => {

    });
});
