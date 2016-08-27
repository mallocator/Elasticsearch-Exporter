/* global describe, it, beforeEach, afterEach */
'use strict';

var fs = require('fs');
var path = require('path');

var expect = require('chai').expect;
var gently = new (require('gently'))();
var nock = require('nock');

var es = require('../../drivers/elasticsearch.driver.js');
var log = require('../../log.js');
var SemVer = require('../../semver');


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
                delete es.scrollId;
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
                delete es.scrollId;
                done();
            });
        });
    });

    describe("#getSourceStats()", () => {
        it("should return a proper stats object from 1.x source", done => {
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/').reply(200, require('./../data/es/get.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_cluster/health').reply(200, require('./../data/es/get.cluster.health.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_alias').reply(200, require('./../data/es/get.alias.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_count').reply(200, require('./../data/es/get.count.json'));
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
                expect(stats).to.be.deep.equal(require('./../data/es/mem.statistics.json'));
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });

    describe("#getTargetStats()", () => {
        it("should return a proper stats object from 1.x source", done => {
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/').reply(200, require('./../data/es/get.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_cluster/health').reply(200, require('./../data/es/get.cluster.health.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_alias').reply(200, require('./../data/es/get.alias.json'));
            let env = {
                options: {
                    drivers: {
                        target: 'elasticsearch'
                    },
                    source: {},
                    target: {
                        host: 'host',
                        port: 9200
                    }
                }
            };
            es.getTargetStats(env, (err, stats) => {
                expect(err).to.be.not.ok;
                expect(stats).to.be.a('object');
                var sourceStats = Object.assign({}, require('./../data/es/mem.statistics.json'));
                delete sourceStats.docs;
                expect(stats).to.be.deep.equal(sourceStats);
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });

    describe("#getMeta()", () => {
        it('should retrieve the meta data and settings for an index and type', done => {
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_mapping').reply(200, require('./../data/es/get.mapping.json'));
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').get('/_settings').reply(200, require('./../data/es/get.settings.json'));
            let env = {
                options: {
                    drivers: {
                        target: 'elasticsearch'
                    },
                    source: {
                        host: 'host',
                        port: 9200
                    },
                    target: {}
                }
            };
            es.getMeta(env, (err, metadata) => {
                expect(err).to.be.not.ok;
                expect(metadata).to.be.a('object');
                expect(metadata).to.be.deep.equal(require('./../data/es/mem.metadata.json'));
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });

    describe("#putMeta()", () => {
        it('should store meta data and settings for an index and type', done => {
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').put('/index1', require('./../data/es/put.index.json')).reply(200);
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').put('/index1/_mapping/type1', require('./../data/es/put.index.type.json')).reply(200);
            let env = {
                statistics: {
                    target: {
                        version: new SemVer('1.0'),
                        indices: []
                    }
                },
                options: {
                    drivers: {
                        target: 'elasticsearch'
                    },
                    target: {
                        host: 'host',
                        port: 9200
                    }
                }
            };
            es.putMeta(env, require('./../data/es/mem.metadata.json'), err => {
                expect(err).to.be.not.ok;
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });

    describe("#getData()", () => {
        it('should query the server for data', done => {
            var docs = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search?search_type=scan&scroll=60m', require('./../data/es/post.query.json')).reply(200, { _scroll_id: '1' });
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search/scroll?scroll=60m', '1').reply(200, { _scroll_id: '2', hits: {hits: docs}});
            let env = {
                options: {
                    source: {
                        host: 'host',
                        port: 9200,
                        size: 100
                    }
                }
            };
            es.getData(env, (err, data) => {
                expect(err).to.be.not.ok;
                expect(data).to.deep.equal(docs);
                expect(es.scrollId).to.equal('2');
                expect(nock.isDone()).to.be.true;
                delete es.scrollId;
                done();
            });
        });

        it('should query the server for just an index', done => {
            var docs = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search?search_type=scan&scroll=60m', require('./../data/es/post.query.index.json')).reply(200, { _scroll_id: '1' });
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search/scroll?scroll=60m', '1').reply(200, { _scroll_id: '2', hits: {hits: docs}});
            let env = {
                options: {
                    source: {
                        host: 'host',
                        port: 9200,
                        size: 100,
                        index: 'index1'
                    }
                }
            };
            es.getData(env, (err, data) => {
                expect(err).to.be.not.ok;
                expect(data).to.deep.equal(docs);
                expect(es.scrollId).to.equal('2');
                expect(nock.isDone()).to.be.true;
                delete es.scrollId;
                done();
            });
        });

        it('should query the server for just a type on ES < 2.0', done => {
            var docs = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search?search_type=scan&scroll=60m', require('./../data/es/post.query.index.type.json')).reply(200, { _scroll_id: '1' });
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search/scroll?scroll=60m', '1').reply(200, { _scroll_id: '2', hits: {hits: docs}});
            let env = {
                statistics: {
                    source: {
                        indices: [ 'index1' ],
                        version: new SemVer(1.3)
                    }
                },
                options: {
                    source: {
                        host: 'host',
                        port: 9200,
                        size: 100,
                        index: 'index1',
                        type: 'type1'
                    }
                }
            };
            es.getData(env, (err, data) => {
                expect(err).to.be.not.ok;
                expect(data).to.deep.equal(docs);
                expect(es.scrollId).to.equal('2');
                expect(nock.isDone()).to.be.true;
                delete es.scrollId;
                done();
            });
        });

        it('should query the server for just a type on ES >=2.0', done => {
            var docs = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search?search_type=scan&scroll=60m', require('./../data/es/post.query.index.type.2.0.json')).reply(200, { _scroll_id: '1' });
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_search/scroll?scroll=60m', '1').reply(200, { _scroll_id: '2', hits: {hits: docs}});
            let env = {
                statistics: {
                    source: {
                        indices: [ 'index1' ],
                        version: new SemVer(2.0)
                    }
                },
                options: {
                    source: {
                        host: 'host',
                        port: 9200,
                        size: 100,
                        index: 'index1',
                        type: 'type1'
                    }
                }
            };
            es.getData(env, (err, data) => {
                expect(err).to.be.not.ok;
                expect(data).to.deep.equal(docs);
                expect(es.scrollId).to.equal('2');
                expect(nock.isDone()).to.be.true;
                delete es.scrollId;
                done();
            });
        });
    });

    describe("#putData()", () => {
        it('should send a bulk request for all documents', done => {
            let env = {
                options: {
                    target: {
                        host: 'host',
                        port: 9200,
                        index: 'index1',
                        type: 'type1'
                    }
                }
            };
            var docs = fs.readFileSync(path.join(__dirname, '../data/es/post.bulk.text'), 'utf8');
            nock('http://host:9200').get('/_nodes/stats/process').reply(200, require('./../data/es/get.nodes.stats.process.json'));
            nock('http://host:9200').post('/_bulk', docs).reply(200, {});
            es.putData(env, require('./../data/es/mem.data.json'), err => {
                expect(err).to.be.not.ok;
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });
});
