/* global describe, it, beforeEach, afterEach */
const fs = require('fs');
const path = require('path');

const expect = require('chai').expect;
const gently = new (require('gently'))();
const nock = require('nock');

const es = require('../../drivers/elasticsearch-client.driver.js');
const log = require('../../log.js');
const SemVer = require('../../semver');


log.capture = true;

describe("drivers/elasticsearch-client", () => {
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
                    source: 'elasticsearch-client',
                    target: 'elasticsearch-client'
                },
                source: {
                    host: 'http://host1:9200',
                    index: 'index1'
                },
                target: {
                    index: 'index2'
                }
            };
            es.verifyOptions(opts, err => {
                expect(err).to.not.exist;
                expect(opts.target.host).to.be.equal('http://host1:9200');
                done();
            });
        });

        it("should set the target index, if a source index is set and no target index", done => {
            let opts = {
                drivers: {
                    source: 'elasticsearch-client',
                    target: 'elasticsearch-client'
                },
                source: {
                    index: 'index1'
                },
                target: {
                    host: 'http://host2:9200'
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
                    source: 'elasticsearch-client',
                    target: 'elasticsearch-client'
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
                        source: 'elasticsearch-client'
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
                        target: 'elasticsearch-client'
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
            let host = 'http://host:9200';
            let scope = nock(host).get('/').reply(200, {"name":"Test","cluster_name":"test","cluster_uuid":"123","version":{"number":"5.2.2"}})
                .get('/_cluster/health').reply(200, require('./../data/es/get.cluster.health.json'))
                .post('/_count').reply(200, require('./../data/es/get.count.json'))
                .get('/_cat/aliases').query({"format":"json"}).reply(200, [{"alias": "alias1","index": "test","filter": "-","routing.index": "-","routing.search": "-"}]);
            let env = {
                options: {
                    drivers: { source: 'elasticsearch-client' },
                    source: { host },
                    target: {}
                }
            };
            es.reset(env, () => {
                es.getSourceStats(env, (err, stats) => {
                    expect(err).to.be.not.ok;
                    expect(stats).to.be.a('object');
                    expect(stats).to.be.deep.equal({
                        aliases: { alias1: 'test' },
                        docs: { total: 10 },
                        indices: [ 'test' ],
                        status: 'green',
                        version: { major: 5, minor: 2, original: '5.2.2', patch: 2 }
                    });
                    expect(scope.isDone()).to.be.true;
                    done();
                });
            });
        });
    });

    describe("#getTargetStats()", () => {
        it("should return a proper stats object from 1.x source", done => {
            let host = 'http://host:9200';
            let scope = nock(host).get('/').reply(200, {"name":"Test","cluster_name":"test","cluster_uuid":"123","version":{"number":"5.2.2"}})
                .get('/_cluster/health').reply(200, require('./../data/es/get.cluster.health.json'))
                .get('/_cat/aliases').query({"format":"json"}).reply(200, [{"alias": "alias1","index": "test","filter": "-","routing.index": "-","routing.search": "-"}]);
            let env = {
                options: {
                    drivers: { target: 'elasticsearch-client' },
                    source: {},
                    target: { host }
                }
            };
            es.reset(env, () => {
                es.getTargetStats(env, (err, stats) => {
                    expect(err).to.be.not.ok;
                    expect(stats).to.be.a('object');
                    expect(stats).to.be.deep.equal({
                        aliases: {alias1: 'test'},
                        indices: ['test'],
                        status: 'green',
                        version: {major: 5, minor: 2, original: '5.2.2', patch: 2}
                    });
                    expect(scope.isDone()).to.be.true;
                    done();
                });
            });
        });
    });

    describe("#getMeta()", () => {
        it('should retrieve the meta data and settings for an index and type', done => {
            let host = 'http://host:9200';
            let scope = nock(host)
                .get('/_mapping').reply(200, require('./../data/es/get.mapping.json'))
                .get('/_settings').reply(200, require('./../data/es/get.settings.json'));
            let env = {
                options: {
                    drivers: { source: 'elasticsearch-client' },
                    source: { host },
                    target: {}
                }
            };
            es.reset(env, () => {
                es.getMeta(env, (err, metadata) => {
                    expect(err).to.be.not.ok;
                    expect(metadata).to.be.a('object');
                    expect(metadata).to.be.deep.equal(require('./../data/es/mem.metadata.json'));
                    expect(scope.isDone()).to.be.true;
                    done();
                });
            });
        });
    });

    describe("#putMeta()", () => {
        it('should store meta data and settings for an index and type', done => {
            let host = 'http://host:9200';
            let scope = nock(host)
                .put('/index1', require('./../data/es/put.index.json')).reply(200)
                .put('/index1/_mapping/type1', require('./../data/es/put.index.type.json')).reply(200);
            let env = {
                statistics: {
                    target: {
                        version: new SemVer('1.0'),
                        indices: []
                    }
                },
                options: {
                    drivers: { target: 'elasticsearch-client' },
                    target: { host, index: 'index1', type: 'type1' }
                }
            };
            es.reset(env, () => {
                es.putMeta(env, require('./../data/es/mem.metadata.json'), err => {
                    expect(err).to.be.not.ok;
                    expect(scope.isDone()).to.be.true;
                    done();
                });
            });
        });
    });

    describe("#getData()", () => {
        it('should query the server for data', done => {
            let docs1 = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            let docs2 = [
                { id: 4 }, { id: 5 }, { id: 6 }
            ];

            let host = 'http://localhost:9200';
            nock(host)
                .post('/_search').query({"scroll":"5m",sort:'_doc'}).reply(200, {"_scroll_id":"1","hits":{"total":3,"hits":docs1}})
                .post('/_search/scroll', "1").query({"scroll":"5m"}).reply(200, {"_scroll_id":"2","hits":{"total":3,"hits":docs2}});

            let env = {
                statistics: { source: { version: new SemVer('1.0') } },
                options: {
                    drivers: { source: 'elasticsearch-client' },
                    source: { host }
                }
            };
            let secondCall = false;
            es.reset(env, () => {
                es.getData(env, (err, data) => {
                    expect(err).to.be.not.ok;
                    if (!secondCall) {
                        expect(data).to.deep.equal(docs1);
                        expect(es.scrollId).to.equal('1');
                        secondCall = true;
                    } else {
                        expect(data).to.deep.equal(docs2);
                        expect(es.scrollId).to.equal('2');
                        expect(nock.isDone()).to.be.true;
                        delete es.scrollId;
                        done();
                    }
                });
            });
        });

        it('should query the server for just an index', done => {
            let docs1 = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            let docs2 = [
                { id: 4 }, { id: 5 }, { id: 6 }
            ];

            let host = 'http://localhost:9200';
            nock(host)
                .post('/index1/_search').query({"scroll":"5m",sort:'_doc'}).reply(200, {"_scroll_id":"1","hits":{"total":3,"hits":docs1}})
                .post('/_search/scroll', "1").query({"scroll":"5m"}).reply(200, {"_scroll_id":"2","hits":{"total":3,"hits":docs2}});

            let env = {
                statistics: { source: { version: new SemVer('1.0') } },
                options: {
                    drivers: { source: 'elasticsearch-client' },
                    source: { host, index: 'index1' }
                }
            };
            let secondCall = false;
            es.reset(env, () => {
                es.getData(env, (err, data) => {
                    expect(err).to.be.not.ok;
                    if (!secondCall) {
                        expect(data).to.deep.equal(docs1);
                        expect(es.scrollId).to.equal('1');
                        secondCall = true;
                    } else {
                        expect(data).to.deep.equal(docs2);
                        expect(es.scrollId).to.equal('2');
                        expect(nock.isDone()).to.be.true;
                        delete es.scrollId;
                        done();
                    }
                });
            });
        });

        it('should query the server for just a type', done => {
            let docs1 = [
                { id: 1 }, { id: 2 }, { id: 3 }
            ];
            let docs2 = [
                { id: 4 }, { id: 5 }, { id: 6 }
            ];

            let host = 'http://localhost:9200';
            nock(host)
            .post('/index1/type1/_search').query({"scroll":"5m",sort:'_doc'}).reply(200, {"_scroll_id":"1","hits":{"total":3,"hits":docs1}})
            .post('/_search/scroll', "1").query({"scroll":"5m"}).reply(200, {"_scroll_id":"2","hits":{"total":3,"hits":docs2}});

            let env = {
                statistics: { source: { version: new SemVer('1.0') } },
                options: {
                    drivers: { source: 'elasticsearch-client' },
                    source: { host, index: 'index1', type: 'type1' }
                }
            };
            let secondCall = false;
            es.reset(env, () => {
                es.getData(env, (err, data) => {
                    expect(err).to.be.not.ok;
                    if (!secondCall) {
                        expect(data).to.deep.equal(docs1);
                        expect(es.scrollId).to.equal('1');
                        secondCall = true;
                    } else {
                        expect(data).to.deep.equal(docs2);
                        expect(es.scrollId).to.equal('2');
                        expect(nock.isDone()).to.be.true;
                        delete es.scrollId;
                        done();
                    }
                });
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
            let docs = fs.readFileSync(path.join(__dirname, '../data/es/post.bulk.text'), 'utf8');
            nock('http://host:9200').post('/_bulk', docs).reply(200, {});
            es.putData(env, require('./../data/es/mem.data.json'), err => {
                expect(err).to.be.not.ok;
                expect(nock.isDone()).to.be.true;
                done();
            });
        });
    });
});
