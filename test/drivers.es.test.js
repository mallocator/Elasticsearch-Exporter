var fs = require('fs');
var expect = require('chai').expect;
var nock = require('nock');
var es = require('../drivers/es.js');


nock.disableNetConnect();

describe('drivers.es', function () {

    describe('#getMeta()', function () {
        it("should return a valid type meta data description", function (done) {
            nock('http://host:9200').get('/index1/type1/_mapping').reply(200, require('./data/get.type.mapping.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1',
                sourceType: 'type1',
                logEnabled: false
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.type.json'));
                done();
            });
        });

        it("should return a valid index meta data description", function (done) {
            nock('http://host:9200').get('/index1/_mapping').reply(200, require('./data/get.index.mapping.json'));
            nock('http://host:9200').get('/index1/_settings').reply(200, require('./data/get.index.settings.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1',
                logEnabled: false
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.index.json'));
                done();
            });
        });

        it("should return a valid meta data description for all indices", function (done) {
            nock('http://host:9200').get('/_mapping').reply(200, require('./data/get.all.mapping.json'));
            nock('http://host:9200').get('/_settings').reply(200, require('./data/get.all.settings.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200,
                logEnabled: false
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.all.json'));
                done();
            });
        });
    });


    describe('#storeMeta()', function () {
        it("should create a valid type meta data request", function (done) {
            nock('http://host2:9200').put('/index2').reply(200);
            nock('http://host2:9200').put('/index2/type2/_mapping').reply(200, function(url, body){
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.type.json'));
            });

            es.storeMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                sourceIndex: 'index1',
                sourceType: 'type1',
                targetHost: 'host2',
                targetIndex: 'index2',
                targetType: 'type2',
                targetPort: 9200,
                logEnabled: false
            }, require('./data/mem.type.json'), function () {
                done();
            });
        });

        it("should create a valid index meta data request", function (done) {
            nock('http://host2:9200').put('/index2').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.index.json'));
            });

            es.storeMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                sourceIndex: 'index1',
                targetHost: 'host2',
                targetIndex: 'index2',
                targetPort: 9200,
                logEnabled: false
            }, require('./data/mem.index.json'), function () {
                done();
            });
        });

        it("should create a valid index meta data request", function (done) {
            nock('http://host2:9200').put('/index1').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.all.1.json'));
            });
            nock('http://host2:9200').put('/index2').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.all.2.json'));
            });

            es.storeMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                targetHost: 'host2',
                targetPort: 9200,
                logEnabled: false
            }, require('./data/mem.all.json'), function () {
                done();
            });
        });
    });


    describe('#getData()', function() {
        it("should return valid data for a type query", function(done) {
            nock('http://host:9200').post('/_search?search_type=scan&scroll=5m').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/query.type.json'));
                return require('./data/get.scroll.1.json');
            });
            nock('http://host:9200').post('/_search/scroll?scroll=5m').reply(200, require('./data/get.scroll.2.json'));
            nock('http://host:9200').post('/_search/scroll?scroll=5m').reply(200, require('./data/get.scroll.3.json'));

            es.scrollId = null;
            var result = [];
            var options = {
                sourceSize: 3,
                sourceQuery: {
                    match_all: {}
                },
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1',
                sourceType: 'type1',
                logEnabled: false
            };
            es.getData(options, function(hits, total) {
                expect(total).to.be.equal(6);
                expect(hits).to.be.undefined;
                es.getData(options, function (hits, total) {
                    expect(total).to.be.equal(6);
                    expect(hits).to.have.length(3);
                    result = result.concat(hits);
                    es.getData(options, function (hits, total) {
                        expect(total).to.be.equal(6);
                        expect(hits).to.have.length(3);
                        result = result.concat(hits);
                        expect(result).to.have.length(6);
                        expect(result).to.be.deep.equal(require('./data/mem.data.json'));
                        done();
                    });
                });
            });
        });

        it("should make a valid index query", function (done) {
            nock('http://host:9200').post('/_search?search_type=scan&scroll=5m').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/query.index.json'));
                return require('./data/get.scroll.1.json');
            });

            es.scrollId = null;
            es.getData({
                sourceSize: 3,
                sourceQuery: {
                    match_all: {}
                },
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1',
                logEnabled: false
            }, function () {
                done();
            });
        });

        it("should make a valid all query", function (done) {
            nock('http://host:9200').post('/_search?search_type=scan&scroll=5m').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/query.all.json'));
                return require('./data/get.scroll.1.json');
            });

            es.scrollId = null;
            es.getData({
                sourceSize: 3,
                sourceQuery: {
                    match_all: {}
                },
                sourceHost: 'host',
                sourcePort: 9200,
                logEnabled: false
            }, function () {
                done();
            });
        });
    });

    describe('#storeData()', function() {
        it("should make a valid bulk data store request", function(done) {
            var bulkdata = fs.readFileSync(__dirname + '/data/put.data.njson', { encoding: 'UTF-8'});
            nock('http://host:9200').post('_bulk').reply(200, function (url, body) {
                expect(body).to.be.equal(bulkdata);
            });

            es.storeData({
                targetHost: 'host',
                targetPort: 9200,
                logEnabled: false
            }, bulkdata, done);
        });
    });
});
