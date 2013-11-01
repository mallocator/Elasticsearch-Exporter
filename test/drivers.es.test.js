var expect = require('chai').expect;
var gently = new (require('gently'));
var nock = require('nock');
global.GENTLY = gently;
var es = require('../drivers/es.js');


nock.disableNetConnect();

describe('drivers.es', function () {
    afterEach(function () {
        gently.verify();
    });

    describe('#getMeta()', function () {
        it("should return a valid type meta data description", function () {
            nock('http://host:9200').get('/index1/type1/_mapping').reply(200, require('./data/get.type.mapping.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1',
                sourceType: 'type1'
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.type.json'));
            });
        });

        it("should return a valid index meta data description", function () {
            nock('http://host:9200').get('/index1/_mapping').reply(200, require('./data/get.index.mapping.json'));
            nock('http://host:9200').get('/index1/_settings').reply(200, require('./data/get.index.settings.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200,
                sourceIndex: 'index1'
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.index.json'));
            });
        });

        it("should return a valid meta data description for all indices", function () {
            nock('http://host:9200').get('/_mapping').reply(200, require('./data/get.all.mapping.json'));
            nock('http://host:9200').get('/_settings').reply(200, require('./data/get.all.settings.json'));

            es.getMeta({
                sourceHost: 'host',
                sourcePort: 9200
            }, function (data) {
                expect(data).to.be.a('object');
                expect(data).to.be.deep.equal(require('./data/mem.all.json'));
            });
        });
    });


    describe('#createMeta()', function () {
        it("should create a valid type meta data request", function (done) {
            nock('http://host2:9200').put('/index2').reply(200);
            nock('http://host2:9200').put('/index2/type2/_mapping').reply(200, function(url, body){
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.type.json'));
            });

            es.createMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                sourceIndex: 'index1',
                sourceType: 'type1',
                targetHost: 'host2',
                targetIndex: 'index2',
                targetType: 'type2',
                targetPort: 9200
            }, require('./data/mem.type.json'), function () {
                done();
            });
        });

        it("should create a valid index meta data request", function (done) {
            nock('http://host2:9200').put('/index2').reply(200, function (url, body) {
                expect(JSON.parse(body)).to.be.deep.equal(require('./data/put.index.json'));
            });

            es.createMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                sourceIndex: 'index1',
                targetHost: 'host2',
                targetIndex: 'index2',
                targetPort: 9200
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

            es.createMeta({
                sourceHost: 'host1',
                sourcePort: 9200,
                targetHost: 'host2',
                targetPort: 9200
            }, require('./data/mem.all.json'), function () {
                done();
            });
        });
    });
});
