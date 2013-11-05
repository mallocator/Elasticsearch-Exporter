var fs = require('fs');
var zlib = require('zlib');
var through = require('through');
var expect = require('chai').expect;
var gently = new (require('gently'));
global.GENTLY = gently;
var file = require('../drivers/file.js');

describe('drivers.file', function () {
    afterEach(function () {
        gently.verify();
    });

    describe('#getMeta()', function () {
        it("should return a valid type meta data description", function (done) {
            var opts = {
                sourceFile: 'test/data/file.type',
                targetIndex: 'index2',
                targetType: 'type2'
            };
            file.getMeta(opts, function(metadata) {
                expect(metadata).to.be.deep.equal(require('./data/mem.type.json'));
                expect(opts).to.be.deep.equal({
                    sourceFile: 'test/data/file.type',
                    targetIndex: 'index2',
                    targetType: 'type2',
                    sourceIndex: 'index1',
                    sourceType: 'type1'
                });
                done();
            });
        });

        it("should return a valid index meta data description", function (done) {
            var opts = {
                sourceFile: 'test/data/file.index',
                targetIndex: 'index2'
            };
            file.getMeta(opts, function (metadata) {
                expect(metadata).to.be.deep.equal(require('./data/mem.index.json'));
                expect(opts).to.be.deep.equal({
                    sourceFile: 'test/data/file.index',
                    targetIndex: 'index2',
                    sourceIndex: 'index1'
                });
                done();
            });
        });

        it("should return a valid meta data description for all indices", function (done) {
            var opts = {
                sourceFile: 'test/data/file.all'
            };
            file.getMeta(opts, function (metadata) {
                expect(metadata).to.be.deep.equal(require('./data/mem.all.json'));
                expect(opts).to.be.deep.equal({
                    sourceFile: 'test/data/file.all'
                });
                done();
            });
        });
    });


    describe('#storeMeta()', function () {
        beforeEach(function() {
            gently.expect(gently.hijacked.fs, 'existsSync', function (dir) {
                expect(dir).to.be.equal('tmp/');
                return false;
            });
            gently.expect(gently.hijacked.fs, 'mkdirSync', function (dir) {
                expect(dir).to.be.equal('tmp/');
            });
        });
        it("should create a valid type meta data file", function (done) {
            var output = fs.readFileSync(__dirname + '/data/file.type.meta', { encoding: 'UTF-8'});
            gently.expect(gently.hijacked.fs, 'writeFile', function(path, data, encoding, callback) {
                expect(path).to.be.equal('tmp/output.meta');
                expect(JSON.parse(data)).to.be.deep.equal(JSON.parse(output));
                expect(encoding).to.be.deep.equal({encoding:'utf8'});
                callback();
            });
            gently.expect(gently.hijacked.fs, 'writeFile', function(path, data, callback) {
                expect(path).to.be.equal('tmp/output.data');
                expect(data).to.be.empty;
                callback();
            });

            file.storeMeta({
                targetFile: 'tmp/output',
                sourceType: 'type1',
                sourceIndex: 'index1'
            }, require('./data/mem.type.json'), done);
        });

        it("should create a valid index meta data file", function (done) {
            var output = fs.readFileSync(__dirname + '/data/file.index.meta', { encoding: 'UTF-8'});
            gently.expect(gently.hijacked.fs, 'writeFile', function (path, data, encoding, callback) {
                expect(path).to.be.equal('tmp/output.meta');
                expect(JSON.parse(data)).to.be.deep.equal(JSON.parse(output));
                expect(encoding).to.be.deep.equal({encoding: 'utf8'});
                callback();
            });
            gently.expect(gently.hijacked.fs, 'writeFile', function (path, data, callback) {
                expect(path).to.be.equal('tmp/output.data');
                expect(data).to.be.empty;
                callback();
            });

            file.storeMeta({
                targetFile: 'tmp/output',
                sourceIndex: 'index1'
            }, require('./data/mem.index.json'), done);
        });

        it("should create a valid index meta data request", function (done) {
            var output = fs.readFileSync(__dirname + '/data/file.all.meta', { encoding: 'UTF-8'});
            gently.expect(gently.hijacked.fs, 'writeFile', function (path, data, encoding, callback) {
                expect(path).to.be.equal('tmp/output.meta');
                expect(JSON.parse(data)).to.be.deep.equal(JSON.parse(output));
                expect(encoding).to.be.deep.equal({encoding: 'utf8'});
                callback();
            });
            gently.expect(gently.hijacked.fs, 'writeFile', function (path, data, callback) {
                expect(path).to.be.equal('tmp/output.data');
                expect(data).to.be.empty;
                callback();
            });

            file.storeMeta({
                targetFile: 'tmp/output'
            }, require('./data/mem.all.json'), done);
        });
    });


    describe('#getData()', function () {
        it("should return valid data from a zip file", function (done) {
            var opts = {
                sourceFile: __dirname + '/data/compressed',
                sourceCompression: true
            };
            file.reset();
            file.getData(opts, function(items, count) {
                expect(count).to.be.equal(6);
                expect(items).to.be.deep.equal(require('./data/mem.data.json'));
                done();
            });
        });

        it("should return valid data from a normal file", function (done) {
            var opts = {
                sourceFile: __dirname + '/data/decompressed',
                sourceCompression: false
            };
            file.reset();
            file.getData(opts, function (items, count) {
                expect(count).to.be.equal(6);
                expect(items).to.be.deep.equal(require('./data/mem.data.json'));
                done();
            });
        });
    });

    describe('#storeData()', function () {
        it("should store data in a zipped file", function (done) {
            var output = fs.readFileSync(__dirname + '/data/compressed.data');
            var input = fs.readFileSync(__dirname + '/data/put.data.njson', {encoding: 'utf8'});

            var data = '';
            file.targetStream = through().pause();
            file.targetStream.pipe(zlib.createGzip());
            file.targetStream.on('data', function(chunk) {
                data += chunk;
            });
            file.targetStream.on('end', function() {
                expect(data).to.be.equal(output);
                done();
            });

            file.storeData({
                sourceCompression: true
            }, input, function() {
                done();
            });
        });

        it("should store data in a normal file", function (done) {
            var output = fs.readFileSync(__dirname + '/data/decompressed.data', {encoding: 'utf8'});
            var input = fs.readFileSync(__dirname + '/data/put.data.njson', {encoding: 'utf8'});
            gently.expect(gently.hijacked.fs, 'appendFile', function(path, data, encoding, callback) {
                expect(path).to.be.equal('tmp/output.data');
                expect(encoding).to.be.deep.equal({encoding:'utf8'});
                expect(data).to.be.equal(output);
                callback();
            });

            file.targetStream = null;
            file.storeData({
                targetFile: 'tmp/output',
                sourceCompression: false
            }, input, done);
        });
    });
});