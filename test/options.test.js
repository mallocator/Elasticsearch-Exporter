var expect = require('chai').expect;
var options = require('../options.js');

options.nomnom = {
    getUsage: function() {}
};

describe('options', function () {
    describe('#detectCompression()', function () {
        it("should set the compression flag with a compressed file", function() {
            var opts = {
                sourceFile: "test/data/compressed"
            };
            options.detectCompression(opts);
            expect(opts).to.have.property('sourceCompression');
            expect(opts.sourceCompression).to.be.true;
        });

        it("should not set the compression flag with an uncompressed file", function() {
            var opts = {
                sourceFile: "test/data/decompressed"
            };
            options.detectCompression(opts);
            expect(opts).to.have.property('sourceCompression');
            expect(opts.sourceCompression).to.be.false;
        });
    });

    describe('#autoFillOptions()', function() {
        it("should set the target host, if a source host is set and no target host", function() {
            var opts = {
                sourceHost: 'host1'
            };
            options.autoFillOptions(opts);
            expect(opts).to.have.property('targetHost');
            expect(opts.targetHost).to.be.equal('host1');
        });

        it("should set the target port, if a source port is set and no target port", function () {
            var opts = {
                sourcePort: 9200
            };
            options.autoFillOptions(opts);
            expect(opts).to.have.property('targetPort');
            expect(opts.targetPort).to.be.equal(9200);
        });

        it("should set the target index, if a source index is set and no target index", function () {
            var opts = {
                sourceIndex: 'index1'
            };
            options.autoFillOptions(opts);
            expect(opts).to.have.property('targetIndex');
            expect(opts.targetIndex).to.be.equal('index1');
        });

        it("should set the target type, if a source type is set and no target type", function () {
            var opts = {
                sourceType: 'type1'
            };
            options.autoFillOptions(opts);
            expect(opts).to.have.property('targetType');
            expect(opts.targetType).to.be.equal('type1');
        });
    });

    describe('#validateOptions()', function () {
        it("should detect that a source file is missing", function() {
            var valid = options.validateOptions({sourceFile:'non.existent'});
            expect(valid).to.be.a("string");
        });

        it("should detect that a source file is there", function () {
            var opts = {
                sourceFile: 'test/data/compressed',
                targetHost: 'index1'
            };
            var valid = options.validateOptions(opts);
            expect(valid).to.be.undefined;
        });
    });

    describe('#readOptionsFile()', function() {
        it("should read options from a file", function() {
            var opts = {
                optionsFile: 'test/data/options.json'
            };
            var valid = options.readOptionsFile(opts);
            expect(valid).to.be.undefined;
            expect(opts.sourceHost).to.be.equal('host1');
            expect(opts.targetHost).to.be.equal('host2');
        });

        it("should not overwrite options that have been previously been set when reading a file", function() {
            options.overrides = {
                sourceHost: 'host3'
            };
            var opts = {
                optionsFile: 'test/data/options.json'
            };
            var valid = options.readOptionsFile(opts);
            expect(valid).to.be.undefined;
            expect(opts.sourceHost).to.be.equal(undefined);
            expect(opts.targetHost).to.be.equal('host2');
        });
    })
});
