var expect = require('chai').expect;
var gently = new (require('gently'))();
var options = require('../options.js');
var drivers = require('../drivers.js');
var mockDriver = require('./driver.mock.js');
var log = require('../log.js');

log.capture = true;

describe("options", function () {
    describe("#defalte()", function() {
        it("should flatten a nested json structure", function() {
            var result = options.deflate({
                group: {
                    test1: {
                        value: 'val1',
                        abbr: 'v'
                    },
                    test2: {
                        value: 'val2',
                        abbr: 'w'
                    }
                }
            }, "group");

            expect(result).to.be.deep.equal({
                'group.test1': {
                    value: 'val1',
                    abbr: 'gv'
                },
                'group.test2': {
                    value: 'val2',
                    abbr: 'gw'
                }
            });
        });
    });

    describe("#inflate()", function() {
        it("should expand a dot noted propert map to a json object", function() {
            var result = options.inflate({
                'test1.option1': 'val1',
                'test1.option2': 'val2',
                'test2.option1': 'val3',
                'test2.option2': 'val4',
                'test3.option1': 'val5'
            });

            expect(result).to.be.deep.equal({
                test1: {
                    option1: 'val1',
                    option2: 'val2'
                }, test2: {
                    option1: 'val3',
                    option2: 'val4'
                }, test3: {
                    option1: 'val5'
                }
            });
        });
    });

    describe("#defalteFile()", function () {
        var result = options.deflateFile({
            source: {
                host: 'test'
            },
            target: {
                port: 1,
                index: 'foo'

            },
            run: {
                test: true
            }
        });

        expect(result).to.be.deep.equal({
            'source.host': 'test',
            'target.port': 1,
            'target.index': 'foo',
            'run.test': true
        });
    });

    describe("#readFile()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should merge the optionsfile with the default options", function() {
            gently.expect(options, 'deflateFile', function (fileContent) {
                expect(fileContent).to.be.deep.equal({
                    "target": {
                        "host": "testhost"
                    }
                });
                return {
                    'source.host': 'testhost',
                    // ignored because not an available option
                    'target.port': 1,
                    'target.index': 'foo',
                    'run.test': true
                };
            });

            var scriptOptions = {
                optionsfile: {
                    value: 'test/data/options.json'
                },
                'run.test': {
                    preset: false
                }
            };

            var sourceOptions = {
                'source.host': {},
                'source.port': {
                    preset: 9200
                }
            };

            var targetOptions = {
                'target.index': {
                    preset: 'bar'
                }
            };

            options.readFile(scriptOptions, sourceOptions , targetOptions);

            expect(sourceOptions).to.be.deep.equal({
                'source.port': {
                    preset: 9200
                },
                'source.host': {
                    preset: 'testhost',
                    required: false
                }
            });

            expect(targetOptions).to.be.deep.equal({
                'target.index': {
                    preset: 'foo',
                    required: false
                }
            });

            expect(scriptOptions).to.be.deep.equal({
                optionsfile: {
                    value: 'test/data/options.json'
                },
                'run.test': {
                    value: true,
                    preset: false
                }
            });
        });
    });

    describe("#verify()", function () {
        afterEach(function () {
            gently.verify();
        });

        it("should call the source driver only once if is the same type as the target driver", function(done) {
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', function() {
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'verifyOptions', function(options, callback) {
                callback();
            });

            options.verify({
                drivers: {
                    source: 'mock',
                    target: 'mock'
                }
            }, function(err) {
                expect(err).to.not.exist();
                done();
            });
        });

        it("should pass on an error if the driver finds any", function(done) {
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', function () {
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'verifyOptions', function (options, callback) {
                callback(['Error1', 'Error2']);
            });

            options.verify({
                drivers: {
                    source: 'mock',
                    target: 'mock'
                }
            }, function (err) {
                expect(err).to.be.deep.equal(['Error1', 'Error2']);
                done();
            });
        });

        it("should call the both the source and target driver to verify options", function (done) {
            var mock = mockDriver.getDriver();

            gently.expect(drivers, 'get', function () {
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'verifyOptions', function (options, callback) {
                callback();
            });

            gently.expect(drivers, 'get', function () {
                return {
                    info: mock.getInfoSync(),
                    options: mock.getOptionsSync(),
                    driver: mock
                };
            });

            gently.expect(mock, 'verifyOptions', function (options, callback) {
                callback();
            });

            options.verify({
                drivers: {
                    source: 'mock1',
                    target: 'mock2'
                }
            }, function (err) {
                expect(err).to.not.exist();
                done();
            });
        });
    });
});