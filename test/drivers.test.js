var expect = require('chai').expect;
var drivers = require('../drivers.js');
var log = require('../log.js');

log.capture = true;

describe("drivers", function() {
    describe("params{}", function() {
        describe("#get()", function() {
            it("should return all parameters of the given function", function() {
                var result = drivers.params.get(function(a, b, c, d) {});
                expect(result).to.deep.equal(['a', 'b', 'c', 'd']);
            });

            it("should return an empty array if the function has no parameters", function() {
                var result = drivers.params.get(function() {});
                expect(result).to.deep.equal([]);
            });
        });

        describe("#verify()", function() {
            it("should return true oif the parameters match", function() {
                var result = drivers.params.verify(function (a, b, c, d) {}, ['a', 'b', 'c', 'd']);
                expect(result).to.be.true;
            });

            it("should return false if the parameters don't match", function() {
                var result = drivers.params.verify(function (a, b) {}, ['a', 'c']);
                expect(result).to.be.false;
            });

            it("should return false if the parameters are in the wrong order", function() {
                var result = drivers.params.verify(function (a, b) {}, ['b', 'a']);
                expect(result).to.be.false;
            });

            it("should return true if too many parameters are defined", function () {
                var result = drivers.params.verify(function (a, b, c) {}, ['a', 'b']);
                expect(result).to.be.true;
            });
        });
    });

    describe("#verify()", function() {
        it("should return true for a valid implementation", function() {
            var result = drivers.verify(require('./data/driver.valid.js'));
            expect(result).to.be.true;
        });

        it("should return false for an implementation with a missing function", function () {
            var result = drivers.verify(require('./data/driver.missingfunction.js'));
            expect(result).to.be.false;
        });

        it("should return false for an implementation with a missing parameter", function () {
            var result = drivers.verify(require('./data/driver.missingparameter.js'));
            expect(result).to.be.false;
        });
    });

    describe("#register()", function() {
        beforeEach(function() {
            drivers.drivers = {};
        });

        it("should register a new driver", function(done) {
            drivers.register(require('./data/driver.valid.js'), function () {
                done();
            });
        });

        it("should not register an invalid driver", function() {
            var error = false;
            try {
                drivers.register(require('./data/driver.missingparameter.js'), function () {});
            } catch (e) {
                error = true;
            }
            expect(error).to.be.true;
        });

        it("should not register a duplicate driver and display a warning", function(done) {
            drivers.register(require('./data/driver.valid.js'), function () {
                try {
                    drivers.register(require('./data/driver.valid.js'), function () {

                    });
                } catch (e) {
                    done();
                }
            });
        });
    });

    describe("#find()", function() {
        beforeEach(function () {
            drivers.drivers = {};
        });

        it("should find at least 3 drivers from the drivers folder", function(done) {
            drivers.find("./drivers", function() {
                expect(Object.keys(drivers.drivers).length).to.be.greaterThan(2);
                done();
            });
        });

        it("should not find any drivers in the test folder", function(done) {
            drivers.find("./tools", function () {
                expect(Object.keys(drivers.drivers).length).to.be.equal(0);
                done();
            });
        });
    });

    describe("#get()", function() {
        beforeEach(function () {
            drivers.drivers = {};
        });

        it("should return one of the existing drivers", function() {
            var original = require('./data/driver.valid.js');
            drivers.drivers.test = {
                id: 'test',
                driver: original,
                options: {}
            };

            var driver = drivers.get('test');
            expect(driver.id).to.be.equal('test');
            expect(driver.driver).to.deep.equal(original);
        });

        it("should throw an error when trying to load a driver that doesn't exist", function() {
            var error = false;
            try {
                var driver = drivers.get('test');
            } catch (e) {
                error = true;
            }
            expect(error).to.be.true;
        });
    });
});