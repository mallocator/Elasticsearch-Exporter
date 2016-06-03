/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var drivers = require('../drivers.js');
var log = require('../log.js');


log.capture = true;

describe("drivers", () => {
    describe("params{}", () => {
        describe("#get()", () => {
            it("should return all parameters of the given function", () => {
                let result = drivers.params.get((a, b, c, d) => {});
                expect(result).to.deep.equal(['a', 'b', 'c', 'd']);
            });

            it("should return an empty array if the function has no parameters", () => {
                let result = drivers.params.get(() => {});
                expect(result).to.deep.equal([]);
            });
        });

        describe("#verify()", () => {
            it("should return true oif the parameters match", () => {
                let result = drivers.params.verify((a, b, c, d) => {}, ['a', 'b', 'c', 'd']);
                expect(result).to.be.true;
            });

            it("should return false if the parameters don't match", () => {
                let result = drivers.params.verify((a, b) => {}, ['a', 'c']);
                expect(result).to.be.false;
            });

            it("should return false if the parameters are in the wrong order", () => {
                let result = drivers.params.verify((a, b) => {}, ['b', 'a']);
                expect(result).to.be.false;
            });

            it("should return true if too many parameters are defined", () => {
                let result = drivers.params.verify((a, b, c) => {}, ['a', 'b']);
                expect(result).to.be.true;
            });
        });
    });

    describe("#verify()", () => {
        it("should return true for a valid implementation", () => {
            let result = drivers.verify(require('./data/driver.valid.js'));
            expect(result).to.be.true;
        });

        it("should return false for an implementation with a missing function", () => {
            let result = drivers.verify(require('./data/driver.missingfunction.js'));
            expect(result).to.be.false;
        });

        it("should return false for an implementation with a missing parameter", () => {
            let result = drivers.verify(require('./data/driver.missingparameter.js'));
            expect(result).to.be.false;
        });
    });

    describe("#register()", () => {
        beforeEach(() => {
            drivers.drivers = {};
        });

        it("should register a new driver", (done) => {
            drivers.register(require('./data/driver.valid.js'), () => {
                done();
            });
        });

        it("should not register an invalid driver", () => {
            let error = false;
            try {
                drivers.register(require('./data/driver.missingparameter.js'), () => {});
            } catch (e) {
                error = true;
            }
            expect(error).to.be.true;
        });

        it("should not register a duplicate driver and display a warning", (done) => {
            drivers.register(require('./data/driver.valid.js'), () => {
                try {
                    drivers.register(require('./data/driver.valid.js'), () => {

                    });
                } catch (e) {
                    done();
                }
            });
        });
    });

    describe("#find()", () => {
        beforeEach(() => {
            drivers.drivers = {};
        });

        it("should find at least 3 drivers from the drivers folder", (done) => {
            drivers.find("./drivers", () => {
                expect(Object.keys(drivers.drivers).length).to.be.greaterThan(2);
                done();
            });
        });

        it("should not find any drivers in the test folder", (done) => {
            drivers.find("./tools", () => {
                expect(Object.keys(drivers.drivers).length).to.be.equal(0);
                done();
            });
        });
    });

    describe("#get()", () => {
        beforeEach(() => {
            drivers.drivers = {};
        });

        it("should return one of the existing drivers", () => {
            let original = require('./data/driver.valid.js');
            drivers.drivers.test = {
                id: 'test',
                driver: original,
                options: {}
            };

            let driver = drivers.get('test');
            expect(driver.id).to.be.equal('test');
            expect(driver.driver).to.deep.equal(original);
        });

        it("should throw an error when trying to load a driver that doesn't exist", () => {
            let error = false;
            try {
                drivers.get('test');
            } catch (e) {
                error = true;
            }
            expect(error).to.be.true;
        });
    });
});
