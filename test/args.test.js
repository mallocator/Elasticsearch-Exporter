/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var args = require('../args.js');
var log = require('../log.js');


log.capture = true;

describe('args', () => {
    describe('#buildOptionMap()', () => {
        beforeEach(log.pollCapturedLogs);

        it("should convert an option list from the driver into a list of globaly usable options", () => {
            let optionMap = args.buildOptionMap({
                optiona: {
                    abbr: 'a',
                    help: 'option a'
                }, optionb: {
                    abbr: 'b',
                    help: 'option b',
                    flag: true
                }, optionc: {
                    abbr: 'c',
                    help: 'option c',
                    flag: true,
                    preset: true
                }, optiond: {
                    abbr: 'd',
                    help: 'option d',
                    required: true
                }, optione: {
                    abbr: 'e',
                    help: 'option e',
                    list: true
                }
            }, 'test.');

            expect(optionMap['-a'].found).to.be.equal(optionMap['--test.optiona'].found);
            expect(optionMap['-a'].help).to.be.equal(optionMap['--test.optiona'].help);
            expect(optionMap['-a'].list).to.be.equal(optionMap['--test.optiona'].list);
            expect(optionMap['-a'].value).to.be.equal(optionMap['--test.optiona'].value);
            expect(optionMap['-a'].required).to.be.equal(optionMap['--test.optiona'].required);

            expect(optionMap['-a'].value).to.be.undefined;
            expect(optionMap['-a'].help).to.be.equal('option a');
            expect(optionMap['-a'].list).to.be.false;
            expect(optionMap['-a'].found).to.be.false;
            expect(optionMap['-a'].required).to.be.false;

            expect(optionMap['-b'].value).to.be.false;
            expect(optionMap['-b'].help).to.be.equal('option b');
            expect(optionMap['-b'].list).to.be.false;
            expect(optionMap['-b'].found).to.be.false;
            expect(optionMap['-b'].required).to.be.false;

            expect(optionMap['-c'].value).to.be.true;
            expect(optionMap['-c'].help).to.be.equal('option c');
            expect(optionMap['-c'].list).to.be.false;
            expect(optionMap['-c'].found).to.be.true;
            expect(optionMap['-c'].required).to.be.true;

            expect(optionMap['-d'].value).to.be.undefined;
            expect(optionMap['-d'].help).to.be.equal('option d');
            expect(optionMap['-d'].list).to.be.false;
            expect(optionMap['-d'].found).to.be.false;
            expect(optionMap['-d'].required).to.be.true;

            expect(optionMap['-e'].value).to.be.undefined;
            expect(optionMap['-e'].help).to.be.equal('option e');
            expect(optionMap['-e'].list).to.be.true;
            expect(optionMap['-e'].found).to.be.false;
            expect(optionMap['-e'].required).to.be.false;
        });

        it("should throw a warning about defining an option twice", () => {
            args.buildOptionMap({
                optiona: {
                    abbr: 'a'
                }, optionb: {
                    abbr: 'a'
                }
            }, 'test.');

            args.buildOptionMap({
                'option.c': {
                    abbr: 'c'
                }, option: {
                    c: {
                        abbr: 'd'
                    }
                }
            }, 'test.');

            let logs = log.pollCapturedLogs();

            expect(logs[0]).to.be.equal('ERROR: Warning: driver is overwriting an existing abbreviated option: -a! (current: --test.optionb, previous: --test.optiona)');
            expect(logs[1]).to.be.equal('ERROR: Warning: driver is overwriting an existing option: --test.option.c!');
        });
    });

    describe('#parse()', () => {
        beforeEach(() => log.pollCapturedLogs());

        it("should read in simple command line options both short and long version", () => {
            args.args = [ '-a', '1', '--optionb', '2'];
            let result = args.parse({
                optiona: {
                    abbr: 'a'
                },
                optionb: {
                    abbr: 'b'
                }
            });

            expect(result.optiona).to.be.equal(1);
            expect(result.optionb).to.be.equal(2);
        });

        it("should return listable arguments as a list", () => {
            args.args = ['-a', '1', '-a', '2', '-a', '3'];
            let result = args.parse({
                optiona: {
                    abbr: 'a',
                    list: true
                }
            });

            expect(result.optiona).to.be.deep.equal([1, 2, 3]);
        });

        it("should ignore unknown arguments and discard them", () => {
            args.args = ['-a', '1', '-b', '2', '-c', '3'];
            let result = args.parse({
                optiona: {
                    abbr: 'a'
                }, optionc: {
                    abbr: 'c'
                }
            });

            expect(result.optiona).to.be.equal(1);
            expect(result.b).to.be.undefined;
            expect(result.optionb).to.be.undefined;
        });

        it("should warn about duplicate arguments that are not a list", () => {
            args.args = ['-a', '1', '-a', '2'];
            try {
                args.parse({
                    optiona: {
                        abbr: 'a'
                    }
                });
            } catch(e) {}

            let logs = log.pollCapturedLogs();

            expect(logs[0]).to.be.equal('ERROR: An option that is not a list has been defined twice: -a');
        });

        it("should recognize a flag as true even without a parameter", () => {
            args.args = ['-a', '-b', '2', '-c', 'false', '-e', '3'];

            let result = args.parse({
                optiona: {
                    abbr: 'a',
                    flag: true
                }, optionb: {
                    abbr: 'b'
                }, optionc: {
                    abbr: 'c',
                    flag: true
                }, optione: {
                    abbr: 'e'
                }
            });

            expect(result.optiona).to.be.true;
            expect(result.optionc).to.be.false;
        });

        it("should cast values to the right types", () => {
            args.args = ['-a', '1', '-b', 'b2', '-c', '3e2', '-d', '1.5', '-e', '0', '-j', '{ "j": true }'];
            let result = args.parse({
                optiona: {
                    abbr: 'a'
                }, optionb: {
                    abbr: 'b'
                }, optionc: {
                    abbr: 'c'
                }, optiond: {
                    abbr: 'd'
                }, optione: {
                    abbr: 'e'
                }, optionj: {
                    abbr: 'j'
                }
            });

            expect(result.optiona).to.be.equal(1);
            expect(result.optionb).to.be.equal('b2');
            expect(result.optionc).to.be.equal(300);
            expect(result.optiond).to.be.equal(1.5);
            expect(result.optione).to.be.equal(0);
            expect(result.optionj).to.be.deep.equal({ j: true});
        });

        it("should override a preset", () => {
            args.args = ['-a', '1', '--optionb', '3'];
            let result = args.parse({
                optiona: {
                    abbr: 'a',
                    preset: 2
                }, optionb: {
                    abbr: 'b',
                    preset: 4
                }
            });

            expect(result.optiona).to.be.equal(1);
            expect(result.optionb).to.be.equal(3);
        });

        it("should stay within min/max constraints", () => {
            args.args = ['-a', '100', '--optionb', '1000', '-c', '1'];
            let result = args.parse({
                optiona: {
                    abbr: 'a',
                    min: 0,
                    max: 100
                }, optiond: {
                    abbr: 'd',
                    preset: 50,
                    min: 50,
                    max: 50
                }
            });

            expect(result.optiona).to.be.equal(100);
            expect(result.optiond).to.be.equal(50);
            let errors = 0;

            try {
                args.parse({
                    optionc: {
                        abbr: 'c',
                        min: 2
                    }
                });
            } catch (e) {
                expect(e.message).to.be.equal("optionc is below constraint (min: 2)");
                errors++;
            }

            try {
                args.parse({
                    optionb: {
                        abbr: 'b',
                        max: 999
                    }
                });
            } catch (e) {
                expect(e.message).to.be.equal("optionb is above constraint (min:999)");
                errors++;
            }

            expect(errors).to.be.equal(2);
        });
    });
});
