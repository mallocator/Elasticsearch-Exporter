var expect = require('chai').expect;
var args = require('../args.js');
var log = require('../log.js');

log.capture = true;

describe('args', function() {
    describe('#buildOptionMap()', function() {
        afterEach(function() {
            log.pollCapturedLogs();
        });

        it("should convert an option list from the driver into a list of globaly usable options", function() {
            var optionMap = args.buildOptionMap({
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
            expect(optionMap['-d'].required).to.be.true

            expect(optionMap['-e'].value).to.be.undefined;
            expect(optionMap['-e'].help).to.be.equal('option e');
            expect(optionMap['-e'].list).to.be.true;
            expect(optionMap['-e'].found).to.be.false;
            expect(optionMap['-e'].required).to.be.false;
        });

        it("should throw a warning about defining an option twice", function() {
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

            var logs = log.pollCapturedLogs();

            expect(logs[0]).to.be.equal('ERROR: Warning: driver is overwriting an existing abbreviated option: -a! (current: --test.optionb, previous: --test.optiona)');
            expect(logs[1]).to.be.equal('ERROR: Warning: driver is overwriting an existing option: --test.option.c!');
        });
    });

    describe('#parse()', function() {
        afterEach(function () {
            log.pollCapturedLogs();
        });

        it("should read in simple command line options both short and long version", function() {
            args.args = [ '-a', '1', '--optionb', '2'];
            var result = args.parse({
                optiona: {
                    abbr: 'a'
                },
                optionb: {
                    abbr: 'b'
                }
            });

            expect(result.optiona).to.be.equal("1");
            expect(result.optionb).to.be.equal("2");
        });

        it("should return listable arguments as a list", function() {
            args.args = ['-a', '1', '-a', '2', '-a', '3'];
            var result = args.parse({
                optiona: {
                    abbr: 'a',
                    list: true
                }
            });

            expect(result.optiona).to.be.deep.equal(['1','2', '3']);
        });

        it("should ignore unknown arguments and discard them", function() {
            args.args = ['-a', '1', '-b', '2', '-c', '3'];
            var result = args.parse({
                optiona: {
                    abbr: 'a'
                }, optionc: {
                    abbr: 'c'
                }
            });

            expect(result.optiona).to.be.equal('1');
            expect(result.b).to.be.undefined;
            expect(result.optionb).to.be.undefined;
        });

        it("should warn about duplicate arguments that are not a list", function() {
            args.args = ['-a', '1', '-a', '2'];
            try {
                args.parse({
                    optiona: {
                        abbr: 'a'
                    }
                });
            } catch(e) {}

            var logs = log.pollCapturedLogs();

            expect(logs[0]).to.be.equal('ERROR: An option that is not a list has been defined twice: -a');
        });

        it("should recognize a flag as true even without a parameter", function() {
            args.args = ['-a', '-b', '2'];

            var result = args.parse({
                optiona: {
                    abbr: 'a',
                    flag: true
                }, optionb: {
                    abbr: 'b'
                }
            });

            expect(result.optiona).to.be.true;
        });
    });
});