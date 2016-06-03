/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var mapper = require('../mapper.js');
var log = require('../log.js');


log.capture = true;

describe("mapper", () => {
    describe("#map()", () => {
        beforeEach(log.pollCapturedLogs);

        it("should map an array of document rows to complex objects", () => {
            let docMapper = new mapper.Mapper({
                _id: 'id',
                person: {
                    name: 'name'
                },
                numerical: {
                    integers: {
                        levels: 'age',
                        now: 'timestamp'
                    }
                }
            });

            let result = docMapper.map([{
                id: '123456',
                name: 'Test Element',
                age: 21,
                timestamp: 1234567890
            }, {
                id: 'abcde',
                name: 'Test Element 2',
                age: 999,
                timestamp: 1234567891
            }]);

            expect(result).to.be.deep.equal([{
                _id: '123456',
                person: {
                    name: 'Test Element'
                },
                numerical: {
                    integers: {
                        levels: 21,
                        now: 1234567890
                    }
                }
            }, {
                _id: 'abcde',
                person: {
                    name: 'Test Element 2'
                },
                numerical: {
                    integers: {
                        levels: 999,
                        now: 1234567891
                    }
                }
            }]);
        });

        it("should map a single document to a single complex object", () => {
            let docMapper = new mapper.Mapper({
                _id: 'id',
                person: {
                    name: 'name'
                },
                numerical: {
                    integers: {
                        levels: 'age',
                        now: 'timestamp'
                    }
                }
            });

            let result = docMapper.map({
                id: '123456',
                name: 'Test Element',
                age: 21,
                timestamp: 1234567890
            });

            expect(result).to.be.deep.equal({
                _id: '123456',
                person: {
                    name: 'Test Element'
                },
                numerical: {
                    integers: {
                        levels: 21,
                        now: 1234567890
                    }
                }
            });
        });
    });
});
