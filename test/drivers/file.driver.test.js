/* global describe, it, beforeEach, afterEach */
'use strict';

var expect = require('chai').expect;
var gently = new (require('gently'))();
var file = require('../../drivers/file.driver.js');
var log = require('../../log.js');


log.capture = true;

describe("drivers/file", () => {
    describe("#getInfo()", () => {
        it("should return two objects with both the info and the options of the driver", done => {
            file.getInfo((error, info, options) => {
                expect(error).to.be.not.ok;
                expect(info.id).to.exist;
                expect(info.name).to.exist;
                expect(info.version).to.exist;
                expect(info.description).to.exist;
                expect(options.source).to.exist;
                expect(options.target).to.exist;
                done();
            });
        });
    });

    describe("#verifyOptions()", () => {
        
    });

    describe("#reset()", () => {

    });

    describe("#getSourceStats()", () => {

    });

    describe("#getTargetStats()", () => {

    });

    describe("#getMeta()", () => {

    });

    describe("#putMeta()", () => {

    });

    describe("#getData()", () => {

    });

    describe("#putData()", () => {

    });
});
