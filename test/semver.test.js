/* global describe, it, beforeEach, afterEach */

'use strict';

var expect = require('chai').expect;

var SemVer = require('../semver');


describe('SemVer', () => {
    var greater = new SemVer('2.1.3-TEST');
    var lesser = new SemVer('1.9.5');
    it('should implement all standard comparison functions', () => {
        expect(greater.gt(lesser)).to.be.true;
        expect(greater.ge(lesser)).to.be.true;
        expect(greater.le(lesser)).to.be.false;
        expect(greater.lt(lesser)).to.be.false;
        expect(greater.eq(lesser)).to.be.false;
        expect(greater.ne(lesser)).to.be.true;
        expect(greater.between(lesser, greater)).to.be.false;
        expect(greater.within(lesser, greater)).to.be.true;
    });

    it('should be compatible with standard operators', () => {
        expect(greater > lesser).to.be.true;
        expect(greater >= lesser).to.be.true;
        expect(greater < lesser).to.be.false;
        expect(greater <= lesser).to.be.false;
        expect(greater == lesser).to.be.false;
        expect(greater != lesser).to.be.true;
    });

    it('should create the same instances no matter the input format', () => {
        var one = new SemVer('1.0');
        var two = new SemVer(one);
        var three = new SemVer({
            major: 1,
            original: '1.0'
        });
        var four = new SemVer({
            original: '1.0'
        });
        var five = new SemVer(1.0);

        expect(one).to.deep.equal(two);
        expect(two).to.deep.equal(three);
        expect(three).to.deep.equal(four);

        expect(four.major).to.equal(five.major);
        expect(four.minor).to.equal(five.minor);
        expect(four.patch).to.equal(five.patch);
    });
});
