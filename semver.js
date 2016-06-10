'use strict';

/**
 * The JSON representation of a semantic version object. Defined here so it can be specified as a valid input for a
 * new SemVer object.
 * @typedef {Object} SemVerJson
 * @property {number} major
 * @property {number} minor
 * @property {number} patch
 * @property {string} original
 */

/**
 * A class that parses a semantic versions and offers compare functions.
 * @property {number} major     The first version number encountered in a semver string.
 * @property {number} minor     The second version number encountered in a semver string.
 * @property {number} patch     The third version number encountered in a semver string.
 * @property {string} suffix    Any string suffixes at the end of the version.
 */
class SemVer {
    /**
     * @param {SemVer|SemVerJson|string|number} version
     */
    constructor(version) {
        if (version instanceof SemVer || version instanceof Object) {
            this.major = version.major;
            this.minor = version.minor || 0;
            this.patch = version.patch || 0;
            version = version.original;
        }
        this.original = version;
        var parts = version.toString().split('.');
        this.major = this.major || parseInt(parts[0]);
        this.minor = this.minor || parts[1] ? parseInt(parts[1]) : 0;
        this.patch = this.patch || parts[2] ? parseInt(parts[2]) : 0;
    }

    /**
     * Returns true if both versions are the same.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    eq(version) {
        version = version instanceof SemVer ? version : new SemVer(version);
        return version.major == this.major && version.minor == this.minor && version.patch == this.patch;
    }

    /**
     * Returns false if both versions are the same.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    ne(version) {
        return !this.eq(version);
    }

    /**
     * Returns true if this version is less than the passed in version.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    lt(version) {
        version = version instanceof SemVer ? version : new SemVer(version);
        if (this.major < version.major) {
            return true;
        }
        if (this.major == version.major && this.minor < version.minor) {
            return true;
        }
        return !!(this.major == version.major && this.minor == version.minor && this.patch < version.patch);
    }

    /**
     * Returns true if this version is greater than the passed in version.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    gt(version) {
        return !this.le(version);
    }

    /**
     * Returns true if this version is less or equal than the passed in version.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    le(version) {
        return this.eq(version) || this.lt(version);
    }

    /**
     * Returns true if this version is greater or equal than the passed in version.
     * @param {SemVer|SemVerJson|string|number} version
     * @returns {boolean}
     */
    ge(version) {
        return this.eq(version) || this.gt(version);
    }

    /**
     * Inclusive range comparison (version1 <= this <= version2).
     * @param {SemVer|SemVerJson|string|number} version1
     * @param {SemVer|SemVerJson|string|number} version2
     * @returns boolean
     */
    within(version1, version2) {
        return this.ge(version1) && this.le(version2);
    }

    /**
     * exclusive range comparison (version1 < this < version2)
     * @param {SemVer|SemVerJson|string|number} version1
     * @param {SemVer|SemVerJson|string|number} version2
     * @returns boolean
     */
    between(version1, version2) {
        return this.gt(version1) && this.lt(version2);
    }

    valueOf() {
        return this.major * 1000000 + this.minor * 1000 + this.patch;
    }

    toString() {
        return this.original;
    }
}

module.exports = SemVer;
