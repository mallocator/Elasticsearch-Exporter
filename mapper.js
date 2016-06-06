'use strict';

/**
 * The mapper will be able to map a flat document map to a complex object using a given pattern. The pattern consists of
 * the structure of the desired object and the key names that are going to mapped as values.
 *
 * @example
 *
 * {
 *   _id: 'id',
 *   person: {
 *     name: 'name'
 *   },
 *   numerical: {
 *     integers: {
 *       levels: 'age',
 *       now: 'timestamp'
 *     }
 *   }
 * }
 *
 * The input data to map to this document would look something like this:
 *
 * {
 *   id: '123456',
 *   name: 'Test Element',
 *   age: 21,
 *   timestamp: 1234567890
 * }
 *
 * And the mapped end result would look like this:
 *
 * {
 *   _id: '123456',
 *   person: {
 *     name: 'Test Element'
 *   },
 *   numerical: {
 *     integers: {
 *       levels: 21,
 *       now: 1234567890
 *     }
 *   }
 * }
 *
 */
class Mapper {
    /**
     *
     * @param {Object|string} pattern
     */
    constructor(pattern) {
        this._fields = {};
        this._findValue([], pattern);
    }

    /**
     *
     * @param {string[]} parents
     * @param {Object|string} obj
     * @private
     */
    _findValue(parents, obj) {
        for (let i in obj) {
            let newParents = parents.concat(i);
            if (typeof obj[i] == 'string') {
                this._fields[obj[i]] = newParents;
            } else {
                this._findValue(newParents, obj[i]);
            }
        }
    }

    /**
     * The mapping function that will convert a flat map to a complex object.
     * @param {Object|Object[]} data
     * @returns {Object}
     */
    map(data) {
        if (Array.isArray(data)) {
            return data.map(this.map.bind(this));
        }
        let result = {};
        for (let i in data) {
            this._fields[i] && this._buildObject(this._fields[i].slice(), data[i], result);
        }
        return result;
    }

    /**
     *
     * @param {string[]} path
     * @param {Object} value
     * @param {Object} result
     * @private
     */
    _buildObject(path, value, result) {
        let field = path.shift();
        if (!path.length) {
            result[field] = value;
        } else {
            result[field] = result[field] || {};
            this._buildObject(path, value, result[field]);
        }
    }
}

exports.Mapper = Mapper;
