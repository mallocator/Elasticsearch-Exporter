/**
 * The mapper will be able to map a flat document map to a complex object using a given pattern. The pattern consists of
 * the structure of the desired object and the key names that are going to mapped as values. an Example for a pattern
 * could look like this:
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
 * @param pattern
 * @constructor
 */
class Mapper {
    constructor(pattern) {
        this.fields = {};
        this._findValue([], pattern);
    }

    _findValue(parents, obj) {
        for (let i in obj) {
            let newParents = parents.concat(i);
            if (typeof obj[i] == 'string') {
                this.fields[obj[i]] = newParents;
            } else {
                this._findValue(newParents, obj[i]);
            }
        }
    }

    /**
     * The mapping function that will convert a flat map to a complex object. This method accepts either an array of
     * objects or just a single object.
     * @param data
     * @returns {*}
     */
    map(data) {
        if (Array.isArray(data)) {
            return data.map(this.map.bind(this));
        }
        let result = {};
        for (let i in data) {
            this.fields[i] && this._buildObject(this.fields[i].slice(), data[i], result);
        }
        return result;
    }

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
