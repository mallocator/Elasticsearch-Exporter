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
exports.Mapper = function(pattern) {
    var that = this;
    function findValue(parents, obj) {
        for (var i in obj) {
            var newParents = parents.concat(i);
            if (typeof obj[i] == 'string') {
                that.fields[obj[i]] = newParents;
            } else {
                findValue(newParents, obj[i]);
            }
        }
    }
    this.fields = {};
    findValue([], pattern);

    this.buildObject = function(path, value, result) {
        var field = path.shift();
        if (!path.length) {
            result[field] = value;
        } else {
            if (!result[field]) {
                result[field] = {};
            }
            that.buildObject(path, value, result[field]);
        }
    };

    /**
     * The mapping function that will convert a flat map to a complex object. This method accepts either an array of
     * objects or just a single object.
     * @param data
     * @returns {*}
     */
    this.map = function (data) {
        if (Array.isArray(data)) {
            return data.map(that.map);
        }
        var result = {};
        for (var i in data) {
            if (!that.fields[i]) {
                continue;
            }
            that.buildObject(that.fields[i].slice(), data[i], result);
        }
        return result;
    };
};