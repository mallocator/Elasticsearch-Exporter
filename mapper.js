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

var mapper = new exports.Mapper({
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

console.log(mapper.map([{
    id: '123456',
    name: 'Test Element',
    age: 21,
    timestamp: 1234567890
}, {
    id: '123456',
    name: 'Test Element',
    age: 21,
    timestamp: 1234567890
}]));