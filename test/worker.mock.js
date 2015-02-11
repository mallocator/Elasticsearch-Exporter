var messages = [];

process.on('message', function (m) {
    if (!m.id) {
        m.id = '0';
    }
    switch (m.type) {
        case 'getMessages':
            exports.send.messages(m.id, m.responseType);
            break;
        case 'sendError':
            exports.send.error(m.id, m.exception);
            break;
        case 'sendDone':
            exports.send.done(m.processed, m.id, m.memUsage);
            break;
        default:
            messages.push(m);
    }
});

exports.send = {
    error: function (id, exception) {
        process.send({
            id: id,
            type: 'Error',
            message: exception
        });
    },
    done: function (processed, id, memUsage) {
        process.send({
            id: id,
            type: 'Done',
            processed: processed,
            memUsage: memUsage
        });
        exports.status = 'ready';
    },
    messages: function(id, type) {
        process.send({
            id: id,
            type: type,
            messages: messages
        });
    }
};

exports.work = function(from, size) {
    exports.send.done(size, 0, {
        heapUsed: 100,
        ratio: 0.5
    });
};

exports.end = function() {};