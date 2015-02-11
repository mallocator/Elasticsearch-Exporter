var messages = [];

process.on('message', function (m) {
    if (!m.id) {
        m.id = '0';
    }
    switch (m.type) {
        case 'getMessages':
            send.messages(m.id, m.responseType);
            break;
        case 'sendError':
            send.error(m.id, m.exception);
            break;
        case 'sendDone':
            send.done(m.id, m.processed, m.memUsage);
            break;
        default:
            messages.push(m);
    }
});

var send = {
    error: function (id, exception) {
        process.send({
            id: id,
            type: 'Error',
            message: exception
        });
    },
    done: function (id, processed, memUsage) {
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