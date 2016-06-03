'use strict';

var messages = [];

process.on('message', (m) => {
    m.id = m.id || '0';
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

exports.initialize_transform = () => exports.transform_function = null;

exports.send = {
    error: (id, exception) => process.send({ id, type: 'Error', message: exception }),
    messages: (id, type) => process.send({ id, type, messages }),
    done: (processed, id, memUsage) => {
        process.send({ id, type: 'Done', processed, memUsage });
        exports.status = 'ready';
    }
};

exports.work = (from, size) => exports.send.done(size, 0, { heapUsed: 100, ratio: 0.5 });

exports.end = () => {};
