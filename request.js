'use strict';

var http = require('http');
var https = require('https');
var url = require('url');

var log = require('./log');

/**
 * @typedef {Object} ServiceEndpoint
 * @property {boolean} useSSL
 * @property {string} host
 * @property {number} port
 * @property {string} auth
 * @property {string} httpProxy
 * @property {number} cpuLimit
 */

/**
 *
 * @param {ServiceEndpoint} service
 * @param {string}path
 * @param {string} method
 * @param {Object|string} data
 * @param {dataCb} callback
 */
exports.create = (service, path, method, data, callback) => {
    let protocol = service.useSSL ? https : http;
    let buffer = null, err = null;
    let reqOpts = { host: service.host, port: service.port, path, auth: service.auth, headers: {}, method };
    if (service.httpProxy) {
        let httpUrl = url.parse(service.httpProxy);
        reqOpts.host = httpUrl.hostname;
        reqOpts.port = httpUrl.port;
        reqOpts.path = 'http://' + service.host + ':' + service.port + path;
        reqOpts.headers.Host = service.host;
    }
    if (data) {
        if (typeof data == 'object') {
            data = JSON.stringify(data);
        }
        buffer = new Buffer(data, 'utf8');
        reqOpts.headers['Content-Length'] = buffer.length;
    }
    let req = protocol.request(reqOpts, res => {
        let buffers = [];
        let nread = 0;
        res.on('data', chunk => {
            buffers.push(chunk);
            nread += chunk.length;
        });
        res.on('end', () => {
            if (!err) {
                if (!buffers.length) {
                    return callback(null, '');
                }
                var data;
                try {
                    data = JSON.parse(Buffer.concat(buffers).toString());
                } catch(e) {
                    log.debug('Unable to parse json response from server', e);
                    return callback("There was an error trying to parse a json response from the server.");
                }
                return callback(null, data);
            }
        });
    });
    req.on('error', e => {
        err = true;
        // TODO pretty print errors, such as "can't connect"
        switch (e.code) {
            case 'ECONNREFUSED':
                callback('Unable to connect to host ' + service.host + ' on port ' + service.port);
                break;
            default: callback(e);
        }
    });
    req.end(buffer);
};

/**
 *
 * @param {Environment} env
 * @param {ServiceEndpoint} service
 * @param {errorCb} callback
 * @param {number} [timeout=0]  Numberr of seconds to wait before trying again (set automatically)
 * @returns {*}
 */
exports.wait = (env, service, callback, timeout = 0) => {
    if (service.cpuLimit>=100) {
        return callback();
    }
    timeout = Math.min(timeout + 1, 30);
    exports.create(service, '/_nodes/stats/process', 'GET', null, (err, nodesData) => {
        if (err) {
            return callback(err);
        }
        for (let nodeName in nodesData.nodes) {
            let nodeCpu = nodesData.nodes[nodeName].process.cpu.percent;
            if (nodeCpu > service.cpuLimit) {
                let destination = (service.host == env.options.source.host) ? 'source' : 'target';
                log.status('Waiting %s seconds for %s cpu to cool down. Current load is %s%%', timeout, destination, nodeCpu);
                return setTimeout(exports.wait, timeout * 1000, env, service, callback, timeout);
            }
        }
        callback();
    });

};

exports.source = {
    /**
     * @param {Environment} env
     * @param {string} path
     * @param {*} data
     * @param {dataCb} callback
     */
    get: (env, path, data, callback) => {
        if (typeof data == 'function') {
            callback = data;
            data = null;
        }
        exports.wait(env, env.options.source, err => err ? callback(err) : exports.create(env.options.source, path, 'GET', data, callback));
    },

    /**
     * @param {Environment} env
     * @param {string} path
     * @param {*} data
     * @param {dataCb} callback
     */
    post: (env, path, data, callback) => {
        exports.wait(env, env.options.source, err => err ? callback(err) : exports.create(env.options.source, path, 'POST', data, callback));
    }
};

exports.target = {
    /**
     * @param {Environment} env
     * @param {string} path
     * @param {*} data
     * @param {dataCb} callback
     */
    get: (env, path, data, callback) => {
        if (typeof data == 'function') {
            callback = data;
            data = null;
        }
        exports.wait(env, env.options.target, err => err ? callback(err) : exports.create(env.options.target, path, 'GET', data, callback));
    },

    /**
     * @param {Environment} env
     * @param {string} path
     * @param {*} data
     * @param {dataCb} callback
     */
    post: (env, path, data, callback) => {
        exports.wait(env, env.options.target, err => err ? callback(err) : exports.create(env.options.target, path, 'POST', data, callback));
    },

    /**
     * @param {Environment} env
     * @param {string} path
     * @param {*} data
     * @param {dataCb} callback
     */
    put: (env, path, data, callback) => {
        if (typeof data == 'function') {
            callback = data;
            data = null;
        }
        exports.wait(env, env.options.target, err => err ? callback(err) : exports.create(env.options.target, path, 'PUT', data, callback));
    }
};
