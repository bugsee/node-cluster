'use strict';

const net = require('net');

const server = net.createServer();
server.listen(0);

process.on('message', function (msg) {
    if (!msg || msg.type !== 'cluster-test') {
        return;
    }
    if (msg.cmd === 'die') {
        process.exit(typeof msg.code === 'number' ? msg.code : 1);
        return;
    }
    if (msg.cmd === 'ping') {
        process.send({
            type: 'cluster-test',
            cmd: 'pong',
            idx: process.env.CLUSTER_IDX,
            pid: process.pid
        }, function () {});
    }
});

process.send({
    type: 'cluster-test',
    cmd: 'ready',
    idx: process.env.CLUSTER_IDX,
    pid: process.pid
}, function () {});
