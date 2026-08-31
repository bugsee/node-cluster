'use strict';

const path = require('path');
const os = require('os');
const fs = require('fs');
const net = require('net');
const cluster = require('cluster');
const { expect } = require('chai');

const clusterPrimary = require('../dist/index.cjs');

const { ClusterPrimary } = clusterPrimary;

const workerPath = path.join(__dirname, 'cluster-worker.cjs');

function waitUntil(fn, timeoutMs) {
    return new Promise(function (resolve, reject) {
        const start = Date.now();
        function check() {
            if (fn()) {
                resolve();
                return;
            }
            if (Date.now() - start > timeoutMs) {
                reject(new Error('timeout waiting, workers=' + Object.keys(cluster.workers).length));
                return;
            }
            setTimeout(check, 25);
        }
        check();
    });
}

function workerCount() {
    return Object.keys(cluster.workers).length;
}

function waitForCount(n) {
    return waitUntil(function () {
        return workerCount() === n;
    }, 5000);
}

function testConfig(extra) {
    return Object.assign({
        exec: workerPath,
        size: 2,
        silent: true,
        signals: false,
        repl: false,
        silenceDebug: true,
        skepticTimeout: 50,
        stopTimeout: 200,
        exit: function () {}
    }, extra || {});
}

describe('@bugsee/node-cluster', function () {
    this.timeout(8000);

    let previousSettings;

    before(function () {
        previousSettings = Object.assign({}, cluster.settings);
    });

    after(function () {
        if (previousSettings) {
            cluster.setupPrimary(previousSettings);
        }
    });

    describe('dual module shape', function () {
        it('CJS require() is the callable default with named properties', function () {
            expect(clusterPrimary).to.be.a('function');
            expect(clusterPrimary.ClusterPrimary).to.equal(ClusterPrimary);
            expect(clusterPrimary.default).to.equal(clusterPrimary);
            expect(clusterPrimary.constants.MIN_ALIVE_MS).to.equal(2000);
            expect(clusterPrimary.constants.ALIVE_TIMEOUT_MS).to.equal(30000);
        });

        it('ESM import default is the same callable', async function () {
            const mod = await import('../dist/index.js');
            expect(mod.default).to.be.a('function');
            expect(mod.ClusterPrimary).to.equal(mod.default.ClusterPrimary);
            expect(mod.default.default).to.equal(mod.default);
            expect(mod.default.constants.STOP_TIMEOUT_MS).to.equal(5000);
        });
    });

    describe('lifecycle', function () {
        let primary;
        let pingThis = null;

        function onMessage(msg) {
            if (msg && msg.type === 'cluster-test' && msg.cmd === 'pong') {
                pingThis = this;
            }
        }

        before(async function () {
            primary = new ClusterPrimary(testConfig({ onMessage: onMessage }));
            await primary.start();
        });

        after(async function () {
            if (primary) {
                await primary.close();
            }
        });

        it('forks the requested number of workers', function () {
            expect(workerCount()).to.equal(2);
            expect(primary.size).to.equal(2);
        });

        it('assigns CLUSTER_IDX and clusterIdx', function () {
            const idxs = Object.keys(cluster.workers).map(function (id) {
                return cluster.workers[id].clusterIdx;
            }).sort();
            expect(idxs).to.deep.equal([0, 1]);
        });

        it('calls onMessage with this bound to the Worker', async function () {
            pingThis = null;
            const worker = cluster.workers[Object.keys(cluster.workers)[0]];
            await new Promise(function (resolve, reject) {
                const timer = setTimeout(function () {
                    reject(new Error('pong timeout'));
                }, 2000);
                function onMsg(msg) {
                    if (msg && msg.type === 'cluster-test' && msg.cmd === 'pong') {
                        worker.removeListener('message', onMsg);
                        clearTimeout(timer);
                        resolve();
                    }
                }
                worker.on('message', onMsg);
                worker.send({ type: 'cluster-test', cmd: 'ping' }, function () {});
            });
            expect(pingThis).to.equal(worker);
        });

        it('resizes up', async function () {
            primary.resize(3);
            await waitForCount(3);
            expect(primary.size).to.equal(3);
        });

        it('resizes down (highest clusterIdx first)', async function () {
            primary.resize(2);
            await waitForCount(2);
            const idxs = Object.keys(cluster.workers).map(function (id) {
                return cluster.workers[id].clusterIdx;
            }).sort();
            expect(idxs).to.deep.equal([0, 1]);
        });

        it('honors the last overlapping resize target', async function () {
            primary.resize(4);
            primary.resize(2);
            await waitForCount(2);
            expect(primary.size).to.equal(2);
            await new Promise(function (resolve) {
                setTimeout(resolve, 200);
            });
            expect(workerCount()).to.equal(2);
        });

        it('replaces workers on rolling restart', async function () {
            const before = Object.keys(cluster.workers).map(function (id) {
                return cluster.workers[id].process.pid;
            }).sort();
            await new Promise(function (resolve, reject) {
                const timer = setTimeout(function () {
                    reject(new Error('restart timeout'));
                }, 5000);
                primary.emitter().once('restartComplete', function () {
                    clearTimeout(timer);
                    resolve();
                });
                primary.restart();
            });
            await waitForCount(2);
            const after = Object.keys(cluster.workers).map(function (id) {
                return cluster.workers[id].process.pid;
            }).sort();
            expect(after).to.have.length(2);
            expect(after).to.not.deep.equal(before);
        });
    });

    describe('fast death', function () {
        let primary;

        before(async function () {
            primary = new ClusterPrimary(testConfig({
                size: 1,
                minAliveMs: 250
            }));
            await primary.start();
        });

        after(async function () {
            if (primary) {
                await primary.close();
            }
        });

        it('does not respawn a worker that dies immediately', async function () {
            expect(workerCount()).to.equal(1);
            const worker = cluster.workers[Object.keys(cluster.workers)[0]];
            await new Promise(function (resolve) {
                worker.on('exit', resolve);
                worker.send({ type: 'cluster-test', cmd: 'die' }, function () {});
            });
            await new Promise(function (resolve) {
                setTimeout(resolve, 400);
            });
            expect(workerCount()).to.equal(0);
            expect(primary.size).to.equal(1);
        });
    });

    describe('respawn after minAlive', function () {
        let primary;

        before(async function () {
            primary = new ClusterPrimary(testConfig({
                size: 1,
                minAliveMs: 50
            }));
            await primary.start();
        });

        after(async function () {
            if (primary) {
                await primary.close();
            }
        });

        it('respawns after a worker lives past minAliveMs then dies', async function () {
            expect(workerCount()).to.equal(1);
            const oldId = Object.keys(cluster.workers)[0];
            const oldPid = cluster.workers[oldId].process.pid;
            await new Promise(function (resolve) {
                setTimeout(resolve, 80);
            });
            await new Promise(function (resolve) {
                cluster.workers[oldId].on('exit', resolve);
                cluster.workers[oldId].process.kill('SIGKILL');
            });
            await waitForCount(1);
            const next = cluster.workers[Object.keys(cluster.workers)[0]];
            expect(next.process.pid).to.not.equal(oldPid);
        });
    });

    describe('quitHard', function () {
        let primary;
        let exitCode;

        before(async function () {
            exitCode = undefined;
            primary = new ClusterPrimary(testConfig({
                size: 2,
                exit: function (code) {
                    exitCode = code;
                }
            }));
            await primary.start();
        });

        after(async function () {
            if (primary) {
                await primary.close();
            }
        });

        it('SIGKILLs workers and calls exit(1)', async function () {
            expect(workerCount()).to.equal(2);
            primary.quitHard();
            await waitUntil(function () {
                return exitCode === 1 && workerCount() === 0;
            }, 4000);
            expect(exitCode).to.equal(1);
            expect(workerCount()).to.equal(0);
        });
    });

    describe('singleton API', function () {
        after(async function () {
            await clusterPrimary.close();
        });

        it('starts once and throws on a second start', async function () {
            clusterPrimary(testConfig({ size: 1 }));
            await waitForCount(1);
            expect(function () {
                clusterPrimary(testConfig({ size: 1 }));
            }).to.throw(/already has a cluster primary/);
        });
    });

    describe('REPL', function () {
        let primary;
        const sockPath = path.join(os.tmpdir(), 'node-cluster-' + process.pid + '.sock');

        before(async function () {
            try {
                fs.unlinkSync(sockPath);
            } catch {
                // missing is fine
            }
            primary = new ClusterPrimary(testConfig({
                size: 1,
                repl: sockPath
            }));
            const listening = new Promise(function (resolve) {
                function onDebug(...args) {
                    const text = args.join(' ');
                    if (String(text).indexOf('repl listening') !== -1) {
                        primary.emitter().removeListener('debug', onDebug);
                        resolve();
                    }
                }
                primary.emitter().on('debug', onDebug);
            });
            await primary.start();
            await listening;
        });

        after(async function () {
            if (primary) {
                await primary.close();
            }
            try {
                fs.unlinkSync(sockPath);
            } catch {
                // ignore
            }
        });

        it('accepts a unix-socket connection', async function () {
            await new Promise(function (resolve, reject) {
                const sock = net.connect(sockPath);
                const timer = setTimeout(function () {
                    sock.destroy();
                    reject(new Error('repl connect timeout'));
                }, 2000);
                sock.on('error', function (err) {
                    clearTimeout(timer);
                    reject(err);
                });
                sock.on('data', function () {
                    clearTimeout(timer);
                    sock.end();
                    resolve();
                });
            });
        });
    });

    describe('REPL close during bind', function () {
        let primary;
        const sockPath = path.join(os.tmpdir(), 'node-cluster-close-' + process.pid + '.sock');

        after(async function () {
            if (primary) {
                await primary.close();
            }
            try {
                fs.unlinkSync(sockPath);
            } catch {
                // ignore
            }
        });

        it('does not leave a listening socket', async function () {
            try {
                fs.unlinkSync(sockPath);
            } catch {
                // missing is fine
            }
            primary = new ClusterPrimary(testConfig({
                size: 0,
                repl: sockPath
            }));
            const started = primary.start();
            await primary.close();
            await started;
            primary = null;
            await new Promise(function (resolve) {
                setTimeout(resolve, 150);
            });
            await new Promise(function (resolve, reject) {
                const sock = net.connect(sockPath);
                const timer = setTimeout(function () {
                    sock.destroy();
                    reject(new Error('repl still listening after close'));
                }, 500);
                sock.on('connect', function () {
                    clearTimeout(timer);
                    sock.destroy();
                    reject(new Error('repl still listening after close'));
                });
                sock.on('error', function () {
                    clearTimeout(timer);
                    resolve();
                });
            });
        });
    });

    describe('process-wide primary lock', function () {
        let primary;

        after(async function () {
            if (primary) {
                await primary.close();
                primary = null;
            }
            await clusterPrimary.close();
        });

        it('rejects a second ClusterPrimary.start()', async function () {
            primary = new ClusterPrimary(testConfig({ size: 1 }));
            await primary.start();
            const other = new ClusterPrimary(testConfig({ size: 1 }));
            expect(function () {
                other.start();
            }).to.throw(/already has a cluster primary/);
        });

        it('rejects a second start across require and import', async function () {
            const mod = await import('../dist/index.js');
            expect(function () {
                mod.default(testConfig({ size: 1 }));
            }).to.throw(/already has a cluster primary/);
        });
    });

    describe('restart abort', function () {
        let primary;

        after(async function () {
            if (primary) {
                await primary.close();
            }
        });

        it('emits restartComplete when the first newbie dies during skeptic', async function () {
            primary = new ClusterPrimary(testConfig({
                size: 1,
                skepticTimeout: 400
            }));
            await primary.start();
            expect(workerCount()).to.equal(1);
            const origId = Object.keys(cluster.workers)[0];
            const origPid = cluster.workers[origId].process.pid;

            const completed = new Promise(function (resolve, reject) {
                const timer = setTimeout(function () {
                    reject(new Error('restartComplete timeout'));
                }, 5000);
                primary.emitter().once('restartComplete', function () {
                    clearTimeout(timer);
                    resolve();
                });
            });

            primary.restart();
            await waitUntil(function () {
                return workerCount() === 2;
            }, 4000);

            const newbie = Object.keys(cluster.workers).map(function (id) {
                return cluster.workers[id];
            }).find(function (w) {
                return w.process.pid !== origPid;
            });
            expect(newbie).to.not.equal(undefined);
            newbie.process.kill('SIGKILL');
            await completed;
        });
    });
});
