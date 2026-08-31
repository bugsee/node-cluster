import * as os from 'node:os';
import * as path from 'node:path';
import * as util from 'node:util';
import cluster from 'node:cluster';
import { EventEmitter } from 'node:events';
import type { ClusterSettings } from 'node:cluster';
import {
    STOP_TIMEOUT_MS,
    SKEPTIC_TIMEOUT_MS,
    MIN_ALIVE_MS,
    ALIVE_TIMEOUT_MS,
    DEFAULT_REPL
} from './constants';
import { startRepl } from './repl';
import type { ReplHandle } from './repl';
import { packageState } from './state';
import type {
    ClusterEmitter,
    ClusterMessageHandler,
    ClusterPrimaryConfig,
    ClusterReplListen,
    ClusterWorker,
    DebugStream,
    RestartSnapshot
} from './types';

function workerList(): ClusterWorker[] {
    const dict = cluster.workers;
    if (!dict) {
        return [];
    }
    return Object.keys(dict)
        .map(function (k) {
            return dict[k] as ClusterWorker | undefined;
        })
        .filter(function (w): w is ClusterWorker {
            return w !== undefined;
        });
}

function sortedWorkers(): ClusterWorker[] {
    return workerList().sort(function (a, b) {
        const aCidx = a.clusterIdx ?? 0;
        const bCidx = b.clusterIdx ?? 0;
        if (aCidx < bCidx) {
            return -1;
        }
        if (bCidx < aCidx) {
            return 1;
        }
        return 0;
    });
}

function isPrimaryProcess(): boolean {
    return cluster.isPrimary === true;
}

export class ClusterPrimary {
    #config: ClusterPrimaryConfig;

    #size: number;

    #env: NodeJS.ProcessEnv;

    #onMessage: ClusterMessageHandler | undefined;

    #signalsEnabled: boolean;

    #stopTimeout: number;

    #skepticTimeout: number;

    #minAliveMs: number;

    #aliveTimeout: number;

    #silenceDebug: boolean;

    #aliveEvent: string;

    #replAddress: ClusterReplListen | undefined;

    #replHelp: string[] | null;

    #replContext: Record<string, unknown>;

    #exitFn: (code: number) => void;

    #emitter = new EventEmitter() as ClusterEmitter;

    #debugStreams: Record<string, DebugStream> = {};

    #nextWorkerIdx = 0;

    #replaceWorkerIdxs: number[] = [];

    #quitting = false;

    #restarting = false;

    #resizing = false;

    #refill = false;

    #resizeTail: Promise<void> = Promise.resolve();

    #started = false;

    #closed = false;

    #exited = false;

    #startPromise: Promise<void> | null = null;

    #disconnectTimers = new Map<number, NodeJS.Timeout>();

    #previousSettings: ClusterSettings | null = null;

    #repl: ReplHandle | null = null;

    #onFork: (worker: ClusterWorker) => void;

    #onSighup: () => void;

    #onSigint: () => void;

    #onSigterm: () => void;

    #onSigabrt: () => void;

    #onProcessExit: () => void;

    constructor(config: string | ClusterPrimaryConfig) {
        const cfg: ClusterPrimaryConfig = typeof config === 'string' ? { exec: config } : config;
        if (!cfg.exec) {
            throw new Error('Must define a \'exec\' script');
        }
        if (!isPrimaryProcess()) {
            throw new Error(
                'Must run in the Node.js cluster primary process'
            );
        }

        this.#config = cfg;
        this.#size = typeof cfg.size === 'number' ? cfg.size : os.cpus().length;
        this.#env = cfg.env || {};
        this.#onMessage = cfg.onMessage || cfg.onmessage;
        this.#signalsEnabled = cfg.signals !== false;
        this.#stopTimeout = cfg.stopTimeout ?? STOP_TIMEOUT_MS;
        this.#skepticTimeout = cfg.skepticTimeout ?? SKEPTIC_TIMEOUT_MS;
        this.#minAliveMs = cfg.minAliveMs ?? MIN_ALIVE_MS;
        this.#aliveTimeout = cfg.aliveTimeout ?? ALIVE_TIMEOUT_MS;
        this.#silenceDebug = Boolean(cfg.silenceDebug);
        this.#aliveEvent = cfg.aliveEvent || 'listening';
        this.#replAddress = typeof cfg.repl !== 'undefined' ? cfg.repl : DEFAULT_REPL;
        this.#replHelp = cfg.replHelp || null;
        this.#replContext = cfg.replContext || {};
        this.#exitFn = cfg.exit || function (code: number) {
            process.exit(code);
        };

        const self = this;
        this.#onFork = function (worker) {
            self.#setupWorker(worker as ClusterWorker);
        };
        this.#onSighup = function () {
            self.restart();
        };
        this.#onSigint = function () {
            self.quit();
        };
        this.#onSigterm = function () {
            self.quit();
        };
        this.#onSigabrt = function () {
            self.quitHard();
        };
        this.#onProcessExit = function () {
            if (!self.#quitting) {
                self.quitHard();
            }
        };
    }

    get size(): number {
        return this.#size;
    }

    start(): Promise<void> {
        if (this.#closed) {
            throw new Error('cluster primary is closed');
        }
        const st = packageState();
        if (this.#started || (st.owner && st.owner !== this)) {
            throw new Error('This process already has a cluster primary');
        }
        this.#started = true;
        st.owner = this;

        const masterConf: ClusterSettings = { exec: path.resolve(this.#config.exec) };
        if (this.#config.silent) {
            masterConf.silent = true;
        }
        if (this.#config.args) {
            masterConf.args = this.#config.args;
        }

        this.#previousSettings = Object.assign({}, cluster.settings);
        cluster.setupPrimary(masterConf);

        cluster.on('fork', this.#onFork);
        if (this.#signalsEnabled) {
            this.#installSignals();
        }

        const self = this;
        this.debug(this.#replAddress ? 'resize and then setup repl' : 'resize');
        this.#startPromise = this.#resizeTo().then(function () {
            if (!self.#closed) {
                self.#startRepl();
            }
        });
        return this.#startPromise;
    }

    emitter(): ClusterEmitter {
        return this.#emitter;
    }

    debug(...args: unknown[]): void {
        if (!this.#silenceDebug) {
            console.error(...args);
        }
        this.#emitter.emit('debug', ...args);
        const msg = util.format(...args);
        const self = this;
        Object.keys(this.#debugStreams).forEach(function (s) {
            const stream = self.#debugStreams[s];
            if (!stream) {
                return;
            }
            try {
                stream.write(msg + '\n');
                if (stream.repl) {
                    stream.repl.displayPrompt();
                }
            } catch {
                delete self.#debugStreams[s];
            }
        });
    }

    resize(n?: number | undefined): void {
        this.#emitter.emit('resize', n);
        const self = this;
        process.nextTick(function () {
            self.#resizeTo(n);
        });
    }

    restart(cb?: (() => void) | undefined): void {
        if (this.#restarting) {
            this.debug('Already restarting.  Cannot restart yet.');
            return;
        }
        const currentWorkers: RestartSnapshot = {};
        workerList().forEach(function (w) {
            currentWorkers[String(w.id)] = { pid: w.pid ?? w.process.pid };
        });
        this.#emitter.emit('restart', currentWorkers);
        const self = this;
        process.nextTick(function () {
            self.#doRestart(function () {
                self.#emitter.emit('restartComplete');
                if (cb) {
                    cb();
                }
            });
        });
    }

    quit(): void {
        this.#emitter.emit('quit');
        const self = this;
        process.nextTick(function () {
            self.#doQuit();
        });
    }

    quitHard(): void {
        this.#emitter.emit('quitHard');
        const self = this;
        process.nextTick(function () {
            self.#size = 0;
            self.#quitting = true;
            self.#doQuit();
        });
    }

    close(): Promise<void> {
        if (this.#closed) {
            return Promise.resolve();
        }
        this.#closed = true;
        this.#quitting = true;
        this.#size = 0;
        this.#removeSignals();
        cluster.removeListener('fork', this.#onFork);

        const st = packageState();
        if (st.owner === this) {
            st.owner = null;
        }
        if (st.singleton === this) {
            st.singleton = null;
        }

        const self = this;
        const pending = this.#startPromise || Promise.resolve();
        return pending.then(function () {
            const replClose = self.#repl ? self.#repl.close() : Promise.resolve();
            self.#repl = null;
            return replClose;
        }).then(function () {
            return self.#killRemaining();
        }).then(function () {
            if (self.#previousSettings) {
                cluster.setupPrimary(self.#previousSettings);
                self.#previousSettings = null;
            }
        });
    }

    #installSignals(): void {
        try {
            process.on('SIGHUP', this.#onSighup);
            process.on('SIGINT', this.#onSigint);
            process.on('SIGTERM', this.#onSigterm);
            process.on('SIGABRT', this.#onSigabrt);
        } catch {
            // Windows: some signals do not exist.
        }
        process.on('exit', this.#onProcessExit);
    }

    #removeSignals(): void {
        process.removeListener('SIGHUP', this.#onSighup);
        process.removeListener('SIGINT', this.#onSigint);
        process.removeListener('SIGTERM', this.#onSigterm);
        process.removeListener('SIGABRT', this.#onSigabrt);
        process.removeListener('exit', this.#onProcessExit);
    }

    #startRepl(): void {
        if (!this.#replAddress) {
            return;
        }
        const self = this;
        this.debug('setup repl');
        this.#repl = startRepl({
            debug: function (...args: unknown[]) {
                self.debug(...args);
            },
            debugStreams: this.#debugStreams,
            resize: function (n?: number | undefined) {
                self.resize(n);
            },
            restart: function (cb?: (() => void) | undefined) {
                self.restart(cb);
            },
            quit: function () {
                self.quit();
            },
            quitHard: function () {
                self.quitHard();
            },
            getSize: function () {
                return self.#size;
            },
            replHelp: this.#replHelp,
            replContext: this.#replContext
        }, this.#replAddress);
    }

    #setupWorker(worker: ClusterWorker): void {
        Object.defineProperty(worker, 'birth', {
            value: Date.now(),
            enumerable: true,
            writable: true,
            configurable: true
        });
        Object.defineProperty(worker, 'age', {
            get: function (this: ClusterWorker) {
                return Date.now() - (this.birth ?? 0);
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(worker, 'pid', {
            value: worker.process.pid,
            enumerable: true,
            writable: true,
            configurable: true
        });
        const id = worker.id;
        this.debug('Worker %j setting up', id);
        if (this.#onMessage) {
            worker.on('message', this.#onMessage);
        }

        const self = this;
        worker.on('exit', function () {
            const timer = self.#disconnectTimers.get(id);
            if (timer) {
                clearTimeout(timer);
                self.#disconnectTimers.delete(id);
            }

            if (!worker.exitedAfterDisconnect) {
                self.debug('Worker %j exited abnormally, idx:', id, worker.clusterIdx);
                if (typeof worker.clusterIdx === 'number') {
                    self.#replaceWorkerIdxs.push(worker.clusterIdx);
                }
                if ((worker.age ?? 0) < self.#minAliveMs) {
                    self.debug('Worker %j died too quickly, not respawning.', id);
                    return;
                }
            } else {
                self.debug('Worker %j exited', id);
            }

            if (self.#closed || self.#quitting || self.#restarting) {
                return;
            }
            if (workerList().length < self.#size) {
                if (self.#resizing) {
                    self.#refill = true;
                } else {
                    self.#resizeTo();
                }
            }
        });

        worker.on('disconnect', function () {
            self.debug('Worker %j disconnect', id);
            const disconnectTimer = setTimeout(function () {
                self.debug('Worker %j, forcefully killing', id);
                if (worker.process) {
                    worker.process.kill('SIGKILL');
                }
            }, self.#stopTimeout);
            self.#disconnectTimers.set(id, disconnectTimer);
        });
    }

    #getNextWorkerIdx(): number {
        const size = this.#size;
        this.#replaceWorkerIdxs = this.#replaceWorkerIdxs.filter(function (wid) {
            return wid < size;
        });
        if (this.#replaceWorkerIdxs.length) {
            return this.#replaceWorkerIdxs.shift() ?? 0;
        }
        if (this.#nextWorkerIdx >= this.#size) {
            this.#nextWorkerIdx = 0;
        }
        const idx = this.#nextWorkerIdx;
        this.#nextWorkerIdx = idx + 1;
        return idx;
    }

    #forkChild(): ClusterWorker {
        if (this.#closed) {
            throw new Error('cluster primary is closed');
        }
        const childEnv: NodeJS.ProcessEnv = {};
        const env = this.#env;
        Object.keys(env).forEach(function (k) {
            childEnv[k] = env[k];
        });
        const nextIdx = this.#getNextWorkerIdx();
        childEnv.CLUSTER_IDX = String(nextIdx);
        const cp = cluster.fork(childEnv) as ClusterWorker;
        cp.clusterIdx = nextIdx;
        return cp;
    }

    #forkAndWaitAlive(): Promise<ClusterWorker> {
        if (this.#closed) {
            return Promise.reject(new Error('cluster primary is closed'));
        }
        try {
            return this.#waitAlive(this.#forkChild());
        } catch (err) {
            return Promise.reject(err);
        }
    }

    #waitAlive(worker: ClusterWorker): Promise<ClusterWorker> {
        const self = this;
        return new Promise(function (resolve, reject) {
            let done = false;
            let onAlive: () => void = function () {};
            let onExit: () => void = function () {};
            let timer: NodeJS.Timeout | undefined;
            function finish(err: Error | null, value?: ClusterWorker): void {
                if (done) {
                    return;
                }
                done = true;
                if (timer) {
                    clearTimeout(timer);
                }
                worker.removeListener(self.#aliveEvent, onAlive);
                worker.removeListener('exit', onExit);
                if (err) {
                    reject(err);
                } else if (value) {
                    resolve(value);
                } else {
                    reject(new Error('fork failed'));
                }
            }
            onAlive = function () {
                finish(null, worker);
            };
            onExit = function () {
                finish(new Error('Worker exited before ' + self.#aliveEvent));
            };
            if (self.#aliveTimeout > 0) {
                timer = setTimeout(function () {
                    if (worker.process) {
                        worker.process.kill('SIGKILL');
                    }
                    finish(new Error('Worker timed out waiting for ' + self.#aliveEvent));
                }, self.#aliveTimeout);
            }
            worker.once(self.#aliveEvent, onAlive);
            worker.once('exit', onExit);
        });
    }

    #waitSkeptic(newbie: ClusterWorker): Promise<boolean> {
        const self = this;
        return new Promise(function (resolve) {
            let settled = false;
            let onExit: () => void;
            const timer = setTimeout(function () {
                if (settled) {
                    return;
                }
                settled = true;
                newbie.removeListener('exit', onExit);
                resolve(true);
            }, self.#skepticTimeout);
            onExit = function () {
                if (settled) {
                    return;
                }
                settled = true;
                clearTimeout(timer);
                resolve(false);
            };
            newbie.on('exit', onExit);
        });
    }

    #emitAndDisconnect(worker: ClusterWorker): void {
        this.#emitter.emit('disconnect', worker);
        process.nextTick(function () {
            if (worker.process && worker.process.connected) {
                worker.disconnect();
            }
        });
    }

    #disconnectAndWaitExit(worker: ClusterWorker): Promise<void> {
        const self = this;
        return new Promise(function (resolve) {
            if (worker.isDead()) {
                resolve();
                return;
            }
            worker.once('exit', function () {
                resolve();
            });
            if (worker.process && worker.process.connected) {
                self.#emitAndDisconnect(worker);
                return;
            }
            if (worker.process) {
                worker.process.kill('SIGKILL');
            }
        });
    }

    #resizeTo(n?: number | (() => void), cb?: (() => void) | undefined): Promise<void> {
        let target: number | undefined;
        let done: (() => void) | undefined = cb;
        if (typeof n === 'function') {
            done = n;
            target = undefined;
        } else {
            target = n;
        }
        if (typeof target === 'number' && target >= 0) {
            if (target < this.#size) {
                this.#nextWorkerIdx = target;
            }
            this.#size = target;
        }

        const self = this;
        this.#resizeTail = this.#resizeTail.then(
            function () {
                return self.#matchSize();
            },
            function () {
                return self.#matchSize();
            }
        );
        const run = this.#resizeTail;
        if (done) {
            const finished = done;
            run.then(function () {
                finished();
            }, function () {
                finished();
            });
        }
        return run;
    }

    #matchSize(): Promise<void> {
        if (this.#closed) {
            return Promise.resolve();
        }
        this.#resizing = true;
        this.#refill = false;
        const self = this;

        function wave(): Promise<void> {
            if (self.#closed) {
                return Promise.resolve();
            }
            const current = sortedWorkers();
            const req = self.#size - current.length;
            if (req === 0) {
                return Promise.resolve();
            }
            if (req > 0) {
                const before = current.length;
                const forks: Promise<ClusterWorker>[] = [];
                for (let i = 0; i < req; i += 1) {
                    self.debug('resizing up', req - i - 1);
                    forks.push(self.#forkAndWaitAlive());
                }
                return Promise.allSettled(forks).then(function () {
                    if (self.#closed) {
                        return undefined;
                    }
                    const after = workerList().length;
                    if (after <= before) {
                        return undefined;
                    }
                    return wave();
                });
            }
            const extras = current.slice(self.#size);
            return Promise.all(extras.map(function (worker) {
                self.debug('resizing down', worker.id);
                return self.#disconnectAndWaitExit(worker);
            })).then(function () {
                return wave();
            });
        }

        return wave().then(function () {
            self.#resizing = false;
            if (self.#refill && !self.#closed && !self.#restarting && !self.#quitting) {
                self.#refill = false;
                return self.#matchSize();
            }
            return undefined;
        }, function () {
            self.#resizing = false;
            return undefined;
        });
    }

    #doRestart(cb?: (() => void) | undefined): void {
        const self = this;

        function finish(): void {
            self.#restarting = false;
            if (!self.#closed && !self.#quitting && workerList().length < self.#size) {
                self.#resizeTo();
            }
            if (cb) {
                cb();
            }
        }

        if (this.#restarting) {
            this.debug('Already restarting.  Cannot restart yet.');
            return;
        }
        this.#restarting = true;

        const current = Object.keys(cluster.workers || {});
        const reqs = this.#size - current.length;

        if (reqs !== 0) {
            this.debug('resize %d -> %d, change = %d', current.length, this.#size, reqs);
            this.#resizeTo(this.#size, function () {
                self.debug('resize cb');
                self.#rollingReplace(Object.keys(cluster.workers || {}), finish);
            });
            return;
        }

        this.#rollingReplace(current, finish);
    }

    #rollingReplace(ids: string[], cb: () => void): void {
        const self = this;
        let i = 0;

        function next(): void {
            if (self.#closed) {
                cb();
                return;
            }
            if (i >= ids.length) {
                self.debug('graceful completion');
                cb();
                return;
            }

            self.debug('graceful %d of %d', i, ids.length);
            const first = i === 0;
            const id = ids[i];
            i += 1;
            const worker = id !== undefined && cluster.workers ? cluster.workers[id] : undefined;

            if (self.#quitting) {
                if (worker && worker.process && worker.process.connected) {
                    self.#emitAndDisconnect(worker);
                }
                next();
                return;
            }

            self.#forkAndWaitAlive().then(function (newbie) {
                if (!first) {
                    if (worker && worker.process && worker.process.connected) {
                        self.#emitAndDisconnect(worker);
                    }
                    return undefined;
                }
                return self.#waitSkeptic(newbie).then(function (ok) {
                    if (!ok) {
                        throw new Error('abort-restart');
                    }
                    if (worker && worker.process && worker.process.connected) {
                        self.#emitAndDisconnect(worker);
                    }
                    return undefined;
                });
            }).then(function () {
                next();
            }).catch(function () {
                self.debug('New worker died quickly. Aborting restart.');
                cb();
            });
        }

        next();
    }

    #doQuit(): void {
        if (this.#quitting) {
            this.debug('Forceful shutdown');
            this.#size = 0;
            workerList().forEach(function (w) {
                if (w.process) {
                    w.process.kill('SIGKILL');
                }
            });
            this.#exitOnce(1);
            return;
        }

        this.debug('Graceful shutdown...');
        this.#size = 0;
        this.#quitting = true;
        const self = this;
        this.#resizeTo(0).then(function () {
            self.debug('Graceful shutdown successful');
            self.#exitOnce(0);
        });
    }

    #exitOnce(code: number): void {
        if (this.#exited) {
            return;
        }
        this.#exited = true;
        this.#exitFn(code);
    }

    #killRemaining(): Promise<void> {
        this.#disconnectTimers.forEach(function (timer) {
            clearTimeout(timer);
        });
        this.#disconnectTimers.clear();

        const remaining = workerList().filter(function (w) {
            return !w.isDead();
        });

        if (remaining.length === 0) {
            return Promise.resolve();
        }

        return new Promise(function (resolve) {
            let left = remaining.length;
            remaining.forEach(function (w) {
                w.once('exit', function () {
                    left -= 1;
                    if (left === 0) {
                        resolve();
                    }
                });
                if (w.process) {
                    w.process.kill('SIGKILL');
                }
            });
        });
    }
}
