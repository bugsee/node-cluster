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
    DEFAULT_REPL
} from './constants';
import { startRepl } from './repl';
import type { ReplHandle } from './repl';
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

    #started = false;

    #closed = false;

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
                'ClusterMaster answers to no one!\n' +
                '(don\'t run in a cluster worker script)'
            );
        }

        this.#config = cfg;
        this.#size = typeof cfg.size === 'number' ? cfg.size : os.cpus().length;
        this.#env = cfg.env || {};
        this.#onMessage = cfg.onMessage || cfg.onmessage;
        this.#signalsEnabled = cfg.signals !== false;
        this.#stopTimeout = cfg.stopTimeout || STOP_TIMEOUT_MS;
        this.#skepticTimeout = cfg.skepticTimeout || SKEPTIC_TIMEOUT_MS;
        this.#minAliveMs = cfg.minAliveMs || MIN_ALIVE_MS;
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
        if (this.#started) {
            throw new Error('This cluster has a master already');
        }
        this.#started = true;

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
        return this.#resizeTo().then(function () {
            if (!self.#closed) {
                self.#startRepl();
            }
        });
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

        const self = this;
        const replClose = this.#repl ? this.#repl.close() : Promise.resolve();
        this.#repl = null;

        return replClose.then(function () {
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

            if (workerList().length < self.#size && !self.#resizing) {
                self.#resizeTo();
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

    #waitAlive(worker: ClusterWorker): Promise<ClusterWorker> {
        const self = this;
        return new Promise(function (resolve, reject) {
            let done = false;
            let onAlive: () => void;
            let onExit: () => void;
            function finish(err: Error | null, value?: ClusterWorker): void {
                if (done) {
                    return;
                }
                done = true;
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
            worker.once(self.#aliveEvent, onAlive);
            worker.once('exit', onExit);
        });
    }

    #forkAndWaitAlive(): Promise<ClusterWorker> {
        return this.#waitAlive(this.#forkChild());
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
            }
        });
    }

    #resizeTo(n?: number | (() => void), cb?: (() => void) | undefined): Promise<void> {
        let target: number | undefined;
        let done: (() => void) | undefined = cb;
        if (typeof n === 'function') {
            done = n;
            target = this.#size;
        } else {
            target = n;
        }

        const p = this.#resizeInner(target);
        if (done) {
            const finished = done;
            p.then(function () {
                finished();
            }, function () {
                finished();
            });
        }
        return p;
    }

    #resizeInner(n: number | undefined): Promise<void> {
        if (this.#closed) {
            return Promise.resolve();
        }
        if (this.#resizing) {
            return Promise.resolve();
        }
        if (typeof n === 'number' && n >= 0) {
            if (n < this.#size) {
                this.#nextWorkerIdx = n;
            }
            this.#size = n;
        }

        const current = sortedWorkers();
        const req = this.#size - current.length;
        if (req === 0) {
            return Promise.resolve();
        }

        this.#resizing = true;
        const self = this;
        let work: Promise<unknown>;
        if (req > 0) {
            const forks: Promise<ClusterWorker>[] = [];
            for (let i = 0; i < req; i += 1) {
                this.debug('resizing up', req - i - 1);
                forks.push(this.#forkAndWaitAlive());
            }
            work = Promise.all(forks);
        } else {
            const extras = current.slice(this.#size);
            work = Promise.all(extras.map(function (worker) {
                self.debug('resizing down', worker.id);
                return self.#disconnectAndWaitExit(worker);
            }));
        }

        return work.then(function () {
            self.#resizing = false;
        }, function () {
            self.#resizing = false;
        });
    }

    #doRestart(cb?: (() => void) | undefined): void {
        if (this.#restarting) {
            this.debug('Already restarting.  Cannot restart yet.');
            return;
        }
        this.#restarting = true;

        const current = Object.keys(cluster.workers || {});
        const reqs = this.#size - current.length;
        const self = this;

        function finish(): void {
            self.#restarting = false;
            if (cb) {
                cb();
            }
        }

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
                self.#restarting = false;
            });
        }

        next();
    }

    #doQuit(): void {
        if (this.#quitting) {
            this.debug('Forceful shutdown');
            workerList().forEach(function (w) {
                if (w.process) {
                    w.process.kill('SIGKILL');
                }
            });
            this.#exitFn(1);
            return;
        }

        this.debug('Graceful shutdown...');
        this.#size = 0;
        this.#quitting = true;
        const self = this;
        this.#resizeTo(0).then(function () {
            self.debug('Graceful shutdown successful');
            self.#exitFn(0);
        });
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
