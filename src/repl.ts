import * as fs from 'node:fs';
import * as net from 'node:net';
import * as path from 'node:path';
import * as repl from 'node:repl';
import cluster from 'node:cluster';
import type { Worker } from 'node:cluster';
import type { REPLServer } from 'node:repl';
import type { ClusterReplListen, DebugStream } from './types';

export interface ReplHost {
    debug: (...args: unknown[]) => void;
    debugStreams: Record<string, DebugStream>;
    resize: (n?: number | undefined) => void;
    restart: (cb?: (() => void) | undefined) => void;
    quit: () => void;
    quitHard: () => void;
    getSize: () => number;
    replHelp: string[] | null;
    replContext: Record<string, unknown>;
}

export interface ReplHandle {
    close: () => Promise<void>;
}

function select(workers: NodeJS.Dict<Worker> | undefined, field: string): Record<string, unknown> {
    const set: Record<string, unknown> = {};
    if (!workers) {
        return set;
    }
    Object.keys(workers).forEach(function (k) {
        const w = workers[k];
        if (w) {
            set[k] = (w as unknown as Record<string, unknown>)[field];
        }
    });
    return set;
}

function createReplWorker(d: { id: string; pid: unknown; state: unknown; age: unknown }): {
    id: string;
    pid: unknown;
    state: unknown;
    age: unknown;
    disconnect: () => void;
    kill: () => void;
} {
    return {
        id: d.id,
        pid: d.pid,
        state: d.state,
        age: d.age,
        disconnect: function () {
            const w = cluster.workers?.[d.id];
            if (w) {
                w.disconnect();
            }
        },
        kill: function () {
            if (typeof d.pid === 'number') {
                process.kill(d.pid);
            }
        }
    };
}

function isAddressPort(value: ClusterReplListen): value is { address: string; port: number } {
    return typeof value === 'object' && value !== null && 'address' in value && 'port' in value;
}

/**
 * Optional REPL on a unix socket, TCP port, or `{ address, port }`.
 * TCP has no authentication — bind it only on a trusted interface.
 */
export function startRepl(host: ReplHost, replAddressPath: Exclude<ClusterReplListen, false | null>): ReplHandle {
    let listenTarget: string | number | null = null;
    let socketAddress: string | undefined;
    if (typeof replAddressPath === 'string') {
        listenTarget = path.resolve(replAddressPath);
    } else if (typeof replAddressPath === 'number') {
        listenTarget = replAddressPath;
    } else if (isAddressPort(replAddressPath)) {
        listenTarget = replAddressPath.port;
        socketAddress = replAddressPath.address;
    }

    let connections = 0;
    let nextSockId = 0;
    let replServer: net.Server | null = null;
    let closed = false;
    const socketPath = typeof listenTarget === 'string' ? listenTarget : null;

    function attachContext(r: REPLServer, sock: DebugStream): void {
        const helpCommands = [
            'help        - display these commands',
            'repl        - access the REPL',
            'resize(n)   - resize the cluster to `n` workers',
            'restart(cb) - gracefully restart workers, cb is optional',
            'stop()      - gracefully stop workers and primary',
            'kill()      - forcefully kill workers and primary',
            'cluster     - node.js cluster module',
            'size        - current cluster size',
            'connections - number of REPL connections to primary',
            'workers     - current workers',
            'select(fld) - map of id to field (from workers)',
            'pids        - map of id to pids',
            'ages        - map of id to worker ages',
            'states      - map of id to worker states',
            'debug(a1)   - output `a1` to stdout and all REPLs',
            'sock        - this REPL socket',
            '.exit       - close this connection to the REPL'
        ];
        if (Array.isArray(host.replHelp)) {
            host.replHelp.forEach(function (line) {
                helpCommands.push(line);
            });
        }

        const context = {
            help: helpCommands,
            repl: r,
            resize: host.resize,
            restart: host.restart,
            stop: host.quit,
            kill: host.quitHard,
            cluster: cluster,
            get size() {
                return host.getSize();
            },
            get connections() {
                return connections;
            },
            get workers() {
                const p = select(cluster.workers, 'pid');
                const s = select(cluster.workers, 'state');
                const a = select(cluster.workers, 'age');
                return Object.keys(cluster.workers || {}).map(function (k) {
                    return createReplWorker({
                        id: k,
                        pid: p[k],
                        state: s[k],
                        age: a[k]
                    });
                });
            },
            select: function (field: string) {
                return select(cluster.workers, field);
            },
            get pids() {
                return select(cluster.workers, 'pid');
            },
            get ages() {
                return select(cluster.workers, 'age');
            },
            get states() {
                return select(cluster.workers, 'state');
            },
            debug: host.debug,
            sock: sock
        };

        const desc: PropertyDescriptorMap = {};
        Object.getOwnPropertyNames(context).forEach(function (n) {
            const property = Object.getOwnPropertyDescriptor(context, n);
            if (property) {
                desc[n] = property;
            }
        });
        Object.getOwnPropertyNames(host.replContext).forEach(function (n) {
            const property = Object.getOwnPropertyDescriptor(host.replContext, n);
            if (property) {
                desc[n] = property;
            }
        });
        Object.defineProperties(r.context, desc);
    }

    function onConnection(sock: DebugStream): void {
        connections += 1;
        const sockId = nextSockId;
        nextSockId += 1;
        const streamKey = 'repl-' + sockId;
        const debugStreams = host.debugStreams;
        Object.defineProperty(sock, 'id', {
            value: sockId,
            enumerable: true,
            writable: true,
            configurable: true
        });
        debugStreams[streamKey] = sock;

        sock.write('Starting repl #' + String(sock.id));
        const r = repl.start({
            prompt: 'cluster (`help` for cmds) ' + process.pid + ' ' + String(sock.id) + '> ',
            input: sock,
            output: sock,
            terminal: true,
            useGlobal: false,
            ignoreUndefined: true
        });

        attachContext(r, sock);
        Object.defineProperty(sock, 'repl', {
            value: r,
            enumerable: true,
            writable: true,
            configurable: true
        });

        let ended = false;
        let replEnded = false;

        function end(): void {
            if (ended) {
                return;
            }
            ended = true;
            if (!replEnded) {
                r.close();
            }
            delete debugStreams[streamKey];
        }

        if (r.commands && r.commands['.exit']) {
            r.commands['.exit'].action = function () {
                end();
                sock.end();
            };
        }

        r.on('exit', function () {
            connections -= 1;
            replEnded = true;
            if (!ended) {
                sock.end();
            }
        });

        sock.on('end', end);
        sock.on('close', end);
        sock.on('error', end);
    }

    function listen(): void {
        if (closed || listenTarget === null) {
            return;
        }
        replServer = net.createServer(onConnection);
        function onListening(): void {
            if (closed) {
                return;
            }
            if (socketAddress) {
                host.debug('cluster repl listening on ' + socketAddress + ':' + String(listenTarget));
            } else {
                host.debug('cluster repl listening on ' + String(listenTarget));
            }
        }
        if (socketAddress) {
            replServer.listen(Number(listenTarget), socketAddress, onListening);
        } else if (listenTarget !== null) {
            replServer.listen(listenTarget, onListening);
        }
    }

    let resolveListenReady: () => void = function () {};
    const listenReady = new Promise<void>(function (resolve) {
        resolveListenReady = resolve;
    });

    if (socketPath) {
        fs.unlink(socketPath, function (err) {
            if (err && (err as NodeJS.ErrnoException).code !== 'ENOENT') {
                host.debug('repl socket unlink failed', err);
            }
            if (!closed) {
                listen();
            }
            resolveListenReady();
        });
    } else {
        listen();
        resolveListenReady();
    }

    return {
        close: function () {
            closed = true;
            return listenReady.then(function () {
                const debugStreams = host.debugStreams;
                Object.keys(debugStreams).forEach(function (key) {
                    try {
                        debugStreams[key]?.destroy();
                    } catch {
                        // ignore
                    }
                    delete debugStreams[key];
                });
                if (!replServer) {
                    return undefined;
                }
                const server = replServer;
                replServer = null;
                return new Promise<void>(function (resolve) {
                    server.close(function () {
                        resolve();
                    });
                });
            });
        }
    };
}

export { select };
