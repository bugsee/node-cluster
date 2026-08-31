import * as node_net from 'node:net';
import { Socket } from 'node:net';
import { EventEmitter } from 'node:events';
import { Worker } from 'node:cluster';
import { REPLServer } from 'node:repl';

/** Worker plus fields this primary attaches (`CLUSTER_IDX`, `age`, `pid`). */
type ClusterWorker = Worker & {
    clusterIdx?: number | undefined;
    birth?: number | undefined;
    readonly age?: number | undefined;
    pid?: number | undefined;
};
/**
 * IPC handler attached with `worker.on('message', handler)` so `this` is the Worker.
 * Same contract as cluster-master-ext / the appserver cache L2 handler.
 */
type ClusterMessageHandler = (this: ClusterWorker, message: unknown, handle?: Socket | node_net.Server) => void;
/** Unix path, TCP port, `{ address, port }`, or `false`/`null` to disable. */
type ClusterReplListen = string | number | false | null | {
    address: string;
    port: number;
};
interface ClusterPrimaryConfig {
    exec: string;
    size?: number | undefined;
    env?: NodeJS.ProcessEnv | undefined;
    args?: string[] | undefined;
    silent?: boolean | undefined;
    onMessage?: ClusterMessageHandler | undefined;
    onmessage?: ClusterMessageHandler | undefined;
    signals?: boolean | undefined;
    stopTimeout?: number | undefined;
    skepticTimeout?: number | undefined;
    minAliveMs?: number | undefined;
    silenceDebug?: boolean | undefined;
    /** Event that means a worker is ready. Default `'listening'` (HTTP). Tests often use `'online'`. */
    aliveEvent?: string | undefined;
    repl?: ClusterReplListen | undefined;
    replHelp?: string[] | undefined;
    replContext?: Record<string, unknown> | undefined;
    /** Injected instead of `process.exit` (tests). */
    exit?: ((code: number) => void) | undefined;
}
interface RestartSnapshot {
    [workerId: string]: {
        pid: number | undefined;
    };
}
interface ClusterPrimaryEvents {
    debug: unknown[];
    disconnect: [worker: ClusterWorker];
    resize: [n: number | undefined];
    restart: [currentWorkers: RestartSnapshot];
    restartComplete: [];
    quit: [];
    quitHard: [];
}
interface ClusterEmitter extends EventEmitter {
    on<K extends keyof ClusterPrimaryEvents>(event: K, listener: (...args: ClusterPrimaryEvents[K]) => void): this;
    once<K extends keyof ClusterPrimaryEvents>(event: K, listener: (...args: ClusterPrimaryEvents[K]) => void): this;
    off<K extends keyof ClusterPrimaryEvents>(event: K, listener: (...args: ClusterPrimaryEvents[K]) => void): this;
    emit<K extends keyof ClusterPrimaryEvents>(event: K, ...args: ClusterPrimaryEvents[K]): boolean;
    removeListener<K extends keyof ClusterPrimaryEvents>(event: K, listener: (...args: ClusterPrimaryEvents[K]) => void): this;
}
interface DebugStream extends Socket {
    repl?: REPLServer | undefined;
    id?: number | undefined;
}
interface ClusterConstants {
    readonly STOP_TIMEOUT_MS: number;
    readonly SKEPTIC_TIMEOUT_MS: number;
    readonly MIN_ALIVE_MS: number;
    readonly DEFAULT_REPL: string;
}

declare class ClusterPrimary {
    #private;
    constructor(config: string | ClusterPrimaryConfig);
    get size(): number;
    start(): Promise<void>;
    emitter(): ClusterEmitter;
    debug(...args: unknown[]): void;
    resize(n?: number | undefined): void;
    restart(cb?: (() => void) | undefined): void;
    quit(): void;
    quitHard(): void;
    close(): Promise<void>;
}

declare const STOP_TIMEOUT_MS = 5000;
declare const SKEPTIC_TIMEOUT_MS = 2000;
declare const MIN_ALIVE_MS = 2000;
declare const DEFAULT_REPL: string;

declare const constants_DEFAULT_REPL: typeof DEFAULT_REPL;
declare const constants_MIN_ALIVE_MS: typeof MIN_ALIVE_MS;
declare const constants_SKEPTIC_TIMEOUT_MS: typeof SKEPTIC_TIMEOUT_MS;
declare const constants_STOP_TIMEOUT_MS: typeof STOP_TIMEOUT_MS;
declare namespace constants {
  export { constants_DEFAULT_REPL as DEFAULT_REPL, constants_MIN_ALIVE_MS as MIN_ALIVE_MS, constants_SKEPTIC_TIMEOUT_MS as SKEPTIC_TIMEOUT_MS, constants_STOP_TIMEOUT_MS as STOP_TIMEOUT_MS };
}

/**
 * Callable default export — same shape as `cluster-master-ext`.
 *
 * CommonJS: `const clusterPrimary = require('@bugsee/node-cluster')`
 * ESM: `import clusterPrimary, { ClusterPrimary } from '@bugsee/node-cluster'`
 */
interface ClusterPrimaryFn {
    (config: string | ClusterPrimaryConfig): ClusterEmitter;
    resize(n?: number | undefined): void;
    restart(cb?: (() => void) | undefined): void;
    quit(): void;
    quitHard(): void;
    debug(...args: unknown[]): void;
    emitter(): ClusterEmitter;
    close(): Promise<void>;
    ClusterPrimary: typeof ClusterPrimary;
    constants: ClusterConstants;
}
declare const clusterPrimary: ClusterPrimaryFn;

// @ts-ignore
export = clusterPrimary;
export { type ClusterConstants, type ClusterEmitter, type ClusterMessageHandler, ClusterPrimary, type ClusterPrimaryConfig, type ClusterPrimaryEvents, type ClusterPrimaryFn, type ClusterReplListen, type ClusterWorker, type DebugStream, type RestartSnapshot, constants };
