import { EventEmitter } from 'node:events';
import type { Worker } from 'node:cluster';
import type { Socket } from 'node:net';
import type { REPLServer } from 'node:repl';

/** Worker plus fields this primary attaches (`CLUSTER_IDX`, `age`, `pid`). */
export type ClusterWorker = Worker & {
    clusterIdx?: number | undefined;
    birth?: number | undefined;
    readonly age?: number | undefined;
    pid?: number | undefined;
};

/**
 * IPC handler attached with `worker.on('message', handler)` so `this` is the Worker.
 */
export type ClusterMessageHandler = (
    this: ClusterWorker,
    message: unknown,
    handle?: Socket | import('node:net').Server
) => void;

/** Unix path, TCP port, `{ address, port }`, or `false`/`null` to disable. */
export type ClusterReplListen =
    | string
    | number
    | false
    | null
    | { address: string; port: number };

export interface ClusterPrimaryConfig {
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
    /** How long to wait for `aliveEvent` before SIGKILL. Default 30000. `0` waits indefinitely. */
    aliveTimeout?: number | undefined;
    silenceDebug?: boolean | undefined;
    /** Event that means a worker is ready. Default `'listening'` (HTTP). Tests often use `'online'`. */
    aliveEvent?: string | undefined;
    repl?: ClusterReplListen | undefined;
    replHelp?: string[] | undefined;
    replContext?: Record<string, unknown> | undefined;
    /** Injected instead of `process.exit` (tests). */
    exit?: ((code: number) => void) | undefined;
}

export interface RestartSnapshot {
    [workerId: string]: { pid: number | undefined };
}

export interface ClusterPrimaryEvents {
    debug: unknown[];
    disconnect: [worker: ClusterWorker];
    resize: [n: number | undefined];
    restart: [currentWorkers: RestartSnapshot];
    restartComplete: [];
    quit: [];
    quitHard: [];
}

export interface ClusterEmitter extends EventEmitter {
    on<K extends keyof ClusterPrimaryEvents>(
        event: K,
        listener: (...args: ClusterPrimaryEvents[K]) => void
    ): this;
    once<K extends keyof ClusterPrimaryEvents>(
        event: K,
        listener: (...args: ClusterPrimaryEvents[K]) => void
    ): this;
    off<K extends keyof ClusterPrimaryEvents>(
        event: K,
        listener: (...args: ClusterPrimaryEvents[K]) => void
    ): this;
    emit<K extends keyof ClusterPrimaryEvents>(
        event: K,
        ...args: ClusterPrimaryEvents[K]
    ): boolean;
    removeListener<K extends keyof ClusterPrimaryEvents>(
        event: K,
        listener: (...args: ClusterPrimaryEvents[K]) => void
    ): this;
}

export interface DebugStream extends Socket {
    repl?: REPLServer | undefined;
    id?: number | undefined;
}

export interface ClusterConstants {
    readonly STOP_TIMEOUT_MS: number;
    readonly SKEPTIC_TIMEOUT_MS: number;
    readonly MIN_ALIVE_MS: number;
    readonly ALIVE_TIMEOUT_MS: number;
    readonly DEFAULT_REPL: string;
}

export type ResizeCallback = () => void;
export type RestartCallback = () => void;
