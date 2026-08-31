import type { ClusterEmitter, ClusterPrimaryConfig, ClusterConstants } from './types';
import { ClusterPrimary } from './primary';
import * as constants from './constants';

export type {
    ClusterMessageHandler,
    ClusterReplListen,
    ClusterPrimaryConfig,
    ClusterPrimaryEvents,
    ClusterEmitter,
    ClusterWorker,
    RestartSnapshot,
    ClusterConstants,
    DebugStream
} from './types';

export { ClusterPrimary };
export { constants };

let instance: ClusterPrimary | null = null;

/**
 * Callable default export — same shape as `cluster-master-ext`.
 *
 * CommonJS: `const clusterPrimary = require('@bugsee/node-cluster')`
 * ESM: `import clusterPrimary, { ClusterPrimary } from '@bugsee/node-cluster'`
 */
export interface ClusterPrimaryFn {
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

function need(): ClusterPrimary {
    if (!instance) {
        throw new Error('cluster primary is not started');
    }
    return instance;
}

const clusterPrimary = function clusterPrimary(
    config: string | ClusterPrimaryConfig
): ClusterEmitter {
    if (instance) {
        throw new Error('This cluster has a master already');
    }
    instance = new ClusterPrimary(config);
    instance.start();
    return instance.emitter();
} as ClusterPrimaryFn;

clusterPrimary.resize = function (n?: number | undefined): void {
    need().resize(n);
};

clusterPrimary.restart = function (cb?: (() => void) | undefined): void {
    need().restart(cb);
};

clusterPrimary.quit = function (): void {
    need().quit();
};

clusterPrimary.quitHard = function (): void {
    need().quitHard();
};

clusterPrimary.debug = function (...args: unknown[]): void {
    if (instance) {
        instance.debug(...args);
        return;
    }
    console.error(...args);
};

clusterPrimary.emitter = function (): ClusterEmitter {
    return need().emitter();
};

clusterPrimary.close = function (): Promise<void> {
    if (!instance) {
        return Promise.resolve();
    }
    const inst = instance;
    instance = null;
    return inst.close();
};

clusterPrimary.ClusterPrimary = ClusterPrimary;
clusterPrimary.constants = constants;

export default clusterPrimary;
