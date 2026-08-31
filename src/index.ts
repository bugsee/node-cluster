import type { ClusterEmitter, ClusterPrimaryConfig, ClusterConstants } from './types';
import { ClusterPrimary } from './primary';
import * as constants from './constants';
import { packageState } from './state';

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

/**
 * Callable default export.
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
    default: ClusterPrimaryFn;
}

function need(): ClusterPrimary {
    const inst = packageState().singleton;
    if (!inst) {
        throw new Error('cluster primary is not started');
    }
    return inst;
}

const clusterPrimary = function clusterPrimary(
    config: string | ClusterPrimaryConfig
): ClusterEmitter {
    const st = packageState();
    if (st.singleton || st.owner) {
        throw new Error('This process already has a cluster primary');
    }
    const inst = new ClusterPrimary(config);
    st.singleton = inst;
    try {
        inst.start();
    } catch (err) {
        if (st.singleton === inst) {
            st.singleton = null;
        }
        throw err;
    }
    return inst.emitter();
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
    const inst = packageState().singleton;
    if (inst) {
        inst.debug(...args);
        return;
    }
    console.error(...args);
};

clusterPrimary.emitter = function (): ClusterEmitter {
    return need().emitter();
};

clusterPrimary.close = function (): Promise<void> {
    const st = packageState();
    const inst = st.singleton;
    if (!inst) {
        return Promise.resolve();
    }
    st.singleton = null;
    return inst.close();
};

clusterPrimary.ClusterPrimary = ClusterPrimary;
clusterPrimary.constants = constants;
clusterPrimary.default = clusterPrimary;

export default clusterPrimary;
