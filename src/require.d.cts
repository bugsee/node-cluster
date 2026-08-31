import type {
    ClusterConstants,
    ClusterEmitter,
    ClusterPrimaryConfig
} from './index.js';
import { ClusterPrimary } from './index.js';

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
    default: ClusterPrimaryFn;
}

declare const clusterPrimary: ClusterPrimaryFn;
export = clusterPrimary;
