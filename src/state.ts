const STATE_KEY = Symbol.for('@bugsee/node-cluster');

export interface PackageState {
    /** Callable-export singleton (shared across ESM and CJS graphs). */
    singleton: import('./primary').ClusterPrimary | null;
    /** Process-wide owner of cluster.setupPrimary / fork listeners. */
    owner: import('./primary').ClusterPrimary | null;
}

export function packageState(): PackageState {
    const g = globalThis as typeof globalThis & { [STATE_KEY]?: PackageState };
    let state = g[STATE_KEY];
    if (!state) {
        state = { singleton: null, owner: null };
        Object.defineProperty(g, STATE_KEY, {
            value: state,
            enumerable: false,
            configurable: true,
            writable: true
        });
    }
    return state;
}
