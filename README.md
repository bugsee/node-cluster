# @bugsee/node-cluster

Node.js cluster primary: fork, rolling restart, crash respawn, signals, optional REPL.

Ships **both ESM and CommonJS**. Zero runtime dependencies.

## Install

Consumed by **git tag**:

```json
"@bugsee/node-cluster": "github:bugsee/node-cluster#v1.0.0"
```

## Dual module

```js
// CommonJS
const clusterPrimary = require('@bugsee/node-cluster');
const { ClusterPrimary } = require('@bugsee/node-cluster');

clusterPrimary({
    exec: 'worker.js',
    onMessage: function (msg) {
        // `this` is the cluster Worker
    }
});
```

```ts
// ESM / TypeScript
import clusterPrimary, { ClusterPrimary } from '@bugsee/node-cluster';
import type { ClusterPrimaryConfig, ClusterMessageHandler } from '@bugsee/node-cluster';

const onMessage: ClusterMessageHandler = function (msg) {
    void this.id;
    void msg;
};

await new ClusterPrimary({
    exec: 'worker.js',
    onMessage,
    signals: false,
    repl: false
}).start();
```

`require()` returns the **callable function**, with `ClusterPrimary` and `constants` as properties.
ESM `import` gets the same function as `default` plus named exports.

Only one primary may run in a process. CommonJS `require` and ESM `import` share that lock
(via `globalThis`), so mixing module graphs cannot start two primaries.

## Behaviour

| Behaviour | Notes |
|---|---|
| `setupPrimary` | `exec` (resolved), `silent`, `args`. `cluster.isPrimary`. |
| `onMessage` | Classic `function`, **`this` = Worker**, `worker.on('message')` on `fork`. |
| Size | Default `os.cpus().length`. |
| `CLUSTER_IDX` | Fork env + `worker.clusterIdx`; crash reuses the idx. |
| Respawn | Abnormal exit (`exitedAfterDisconnect`) → resize if below size. Age &lt; 2s → no respawn. |
| Disconnect | `SIGKILL` after `stopTimeout` (default 5000). |
| Rolling restart | One-in-one-out. First newbie must live `skepticTimeout` (2000) or abort. Default `aliveEvent` is `'listening'`. |
| Worker ready | Wait up to `aliveTimeout` (default 30000) for `aliveEvent`, then `SIGKILL`. |
| Signals | SIGHUP restart; SIGINT/SIGTERM quit; SIGABRT quitHard. |
| REPL | Default `CLUSTER_REPL` or `node-cluster.sock`. **TCP REPL is unauthenticated** — bind only on a trusted interface. |
| Events | `debug`, `disconnect`, `resize`, `restart`, `restartComplete`, `quit`, `quitHard`. |

Resize targets are serialized: a later `resize(n)` is never dropped while an earlier one is in flight.
`close()` and injectable `exit` are for tests. `minAliveMs` is overridable (default 2000).

## Releasing

1. Change `src/`, tests, `types/cluster.test-d.ts`.
2. `npm test` — builds `dist/` (ESM + CJS + `.d.ts` / `.d.cts`) and runs mocha.
3. `npm run typecheck`.
4. `npm run check:dist`.
5. Commit including rebuilt `dist/`, tag `vX.Y.Z`, push tags.
6. Bump the tag in consumers.
