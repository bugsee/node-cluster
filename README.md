# @bugsee/node-cluster

Node.js cluster primary: fork, rolling restart, crash respawn, signals, optional REPL.

Drop-in successor to [`cluster-master-ext`](https://www.npmjs.com/package/cluster-master-ext), extracted from
Bugsee appserver. Ships **both ESM and CommonJS**. Zero runtime dependencies.

Not wired into appserver yet — iterate here, then swap `require('cluster-master-ext')`.

## Install

Consumed by **git tag** (same as `@bugsee/backend-metrics`):

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

`require()` returns the **callable function** (cluster-master-ext shape), with `ClusterPrimary` and
`constants` as properties. ESM `import` gets the same function as `default` plus named exports.

## Behaviour (ported from cluster-master-ext)

| Behaviour | Notes |
|---|---|
| `setupPrimary` | `exec` (resolved), `silent`, `args`. `cluster.isPrimary`. |
| `onMessage` | Classic `function`, **`this` = Worker**, `worker.on('message')` on `fork`. |
| Size | Default `os.cpus().length`. |
| `CLUSTER_IDX` | Fork env + `worker.clusterIdx`; crash reuses the idx. |
| Respawn | Abnormal exit (`exitedAfterDisconnect`) → resize if below size. Age &lt; 2s → no respawn. |
| Disconnect | `SIGKILL` after `stopTimeout` (default 5000). |
| Rolling restart | One-in-one-out. First newbie must live `skepticTimeout` (2000) or abort. Default `aliveEvent` is `'listening'`. |
| Signals | SIGHUP restart; SIGINT/SIGTERM quit; SIGABRT quitHard. |
| REPL | Default `CLUSTER_MASTER_REPL` or `cluster-master-socket`. **TCP REPL is unauthenticated.** |
| Events | `debug`, `disconnect`, `resize`, `restart`, `restartComplete`, `quit`, `quitHard`. |

Intentional deltas vs cluster-master-ext: serialized restart (the old library forked remaining workers
racy); per-connection REPL `ended`; debug stream key uses `sock.id`; `close()` + injectable `exit` for tests;
`minAliveMs` overridable (default 2000).

## Releasing

1. Change `src/`, tests, `types/cluster.test-d.ts`.
2. `npm test` — builds `dist/` (ESM + CJS + `.d.ts` / `.d.cts`) and runs mocha.
3. `npm run typecheck`.
4. `npm run check:dist`.
5. Commit including rebuilt `dist/`, tag `vX.Y.Z`, push tags.
6. Bump the tag in consumers.
