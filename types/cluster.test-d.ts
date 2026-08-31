/**
 * Compile-time tests for the generated declarations (ESM `.d.ts` and CJS `.d.cts`).
 * Compiled by `npm run typecheck`. `@ts-expect-error` cases must keep erroring.
 */

import clusterPrimary, {
    ClusterPrimary,
    constants,
    type ClusterPrimaryConfig,
    type ClusterMessageHandler,
    type ClusterEmitter,
    type ClusterWorker
} from '../dist/index.js';

// --- construction --------------------------------------------------------

const cfg: ClusterPrimaryConfig = {
    exec: './worker.js',
    size: 2,
    silent: true,
    signals: false,
    repl: false,
    onMessage: function (this: ClusterWorker, _message: unknown) {
        void this.id;
    }
};

const primary: ClusterPrimary = new ClusterPrimary(cfg);
const started: Promise<void> = primary.start();
const emitter: ClusterEmitter = primary.emitter();
const size: number = primary.size;

emitter.on('restartComplete', () => undefined);
emitter.on('disconnect', (_worker: ClusterWorker) => undefined);
emitter.on('debug', (..._args: unknown[]) => undefined);

primary.resize(4);
primary.restart(() => undefined);
primary.debug('hello');
const closed: Promise<void> = primary.close();

// @ts-expect-error -- exec is required
new ClusterPrimary({});

// @ts-expect-error -- exec must be a string
new ClusterPrimary({ exec: 1 });

// String shorthand is allowed.
new ClusterPrimary('./worker.js');

// --- callable default (cluster-master-ext shape) -------------------------

const ev: ClusterEmitter = clusterPrimary({ exec: './worker.js', repl: false, signals: false });
clusterPrimary.resize(2);
clusterPrimary.restart();
clusterPrimary.debug('x');
const viaDefault: typeof ClusterPrimary = clusterPrimary.ClusterPrimary;
const minAlive: number = clusterPrimary.constants.MIN_ALIVE_MS;
const namedMinAlive: number = constants.MIN_ALIVE_MS;

// @ts-expect-error -- cannot start without exec
clusterPrimary({});

const handler: ClusterMessageHandler = function (this: ClusterWorker, message: unknown) {
    void this.id;
    void message;
};

void started;
void closed;
void size;
void ev;
void viaDefault;
void minAlive;
void namedMinAlive;
void handler;

// --- CJS require types (.d.cts) ------------------------------------------
// Do not compare CJS vs ESM class identity: #private fields make them
// distinct under `resolution-mode: import` vs `require`.

import clusterPrimaryCjs = require('../dist/index.cjs');

const cjsEmitter = clusterPrimaryCjs({ exec: './worker.js', repl: false, signals: false });
const cjsPrimary = new clusterPrimaryCjs.ClusterPrimary({ exec: './worker.js', repl: false });
void cjsEmitter;
void cjsPrimary;
