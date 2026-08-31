'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var os = require('node:os');
var path = require('node:path');
var util = require('node:util');
var cluster2 = require('node:cluster');
var node_events = require('node:events');
var fs = require('node:fs');
var net = require('node:net');
var repl = require('node:repl');

function _interopDefault (e) { return e && e.__esModule ? e : { default: e }; }

function _interopNamespace(e) {
  if (e && e.__esModule) return e;
  var n = Object.create(null);
  if (e) {
    Object.keys(e).forEach(function (k) {
      if (k !== 'default') {
        var d = Object.getOwnPropertyDescriptor(e, k);
        Object.defineProperty(n, k, d.get ? d : {
          enumerable: true,
          get: function () { return e[k]; }
        });
      }
    });
  }
  n.default = e;
  return Object.freeze(n);
}

var os__namespace = /*#__PURE__*/_interopNamespace(os);
var path__namespace = /*#__PURE__*/_interopNamespace(path);
var util__namespace = /*#__PURE__*/_interopNamespace(util);
var cluster2__default = /*#__PURE__*/_interopDefault(cluster2);
var fs__namespace = /*#__PURE__*/_interopNamespace(fs);
var net__namespace = /*#__PURE__*/_interopNamespace(net);
var repl__namespace = /*#__PURE__*/_interopNamespace(repl);

var __defProp = Object.defineProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// src/constants.ts
var constants_exports = {};
__export(constants_exports, {
  ALIVE_TIMEOUT_MS: () => ALIVE_TIMEOUT_MS,
  DEFAULT_REPL: () => DEFAULT_REPL,
  MIN_ALIVE_MS: () => MIN_ALIVE_MS,
  SKEPTIC_TIMEOUT_MS: () => SKEPTIC_TIMEOUT_MS,
  STOP_TIMEOUT_MS: () => STOP_TIMEOUT_MS
});
var STOP_TIMEOUT_MS = 5e3;
var SKEPTIC_TIMEOUT_MS = 2e3;
var MIN_ALIVE_MS = 2e3;
var ALIVE_TIMEOUT_MS = 3e4;
var DEFAULT_REPL = process.env.CLUSTER_REPL || "node-cluster.sock";
function select(workers, field) {
  const set = {};
  if (!workers) {
    return set;
  }
  Object.keys(workers).forEach(function(k) {
    const w = workers[k];
    if (w) {
      set[k] = w[field];
    }
  });
  return set;
}
function createReplWorker(d) {
  return {
    id: d.id,
    pid: d.pid,
    state: d.state,
    age: d.age,
    disconnect: function() {
      const w = cluster2__default.default.workers?.[d.id];
      if (w) {
        w.disconnect();
      }
    },
    kill: function() {
      if (typeof d.pid === "number") {
        process.kill(d.pid);
      }
    }
  };
}
function isAddressPort(value) {
  return typeof value === "object" && value !== null && "address" in value && "port" in value;
}
function startRepl(host, replAddressPath) {
  let listenTarget = null;
  let socketAddress;
  if (typeof replAddressPath === "string") {
    listenTarget = path__namespace.resolve(replAddressPath);
  } else if (typeof replAddressPath === "number") {
    listenTarget = replAddressPath;
  } else if (isAddressPort(replAddressPath)) {
    listenTarget = replAddressPath.port;
    socketAddress = replAddressPath.address;
  }
  let connections = 0;
  let nextSockId = 0;
  let replServer = null;
  let closed = false;
  const socketPath = typeof listenTarget === "string" ? listenTarget : null;
  function attachContext(r, sock) {
    const helpCommands = [
      "help        - display these commands",
      "repl        - access the REPL",
      "resize(n)   - resize the cluster to `n` workers",
      "restart(cb) - gracefully restart workers, cb is optional",
      "stop()      - gracefully stop workers and primary",
      "kill()      - forcefully kill workers and primary",
      "cluster     - node.js cluster module",
      "size        - current cluster size",
      "connections - number of REPL connections to primary",
      "workers     - current workers",
      "select(fld) - map of id to field (from workers)",
      "pids        - map of id to pids",
      "ages        - map of id to worker ages",
      "states      - map of id to worker states",
      "debug(a1)   - output `a1` to stdout and all REPLs",
      "sock        - this REPL socket",
      ".exit       - close this connection to the REPL"
    ];
    if (Array.isArray(host.replHelp)) {
      host.replHelp.forEach(function(line) {
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
      cluster: cluster2__default.default,
      get size() {
        return host.getSize();
      },
      get connections() {
        return connections;
      },
      get workers() {
        const p = select(cluster2__default.default.workers, "pid");
        const s = select(cluster2__default.default.workers, "state");
        const a = select(cluster2__default.default.workers, "age");
        return Object.keys(cluster2__default.default.workers || {}).map(function(k) {
          return createReplWorker({
            id: k,
            pid: p[k],
            state: s[k],
            age: a[k]
          });
        });
      },
      select: function(field) {
        return select(cluster2__default.default.workers, field);
      },
      get pids() {
        return select(cluster2__default.default.workers, "pid");
      },
      get ages() {
        return select(cluster2__default.default.workers, "age");
      },
      get states() {
        return select(cluster2__default.default.workers, "state");
      },
      debug: host.debug,
      sock
    };
    const desc = {};
    Object.getOwnPropertyNames(context).forEach(function(n) {
      const property = Object.getOwnPropertyDescriptor(context, n);
      if (property) {
        desc[n] = property;
      }
    });
    Object.getOwnPropertyNames(host.replContext).forEach(function(n) {
      const property = Object.getOwnPropertyDescriptor(host.replContext, n);
      if (property) {
        desc[n] = property;
      }
    });
    Object.defineProperties(r.context, desc);
  }
  function onConnection(sock) {
    connections += 1;
    const sockId = nextSockId;
    nextSockId += 1;
    const streamKey = "repl-" + sockId;
    const debugStreams = host.debugStreams;
    Object.defineProperty(sock, "id", {
      value: sockId,
      enumerable: true,
      writable: true,
      configurable: true
    });
    debugStreams[streamKey] = sock;
    sock.write("Starting repl #" + String(sock.id));
    const r = repl__namespace.start({
      prompt: "cluster (`help` for cmds) " + process.pid + " " + String(sock.id) + "> ",
      input: sock,
      output: sock,
      terminal: true,
      useGlobal: false,
      ignoreUndefined: true
    });
    attachContext(r, sock);
    Object.defineProperty(sock, "repl", {
      value: r,
      enumerable: true,
      writable: true,
      configurable: true
    });
    let ended = false;
    let replEnded = false;
    function end() {
      if (ended) {
        return;
      }
      ended = true;
      if (!replEnded) {
        r.close();
      }
      delete debugStreams[streamKey];
    }
    if (r.commands && r.commands[".exit"]) {
      r.commands[".exit"].action = function() {
        end();
        sock.end();
      };
    }
    r.on("exit", function() {
      connections -= 1;
      replEnded = true;
      if (!ended) {
        sock.end();
      }
    });
    sock.on("end", end);
    sock.on("close", end);
    sock.on("error", end);
  }
  function listen() {
    if (closed || listenTarget === null) {
      return;
    }
    replServer = net__namespace.createServer(onConnection);
    function onListening() {
      if (closed) {
        return;
      }
      if (socketAddress) {
        host.debug("cluster repl listening on " + socketAddress + ":" + String(listenTarget));
      } else {
        host.debug("cluster repl listening on " + String(listenTarget));
      }
    }
    if (socketAddress) {
      replServer.listen(Number(listenTarget), socketAddress, onListening);
    } else if (listenTarget !== null) {
      replServer.listen(listenTarget, onListening);
    }
  }
  let resolveListenReady = function() {
  };
  const listenReady = new Promise(function(resolve3) {
    resolveListenReady = resolve3;
  });
  if (socketPath) {
    fs__namespace.unlink(socketPath, function(err) {
      if (err && err.code !== "ENOENT") {
        host.debug("repl socket unlink failed", err);
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
    close: function() {
      closed = true;
      return listenReady.then(function() {
        const debugStreams = host.debugStreams;
        Object.keys(debugStreams).forEach(function(key) {
          try {
            debugStreams[key]?.destroy();
          } catch {
          }
          delete debugStreams[key];
        });
        if (!replServer) {
          return void 0;
        }
        const server = replServer;
        replServer = null;
        return new Promise(function(resolve3) {
          server.close(function() {
            resolve3();
          });
        });
      });
    }
  };
}

// src/state.ts
var STATE_KEY = /* @__PURE__ */ Symbol.for("@bugsee/node-cluster");
function packageState() {
  const g = globalThis;
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

// src/primary.ts
function workerList() {
  const dict = cluster2__default.default.workers;
  if (!dict) {
    return [];
  }
  return Object.keys(dict).map(function(k) {
    return dict[k];
  }).filter(function(w) {
    return w !== void 0;
  });
}
function sortedWorkers() {
  return workerList().sort(function(a, b) {
    const aCidx = a.clusterIdx ?? 0;
    const bCidx = b.clusterIdx ?? 0;
    if (aCidx < bCidx) {
      return -1;
    }
    if (bCidx < aCidx) {
      return 1;
    }
    return 0;
  });
}
function isPrimaryProcess() {
  return cluster2__default.default.isPrimary === true;
}
var ClusterPrimary = class {
  #config;
  #size;
  #env;
  #onMessage;
  #signalsEnabled;
  #stopTimeout;
  #skepticTimeout;
  #minAliveMs;
  #aliveTimeout;
  #silenceDebug;
  #aliveEvent;
  #replAddress;
  #replHelp;
  #replContext;
  #exitFn;
  #emitter = new node_events.EventEmitter();
  #debugStreams = {};
  #nextWorkerIdx = 0;
  #replaceWorkerIdxs = [];
  #quitting = false;
  #restarting = false;
  #resizing = false;
  #refill = false;
  #resizeTail = Promise.resolve();
  #started = false;
  #closed = false;
  #exited = false;
  #startPromise = null;
  #disconnectTimers = /* @__PURE__ */ new Map();
  #previousSettings = null;
  #repl = null;
  #onFork;
  #onSighup;
  #onSigint;
  #onSigterm;
  #onSigabrt;
  #onProcessExit;
  constructor(config) {
    const cfg = typeof config === "string" ? { exec: config } : config;
    if (!cfg.exec) {
      throw new Error("Must define a 'exec' script");
    }
    if (!isPrimaryProcess()) {
      throw new Error(
        "Must run in the Node.js cluster primary process"
      );
    }
    this.#config = cfg;
    this.#size = typeof cfg.size === "number" ? cfg.size : os__namespace.cpus().length;
    this.#env = cfg.env || {};
    this.#onMessage = cfg.onMessage || cfg.onmessage;
    this.#signalsEnabled = cfg.signals !== false;
    this.#stopTimeout = cfg.stopTimeout ?? STOP_TIMEOUT_MS;
    this.#skepticTimeout = cfg.skepticTimeout ?? SKEPTIC_TIMEOUT_MS;
    this.#minAliveMs = cfg.minAliveMs ?? MIN_ALIVE_MS;
    this.#aliveTimeout = cfg.aliveTimeout ?? ALIVE_TIMEOUT_MS;
    this.#silenceDebug = Boolean(cfg.silenceDebug);
    this.#aliveEvent = cfg.aliveEvent || "listening";
    this.#replAddress = typeof cfg.repl !== "undefined" ? cfg.repl : DEFAULT_REPL;
    this.#replHelp = cfg.replHelp || null;
    this.#replContext = cfg.replContext || {};
    this.#exitFn = cfg.exit || function(code) {
      process.exit(code);
    };
    const self = this;
    this.#onFork = function(worker) {
      self.#setupWorker(worker);
    };
    this.#onSighup = function() {
      self.restart();
    };
    this.#onSigint = function() {
      self.quit();
    };
    this.#onSigterm = function() {
      self.quit();
    };
    this.#onSigabrt = function() {
      self.quitHard();
    };
    this.#onProcessExit = function() {
      if (!self.#quitting) {
        self.quitHard();
      }
    };
  }
  get size() {
    return this.#size;
  }
  start() {
    if (this.#closed) {
      throw new Error("cluster primary is closed");
    }
    const st = packageState();
    if (this.#started || st.owner && st.owner !== this) {
      throw new Error("This process already has a cluster primary");
    }
    this.#started = true;
    st.owner = this;
    const masterConf = { exec: path__namespace.resolve(this.#config.exec) };
    if (this.#config.silent) {
      masterConf.silent = true;
    }
    if (this.#config.args) {
      masterConf.args = this.#config.args;
    }
    this.#previousSettings = Object.assign({}, cluster2__default.default.settings);
    cluster2__default.default.setupPrimary(masterConf);
    cluster2__default.default.on("fork", this.#onFork);
    if (this.#signalsEnabled) {
      this.#installSignals();
    }
    const self = this;
    this.debug(this.#replAddress ? "resize and then setup repl" : "resize");
    this.#startPromise = this.#resizeTo().then(function() {
      if (!self.#closed) {
        self.#startRepl();
      }
    });
    return this.#startPromise;
  }
  emitter() {
    return this.#emitter;
  }
  debug(...args) {
    if (!this.#silenceDebug) {
      console.error(...args);
    }
    this.#emitter.emit("debug", ...args);
    const msg = util__namespace.format(...args);
    const self = this;
    Object.keys(this.#debugStreams).forEach(function(s) {
      const stream = self.#debugStreams[s];
      if (!stream) {
        return;
      }
      try {
        stream.write(msg + "\n");
        if (stream.repl) {
          stream.repl.displayPrompt();
        }
      } catch {
        delete self.#debugStreams[s];
      }
    });
  }
  resize(n) {
    this.#emitter.emit("resize", n);
    const self = this;
    process.nextTick(function() {
      self.#resizeTo(n);
    });
  }
  restart(cb) {
    if (this.#restarting) {
      this.debug("Already restarting.  Cannot restart yet.");
      return;
    }
    const currentWorkers = {};
    workerList().forEach(function(w) {
      currentWorkers[String(w.id)] = { pid: w.pid ?? w.process.pid };
    });
    this.#emitter.emit("restart", currentWorkers);
    const self = this;
    process.nextTick(function() {
      self.#doRestart(function() {
        self.#emitter.emit("restartComplete");
        if (cb) {
          cb();
        }
      });
    });
  }
  quit() {
    this.#emitter.emit("quit");
    const self = this;
    process.nextTick(function() {
      self.#doQuit();
    });
  }
  quitHard() {
    this.#emitter.emit("quitHard");
    const self = this;
    process.nextTick(function() {
      self.#size = 0;
      self.#quitting = true;
      self.#doQuit();
    });
  }
  close() {
    if (this.#closed) {
      return Promise.resolve();
    }
    this.#closed = true;
    this.#quitting = true;
    this.#size = 0;
    this.#removeSignals();
    cluster2__default.default.removeListener("fork", this.#onFork);
    const st = packageState();
    if (st.owner === this) {
      st.owner = null;
    }
    if (st.singleton === this) {
      st.singleton = null;
    }
    const self = this;
    const pending = this.#startPromise || Promise.resolve();
    return pending.then(function() {
      const replClose = self.#repl ? self.#repl.close() : Promise.resolve();
      self.#repl = null;
      return replClose;
    }).then(function() {
      return self.#killRemaining();
    }).then(function() {
      if (self.#previousSettings) {
        cluster2__default.default.setupPrimary(self.#previousSettings);
        self.#previousSettings = null;
      }
    });
  }
  #installSignals() {
    try {
      process.on("SIGHUP", this.#onSighup);
      process.on("SIGINT", this.#onSigint);
      process.on("SIGTERM", this.#onSigterm);
      process.on("SIGABRT", this.#onSigabrt);
    } catch {
    }
    process.on("exit", this.#onProcessExit);
  }
  #removeSignals() {
    process.removeListener("SIGHUP", this.#onSighup);
    process.removeListener("SIGINT", this.#onSigint);
    process.removeListener("SIGTERM", this.#onSigterm);
    process.removeListener("SIGABRT", this.#onSigabrt);
    process.removeListener("exit", this.#onProcessExit);
  }
  #startRepl() {
    if (!this.#replAddress) {
      return;
    }
    const self = this;
    this.debug("setup repl");
    this.#repl = startRepl({
      debug: function(...args) {
        self.debug(...args);
      },
      debugStreams: this.#debugStreams,
      resize: function(n) {
        self.resize(n);
      },
      restart: function(cb) {
        self.restart(cb);
      },
      quit: function() {
        self.quit();
      },
      quitHard: function() {
        self.quitHard();
      },
      getSize: function() {
        return self.#size;
      },
      replHelp: this.#replHelp,
      replContext: this.#replContext
    }, this.#replAddress);
  }
  #setupWorker(worker) {
    Object.defineProperty(worker, "birth", {
      value: Date.now(),
      enumerable: true,
      writable: true,
      configurable: true
    });
    Object.defineProperty(worker, "age", {
      get: function() {
        return Date.now() - (this.birth ?? 0);
      },
      enumerable: true,
      configurable: true
    });
    Object.defineProperty(worker, "pid", {
      value: worker.process.pid,
      enumerable: true,
      writable: true,
      configurable: true
    });
    const id = worker.id;
    this.debug("Worker %j setting up", id);
    if (this.#onMessage) {
      worker.on("message", this.#onMessage);
    }
    const self = this;
    worker.on("exit", function() {
      const timer = self.#disconnectTimers.get(id);
      if (timer) {
        clearTimeout(timer);
        self.#disconnectTimers.delete(id);
      }
      if (!worker.exitedAfterDisconnect) {
        self.debug("Worker %j exited abnormally, idx:", id, worker.clusterIdx);
        if (typeof worker.clusterIdx === "number") {
          self.#replaceWorkerIdxs.push(worker.clusterIdx);
        }
        if ((worker.age ?? 0) < self.#minAliveMs) {
          self.debug("Worker %j died too quickly, not respawning.", id);
          return;
        }
      } else {
        self.debug("Worker %j exited", id);
      }
      if (self.#closed || self.#quitting || self.#restarting) {
        return;
      }
      if (workerList().length < self.#size) {
        if (self.#resizing) {
          self.#refill = true;
        } else {
          self.#resizeTo();
        }
      }
    });
    worker.on("disconnect", function() {
      self.debug("Worker %j disconnect", id);
      const disconnectTimer = setTimeout(function() {
        self.debug("Worker %j, forcefully killing", id);
        if (worker.process) {
          worker.process.kill("SIGKILL");
        }
      }, self.#stopTimeout);
      self.#disconnectTimers.set(id, disconnectTimer);
    });
  }
  #getNextWorkerIdx() {
    const size = this.#size;
    this.#replaceWorkerIdxs = this.#replaceWorkerIdxs.filter(function(wid) {
      return wid < size;
    });
    if (this.#replaceWorkerIdxs.length) {
      return this.#replaceWorkerIdxs.shift() ?? 0;
    }
    if (this.#nextWorkerIdx >= this.#size) {
      this.#nextWorkerIdx = 0;
    }
    const idx = this.#nextWorkerIdx;
    this.#nextWorkerIdx = idx + 1;
    return idx;
  }
  #forkChild() {
    if (this.#closed) {
      throw new Error("cluster primary is closed");
    }
    const childEnv = {};
    const env = this.#env;
    Object.keys(env).forEach(function(k) {
      childEnv[k] = env[k];
    });
    const nextIdx = this.#getNextWorkerIdx();
    childEnv.CLUSTER_IDX = String(nextIdx);
    const cp = cluster2__default.default.fork(childEnv);
    cp.clusterIdx = nextIdx;
    return cp;
  }
  #forkAndWaitAlive() {
    if (this.#closed) {
      return Promise.reject(new Error("cluster primary is closed"));
    }
    try {
      return this.#waitAlive(this.#forkChild());
    } catch (err) {
      return Promise.reject(err);
    }
  }
  #waitAlive(worker) {
    const self = this;
    return new Promise(function(resolve3, reject) {
      let done = false;
      let onAlive = function() {
      };
      let onExit = function() {
      };
      let timer;
      function finish(err, value) {
        if (done) {
          return;
        }
        done = true;
        if (timer) {
          clearTimeout(timer);
        }
        worker.removeListener(self.#aliveEvent, onAlive);
        worker.removeListener("exit", onExit);
        if (err) {
          reject(err);
        } else if (value) {
          resolve3(value);
        } else {
          reject(new Error("fork failed"));
        }
      }
      onAlive = function() {
        finish(null, worker);
      };
      onExit = function() {
        finish(new Error("Worker exited before " + self.#aliveEvent));
      };
      if (self.#aliveTimeout > 0) {
        timer = setTimeout(function() {
          if (worker.process) {
            worker.process.kill("SIGKILL");
          }
          finish(new Error("Worker timed out waiting for " + self.#aliveEvent));
        }, self.#aliveTimeout);
      }
      worker.once(self.#aliveEvent, onAlive);
      worker.once("exit", onExit);
    });
  }
  #waitSkeptic(newbie) {
    const self = this;
    return new Promise(function(resolve3) {
      let settled = false;
      let onExit;
      const timer = setTimeout(function() {
        if (settled) {
          return;
        }
        settled = true;
        newbie.removeListener("exit", onExit);
        resolve3(true);
      }, self.#skepticTimeout);
      onExit = function() {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve3(false);
      };
      newbie.on("exit", onExit);
    });
  }
  #emitAndDisconnect(worker) {
    this.#emitter.emit("disconnect", worker);
    process.nextTick(function() {
      if (worker.process && worker.process.connected) {
        worker.disconnect();
      }
    });
  }
  #disconnectAndWaitExit(worker) {
    const self = this;
    return new Promise(function(resolve3) {
      if (worker.isDead()) {
        resolve3();
        return;
      }
      worker.once("exit", function() {
        resolve3();
      });
      if (worker.process && worker.process.connected) {
        self.#emitAndDisconnect(worker);
        return;
      }
      if (worker.process) {
        worker.process.kill("SIGKILL");
      }
    });
  }
  #resizeTo(n, cb) {
    let target;
    let done = cb;
    if (typeof n === "function") {
      done = n;
      target = void 0;
    } else {
      target = n;
    }
    if (typeof target === "number" && target >= 0) {
      if (target < this.#size) {
        this.#nextWorkerIdx = target;
      }
      this.#size = target;
    }
    const self = this;
    this.#resizeTail = this.#resizeTail.then(
      function() {
        return self.#matchSize();
      },
      function() {
        return self.#matchSize();
      }
    );
    const run = this.#resizeTail;
    if (done) {
      const finished = done;
      run.then(function() {
        finished();
      }, function() {
        finished();
      });
    }
    return run;
  }
  #matchSize() {
    if (this.#closed) {
      return Promise.resolve();
    }
    this.#resizing = true;
    this.#refill = false;
    const self = this;
    function wave() {
      if (self.#closed) {
        return Promise.resolve();
      }
      const current = sortedWorkers();
      const req = self.#size - current.length;
      if (req === 0) {
        return Promise.resolve();
      }
      if (req > 0) {
        const before = current.length;
        const forks = [];
        for (let i = 0; i < req; i += 1) {
          self.debug("resizing up", req - i - 1);
          forks.push(self.#forkAndWaitAlive());
        }
        return Promise.allSettled(forks).then(function() {
          if (self.#closed) {
            return void 0;
          }
          const after = workerList().length;
          if (after <= before) {
            return void 0;
          }
          return wave();
        });
      }
      const extras = current.slice(self.#size);
      return Promise.all(extras.map(function(worker) {
        self.debug("resizing down", worker.id);
        return self.#disconnectAndWaitExit(worker);
      })).then(function() {
        return wave();
      });
    }
    return wave().then(function() {
      self.#resizing = false;
      if (self.#refill && !self.#closed && !self.#restarting && !self.#quitting) {
        self.#refill = false;
        return self.#matchSize();
      }
      return void 0;
    }, function() {
      self.#resizing = false;
      return void 0;
    });
  }
  #doRestart(cb) {
    const self = this;
    function finish() {
      self.#restarting = false;
      if (!self.#closed && !self.#quitting && workerList().length < self.#size) {
        self.#resizeTo();
      }
      if (cb) {
        cb();
      }
    }
    if (this.#restarting) {
      this.debug("Already restarting.  Cannot restart yet.");
      return;
    }
    this.#restarting = true;
    const current = Object.keys(cluster2__default.default.workers || {});
    const reqs = this.#size - current.length;
    if (reqs !== 0) {
      this.debug("resize %d -> %d, change = %d", current.length, this.#size, reqs);
      this.#resizeTo(this.#size, function() {
        self.debug("resize cb");
        self.#rollingReplace(Object.keys(cluster2__default.default.workers || {}), finish);
      });
      return;
    }
    this.#rollingReplace(current, finish);
  }
  #rollingReplace(ids, cb) {
    const self = this;
    let i = 0;
    function next() {
      if (self.#closed) {
        cb();
        return;
      }
      if (i >= ids.length) {
        self.debug("graceful completion");
        cb();
        return;
      }
      self.debug("graceful %d of %d", i, ids.length);
      const first = i === 0;
      const id = ids[i];
      i += 1;
      const worker = id !== void 0 && cluster2__default.default.workers ? cluster2__default.default.workers[id] : void 0;
      if (self.#quitting) {
        if (worker && worker.process && worker.process.connected) {
          self.#emitAndDisconnect(worker);
        }
        next();
        return;
      }
      self.#forkAndWaitAlive().then(function(newbie) {
        if (!first) {
          if (worker && worker.process && worker.process.connected) {
            self.#emitAndDisconnect(worker);
          }
          return void 0;
        }
        return self.#waitSkeptic(newbie).then(function(ok) {
          if (!ok) {
            throw new Error("abort-restart");
          }
          if (worker && worker.process && worker.process.connected) {
            self.#emitAndDisconnect(worker);
          }
          return void 0;
        });
      }).then(function() {
        next();
      }).catch(function() {
        self.debug("New worker died quickly. Aborting restart.");
        cb();
      });
    }
    next();
  }
  #doQuit() {
    if (this.#quitting) {
      this.debug("Forceful shutdown");
      this.#size = 0;
      workerList().forEach(function(w) {
        if (w.process) {
          w.process.kill("SIGKILL");
        }
      });
      this.#exitOnce(1);
      return;
    }
    this.debug("Graceful shutdown...");
    this.#size = 0;
    this.#quitting = true;
    const self = this;
    this.#resizeTo(0).then(function() {
      self.debug("Graceful shutdown successful");
      self.#exitOnce(0);
    });
  }
  #exitOnce(code) {
    if (this.#exited) {
      return;
    }
    this.#exited = true;
    this.#exitFn(code);
  }
  #killRemaining() {
    this.#disconnectTimers.forEach(function(timer) {
      clearTimeout(timer);
    });
    this.#disconnectTimers.clear();
    const remaining = workerList().filter(function(w) {
      return !w.isDead();
    });
    if (remaining.length === 0) {
      return Promise.resolve();
    }
    return new Promise(function(resolve3) {
      let left = remaining.length;
      remaining.forEach(function(w) {
        w.once("exit", function() {
          left -= 1;
          if (left === 0) {
            resolve3();
          }
        });
        if (w.process) {
          w.process.kill("SIGKILL");
        }
      });
    });
  }
};

// src/index.ts
function need() {
  const inst = packageState().singleton;
  if (!inst) {
    throw new Error("cluster primary is not started");
  }
  return inst;
}
var clusterPrimary = function clusterPrimary2(config) {
  const st = packageState();
  if (st.singleton || st.owner) {
    throw new Error("This process already has a cluster primary");
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
};
clusterPrimary.resize = function(n) {
  need().resize(n);
};
clusterPrimary.restart = function(cb) {
  need().restart(cb);
};
clusterPrimary.quit = function() {
  need().quit();
};
clusterPrimary.quitHard = function() {
  need().quitHard();
};
clusterPrimary.debug = function(...args) {
  const inst = packageState().singleton;
  if (inst) {
    inst.debug(...args);
    return;
  }
  console.error(...args);
};
clusterPrimary.emitter = function() {
  return need().emitter();
};
clusterPrimary.close = function() {
  const st = packageState();
  const inst = st.singleton;
  if (!inst) {
    return Promise.resolve();
  }
  st.singleton = null;
  return inst.close();
};
clusterPrimary.ClusterPrimary = ClusterPrimary;
clusterPrimary.constants = constants_exports;
clusterPrimary.default = clusterPrimary;
var index_default = clusterPrimary;

exports.ClusterPrimary = ClusterPrimary;
exports.constants = constants_exports;
exports.default = index_default;
//# sourceMappingURL=index.internal.cjs.map
//# sourceMappingURL=index.internal.cjs.map