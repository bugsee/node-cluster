import * as os from 'node:os';
import * as path from 'node:path';
import * as util from 'node:util';
import cluster2 from 'node:cluster';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as repl from 'node:repl';

var __defProp = Object.defineProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// src/constants.ts
var constants_exports = {};
__export(constants_exports, {
  DEFAULT_REPL: () => DEFAULT_REPL,
  MIN_ALIVE_MS: () => MIN_ALIVE_MS,
  SKEPTIC_TIMEOUT_MS: () => SKEPTIC_TIMEOUT_MS,
  STOP_TIMEOUT_MS: () => STOP_TIMEOUT_MS
});
var STOP_TIMEOUT_MS = 5e3;
var SKEPTIC_TIMEOUT_MS = 2e3;
var MIN_ALIVE_MS = 2e3;
var DEFAULT_REPL = process.env.CLUSTER_MASTER_REPL || "cluster-master-socket";
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
      const w = cluster2.workers?.[d.id];
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
    listenTarget = path.resolve(replAddressPath);
  } else if (typeof replAddressPath === "number") {
    listenTarget = replAddressPath;
  } else if (isAddressPort(replAddressPath)) {
    listenTarget = replAddressPath.port;
    socketAddress = replAddressPath.address;
  }
  let connections = 0;
  let nextSockId = 0;
  let replServer = null;
  const socketPath = typeof listenTarget === "string" ? listenTarget : null;
  function attachContext(r, sock) {
    const helpCommands = [
      "help        - display these commands",
      "repl        - access the REPL",
      "resize(n)   - resize the cluster to `n` workers",
      "restart(cb) - gracefully restart workers, cb is optional",
      "stop()      - gracefully stop workers and master",
      "kill()      - forcefully kill workers and master",
      "cluster     - node.js cluster module",
      "size        - current cluster size",
      "connections - number of REPL connections to master",
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
      cluster: cluster2,
      get size() {
        return host.getSize();
      },
      get connections() {
        return connections;
      },
      get workers() {
        const p = select(cluster2.workers, "pid");
        const s = select(cluster2.workers, "state");
        const a = select(cluster2.workers, "age");
        return Object.keys(cluster2.workers || {}).map(function(k) {
          return createReplWorker({
            id: k,
            pid: p[k],
            state: s[k],
            age: a[k]
          });
        });
      },
      select: function(field) {
        return select(cluster2.workers, field);
      },
      get pids() {
        return select(cluster2.workers, "pid");
      },
      get ages() {
        return select(cluster2.workers, "age");
      },
      get states() {
        return select(cluster2.workers, "state");
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
    const r = repl.start({
      prompt: "ClusterMaster (`help` for cmds) " + process.pid + " " + String(sock.id) + "> ",
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
    if (listenTarget === null) {
      return;
    }
    replServer = net.createServer(onConnection);
    function onListening() {
      if (socketAddress) {
        host.debug("ClusterMaster repl listening on " + socketAddress + ":" + String(listenTarget));
      } else {
        host.debug("ClusterMaster repl listening on " + String(listenTarget));
      }
    }
    if (socketAddress) {
      replServer.listen(Number(listenTarget), socketAddress, onListening);
    } else if (listenTarget !== null) {
      replServer.listen(listenTarget, onListening);
    }
  }
  if (socketPath) {
    fs.unlink(socketPath, function(err) {
      if (err && err.code !== "ENOENT") {
        throw err;
      }
      listen();
    });
  } else {
    listen();
  }
  return {
    close: function() {
      return new Promise(function(resolve3) {
        const debugStreams = host.debugStreams;
        Object.keys(debugStreams).forEach(function(key) {
          try {
            debugStreams[key]?.destroy();
          } catch {
          }
          delete debugStreams[key];
        });
        if (!replServer) {
          resolve3();
          return;
        }
        const server = replServer;
        replServer = null;
        server.close(function() {
          if (!socketPath) {
            resolve3();
            return;
          }
          fs.unlink(socketPath, function() {
            resolve3();
          });
        });
      });
    }
  };
}

// src/primary.ts
function workerList() {
  const dict = cluster2.workers;
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
  return cluster2.isPrimary === true;
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
  #silenceDebug;
  #aliveEvent;
  #replAddress;
  #replHelp;
  #replContext;
  #exitFn;
  #emitter = new EventEmitter();
  #debugStreams = {};
  #nextWorkerIdx = 0;
  #replaceWorkerIdxs = [];
  #quitting = false;
  #restarting = false;
  #resizing = false;
  #started = false;
  #closed = false;
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
        "ClusterMaster answers to no one!\n(don't run in a cluster worker script)"
      );
    }
    this.#config = cfg;
    this.#size = typeof cfg.size === "number" ? cfg.size : os.cpus().length;
    this.#env = cfg.env || {};
    this.#onMessage = cfg.onMessage || cfg.onmessage;
    this.#signalsEnabled = cfg.signals !== false;
    this.#stopTimeout = cfg.stopTimeout || STOP_TIMEOUT_MS;
    this.#skepticTimeout = cfg.skepticTimeout || SKEPTIC_TIMEOUT_MS;
    this.#minAliveMs = cfg.minAliveMs || MIN_ALIVE_MS;
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
    if (this.#started) {
      throw new Error("This cluster has a master already");
    }
    this.#started = true;
    const masterConf = { exec: path.resolve(this.#config.exec) };
    if (this.#config.silent) {
      masterConf.silent = true;
    }
    if (this.#config.args) {
      masterConf.args = this.#config.args;
    }
    this.#previousSettings = Object.assign({}, cluster2.settings);
    cluster2.setupPrimary(masterConf);
    cluster2.on("fork", this.#onFork);
    if (this.#signalsEnabled) {
      this.#installSignals();
    }
    const self = this;
    this.debug(this.#replAddress ? "resize and then setup repl" : "resize");
    return this.#resizeTo().then(function() {
      if (!self.#closed) {
        self.#startRepl();
      }
    });
  }
  emitter() {
    return this.#emitter;
  }
  debug(...args) {
    if (!this.#silenceDebug) {
      console.error(...args);
    }
    this.#emitter.emit("debug", ...args);
    const msg = util.format(...args);
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
    cluster2.removeListener("fork", this.#onFork);
    const self = this;
    const replClose = this.#repl ? this.#repl.close() : Promise.resolve();
    this.#repl = null;
    return replClose.then(function() {
      return self.#killRemaining();
    }).then(function() {
      if (self.#previousSettings) {
        cluster2.setupPrimary(self.#previousSettings);
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
      if (workerList().length < self.#size && !self.#resizing) {
        self.#resizeTo();
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
    const cp = cluster2.fork(childEnv);
    cp.clusterIdx = nextIdx;
    return cp;
  }
  #waitAlive(worker) {
    const self = this;
    return new Promise(function(resolve3, reject) {
      let done = false;
      let onAlive;
      let onExit;
      function finish(err, value) {
        if (done) {
          return;
        }
        done = true;
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
      worker.once(self.#aliveEvent, onAlive);
      worker.once("exit", onExit);
    });
  }
  #forkAndWaitAlive() {
    return this.#waitAlive(this.#forkChild());
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
      }
    });
  }
  #resizeTo(n, cb) {
    let target;
    let done = cb;
    if (typeof n === "function") {
      done = n;
      target = this.#size;
    } else {
      target = n;
    }
    const p = this.#resizeInner(target);
    if (done) {
      const finished = done;
      p.then(function() {
        finished();
      }, function() {
        finished();
      });
    }
    return p;
  }
  #resizeInner(n) {
    if (this.#closed) {
      return Promise.resolve();
    }
    if (this.#resizing) {
      return Promise.resolve();
    }
    if (typeof n === "number" && n >= 0) {
      if (n < this.#size) {
        this.#nextWorkerIdx = n;
      }
      this.#size = n;
    }
    const current = sortedWorkers();
    const req = this.#size - current.length;
    if (req === 0) {
      return Promise.resolve();
    }
    this.#resizing = true;
    const self = this;
    let work;
    if (req > 0) {
      const forks = [];
      for (let i = 0; i < req; i += 1) {
        this.debug("resizing up", req - i - 1);
        forks.push(this.#forkAndWaitAlive());
      }
      work = Promise.all(forks);
    } else {
      const extras = current.slice(this.#size);
      work = Promise.all(extras.map(function(worker) {
        self.debug("resizing down", worker.id);
        return self.#disconnectAndWaitExit(worker);
      }));
    }
    return work.then(function() {
      self.#resizing = false;
    }, function() {
      self.#resizing = false;
    });
  }
  #doRestart(cb) {
    if (this.#restarting) {
      this.debug("Already restarting.  Cannot restart yet.");
      return;
    }
    this.#restarting = true;
    const current = Object.keys(cluster2.workers || {});
    const reqs = this.#size - current.length;
    const self = this;
    function finish() {
      self.#restarting = false;
      if (cb) {
        cb();
      }
    }
    if (reqs !== 0) {
      this.debug("resize %d -> %d, change = %d", current.length, this.#size, reqs);
      this.#resizeTo(this.#size, function() {
        self.debug("resize cb");
        self.#rollingReplace(Object.keys(cluster2.workers || {}), finish);
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
      const worker = id !== void 0 && cluster2.workers ? cluster2.workers[id] : void 0;
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
        self.#restarting = false;
      });
    }
    next();
  }
  #doQuit() {
    if (this.#quitting) {
      this.debug("Forceful shutdown");
      workerList().forEach(function(w) {
        if (w.process) {
          w.process.kill("SIGKILL");
        }
      });
      this.#exitFn(1);
      return;
    }
    this.debug("Graceful shutdown...");
    this.#size = 0;
    this.#quitting = true;
    const self = this;
    this.#resizeTo(0).then(function() {
      self.debug("Graceful shutdown successful");
      self.#exitFn(0);
    });
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
var instance = null;
function need() {
  if (!instance) {
    throw new Error("cluster primary is not started");
  }
  return instance;
}
var clusterPrimary = function clusterPrimary2(config) {
  if (instance) {
    throw new Error("This cluster has a master already");
  }
  instance = new ClusterPrimary(config);
  instance.start();
  return instance.emitter();
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
  if (instance) {
    instance.debug(...args);
    return;
  }
  console.error(...args);
};
clusterPrimary.emitter = function() {
  return need().emitter();
};
clusterPrimary.close = function() {
  if (!instance) {
    return Promise.resolve();
  }
  const inst = instance;
  instance = null;
  return inst.close();
};
clusterPrimary.ClusterPrimary = ClusterPrimary;
clusterPrimary.constants = constants_exports;
var index_default = clusterPrimary;

export { ClusterPrimary, constants_exports as constants, index_default as default };
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map