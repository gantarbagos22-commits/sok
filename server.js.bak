const express = require("express");
const path = require("path");
const WebSocket = require("ws");
const crypto = require("crypto");

const app = express();
app.use(express.json({ limit: "128kb" }));

// Always serve the frontend JavaScript fresh. This route must be registered
// BEFORE express.static(), otherwise the static middleware handles it first.
app.get("/frontend.js", (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(path.join(__dirname, "public", "frontend.js"));
});
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;
const API_WS = "wss://developer.mig33.id/developer/ws";

// One authenticated MigReborn account = one WebSocket, as required by the official API.
// The UI can issue ONE batch command that dispatches concurrently to up to 10 sockets.
const sessions = new Map();
const subscribers = new Map();
const kickExecutions = new Map();
const kickProgressSubscribers = new Map();
const balanceWaiters = new Map();
const messageWaiters = new Map();
const participantWaiters = new Map();

function makeId() { return crypto.randomBytes(16).toString("hex"); }
function safeError(err) { return String(err?.message || err || "Unknown error"); }

function extractApiError(msg) {
  const data = msg?.data || {};
  const code = String(data.code ?? data.error_code ?? data.error ?? msg?.code ?? msg?.error_code ?? "").trim();
  const message = String(data.message ?? data.detail ?? data.error_message ?? msg?.message ?? msg?.error ?? "Login failed").trim();
  return { code, message };
}

// The public MigReborn Developer API documents developer_login_failed for bad
// credentials. It does not publish a dedicated suspend error code in the docs,
// so SUSPEND is only inferred when the API itself explicitly reports a
// suspension/blocked-account code or message; otherwise the result is ERROR.
function classifyLoginFailure(err) {
  const code = String(err?.code || "").toLowerCase();
  const message = String(err?.message || err || "").toLowerCase();
  const combined = `${code} ${message}`;
  const suspended = /(?:account[_ .-]?suspended|user[_ .-]?suspended|developer[_ .-]?suspended|suspend(?:ed|ion)|account[_ .-]?(?:blocked|disabled|banned)|login[_ .-]?(?:blocked|disabled))/.test(combined);
  if (suspended) return "suspend";
  if (/developer[_ .-]?login[_ .-]?failed/.test(code)) return "error";
  if (/invalid|credential|password|username|unauthori[sz]ed|authentication|auth|timeout|connection|websocket|network|socket|server/.test(combined)) return "error";
  return "error";
}


function publish(sessionId, msg) {
  const set = subscribers.get(sessionId);
  if (!set) return;
  const payload = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of set) {
    try { res.write(payload); } catch {}
  }
}

function closeSession(sessionId, reason = "logout") {
  const account = sessions.get(sessionId);
  if (!account) return false;
  if (account.pingTimer) clearInterval(account.pingTimer);
  try { if (account.socket.readyState === WebSocket.OPEN) account.socket.close(1000, reason); } catch {}
  sessions.delete(sessionId);
  const set = subscribers.get(sessionId);
  if (set) {
    for (const res of set) { try { res.end(); } catch {} }
    subscribers.delete(sessionId);
  }
  const bw = balanceWaiters.get(sessionId);
  if (bw) { clearTimeout(bw.timer); bw.reject(new Error("Session ditutup sebelum saldo diterima.")); balanceWaiters.delete(sessionId); }
  return true;
}


function isVoteStartedKickEventServer(msg) {
  const data = msg?.data ?? msg ?? {};
  const eventType = String(msg?.type ?? data?.event_type ?? "").toLowerCase();
  const action = String(msg?.action ?? data?.action ?? "").toLowerCase();
  const status = String(msg?.status_message ?? data?.status_message ?? "").toLowerCase();
  return eventType === "room.kick.state" && action === "vote_started" &&
    /vote\s+to\s+kick/.test(status) && /\d+\s*s\s+remaining/.test(status);
}

function getVoteTriggerKeyServer(msg) {
  const data = msg?.data ?? msg ?? {};
  return [
    String(data?.room ?? "").trim().toLowerCase(),
    String(data?.target_username ?? "").trim().toLowerCase(),
    String(data?.username ?? "").trim().toLowerCase(),
    String(data?.time ?? "").trim(),
    String(data?.action ?? "").trim().toLowerCase()
  ].join("|");
}

function connectAccount(username, password, socketIndex = null) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(API_WS);
    const sessionId = makeId();
    let settled = false;
    let timeout;

    const finishReject = (err) => {
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        try { socket.close(); } catch {}
        reject(err);
      }
    };

    socket.on("open", () => {
      socket.send(JSON.stringify({ type: "developer.login", username, password }));
    });

    socket.on("message", (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }

      resolveBalance(sessionId, msg);
      resolveMessageResult(sessionId, msg);
      resolveParticipants(sessionId, msg);

      // Forward the raw API event. Socket 1 is explicitly tagged here so
      // the frontend never has to guess which authenticated WebSocket sent it.
      const receivedAt = Date.now();

      // Socket 1 is the authoritative source for the kick-vote countdown.
      // Publish its dedicated trigger first so the countdown can use the
      // server receive time even if the browser/SSE connection is briefly slow.
      // Keep the latest vote-start event server-side so a brief SSE reconnect
      // cannot make the browser miss the trigger.
      if (socketIndex === 0 && isVoteStartedKickEventServer(msg)) {
        const account = sessions.get(sessionId);
        if (account) {
          const key = getVoteTriggerKeyServer(msg);
          account.countdownTrigger = { key, event: msg, receivedAt };
          publish(sessionId, { type: "countdown.trigger", socketIndex: 0, event: msg, receivedAt });
        }
      }

      publish(sessionId, { type: "api.event", socketIndex, event: msg, receivedAt });

      if (msg.type === "auth.required") return;

      if (msg.type === "session.ready") {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);

        const account = {
          sessionId,
          username,
          socket,
          connectedAt: Date.now(),
          joinedRoom: null,
          socketIndex,
          permissions: Array.isArray(msg.data?.developer?.permissions) ? msg.data.developer.permissions : [],
          pingTimer: null,
          countdownTrigger: null
        };
        sessions.set(sessionId, account);

        socket.on("close", () => {
          if (sessions.get(sessionId)?.socket === socket) {
            publish(sessionId, { type: "session.closed", reason: "WebSocket closed" });
            closeSession(sessionId, "socket closed");
          }
        });
        socket.on("error", (err) => publish(sessionId, { type: "session.error", error: safeError(err) }));

        account.pingTimer = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) {
            try { socket.send(JSON.stringify({ type: "ping" })); } catch {}
          }
        }, 60000);

        resolve({
          sessionId,
          username,
          permissions: msg.data?.developer?.permissions || [],
          wallet: msg.data?.wallet || msg.data?.developer?.wallet || null
        });
        return;
      }

      if (msg.type === "room.join.result" && msg.data?.room) {
        const account = sessions.get(sessionId);
        if (account) account.joinedRoom = msg.data.room;
      }

      if (msg.type === "session.replaced") {
        publish(sessionId, { type: "login.status", status: "error", code: "session.replaced", message: "Session digantikan oleh login lain." });
        return;
      }

      if (msg.type === "error" && !settled) {
        const apiErr = extractApiError(msg);
        const err = new Error(apiErr.message || "Login failed");
        err.code = apiErr.code;
        err.status = classifyLoginFailure(err);
        finishReject(err);
      }
    });

    socket.on("error", finishReject);
    timeout = setTimeout(() => finishReject(new Error("Login timeout")), 15000);
  });
}


function send(sessionId, payload) {
  const account = sessions.get(sessionId);
  if (!account) throw new Error("Session tidak ditemukan / sudah terputus.");
  if (account.socket.readyState !== WebSocket.OPEN) throw new Error("WebSocket tidak terhubung.");
  account.socket.send(JSON.stringify(payload));
}

function waitForParticipants(sessionId, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const old = participantWaiters.get(sessionId);
    if (old?.timer) clearTimeout(old.timer);
    const entry = { resolve, reject, timer: null };
    entry.timer = setTimeout(() => {
      if (participantWaiters.get(sessionId) === entry) participantWaiters.delete(sessionId);
      reject(new Error("Timeout menunggu room.participants."));
    }, timeoutMs);
    participantWaiters.set(sessionId, entry);
  });
}

function resolveParticipants(sessionId, msg) {
  const type = String(msg?.type || "").toLowerCase();
  if (!type.includes("participant")) return false;
  const entry = participantWaiters.get(sessionId);
  if (!entry) return false;
  clearTimeout(entry.timer);
  participantWaiters.delete(sessionId);
  entry.resolve(msg);
  return true;
}

function waitForBalance(sessionId, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const old = balanceWaiters.get(sessionId);
    if (old?.timer) clearTimeout(old.timer);
    const entry = { resolve, reject, timer: null };
    entry.timer = setTimeout(() => {
      if (balanceWaiters.get(sessionId) === entry) balanceWaiters.delete(sessionId);
      reject(new Error("Timeout menunggu wallet.balance.result."));
    }, timeoutMs);
    balanceWaiters.set(sessionId, entry);
  });
}

function resolveBalance(sessionId, msg) {
  if (msg?.type !== "wallet.balance.result") return false;
  const entry = balanceWaiters.get(sessionId);
  if (!entry) return false;
  clearTimeout(entry.timer);
  balanceWaiters.delete(sessionId);
  const wallet = msg?.data?.wallet || null;
  if (!wallet) {
    entry.reject(new Error("wallet.balance.result tidak berisi data wallet."));
    return true;
  }
  entry.resolve(wallet);
  return true;
}

function waitForMessageResult(sessionId, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const key = String(sessionId);
    const list = messageWaiters.get(key) || [];
    const entry = { resolve, reject, timer: null };
    entry.timer = setTimeout(() => {
      const current = messageWaiters.get(key) || [];
      const next = current.filter(x => x !== entry);
      if (next.length) messageWaiters.set(key, next); else messageWaiters.delete(key);
      reject(new Error("Timeout menunggu respons room.send_message."));
    }, timeoutMs);
    list.push(entry);
    messageWaiters.set(key, list);
  });
}

function resolveMessageResult(sessionId, msg) {
  const type = String(msg?.type || "");
  if(type !== "room.send_message.queued" && type !== "error") return false;
  const key = String(sessionId);
  const list = messageWaiters.get(key);
  if(!list?.length) return false;
  messageWaiters.delete(key);
  for(const entry of list) clearTimeout(entry.timer);
  if(type === "error") {
    const apiErr = extractApiError(msg);
    for(const entry of list) entry.reject(new Error(apiErr.message || "room.send_message gagal."));
  } else {
    for(const entry of list) entry.resolve(msg);
  }
  return true;
}

function extractJobId(msg) {
  return String(msg?.data?.job?.job_id ?? msg?.data?.job_id ?? msg?.job_id ?? "").trim();
}

function getActiveSessionIds() { return [...sessions.keys()]; }

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "MIG Duel Kick 10", activeSessions: sessions.size });
});

// Single-account login retained for individual Troop controls.
app.post("/api/login", async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ ok: false, error: "Username dan password wajib diisi." });
  try {
    const result = await connectAccount(String(username).trim(), String(password), 0);
    res.json({ ok: true, account: result });
  } catch (e) {
    const status = classifyLoginFailure(e);
    res.status(401).json({ ok: false, status, code: String(e?.code || ""), error: safeError(e) });
  }
});

// ONE HTTP command opens the 10 required, separate WebSockets concurrently.
app.post("/api/login-batch", async (req, res) => {
  const input = Array.isArray(req.body?.accounts) ? req.body.accounts.slice(0, 10) : [];
  if (!input.length) return res.status(400).json({ ok: false, error: "Tidak ada Troop untuk login." });

  const jobs = input.map(async (item) => {
    const index = Number.isInteger(item?.index) ? item.index : input.indexOf(item);
    const username = String(item?.username || "").trim();
    const password = String(item?.password || "");
    if (!username || !password) return { index, ok: false, error: "Nama dan password kosong." };

    // Replace an existing session for the same Troop slot before reconnecting.
    const oldSessionId = String(item?.sessionId || "");
    if (oldSessionId) closeSession(oldSessionId, "relogin");

    try {
      const account = await connectAccount(username, password, index);
      return { index, ok: true, account };
    } catch (e) {
      return { index, ok: false, username, status: classifyLoginFailure(e), code: String(e?.code || ""), error: safeError(e) };
    }
  });

  const results = await Promise.all(jobs);
  res.json({ ok: results.some(x => x.ok), results });
});

function createKickExecution(meta) {
  const id = makeId();
  const execution = { id, meta, done: false, result: null, latest: { type: "kick.progress", phase: "created", ...meta, completedSteps: 0, totalSteps: Number(meta.totalSteps) || 0, percent: 0 } };
  kickExecutions.set(id, execution);
  setTimeout(() => {
    const current = kickExecutions.get(id);
    if (current && current.done) kickExecutions.delete(id);
  }, 10 * 60 * 1000);
  return execution;
}

function publishKickProgress(execution, event) {
  if (!execution) return;
  execution.latest = { type: "kick.progress", ...event };
  const set = kickProgressSubscribers.get(execution.id);
  if (!set) return;
  const payload = `data: ${JSON.stringify({
    ok: true,
    executionId: execution.id,
    done: execution.done,
    progress: execution.latest,
    result: execution.done ? execution.result : null
  })}\n\n`;
  for (const res of set) {
    try { res.write(payload); } catch {}
  }
}

app.get("/api/kick-progress-stream", (req, res) => {
  const id = String(req.query.id || "");
  const execution = kickExecutions.get(id);
  if (!execution) return res.status(404).end();

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  if (!kickProgressSubscribers.has(id)) kickProgressSubscribers.set(id, new Set());
  kickProgressSubscribers.get(id).add(res);

  res.write(`data: ${JSON.stringify({
    ok: true,
    executionId: id,
    done: execution.done,
    progress: execution.latest,
    result: execution.done ? execution.result : null
  })}\n\n`);

  const keepAlive = setInterval(() => {
    try { res.write(": keep-alive\n\n"); } catch {}
  }, 20000);

  req.on("close", () => {
    clearInterval(keepAlive);
    const set = kickProgressSubscribers.get(id);
    if (set) {
      set.delete(res);
      if (!set.size) kickProgressSubscribers.delete(id);
    }
  });
});


app.get("/api/kick-progress-state", (req, res) => {
  const id = String(req.query.id || "");
  const execution = kickExecutions.get(id);
  if (!execution) return res.status(404).json({ ok: false, error: "Execution tidak ditemukan." });
  return res.json({ ok: true, executionId: id, done: execution.done, progress: execution.latest, result: execution.done ? execution.result : null });
});


app.get("/api/events", (req, res) => {
  const sessionId = String(req.query.sessionId || "");
  if (!sessionId || !sessions.has(sessionId)) return res.status(401).end();
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
  if (!subscribers.has(sessionId)) subscribers.set(sessionId, new Set());
  subscribers.get(sessionId).add(res);
  res.write(`data: ${JSON.stringify({ type: "stream.ready" })}\n\n`);
  const account = sessions.get(sessionId);
  if (account?.socketIndex === 0 && account.countdownTrigger) {
    const t = account.countdownTrigger;
    res.write(`data: ${JSON.stringify({ type: "countdown.trigger", socketIndex: 0, event: t.event, receivedAt: t.receivedAt })}\n\n`);
  }
  const keepAlive = setInterval(() => { try { res.write(": keep-alive\n\n"); } catch {} }, 20000);
  req.on("close", () => {
    clearInterval(keepAlive);
    const set = subscribers.get(sessionId);
    if (set) { set.delete(res); if (!set.size) subscribers.delete(sessionId); }
  });
});

// Single-account action retained for individual Troop controls.
app.post("/api/action", async (req, res) => {
  const { sessionId, action, room, targetUsername, message } = req.body || {};
  if (!sessionId || !action) return res.status(400).json({ ok: false, error: "Parameter tidak lengkap." });
  try {
    if (action === "join") { if (!room) throw new Error("Room wajib diisi."); send(sessionId, { type: "room.join", room }); }
    else if (action === "leave") { if (!room) throw new Error("Room wajib diisi."); send(sessionId, { type: "room.leave", room }); }
    else if (action === "participants") {
      if (!room) throw new Error("Room wajib diisi.");
      const waiter = waitForParticipants(sessionId, 8000);
      try {
        send(sessionId, { type: "room.participants", room });
        const event = await waiter;
        return res.json({ ok: true, sent: action, event });
      } catch (e) {
        const pending = participantWaiters.get(sessionId);
        if (pending?.timer) clearTimeout(pending.timer);
        participantWaiters.delete(sessionId);
        throw e;
      }
    }
    else if (action === "kick") { if (!room || !targetUsername) throw new Error("Room dan target wajib diisi."); send(sessionId, { type: "room.kick", room, target_username: targetUsername }); }
    else if (action === "message") { if (!room || !message) throw new Error("Room dan pesan wajib diisi."); send(sessionId, { type: "room.send_message", room, message }); }
    else if (action === "balance") send(sessionId, { type: "wallet.balance" });
    else throw new Error("Action tidak dikenal.");
    res.json({ ok: true, sent: action });
  } catch (e) { res.status(400).json({ ok: false, error: safeError(e) }); }
});

// Balance is a direct WebSocket response. Collect the response per session so
// CEK SALDO ALL does not depend on the single room-event SSE connection.
app.post("/api/balance-all", async (req, res) => {
  const ids = Array.isArray(req.body?.sessionIds)
    ? [...new Set(req.body.sessionIds.map(String))].slice(0, 10)
    : [];
  if (!ids.length) return res.status(400).json({ ok: false, error: "Tidak ada Troop yang ONLINE." });

  const results = await Promise.all(ids.map(async (sessionId) => {
    try {
      const waiter = waitForBalance(sessionId, 8000);
      send(sessionId, { type: "wallet.balance" });
      const wallet = await waiter;
      return { sessionId, ok: true, wallet };
    } catch (e) {
      const pending = balanceWaiters.get(sessionId);
      if (pending?.timer) clearTimeout(pending.timer);
      balanceWaiters.delete(sessionId);
      return { sessionId, ok: false, error: safeError(e) };
    }
  }));

  const success = results.filter(x => x.ok).length;
  res.json({ ok: success > 0, action: "balance", sent: ids.length, success, total: ids.length, results });
});

// ONE HTTP command dispatches the same official command concurrently to up to 10 WebSockets.
app.post("/api/batch-action", (req, res) => {
  const { sessionIds, action, room, targetUsername, message } = req.body || {};
  const ids = Array.isArray(sessionIds) ? [...new Set(sessionIds.map(String))].slice(0, 10) : [];
  if (!ids.length || !action) return res.status(400).json({ ok: false, error: "Session atau action tidak lengkap." });

  let payload;
  if (action === "join") { if (!room) return res.status(400).json({ ok: false, error: "Room wajib diisi." }); payload = { type: "room.join", room }; }
  else if (action === "leave") { if (!room) return res.status(400).json({ ok: false, error: "Room wajib diisi." }); payload = { type: "room.leave", room }; }
  else if (action === "participants") { if (!room) return res.status(400).json({ ok: false, error: "Room wajib diisi." }); payload = { type: "room.participants", room }; }
  else if (action === "balance") payload = { type: "wallet.balance" };
  else if (action === "kick") { if (!room || !targetUsername) return res.status(400).json({ ok: false, error: "Room dan target wajib diisi." }); payload = { type: "room.kick", room, target_username: targetUsername }; }
  else if (action === "message") { if (!room || !message) return res.status(400).json({ ok: false, error: "Room dan pesan wajib diisi." }); payload = { type: "room.send_message", room, message }; }
  else return res.status(400).json({ ok: false, error: "Action tidak dikenal." });

  const results = [];
  for (const sessionId of ids) {
    try { send(sessionId, payload); results.push({ sessionId, ok: true }); }
    catch (e) { results.push({ sessionId, ok: false, error: safeError(e) }); }
  }
  res.json({ ok: results.some(x => x.ok), action, sent: results.filter(x => x.ok).length, total: results.length, results });
});


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function waitBatchDelay(delayMs) {
  const ms = Math.max(0, Number(delayMs) || 0);
  return ms > 0 ? sleep(ms) : Promise.resolve();
}


app.post("/api/kick-loop", async (req, res) => {
  const body = req.body || {};
  const { sessionIds, room, targets, websocketSlots } = body;
  const textdelay = body.textdelay;
  const delayBatch = body.delayBatch;
  const textloop = body.textloop;
  const burstSize = Math.max(1, Math.min(parseInt(body.burstSize, 10) || 3, 10));

  // Preserve the physical Troop/WebSocket slot. Do not compact the list when
  // a middle Troop is offline: T1 must always mean WebSocket slot 1, etc.
  const slotEntries = Array.isArray(websocketSlots)
    ? websocketSlots
        .map(x => ({ sessionId: String(x?.sessionId || "").trim(), websocket: Number(x?.websocket) }))
        .filter(x => x.sessionId && Number.isInteger(x.websocket) && x.websocket >= 1 && x.websocket <= 10)
        .sort((a, b) => a.websocket - b.websocket)
        .filter((x, i, arr) => i === arr.findIndex(y => y.websocket === x.websocket))
    : [];
  const ids = slotEntries.length
    ? slotEntries.map(x => x.sessionId)
    : (Array.isArray(sessionIds)
        ? [...new Set(sessionIds.map(String).filter(Boolean))].slice(0, 10)
        : []);
  const targetList = Array.isArray(targets)
    ? targets.map(x => String(x).trim()).filter(Boolean).slice(0, 10)
    : [];
  const targetDelayMs = Math.max(0, Math.min(Number(textdelay) || 0, 86400000));
  const delayMs = Math.max(0, Math.min(Number(delayBatch) || 0, 86400000));
  const loopCount = Math.max(1, Math.min(parseInt(textloop, 10) || 1, 100));

  if (!ids.length) return res.status(400).json({ ok: false, error: "Tidak ada Troop yang ONLINE." });
  if (!room) return res.status(400).json({ ok: false, error: "Room wajib diisi." });
  if (!targetList.length) return res.status(400).json({ ok: false, error: "Target kick kosong." });

  // One independent sequence per WebSocket:
  // Troop-1: target 1 -> delay -> target 2 -> ... -> target 10 -> delay -> loop 2
  // Troop-2 does the same sequence concurrently, and so on.
  // Race mode: dispatch small bursts per WebSocket without response verification.
  const totalSteps = loopCount * targetList.length;
  const totalJobs = totalSteps * ids.length;
  const execution = createKickExecution({
    room, websockets: ids.length, loops: loopCount, targets: targetList.length,
    textdelay: targetDelayMs, delayBatch, targetDelayMs, textloop: loopCount, burstSize, totalSteps, totalJobs,
    targetProgress: targetList.map((target, i) => ({ targetIndex: i + 1, target, completed: 0, dispatched: 0, total: ids.length * loopCount })),
    wsProgress: (slotEntries.length ? slotEntries : ids.map((sessionId, i) => ({ sessionId, websocket: i + 1 })))
      .map(x => ({ websocket: x.websocket, sessionId: x.sessionId, completed: 0, dispatched: 0, total: totalSteps, failed: 0 }))
  });

  (async () => {
    let completedSteps = 0;
    let failedJobs = 0;
    let dispatchedJobs = 0;
    const targetProgress = targetList.map((target, i) => ({ targetIndex: i + 1, target, completed: 0, dispatched: 0, total: ids.length * loopCount }));
    const sequenceResults = [];
    const wsProgress = (slotEntries.length ? slotEntries : ids.map((sessionId, i) => ({ sessionId, websocket: i + 1 })))
      .map(x => ({ websocket: x.websocket, sessionId: x.sessionId, completed: 0, dispatched: 0, total: totalSteps, failed: 0 }));
    // O(1) WebSocket progress lookup for the dispatch hot path.
    const wsProgressBySlot = Object.create(null);
    for (const state of wsProgress) wsProgressBySlot[state.websocket] = state;

    let kickProgressScheduled = false;
    let kickProgressTimer = null;
    let kickProgressContext = null;

    // Coalesce frequent progress updates so the dispatch hot path does not
    // create a Promise-chain entry for every target. UI polling still receives
    // the latest counters shortly after a burst.
    function scheduleKickProgress(context) {
      kickProgressContext = context;
      if (kickProgressScheduled) return;
      kickProgressScheduled = true;
      kickProgressTimer = setTimeout(() => {
        kickProgressTimer = null;
        kickProgressScheduled = false;
        const ctx = kickProgressContext;
        kickProgressContext = null;
        if (!ctx) return;
        for (const tp of targetProgress) {
          tp.completed = Math.min(tp.total, Math.floor(tp.dispatched / Math.max(1, ids.length)));
        }
        completedSteps = Math.min(totalSteps, targetProgress.reduce((sum, tp) => sum + tp.completed, 0));
        publishKickProgress(execution, {
          ...ctx,
          completedSteps,
          totalSteps,
          dispatchedJobs,
          totalJobs,
          percent: totalJobs > 0 ? Math.round((dispatchedJobs / totalJobs) * 100) : 0,
          sent: dispatchedJobs,
          failedJobs,
          targetProgress: targetProgress.map(x => ({ ...x })),
          wsProgress: wsProgress.map(x => ({ ...x }))
        });
      }, 25);
    }


    // Independent pair execution per WebSocket:
    // WS 1-10: 1-2 -> delay -> 3-4 -> delay -> 5-6 -> delay -> 7-8 -> delay -> 9-10
    // Each WebSocket runs its own sequence independently; there is NO barrier between WebSockets.
    const wsEntries = slotEntries.length
      ? slotEntries.map(x => ({ sessionId: x.sessionId, websocket: x.websocket }))
      : ids.map((sessionId, i) => ({ sessionId, websocket: i + 1 }));
    const RACE_BURST = burstSize;

    // HARD SAFETY LIMIT: maximum 100 kick dispatches per physical WebSocket
    // in any rolling 1-second window for KICK ALL.
    // It is intentionally per-WebSocket, not global: 6 sockets can each send
    // at most 100 kicks/sec, while no individual socket can exceed 100/sec.
    const kickRateLimit = 150;
    const kickRateWindowMs = 1000;
    const wsDispatchHistory = new Map();

    async function waitForKickRateLimit(websocket) {
      let history = wsDispatchHistory.get(websocket);
      if (!history) {
        history = { times: [], head: 0 };
        wsDispatchHistory.set(websocket, history);
      }
      while (true) {
        const now = Date.now();
        const times = history.times;
        let head = history.head;
        while (head < times.length && now - times[head] >= kickRateWindowMs) head++;
        history.head = head;

        if (times.length - head < kickRateLimit) {
          times.push(now);
          return;
        }

        const waitMs = Math.max(1, kickRateWindowMs - (now - times[head]));
        await sleep(waitMs);

        // Compact only after the stale prefix becomes substantial. This avoids
        // repeated Array.shift() work while preserving the same rolling-window limit.
        if (history.head > 256 && history.head * 2 > times.length) {
          history.times = times.slice(history.head);
          history.head = 0;
        }
      }
    }

    // Payload strings are prebuilt once. WebSocket validation is performed
    // inside the protected execution block so a disconnected troop is reported
    // as an execution error instead of becoming an unhandled async rejection.
    const kickPayloads = targetList.map(targetUsername =>
      JSON.stringify({ type: "room.kick", room, target_username: targetUsername })
    );

    async function sendTarget(runtime, round, targetIndex, sequencePosition) {
      const { sessionId, websocket, socket } = runtime;
      const targetUsername = targetList[targetIndex];
      const startedAt = Date.now();
      const result = {
        sessionId, websocket, loop: round + 1, target: targetUsername,
        targetIndex: targetIndex + 1, sequencePosition, direction: "forward",
        ok: false, jobStatus: "sent", jobId: null, error: null, totalMs: 0
      };

      try {
        if (socket.readyState !== WebSocket.OPEN) throw new Error("WebSocket tidak terhubung.");

        // Per-WebSocket rate limit only; no API response/ACK is awaited.
        await waitForKickRateLimit(websocket);
        // Instant dispatch: send directly without waiting for an API response.
        socket.send(kickPayloads[targetIndex]);

        dispatchedJobs++;
        targetProgress[targetIndex].dispatched++;
        targetProgress[targetIndex].completed = Math.min(
          targetProgress[targetIndex].total,
          Math.floor(targetProgress[targetIndex].dispatched / Math.max(1, ids.length))
        );
        const wsState = wsProgressBySlot[websocket];
        if (wsState) wsState.dispatched++;
        result.ok = true;
        result.jobStatus = "sent";
        result.totalMs = Math.max(0, Date.now() - startedAt);

        scheduleKickProgress({
          phase: "dispatched",
          loop: round + 1, targetIndex: targetIndex + 1, target: targetUsername,
          sessionId, websocket, direction: "forward",
          sendConfirmed: true, noAck: true, burstSize: RACE_BURST,
          burst: Math.floor(targetIndex / RACE_BURST) + 1,
          burstTotal: Math.ceil(targetList.length / RACE_BURST)
        });

        return result;
      } catch (e) {
        result.ok = false;
        result.jobStatus = "send_failed";
        result.error = safeError(e);
        result.totalMs = Math.max(0, Date.now() - startedAt);
        failedJobs++;
        const wsState = wsProgressBySlot[websocket];
        if (wsState) wsState.failed++;

        scheduleKickProgress({
          phase: "send_failed",
          loop: round + 1, targetIndex: targetIndex + 1, target: targetUsername,
          sessionId, websocket, direction: "forward",
          sendConfirmed: false, noAck: true, error: result.error
        });
        return result;
      }
    }


    async function runTroop(runtime) {
      const { sessionId, websocket: wsOrdinal } = runtime;
      const troopResults = [];
      const orderedIndices = Array.from({ length: targetList.length }, (_, i) => i);

      for (let round = 0; round < loopCount; round++) {
        for (let pos = 0; pos < orderedIndices.length; pos += RACE_BURST) {
          const burstIndexes = orderedIndices.slice(pos, pos + RACE_BURST);

          // Dispatch targets in the burst with the configured per-target delay.
          for (let burstPos = 0; burstPos < burstIndexes.length; burstPos++) {
            const targetIndex = burstIndexes[burstPos];
            const targetUsername = targetList[targetIndex];
            const dispatched = await sendTarget(
              runtime, round, targetIndex, pos + 1
            );
            troopResults.push(dispatched);
            if (targetDelayMs > 0 && burstPos < burstIndexes.length - 1) {
              await sleep(targetDelayMs);
            }
          }
          // Delay hanya diterapkan di antara burst dalam loop yang sama.
          const isEndOfLoop = pos + RACE_BURST >= orderedIndices.length;
          if (delayMs > 0 && !isEndOfLoop) {
            await waitBatchDelay(delayMs);
          }
        }
      }
      return { sessionId, websocket: wsOrdinal, results: troopResults, steps: troopResults.length, orderedIndices };
    }

    try {
      const troopRuntime = wsEntries.map(({ sessionId, websocket }) => {
        const account = sessions.get(sessionId);
        if (!account || account.socket.readyState !== WebSocket.OPEN) {
          throw new Error(`WebSocket T${websocket} tidak terhubung.`);
        }
        // Jangan blokir berdasarkan metadata permission lokal. KICK ALL normal
        // juga mengirim langsung melalui WebSocket; metadata permission dapat
        // tidak tersedia/tidak sinkron walaupun socket sebenarnya bisa kick.
        return { sessionId, websocket, socket: account.socket };
      });


      publishKickProgress(execution, {
        phase: "started",
        completedSteps: 0,
        totalSteps,
                dispatchedJobs: 0,
        totalJobs,
        percent: 0,
        loop: 1,
        targetIndex: 1,
        target: targetList[0],
        total: ids.length,
        sent: 0,
        failedJobs: 0,
        sendConfirmed: true,
        noAck: true,
        targetProgress: targetProgress.map(x => ({ ...x })),
        wsProgress: wsProgress.map(x => ({ ...x }))
      });

      let flatResults;
      let results;
      // KICK ALL normal.
      results = await Promise.all(
        troopRuntime.map(runtime => runTroop(runtime))
      );
      flatResults = results.map(x => x.results).flat();
      sequenceResults.push(...results);

      // Flush any delayed coalesced progress before the final state.
      if (kickProgressTimer) { clearTimeout(kickProgressTimer); kickProgressTimer = null; }
      kickProgressScheduled = false;
      kickProgressContext = null;

      // Completion follows transport dispatch; no API response is awaited.
      const allJobsSucceeded = dispatchedJobs === totalJobs && failedJobs === 0;
      publishKickProgress(execution, {
        phase: allJobsSucceeded ? "completed" : "completed_with_errors",
        completedSteps: Math.min(totalSteps, targetProgress.reduce((sum, tp) => sum + tp.completed, 0)),
        totalSteps,
        dispatchedJobs,
        totalJobs,
        percent: totalJobs > 0 ? Math.round((dispatchedJobs / totalJobs) * 100) : 0,
        loop: loopCount,
        targetIndex: targetList.length,
        target: targetList[targetList.length - 1],
        total: ids.length,
        sent: dispatchedJobs,
        failedJobs,
        sendConfirmed: true,
        noAck: true,
        targetProgress: targetProgress.map(x => ({ ...x })),
        wsProgress: wsProgress.map(x => ({ ...x }))
      });
      execution.done = true;
      execution.completedAt = Date.now();
      execution.results = flatResults;
    } catch (e) {
      execution.done = true;
      execution.error = safeError(e);
      publishKickProgress(execution, { phase: "error", error: execution.error, dispatchedJobs, totalJobs, sent: dispatchedJobs, failedJobs, targetProgress: targetProgress.map(x => ({ ...x })), wsProgress: wsProgress.map(x => ({ ...x })) });
    }

  })();

  res.json({
    ok: true,
    action: "kick-loop",
    executionId: execution.id,
    mode: `race_burst_${burstSize}_instant_dispatch`,
    websockets: ids.length,
    targets: targetList.length,
    loops: loopCount,
    textdelay: delayMs,
    totalSteps,
    totalJobs,
    noAck: true
  });
});

app.post("/api/logout", (req, res) => {
  const { sessionId } = req.body || {};
  closeSession(String(sessionId || ""), "logout");
  res.json({ ok: true });
});

// ONE logout command for all active sessions.
app.post("/api/logout-batch", (req, res) => {
  const ids = Array.isArray(req.body?.sessionIds) ? [...new Set(req.body.sessionIds.map(String))].slice(0, 10) : getActiveSessionIds();
  let closed = 0;
  for (const id of ids) if (closeSession(id, "logout all")) closed++;
  res.json({ ok: true, closed });
});

app.get("*", (_req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.listen(PORT, "0.0.0.0", () => console.log(`MIG Duel Kick 10 running on port ${PORT}`));
