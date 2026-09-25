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
const usernameSessions = new Map();
const subscribers = new Map();
const kickExecutions = new Map();
const kickProgressSubscribers = new Map();

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

function clearPending(account, reason) {
  if (!account?.pending) return;
  for (const key of ["join", "leave", "participants", "balance", "message"]) {
    const pending = account.pending[key];
    if (!pending) continue;
    if (pending.timer) clearTimeout(pending.timer);
    try { pending.reject(new Error(reason)); } catch {}
    account.pending[key] = null;
  }
}

function closeSession(sessionId, reason = "logout") {
  const account = sessions.get(sessionId);
  if (!account) return false;
  clearPending(account, `Session ditutup: ${reason}`);
  if (account.pingTimer) clearInterval(account.pingTimer);
  sessions.delete(sessionId);
  if (usernameSessions.get(String(account.username).toLowerCase()) === sessionId) usernameSessions.delete(String(account.username).toLowerCase());
  const set = subscribers.get(sessionId);
  if (set) { for (const res of set) { try { res.end(); } catch {} } subscribers.delete(sessionId); }
  try {
    if (account.socket.readyState === WebSocket.OPEN || account.socket.readyState === WebSocket.CONNECTING) account.socket.close(1000, String(reason).slice(0,120));
  } catch {}
  return true;
}

function getActiveSessionIds() { return [...sessions.keys()]; }
function getSession(sessionId) {
  const account = sessions.get(String(sessionId || ""));
  if (!account) throw new Error("Session tidak ditemukan / sudah logout.");
  if (account.socket.readyState !== WebSocket.OPEN) throw new Error("WebSocket tidak terhubung.");
  return account;
}
function normalizeRoomName(value) { return String(value ?? "").trim().toLowerCase(); }
function roomName(value) { return String(value ?? "").trim(); }
function apiErrorFromMessage(msg) {
  const data = msg?.data || {};
  return {
    code: String(data.code ?? data.error_code ?? data.error ?? msg?.code ?? msg?.error_code ?? "").trim(),
    message: String(data.message ?? data.detail ?? data.error_message ?? msg?.message ?? msg?.error ?? "API error").trim()
  };
}
function directResultOK(msg) {
  if (String(msg?.type || "") === "error") return false;
  const data = msg?.data || {};
  const status = String(data.status ?? msg?.status ?? "").trim().toLowerCase();
  const err = String(data.error ?? data.error_code ?? msg?.error ?? msg?.error_code ?? "").trim();
  return !err && !["error", "failed", "failure", "denied", "rejected", "forbidden"].includes(status);
}
function waitForDirect(account, key, matcher, label, timeoutMs = 10000) {
  if (account.pending[key]) throw new Error(`${label} sedang diproses.`);
  return new Promise((resolve, reject) => {
    const pending = { resolve, reject, matcher, timer: null };
    pending.timer = setTimeout(() => {
      if (account.pending[key] !== pending) return;
      account.pending[key] = null;
      reject(new Error(`${label} tidak menerima response API.`));
    }, timeoutMs);
    account.pending[key] = pending;
  });
}
function resolveDirect(account, key, msg) {
  const pending = account?.pending?.[key];
  if (!pending || !pending.matcher(msg)) return false;
  clearTimeout(pending.timer);
  account.pending[key] = null;
  if (!directResultOK(msg)) {
    const e = apiErrorFromMessage(msg); const err = new Error(e.message || `${key} gagal.`); err.code=e.code; err.event=msg; pending.reject(err);
  } else pending.resolve(msg);
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

      const liveAccount = sessions.get(sessionId);
      if (liveAccount) {
        resolveDirect(liveAccount, "join", msg);
        resolveDirect(liveAccount, "leave", msg);
        resolveDirect(liveAccount, "participants", msg);
        resolveDirect(liveAccount, "balance", msg);
        resolveDirect(liveAccount, "message", msg);
      }

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
          countdownTrigger: null,
          pending: { join:null, leave:null, participants:null, balance:null, message:null }
        };
        const previous = usernameSessions.get(username.toLowerCase());
        if (previous && previous !== sessionId) closeSession(previous, "relogin");
        sessions.set(sessionId, account);
        usernameSessions.set(username.toLowerCase(), sessionId);

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
        }, 30000);

        resolve({
          sessionId,
          username,
          permissions: msg.data?.developer?.permissions || [],
          wallet: msg.data?.wallet || msg.data?.developer?.wallet || null
        });
        return;
      }

      if (msg.type === "room.join.result") {
        const account = sessions.get(sessionId);
        const joinedRoom = String(msg.data?.room ?? msg.room ?? "").trim();
        if (account && joinedRoom) account.joinedRoom = joinedRoom;
      }

      if (msg.type === "session.replaced") {
        publish(sessionId, { type: "login.status", status: "error", code: "session.replaced", message: "Session digantikan oleh login lain." });
        closeSession(sessionId, "session replaced");
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
  const account = getSession(sessionId);
  account.socket.send(JSON.stringify(payload));
}

function commandJoin(sessionId, room) {
  const account=getSession(sessionId), target=roomName(room), norm=normalizeRoomName(room);
  if(!target) throw new Error("Room wajib diisi.");
  if(account.joinedRoom) {
    if(normalizeRoomName(account.joinedRoom)===norm) throw new Error(`Sudah berada di room "${account.joinedRoom}".`);
    throw new Error(`Session sudah berada di room "${account.joinedRoom}".`);
  }
  const waiter=waitForDirect(account,"join",msg=>String(msg?.type||"")==="room.join.result" && (!String(msg?.data?.room??msg?.room??"").trim() || normalizeRoomName(msg?.data?.room??msg?.room)===norm),`Enter Room ${target}`);
  account.socket.send(JSON.stringify({type:"room.join",room:target}));
  return waiter.then(event=>{ account.joinedRoom=roomName(event?.data?.room??event?.room??target)||target; return event; });
}
function commandLeave(sessionId, room) {
  const account=getSession(sessionId), target=roomName(room), norm=normalizeRoomName(room);
  if(!target) throw new Error("Room wajib diisi.");
  if(!account.joinedRoom || normalizeRoomName(account.joinedRoom)!==norm) throw new Error(`Session tidak sedang berada di room "${target}".`);
  const waiter=waitForDirect(account,"leave",msg=>String(msg?.type||"")==="room.leave.result" && (!String(msg?.data?.room??msg?.room??"").trim() || normalizeRoomName(msg?.data?.room??msg?.room)===norm),`Leave Room ${target}`);
  account.socket.send(JSON.stringify({type:"room.leave",room:target}));
  return waiter.then(event=>{account.joinedRoom=null;return event;});
}
function commandParticipants(sessionId, room) {
  const account=getSession(sessionId), target=roomName(room), norm=normalizeRoomName(room);
  if(!target) throw new Error("Room wajib diisi.");
  if(!account.joinedRoom || normalizeRoomName(account.joinedRoom)!==norm) throw new Error(`Session belum join room "${target}".`);
  const waiter=waitForDirect(account,"participants",msg=>String(msg?.type||"")==="room.participants.result" && (!String(msg?.data?.room??msg?.room??"").trim() || normalizeRoomName(msg?.data?.room??msg?.room)===norm),`List Room ${target}`);
  account.socket.send(JSON.stringify({type:"room.participants",room:target}));
  return waiter;
}
function commandBalance(sessionId) {
  const account=getSession(sessionId);
  const waiter=waitForDirect(account,"balance",msg=>String(msg?.type||"")==="wallet.balance.result","Cek saldo");
  account.socket.send(JSON.stringify({type:"wallet.balance"}));
  return waiter.then(msg=>msg?.data?.wallet||null);
}
function commandMessage(sessionId, room, message) {
  const account=getSession(sessionId), target=roomName(room), text=String(message||"").trim();
  if(!target||!text) throw new Error("Room dan pesan wajib diisi.");
  const waiter=waitForDirect(account,"message",msg=>["room.send_message.queued","error"].includes(String(msg?.type||"")),"Kirim pesan");
  account.socket.send(JSON.stringify({type:"room.send_message",room:target,message:text}));
  return waiter;
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "MIG Duel Kick 10", activeSessions: sessions.size });
});

// Single-account login retained for individual Troop controls.
app.post("/api/login", async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ ok: false, error: "Username dan password wajib diisi." });
  try {
    const cleanUsername = String(username).trim();
    const oldSession = usernameSessions.get(cleanUsername.toLowerCase());
    if (oldSession) closeSession(oldSession, "relogin");
    const result = await connectAccount(cleanUsername, String(password), 0);
    res.json({ ok: true, account: result });
  } catch (e) {
    const status = classifyLoginFailure(e);
    res.status(401).json({ ok: false, status, code: String(e?.code || ""), error: safeError(e) });
  }
});

// ONE HTTP command opens the 10 required, separate WebSockets concurrently.
app.post("/api/login-batch", async (req, res) => {
  const input=Array.isArray(req.body?.accounts)?req.body.accounts.slice(0,10):[];
  if(!input.length) return res.status(400).json({ok:false,error:"Tidak ada akun untuk login."});
  const seen=new Set();
  const results=await Promise.all(input.map(async(item,pos)=>{
    const index=Number.isInteger(item?.index)?item.index:pos, username=String(item?.username||"").trim(), password=String(item?.password||"");
    if(!username||!password) return {index,ok:false,error:"Username dan password wajib diisi."};
    const key=username.toLowerCase(); if(seen.has(key)) return {index,ok:false,error:"Username yang sama tidak boleh login dua kali."}; seen.add(key);
    const old=usernameSessions.get(username.toLowerCase()); if(old) closeSession(old,"relogin"); if(item?.sessionId) closeSession(String(item.sessionId),"relogin");
    try { return {index,ok:true,account:await connectAccount(username,password,index)}; }
    catch(e){ return {index,ok:false,username,status:classifyLoginFailure(e),code:String(e?.code||""),error:safeError(e)}; }
  }));
  res.json({ok:results.some(x=>x.ok),results});
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
app.post("/api/action", async (req,res)=>{
  const {sessionId,action,room,targetUsername,message}=req.body||{};
  if(!sessionId||!action) return res.status(400).json({ok:false,error:"Parameter tidak lengkap."});
  try {
    let event;
    if(action==="join") event=await commandJoin(sessionId,room);
    else if(action==="leave") event=await commandLeave(sessionId,room);
    else if(action==="participants") event=await commandParticipants(sessionId,room);
    else if(action==="balance") event=await commandBalance(sessionId);
    else if(action==="message") event=await commandMessage(sessionId,room,message);
    else if(action==="kick"){if(!room||!targetUsername)throw new Error("Room dan target wajib diisi.");send(sessionId,{type:"room.kick",room:roomName(room),target_username:String(targetUsername).trim()});}
    else throw new Error("Action tidak dikenal.");
    res.json({ok:true,sent:action,event:action==="balance"?null:event,wallet:action==="balance"?event:null});
  } catch(e){res.status(400).json({ok:false,error:safeError(e),event:e?.event||null});}
});

app.post("/api/balance-all", async(req,res)=>{
  const ids=Array.isArray(req.body?.sessionIds)?[...new Set(req.body.sessionIds.map(String))].slice(0,10):[];
  if(!ids.length)return res.status(400).json({ok:false,error:"Tidak ada Troop yang ONLINE."});
  const results=await Promise.all(ids.map(async sessionId=>{try{const wallet=await commandBalance(sessionId);return {sessionId,ok:true,wallet};}catch(e){return {sessionId,ok:false,error:safeError(e)};}}));
  const success=results.filter(x=>x.ok).length; res.json({ok:success>0,action:"balance",sent:ids.length,success,total:results.length,results});
});

app.post("/api/batch-action", async(req,res)=>{
  const {sessionIds,action,room,targetUsername,message}=req.body||{};
  const ids=Array.isArray(sessionIds)?[...new Set(sessionIds.map(String))].slice(0,10):[];
  if(!ids.length||!action)return res.status(400).json({ok:false,error:"Session atau action tidak lengkap."});
  const results=await Promise.all(ids.map(async sessionId=>{
    try{
      let event;
      if(action==="join")event=await commandJoin(sessionId,room);
      else if(action==="leave")event=await commandLeave(sessionId,room);
      else if(action==="participants")event=await commandParticipants(sessionId,room);
      else if(action==="balance")event=await commandBalance(sessionId);
      else if(action==="message")event=await commandMessage(sessionId,room,message);
      else if(action==="kick"){if(!room||!targetUsername)throw new Error("Room dan target wajib diisi.");send(sessionId,{type:"room.kick",room:roomName(room),target_username:String(targetUsername).trim()});}
      else throw new Error("Action tidak dikenal.");
      return {sessionId,ok:true,event:action==="balance"?null:event,wallet:action==="balance"?event:null};
    }catch(e){return {sessionId,ok:false,error:safeError(e),event:e?.event||null};}
  }));
  const success=results.filter(x=>x.ok).length; res.json({ok:success===results.length&&results.length>0,action,room:roomName(room),sent:ids.length,success,total:results.length,results});
});

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
    // Hybrid safe-rate + burst dispatch.
    const burstGapMs = Math.max(1, Math.ceil((RACE_BURST / kickRateLimit) * 1000));
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
          // Respect configured batch delay; otherwise use a calculated small gap
          // so bursts stay close while the average rate remains near the safety limit.
          const isEndOfLoop = pos + RACE_BURST >= orderedIndices.length;
          if (!isEndOfLoop) {
            if (delayMs > 0) await waitBatchDelay(delayMs);
            else await sleep(burstGapMs);
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

app.post("/api/logout",(req,res)=>{
  const id=String(req.body?.sessionId||"");
  const closed=closeSession(id,"logout");
  res.json({ok:true,closed,remaining:sessions.size});
});
app.post("/api/logout-batch",(_req,res)=>{
  const ids=getActiveSessionIds(); let closed=0; for(const id of ids)if(closeSession(id,"logout all"))closed++;
  res.json({ok:true,closed,remaining:sessions.size});
});

app.get("*", (_req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.listen(PORT, "0.0.0.0", () => console.log(`MIG Duel Kick 10 running on port ${PORT}`));
