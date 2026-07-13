// Slime Soccer (Cap Football) online server
// Serves the game page from ./public and runs rooms over WebSocket at /ws.
//
// Fixes over the previous deployment:
//  - A joiner who disconnects BEFORE the game starts frees their seat again
//    (the old server kept the room "full" forever -> "Room not found or full")
//  - The host is told when a lobby opponent drops ("opponentLeftLobby")
//  - Idle rooms expire, dead connections are detected via ping/pong
//
// Two room modes:
//  - "duo": the original 1v1 (host = left, guest = right)
//  - "quad": 2v2 — Attacker + Goalkeeper per team, on a wider field.
//    Seat order is LA (host), LK, RA, RK; all 4 seats are real players
//    online (bots only exist client-side, in the offline quad modes).

const http = require("http");
const express = require("express");
const path = require("path");
const fs = require("fs");
const WebSocket = require("ws");

// ── Player records / hall of fame (nickname-based, no passwords) ─────────────
// `records` is an in-memory cache — the source of truth for all reads (hall of
// fame, lookups). Writes are mirrored to a durable backend: Firebase Firestore
// when configured (set FIREBASE_SERVICE_ACCOUNT or GOOGLE_APPLICATION_CREDENTIALS),
// otherwise a local JSON file. The file resets on ephemeral hosts (e.g. a free
// Render dyno) on redeploy — that's exactly what Firestore fixes.
const RECORDS_FILE = path.join(__dirname, "records.json");
let records = {}; // { lowercaseKey: { name, wins, losses, games } }
let firestore = null; // Firestore instance when the Firebase backend is active
let firebaseAuth = null;
let usingOnePassUsers = false;
let loonyOnlinePlayers = [];
let stopLoonyPresenceWatch = null;
const ONEPASS_USERS_COLLECTION = process.env.ONEPASS_USERS_COLLECTION || process.env.USERS_COLLECTION || "users";
const RECORDS_COLLECTION = process.env.RECORDS_COLLECTION || "records";
const CHAT_COLLECTION = process.env.SLIME_CHAT_COLLECTION || "slimeSoccerChat";
const CHAT_HISTORY_LIMIT = 100;
const LOONY_PRESENCE_STALE_MS = Number(process.env.LOONY_PRESENCE_STALE_MS || 2.5 * 60 * 1000);
const EXPECTED_FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "generaluserstation";
const REQUIRE_ONEPASS_FIREBASE = String(process.env.REQUIRE_ONEPASS_FIREBASE || "true").toLowerCase() !== "false";

function userDisplayName(id, data) {
  return cleanName(
    data.name ||
    data.username ||
    data.displayName ||
    data.playerName ||
    (data.email ? String(data.email).split("@")[0] : "") ||
    id
  );
}
function readGameStats(data) {
  const nested = data.slimeSoccer || data.slime_soccer || {};
  return {
    wins: Number(nested.wins ?? data.wins ?? 0) || 0,
    losses: Number(nested.losses ?? data.losses ?? 0) || 0,
    games: Number(nested.games ?? data.games ?? 0) || 0,
  };
}
function rememberRecordAliases(id, data) {
  const name = userDisplayName(id, data);
  if (!name) return;
  const stats = readGameStats(data);
  const record = { name, wins: stats.wins, losses: stats.losses, games: stats.games, userId: id };
  const aliases = [id, data.name, data.username, data.displayName, data.playerName, data.email, name]
    .filter(Boolean)
    .map(v => cleanName(String(v).split("@")[0]).toLowerCase())
    .filter(Boolean);
  aliases.forEach(alias => { records[alias] = record; });
}
function publicRecord(record) {
  return record ? { name: record.name, wins: record.wins || 0, losses: record.losses || 0, games: record.games || 0 } : null;
}
function uniqueRecords() {
  const seen = new Set();
  return Object.values(records).filter(r => {
    const key = r.userId || r.name.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
function loonyPresence(data) {
  const nested = data.slimeSoccer || data.slime_soccer || {};
  const online = Boolean(nested.online ?? data.online ?? data.isOnline ?? data.loggedIn);
  const inChat = Boolean(nested.inChat ?? data.inChat);
  return {
    online,
    inChat,
    lastSeen: normalizeTimestamp(nested.lastSeen || data.lastSeen || null),
  };
}
function normalizeTimestamp(value) {
  if (!value) return null;
  if (typeof value === "string") return value;
  if (typeof value.toDate === "function") return value.toDate().toISOString();
  if (typeof value.toMillis === "function") return new Date(value.toMillis()).toISOString();
  if (typeof value.seconds === "number") return new Date(value.seconds * 1000).toISOString();
  return null;
}
function timestampMs(value) {
  if (!value) return 0;
  if (typeof value === "string") return new Date(value).getTime() || 0;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value.seconds === "number") return value.seconds * 1000;
  return 0;
}

function playersFromPresenceSnapshot(snap) {
  const players = [];
  const freshAfter = Date.now() - LOONY_PRESENCE_STALE_MS;
  snap.forEach(doc => {
    const data = doc.data() || {};
    const presence = loonyPresence(data);
    if (!presence.inChat) return;
    const seenAt = timestampMs((data.slimeSoccer || data.slime_soccer || {}).lastSeen || data.lastSeen);
    if (seenAt && seenAt < freshAfter) return;
    const name = userDisplayName(doc.id, data);
    if (!name) return;
    players.push({ id: doc.id, name, online: presence.online, inChat: presence.inChat, lastSeen: presence.lastSeen, source: "firestore" });
  });
  players.sort((a, b) => Number(b.inChat) - Number(a.inChat) || a.name.localeCompare(b.name));
  return players.slice(0, 80);
}

function startLoonyPresenceWatch() {
  if (!firestore || stopLoonyPresenceWatch) return;
  stopLoonyPresenceWatch = firestore.collection(ONEPASS_USERS_COLLECTION).onSnapshot(snap => {
    loonyOnlinePlayers = playersFromPresenceSnapshot(snap);
    broadcastLobby({ type: "loonyPresence", players: loonyOnlinePlayers });
  }, e => console.error("Loony presence watch:", e.message));
}
function loadServiceAccountFromEnv() {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    return JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  }
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, "utf8"));
  }
  return null;
}

// Load whichever backend is configured, populating the cache. Returns a promise.
function initRecordStore() {
  const hasFirebaseConfig = Boolean(process.env.FIREBASE_SERVICE_ACCOUNT || process.env.GOOGLE_APPLICATION_CREDENTIALS);
  if (hasFirebaseConfig) {
    try {
      const serviceAccount = loadServiceAccountFromEnv();
      if (serviceAccount && serviceAccount.project_id && serviceAccount.project_id !== EXPECTED_FIREBASE_PROJECT_ID) {
        throw new Error("Firebase service account is for project " + serviceAccount.project_id + ", expected " + EXPECTED_FIREBASE_PROJECT_ID);
      }
      const admin = require("firebase-admin");
      admin.initializeApp({
        credential: serviceAccount ? admin.credential.cert(serviceAccount) : admin.credential.applicationDefault(),
        projectId: EXPECTED_FIREBASE_PROJECT_ID,
      });
      firestore = admin.firestore();
      firebaseAuth = admin.auth();
      usingOnePassUsers = true;
      return firestore.collection(ONEPASS_USERS_COLLECTION).get().then(snap => {
        snap.forEach(doc => rememberRecordAliases(doc.id, doc.data() || {}));
        loonyOnlinePlayers = playersFromPresenceSnapshot(snap);
        startLoonyPresenceWatch();
        console.log("Records: Firebase " + EXPECTED_FIREBASE_PROJECT_ID + "/" + ONEPASS_USERS_COLLECTION + " (" + snap.size + " loaded)");
      }).catch(e => {
        console.error("Firestore initial load failed:", e.message);
      });
    } catch (e) {
      firestore = null;
      firebaseAuth = null;
      console.error("Firestore init failed:", e.message);
    }
  }
  if (REQUIRE_ONEPASS_FIREBASE) {
    usingOnePassUsers = true;
    records = {};
    if (!hasFirebaseConfig) console.error("Records: Firebase Admin is not configured; Hall of Fame will not use local fallback.");
    return Promise.resolve();
  }
  try { records = JSON.parse(fs.readFileSync(RECORDS_FILE, "utf8")); } catch { records = {}; }
  console.log("Records: local file backend (" + Object.keys(records).length + " loaded)");
  return Promise.resolve();
}
async function verifiedOnePassUser(idToken) {
  if (!firebaseAuth) throw new Error("Loony Firebase Admin authentication is not configured");
  const decoded = await firebaseAuth.verifyIdToken(String(idToken || ""));
  let data = {
    displayName: decoded.name || String(decoded.email || "").split("@")[0] || decoded.uid,
    email: decoded.email || "",
    score: 0
  };
  if (firestore) {
    try {
      const ref = firestore.collection(ONEPASS_USERS_COLLECTION).doc(decoded.uid);
      const snap = await ref.get();
      data = snap.exists ? snap.data() : data;
      if (!snap.exists) await ref.set(data, { merge: true });
    } catch (e) {
      console.error("Loony user profile lookup failed:", e.message);
    }
  }
  rememberRecordAliases(decoded.uid, data);
  const record = records[decoded.uid.toLowerCase()] || findPlayerRecord(decoded.uid);
  if (record) record.userId = decoded.uid;
  return record;
}

function setLoonyPresence(ws, patch) {
  if (!firestore || !ws || !ws.userId) return;
  firestore.collection(ONEPASS_USERS_COLLECTION).doc(ws.userId).set({
    slimeSoccer: {
      ...patch,
      lastSeen: new Date().toISOString()
    }
  }, { merge: true }).catch(e => console.error("Loony presence write:", e.message));
}

function persistLobbyChat(ws, text) {
  if (!firestore || !text) return;
  firestore.collection(CHAT_COLLECTION).add({
    userId: ws.userId || null,
    name: playerLabel(ws),
    text,
    ts: new Date().toISOString()
  }).catch(e => console.error("Slime chat write:", e.message));
}

function sendLobbyChatHistory(ws) {
  if (!firestore) return;
  firestore.collection(CHAT_COLLECTION).orderBy("ts", "desc").limit(CHAT_HISTORY_LIMIT).get()
    .then(snap => {
      const messages = [];
      snap.forEach(doc => {
        const data = doc.data() || {};
        const text = String(data.text || "").trim().slice(0, 180);
        if (text) messages.push({ from: cleanName(data.name) || "Player", text, ts: normalizeTimestamp(data.ts) || data.ts || null });
      });
      messages.reverse();
      send(ws, { type: "lobbyHistory", messages });
    })
    .catch(e => console.error("Slime chat history:", e.message));
}

let _saveTimer = null;
function saveRecordsFile() {
  if (_saveTimer) return; // debounce bursts of writes into one
  _saveTimer = setTimeout(() => {
    _saveTimer = null;
    fs.writeFile(RECORDS_FILE, JSON.stringify(records), err => { if (err) console.error("saveRecordsFile:", err); });
  }, 1000);
}
// Persist a single record by key to the active backend.
function persistRecord(key) {
  if (firestore) {
    if (usingOnePassUsers) {
      const r = records[key];
      if (!r || !r.userId) return;
      firestore.collection(ONEPASS_USERS_COLLECTION).doc(r.userId).set({
        slimeSoccer: { wins: r.wins || 0, losses: r.losses || 0, games: r.games || 0 },
      }, { merge: true }).catch(e => console.error("OnePass Users write:", e.message));
    } else {
      firestore.collection(RECORDS_COLLECTION).doc(key).set(records[key]).catch(e => console.error("Firestore write:", e.message));
    }
  } else if (!REQUIRE_ONEPASS_FIREBASE) {
    saveRecordsFile();
  }
}
function cleanName(raw) {
  return String(raw || "").replace(/[^\w \-]/g, "").replace(/\s+/g, " ").trim().slice(0, 32);
}
function getOrCreateRecord(name) {
  const key = name.toLowerCase();
  if (usingOnePassUsers) return records[key] || null;
  if (!records[key]) records[key] = { name, wins: 0, losses: 0, games: 0 };
  return records[key];
}
function findPlayerRecord(raw) {
  const rawText = String(raw || "").trim();
  const name = cleanName(rawText);
  if (!name) return null;
  if (!usingOnePassUsers) return getOrCreateRecord(name);
  const candidates = [rawText, rawText.split("@")[0], name]
    .map(v => cleanName(v).toLowerCase())
    .filter(Boolean);
  for (const key of candidates) {
    if (records[key]) return records[key];
  }
  return null;
}
function recordResult(name, outcome) { // outcome: "win" | "loss" | "draw"
  if (!name) return;
  const key = name.toLowerCase();
  const r = getOrCreateRecord(name);
  if (!r) return;
  r.games++;
  if (outcome === "win") r.wins++;
  else if (outcome === "loss") r.losses++;
  persistRecord(key);
}

// ── Game constants (must match the client) ──────────────────────────────────
const W_DUO = 1000, W_QUAD = 1400;
const H = 520, GH = 80, SR = 40, BR = 12, GW = 80, GHEIGHT = 130;
const GRAV = 0.6, SPEED = 6, BOOST_SPEED = 11, JUMP = -13, BDAMP = 0.99, BBOUNCE = 0.8, MAXSPEED = 14;
// Roll: a mid-air 360° spin, usable off a short cooldown ONLY while airborne.
// It doesn't dash the player, but while spinning any ball the slime touches
// rockets away hard, ignoring the normal MAXSPEED clamp. Authoritative here —
// the client only requests it, same trust model as every other physics effect.
const ROLL_COOLDOWN_TICKS = 90, ROLL_TICKS = 22, ROLL_HIT_SPEED = 18;
const TICK_MS = 1000 / 60;
const MIN_DURATION = 30, MAX_DURATION = 900;
const ROOM_IDLE_MS = 20 * 60 * 1000;
const QUAD_SEATS = ["LA", "LK", "RA", "RK"];

function applyPlayerInput(sl, inp) {
  const moveDir = inp.left ? -1 : inp.right ? 1 : 0;
  const boostDir = moveDir || Math.sign(sl.vx);
  sl.vx = moveDir * SPEED;
  sl.grab = false;
  if (inp.down && boostDir) sl.vx = boostDir * BOOST_SPEED;
  if (inp.up && sl.y >= H - GH - 1 && !sl.grab) sl.vy = JUMP;
}

const rooms = new Map(); // roomId -> room
const lobbyClients = new Set();
const challenges = new Map();
let nextClientId = 1;
let nextChallengeId = 1;

// Unambiguous alphabet: no 0/O or 1/I
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
function makeRoomId() {
  let id;
  do {
    id = "";
    for (let i = 0; i < 6; i++) id += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  } while (rooms.has(id));
  return id;
}

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}
function roomSockets(room) {
  return room.mode === "quad"
    ? QUAD_SEATS.map(s => room.seats[s]).filter(Boolean)
    : [room.host, room.guest].filter(Boolean);
}
function broadcast(room, msg) {
  roomSockets(room).forEach(ws => send(ws, msg));
}
function playerLabel(ws) {
  return ws.playerName || ws.lobbyName || ("Guest " + ws.clientId);
}
function lobbyRoster() {
  return Array.from(lobbyClients)
    .filter(ws => ws.readyState === WebSocket.OPEN)
    .map(ws => ({ id: ws.clientId, name: playerLabel(ws), registered: !!ws.userId, busy: !!ws.room, ping: Number.isFinite(ws.lobbyPing) ? Math.round(ws.lobbyPing) : null }));
}
function broadcastLobby(msg) {
  const data = JSON.stringify(msg);
  lobbyClients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  });
}
function updateLobbyRoster() {
  broadcastLobby({ type: "lobbyRoster", players: lobbyRoster() });
}
function joinLobby(ws, name) {
  const alreadyJoined = lobbyClients.has(ws);
  ws.lobbyName = cleanName(name) || ws.playerName || ("Guest " + ws.clientId);
  lobbyClients.add(ws);
  setLoonyPresence(ws, { online: true, inChat: true, clientId: ws.clientId });
  send(ws, { type: "lobbyJoined", id: ws.clientId, name: playerLabel(ws), players: lobbyRoster() });
  send(ws, { type: "loonyPresence", players: loonyOnlinePlayers });
  sendLobbyChatHistory(ws);
  if (alreadyJoined) {
    updateLobbyRoster();
    return;
  }
  broadcastLobby({ type: "lobbyChat", system: true, from: "Lobby", text: playerLabel(ws) + " joined" });
  updateLobbyRoster();
}
function leaveLobby(ws) {
  if (!lobbyClients.delete(ws)) return;
  setLoonyPresence(ws, { online: true, inChat: false });
  challenges.forEach((ch, id) => {
    if (ch.from === ws || ch.to === ws) challenges.delete(id);
  });
  broadcastLobby({ type: "lobbyChat", system: true, from: "Lobby", text: playerLabel(ws) + " left" });
  updateLobbyRoster();
}
function startDuoChallenge(hostWs, guestWs) {
  if (!hostWs || !guestWs || hostWs.readyState !== WebSocket.OPEN || guestWs.readyState !== WebSocket.OPEN) return;
  if (hostWs.room) leaveRoom(hostWs);
  if (guestWs.room) leaveRoom(guestWs);
  const r = createRoom(hostWs, "duo");
  hostWs.room = r; hostWs.side = "left";
  r.guest = guestWs; guestWs.room = r; guestWs.side = "right";
  r.lastActivity = Date.now();
  send(hostWs, { type: "assigned", mode: "duo", side: "left", roomId: r.id, challenged: true, opponent: playerLabel(guestWs) });
  send(guestWs, { type: "assigned", mode: "duo", side: "right", roomId: r.id, challenged: true, opponent: playerLabel(hostWs) });
  send(guestWs, { type: "waitingForHost" });
  send(hostWs, { type: "chooseDuration" });
  broadcastLobby({ type: "lobbyChat", system: true, from: "Lobby", text: playerLabel(hostWs) + " challenged " + playerLabel(guestWs) });
  updateLobbyRoster();
}

// ── Room / game state (duo) ──────────────────────────────────────────────────
function freshGameStateDuo() {
  return {
    L: { x: 250, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, rollCooldown: 0, rollTimer: 0, rollDir: 0 },
    R: { x: 750, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, rollCooldown: 0, rollTimer: 0, rollDir: 0 },
    ball: { x: W_DUO / 2, y: H / 2 - 60, vx: 0, vy: 0, owner: null, angle: 0, angVel: 0 },
  };
}
function resetPositionsDuo(room, fullReset) {
  const fresh = freshGameStateDuo();
  // Roll cooldown only resets on a true match start (fullReset), not on
  // every goal — otherwise scoring would refill it for free.
  const fields = ["x", "y", "vx", "vy", "grab", "hasBall", "glTime"];
  if (fullReset) fields.push("rollCooldown", "rollTimer", "rollDir");
  fields.forEach(f => { room.gs.L[f] = fresh.L[f]; room.gs.R[f] = fresh.R[f]; });
  Object.assign(room.gs.ball, fresh.ball);
}
function freshInputsDuo() {
  return {
    left:  { left: false, right: false, up: false, down: false },
    right: { left: false, right: false, up: false, down: false },
  };
}

// ── Room / game state (quad / 2v2) ───────────────────────────────────────────
function freshGameStateQuad() {
  const rollDefaults = { rollCooldown: 0, rollTimer: 0, rollDir: 0 };
  return {
    LA: { id: "LA", team: "left",  x: W_QUAD * 0.32, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, ...rollDefaults },
    LK: { id: "LK", team: "left",  x: W_QUAD * 0.09, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, ...rollDefaults },
    RA: { id: "RA", team: "right", x: W_QUAD * 0.68, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, ...rollDefaults },
    RK: { id: "RK", team: "right", x: W_QUAD * 0.91, y: H - GH, vx: 0, vy: 0, grab: false, hasBall: false, glTime: 0, ...rollDefaults },
    ball: { x: W_QUAD / 2, y: H / 2 - 60, vx: 0, vy: 0, owner: null, angle: 0, angVel: 0 },
  };
}
function resetPositionsQuad(room, fullReset) {
  const fresh = freshGameStateQuad();
  const fields = ["x", "y", "vx", "vy", "grab", "hasBall", "glTime"];
  if (fullReset) fields.push("rollCooldown", "rollTimer", "rollDir");
  QUAD_SEATS.forEach(k => fields.forEach(f => { room.gs[k][f] = fresh[k][f]; }));
  Object.assign(room.gs.ball, fresh.ball);
}
function freshInputsQuad() {
  const mk = () => ({ left: false, right: false, up: false, down: false });
  return { LA: mk(), LK: mk(), RA: mk(), RK: mk() };
}

function createRoom(hostWs, mode) {
  mode = mode === "quad" ? "quad" : "duo";
  const room = {
    id: makeRoomId(),
    mode,
    host: mode === "duo" ? hostWs : null,
    guest: null,
    seats: mode === "quad" ? { LA: hostWs, LK: null, RA: null, RK: null } : null,
    phase: "lobby", // lobby | playing | ended
    autoStart: false,
    duration: 60,
    timeLeft: 0,
    score: { left: 0, right: 0 },
    gs: mode === "quad" ? freshGameStateQuad() : freshGameStateDuo(),
    inputs: mode === "quad" ? freshInputsQuad() : freshInputsDuo(),
    tickTimer: null,
    secondTimer: null,
    lastActivity: Date.now(),
  };
  rooms.set(room.id, room);
  return room;
}

function stopTimers(room) {
  if (room.tickTimer) { clearInterval(room.tickTimer); room.tickTimer = null; }
  if (room.secondTimer) { clearInterval(room.secondTimer); room.secondTimer = null; }
}

function destroyRoom(room) {
  stopTimers(room);
  rooms.delete(room.id);
  roomSockets(room).forEach(ws => { ws.room = null; });
}

function startGame(room, duration) {
  room.duration = Math.max(MIN_DURATION, Math.min(MAX_DURATION, Math.round(duration) || 60));
  room.score = { left: 0, right: 0 };
  room.timeLeft = room.duration;
  room.inputs = room.mode === "quad" ? freshInputsQuad() : freshInputsDuo();
  if (room.mode === "quad") resetPositionsQuad(room, true); else resetPositionsDuo(room, true);
  room.phase = "playing";
  room.lastActivity = Date.now();
  broadcast(room, { type: "gameStarted", mode: room.mode, duration: room.duration, score: room.score });

  stopTimers(room);
  room.tickTimer = setInterval(() => (room.mode === "quad" ? tickQuad(room) : tickDuo(room)), TICK_MS);
  room.secondTimer = setInterval(() => {
    room.timeLeft = Math.max(0, room.timeLeft - 1);
    if (room.timeLeft <= 0) endGame(room);
  }, 1000);
}

function endGame(room) {
  stopTimers(room);
  room.phase = "ended";
  recordGameResult(room);
  broadcast(room, { type: "gameEnded", score: room.score });
}

// Credit W/L to every registered (named) player in the match. A draw counts as
// a game for everyone but no win or loss.
function recordGameResult(room) {
  const { left, right } = room.score;
  const outcomeFor = side =>
    left === right ? "draw" : ((side === "left") === (left > right) ? "win" : "loss");
  if (room.mode === "quad") {
    const seatSide = { LA: "left", LK: "left", RA: "right", RK: "right" };
    QUAD_SEATS.forEach(s => {
      const ws = room.seats[s];
      if (ws && ws.playerName) recordResult(ws.playerName, outcomeFor(seatSide[s]));
    });
  } else {
    if (room.host && room.host.playerName) recordResult(room.host.playerName, outcomeFor("left"));
    if (room.guest && room.guest.playerName) recordResult(room.guest.playerName, outcomeFor("right"));
  }
}

// ── Physics (duo): direct port of the client's local-mode physics() ─────────
function tickDuo(room) {
  const gs = room.gs, b = gs.ball;
  const inL = room.inputs.left, inR = room.inputs.right;
  const events = [];
  const goal = (scorer) => {
    if (scorer === "left") room.score.left++; else room.score.right++;
    events.push({ type: "goal", scorer, score: room.score });
    resetPositionsDuo(room);
  };

  // Controls
  applyPlayerInput(gs.L, inL);
  applyPlayerInput(gs.R, inR);

  // Slime movement + goal-walk detection
  [[gs.L, 0], [gs.R, 1]].forEach(([sl, idx]) => {
    if (sl.rollCooldown > 0) sl.rollCooldown--;
    if (sl.rollTimer > 0) sl.rollTimer--;
    sl.vy += GRAV; sl.x += sl.vx; sl.y += sl.vy;
    if (sl.x < SR) sl.x = SR; if (sl.x > W_DUO - SR) sl.x = W_DUO - SR;
    if (sl.y > H - GH) { sl.y = H - GH; sl.vy = 0; }
    const inOwn = (idx === 0 && sl.x < GW) || (idx === 1 && sl.x > W_DUO - GW);
    if (inOwn) {
      sl.glTime += 1 / 60;
      if (sl.glTime >= 1) goal(idx === 0 ? "right" : "left");
    } else sl.glTime = 0;
  });

  // Ball
  if (b.owner) {
    const gr = b.owner === "left" ? gs.L : gs.R, dir = b.owner === "left" ? 1 : -1;
    b.angVel += (-gr.vx * 0.008 * dir); b.angVel *= 0.85; b.angle += b.angVel;
    if (b.owner === "left") {
      if (b.angle < -Math.PI / 2) { b.angle = -Math.PI / 2; b.angVel = 0; }
      else if (b.angle > Math.PI / 2) { b.angle = Math.PI / 2; b.angVel = 0; }
    } else {
      while (b.angle < 0) b.angle += Math.PI * 2;
      while (b.angle > Math.PI * 2) b.angle -= Math.PI * 2;
      if (b.angle < Math.PI / 2) { b.angle = Math.PI / 2; b.angVel = 0; }
      else if (b.angle > 3 * Math.PI / 2) { b.angle = 3 * Math.PI / 2; b.angVel = 0; }
    }
    const hd = SR + BR - 5;
    b.x = gr.x + Math.cos(b.angle) * hd; b.y = gr.y + Math.sin(b.angle) * hd;
    b.vx = gr.vx; b.vy = gr.vy;
    if (!gr.grab) {
      const sp = Math.abs(b.angVel) * 20;
      b.vx = gr.vx * 1.5 + Math.cos(b.angle) * (3 + sp);
      b.vy = gr.vy - 2 + Math.sin(b.angle) * sp * 0.3;
      b.owner = null; b.angle = 0; b.angVel = 0; gr.hasBall = false;
    }
  } else {
    b.vy += GRAV; b.vx *= BDAMP; b.x += b.vx; b.y += b.vy;
  }

  // Ball walls/floor
  if (b.x < BR) { b.x = BR; b.vx = -b.vx * BBOUNCE; }
  if (b.x > W_DUO - BR) { b.x = W_DUO - BR; b.vx = -b.vx * BBOUNCE; }
  if (b.y > H - GH - BR) { b.y = H - GH - BR; b.vy = -b.vy * BBOUNCE; }
  if (b.y < BR) { b.y = BR; b.vy = -b.vy * BBOUNCE; }

  // Ball in goal
  let scored = false;
  if (b.x <= BR && b.y > H - GH - GHEIGHT) { goal("right"); scored = true; }
  else if (b.x >= W_DUO - BR && b.y > H - GH - GHEIGHT) { goal("left"); scored = true; }

  // Slime-ball collision
  if (!scored) {
    [[gs.L, "left", gs.R], [gs.R, "right", gs.L]].forEach(([sl, nm, other]) => {
      const dx = b.x - sl.x, dy = b.y - sl.y, dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < SR + BR) {
        if (b.owner && b.owner !== nm) {
          const sp = Math.hypot(sl.vx, sl.vy);
          if (sp > 2 || Math.abs(sl.vy) > 5) {
            const ang = Math.atan2(dy, dx);
            b.owner = null; b.angle = 0; b.angVel = 0; other.hasBall = false;
            const kick = sl.rollTimer > 0 ? ROLL_HIT_SPEED : 8; // rolling = harder knock-away
            b.vx = Math.cos(ang) * kick + sl.vx; b.vy = Math.sin(ang) * kick + sl.vy;
            events.push({ type: "hit", x: b.x, y: b.y });
          }
        } else if (sl.grab && !b.owner) {
          b.owner = nm; b.angle = Math.atan2(dy, dx); b.angVel = 0; sl.hasBall = true;
          events.push({ type: "grab" });
        } else if (!b.owner) {
          const ang = Math.atan2(dy, dx);
          if (b.y < sl.y || Math.abs(ang) < Math.PI * 0.5) {
            b.x = sl.x + Math.cos(ang) * (SR + BR); b.y = sl.y + Math.sin(ang) * (SR + BR);
            if (sl.rollTimer > 0) {
              // Rolling into a loose ball rockets it away — hard hit, no clamp.
              b.vx = Math.cos(ang) * ROLL_HIT_SPEED + sl.vx; b.vy = Math.sin(ang) * ROLL_HIT_SPEED + sl.vy;
            } else {
              const sp = Math.hypot(b.vx, b.vy);
              b.vx = Math.cos(ang) * sp * 1.5 + sl.vx * 0.5; b.vy = Math.sin(ang) * sp * 1.5 + sl.vy * 0.5;
              const ns = Math.hypot(b.vx, b.vy);
              if (ns > MAXSPEED) { b.vx *= MAXSPEED / ns; b.vy *= MAXSPEED / ns; }
            }
            events.push({ type: "hit", x: b.x, y: b.y });
          }
        }
      }
    });
  }

  events.forEach(ev => broadcast(room, ev));

  // Rounded numbers keep the packet small; a state frame is skipped for a
  // client whose socket is backlogged (it just gets the next one) so slow
  // links don't build up a growing latency queue. Events above always send.
  const r2 = (n) => Math.round(n * 100) / 100;
  const r3 = (n) => Math.round(n * 1000) / 1000;
  const pick = (sl) => ({ x: r2(sl.x), y: r2(sl.y), vx: r2(sl.vx), vy: r2(sl.vy), grab: sl.grab, hasBall: sl.hasBall, rollTimer: sl.rollTimer, rollDir: sl.rollDir, rollCooldown: sl.rollCooldown });
  const state = JSON.stringify({
    type: "state", mode: "duo",
    L: pick(gs.L), R: pick(gs.R),
    ball: { x: r2(b.x), y: r2(b.y), vx: r2(b.vx), vy: r2(b.vy), owner: b.owner, angle: r3(b.angle), angVel: r3(b.angVel) },
    score: room.score, timeLeft: room.timeLeft,
  });
  [room.host, room.guest].forEach(ws => {
    if (ws && ws.readyState === WebSocket.OPEN && ws.bufferedAmount < 4096) ws.send(state);
  });
}

// ── Physics (quad / 2v2): all 4 seats are real players, no server-side bots ──
function tickQuad(room) {
  const gs = room.gs, b = gs.ball;
  const roster = QUAD_SEATS.map(k => gs[k]);
  const events = [];
  const goal = (scorer) => {
    if (scorer === "left") room.score.left++; else room.score.right++;
    events.push({ type: "goal", scorer, score: room.score });
    resetPositionsQuad(room);
  };

  // Controls
  QUAD_SEATS.forEach(seatId => {
    const sl = gs[seatId], inp = room.inputs[seatId];
    applyPlayerInput(sl, inp);
  });

  // Slime movement + goal-walk detection
  roster.forEach(sl => {
    if (sl.rollCooldown > 0) sl.rollCooldown--;
    if (sl.rollTimer > 0) sl.rollTimer--;
    sl.vy += GRAV; sl.x += sl.vx; sl.y += sl.vy;
    if (sl.x < SR) sl.x = SR; if (sl.x > W_QUAD - SR) sl.x = W_QUAD - SR;
    if (sl.y > H - GH) { sl.y = H - GH; sl.vy = 0; }
    const inOwn = (sl.team === "left" && sl.x < GW) || (sl.team === "right" && sl.x > W_QUAD - GW);
    if (inOwn) {
      sl.glTime += 1 / 60;
      if (sl.glTime >= 1) goal(sl.team === "left" ? "right" : "left");
    } else sl.glTime = 0;
  });

  // Ball
  if (b.owner) {
    const gr = roster.find(s => s.id === b.owner), dir = gr.team === "left" ? 1 : -1;
    b.angVel += (-gr.vx * 0.008 * dir); b.angVel *= 0.85; b.angle += b.angVel;
    if (gr.team === "left") {
      if (b.angle < -Math.PI / 2) { b.angle = -Math.PI / 2; b.angVel = 0; }
      else if (b.angle > Math.PI / 2) { b.angle = Math.PI / 2; b.angVel = 0; }
    } else {
      while (b.angle < 0) b.angle += Math.PI * 2;
      while (b.angle > Math.PI * 2) b.angle -= Math.PI * 2;
      if (b.angle < Math.PI / 2) { b.angle = Math.PI / 2; b.angVel = 0; }
      else if (b.angle > 3 * Math.PI / 2) { b.angle = 3 * Math.PI / 2; b.angVel = 0; }
    }
    const hd = SR + BR - 5;
    b.x = gr.x + Math.cos(b.angle) * hd; b.y = gr.y + Math.sin(b.angle) * hd;
    b.vx = gr.vx; b.vy = gr.vy;
    if (!gr.grab) {
      const sp = Math.abs(b.angVel) * 20;
      b.vx = gr.vx * 1.5 + Math.cos(b.angle) * (3 + sp);
      b.vy = gr.vy - 2 + Math.sin(b.angle) * sp * 0.3;
      b.owner = null; b.angle = 0; b.angVel = 0; gr.hasBall = false;
    }
  } else {
    b.vy += GRAV; b.vx *= BDAMP; b.x += b.vx; b.y += b.vy;
  }

  // Ball walls/floor
  if (b.x < BR) { b.x = BR; b.vx = -b.vx * BBOUNCE; }
  if (b.x > W_QUAD - BR) { b.x = W_QUAD - BR; b.vx = -b.vx * BBOUNCE; }
  if (b.y > H - GH - BR) { b.y = H - GH - BR; b.vy = -b.vy * BBOUNCE; }
  if (b.y < BR) { b.y = BR; b.vy = -b.vy * BBOUNCE; }

  // Ball in goal
  let scored = false;
  if (b.x <= BR && b.y > H - GH - GHEIGHT) { goal("right"); scored = true; }
  else if (b.x >= W_QUAD - BR && b.y > H - GH - GHEIGHT) { goal("left"); scored = true; }

  // Slime-ball collision (team-aware: a teammate never steals from a teammate)
  if (!scored) {
    roster.forEach(sl => {
      const dx = b.x - sl.x, dy = b.y - sl.y, dist = Math.sqrt(dx * dx + dy * dy);
      if (dist >= SR + BR) return;
      if (b.owner === sl.id) return;
      if (b.owner) {
        const owner = roster.find(s => s.id === b.owner);
        if (owner && owner.team !== sl.team) {
          const sp = Math.hypot(sl.vx, sl.vy);
          if (sp > 2 || Math.abs(sl.vy) > 5) {
            const ang = Math.atan2(dy, dx);
            b.owner = null; b.angle = 0; b.angVel = 0; owner.hasBall = false;
            const kick = sl.rollTimer > 0 ? ROLL_HIT_SPEED : 8; // rolling = harder knock-away
            b.vx = Math.cos(ang) * kick + sl.vx; b.vy = Math.sin(ang) * kick + sl.vy;
            events.push({ type: "hit", x: b.x, y: b.y });
          }
        }
        // else: teammate holds the ball — ignore, no interaction
      } else if (sl.grab) {
        b.owner = sl.id; b.angle = Math.atan2(dy, dx); b.angVel = 0; sl.hasBall = true;
        events.push({ type: "grab" });
      } else {
        const ang = Math.atan2(dy, dx);
        if (b.y < sl.y || Math.abs(ang) < Math.PI * 0.5) {
          b.x = sl.x + Math.cos(ang) * (SR + BR); b.y = sl.y + Math.sin(ang) * (SR + BR);
          if (sl.rollTimer > 0) {
            // Rolling into a loose ball rockets it away — hard hit, no clamp.
            b.vx = Math.cos(ang) * ROLL_HIT_SPEED + sl.vx; b.vy = Math.sin(ang) * ROLL_HIT_SPEED + sl.vy;
          } else {
            const sp = Math.hypot(b.vx, b.vy);
            b.vx = Math.cos(ang) * sp * 1.5 + sl.vx * 0.5; b.vy = Math.sin(ang) * sp * 1.5 + sl.vy * 0.5;
            const ns = Math.hypot(b.vx, b.vy);
            if (ns > MAXSPEED) { b.vx *= MAXSPEED / ns; b.vy *= MAXSPEED / ns; }
          }
          events.push({ type: "hit", x: b.x, y: b.y });
        }
      }
    });
  }

  events.forEach(ev => broadcast(room, ev));

  const r2 = (n) => Math.round(n * 100) / 100;
  const r3 = (n) => Math.round(n * 1000) / 1000;
  const pick = (sl) => ({ x: r2(sl.x), y: r2(sl.y), vx: r2(sl.vx), vy: r2(sl.vy), grab: sl.grab, hasBall: sl.hasBall, rollTimer: sl.rollTimer, rollDir: sl.rollDir, rollCooldown: sl.rollCooldown });
  const state = JSON.stringify({
    type: "state", mode: "quad",
    LA: pick(gs.LA), LK: pick(gs.LK), RA: pick(gs.RA), RK: pick(gs.RK),
    ball: { x: r2(b.x), y: r2(b.y), vx: r2(b.vx), vy: r2(b.vy), owner: b.owner, angle: r3(b.angle), angVel: r3(b.angVel) },
    score: room.score, timeLeft: room.timeLeft,
  });
  QUAD_SEATS.forEach(seatId => {
    const ws = room.seats[seatId];
    if (ws && ws.readyState === WebSocket.OPEN && ws.bufferedAmount < 4096) ws.send(state);
  });
}

// ── Connection handling ──────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  if (msg.type === "ping") { send(ws, { type: "pong", t: msg.t }); return; }

  const room = ws.room;
  if (room) room.lastActivity = Date.now();

  switch (msg.type) {
    case "identify": {
      // Associate a registered OnePass user/nickname with this socket so
      // results can be credited when a game ends.
      verifiedOnePassUser(msg.idToken).then(record => {
        ws.playerName = record ? record.name : null;
        ws.userId = record ? record.userId : null;
        setLoonyPresence(ws, { online: true, inChat: lobbyClients.has(ws), clientId: ws.clientId });
        send(ws, { type: "identified", ok: Boolean(record), name: ws.playerName });
        if (lobbyClients.has(ws)) updateLobbyRoster();
      }).catch(error => {
        ws.playerName = null;
        send(ws, {
          type: "identified",
          ok: false,
          error: /not configured/i.test(error.message || "") ? "OnePass Firebase Admin is not configured on this server" : "OnePass sign-in could not be verified"
        });
      });
      return;
    }
    case "lobbyJoin": {
      if (msg.idToken) {
        verifiedOnePassUser(msg.idToken).then(record => {
          ws.playerName = record ? record.name : null;
          ws.userId = record ? record.userId : null;
          joinLobby(ws, msg.name);
        }).catch(() => joinLobby(ws, msg.name));
        return;
      }
      joinLobby(ws, msg.name);
      return;
    }
    case "lobbyLeave": {
      leaveLobby(ws);
      return;
    }
    case "lobbyPing": {
      ws.lobbyPing = Number(msg.ms);
      if (lobbyClients.has(ws)) updateLobbyRoster();
      return;
    }
    case "lobbyChat": {
      if (!lobbyClients.has(ws)) joinLobby(ws, msg.name);
      const text = String(msg.text || "").replace(/\s+/g, " ").trim().slice(0, 180);
      if (text) {
        persistLobbyChat(ws, text);
        broadcastLobby({ type: "lobbyChat", from: playerLabel(ws), text });
      }
      return;
    }
    case "challenge": {
      if (!lobbyClients.has(ws)) joinLobby(ws, msg.name);
      const toId = Number(msg.to ?? msg.target);
      const target = Array.from(lobbyClients).find(client => client.clientId === toId);
      if (!target || target === ws || target.readyState !== WebSocket.OPEN) { send(ws, { type: "lobbyNotice", text: "Player not available" }); return; }
      if (ws.room || target.room) { send(ws, { type: "lobbyNotice", text: "Player is busy" }); return; }
      const id = "c" + nextChallengeId++;
      challenges.set(id, { id, from: ws, to: target, createdAt: Date.now() });
      send(ws, { type: "challengeSent", id, to: target.clientId, name: playerLabel(target) });
      send(target, { type: "challengeReceived", id, from: ws.clientId, name: playerLabel(ws) });
      return;
    }
    case "acceptChallenge": {
      const ch = challenges.get(String(msg.id || ""));
      if (!ch || ch.to !== ws) { send(ws, { type: "lobbyNotice", text: "Challenge expired" }); return; }
      challenges.delete(ch.id);
      startDuoChallenge(ch.from, ch.to);
      return;
    }
    case "declineChallenge": {
      const ch = challenges.get(String(msg.id || ""));
      if (!ch || ch.to !== ws) return;
      challenges.delete(ch.id);
      send(ch.from, { type: "lobbyNotice", text: playerLabel(ws) + " declined the challenge" });
      return;
    }
    case "create": {
      if (room) leaveRoom(ws); // a socket can only be in one room
      const mode = msg.mode === "quad" ? "quad" : "duo";
      const r = createRoom(ws, mode);
      r.autoStart = mode === "duo" && !!msg.autoStart;
      ws.room = r;
      if (mode === "quad") {
        ws.seat = "LA";
        send(ws, { type: "assigned", mode: "quad", side: "LA", roomId: r.id, filled: 1, total: 4 });
      } else {
        ws.side = "left";
        send(ws, { type: "assigned", mode: "duo", side: "left", roomId: r.id });
      }
      updateLobbyRoster();
      break;
    }
    case "join": {
      if (room) leaveRoom(ws);
      const code = String(msg.roomId || "").trim().toUpperCase();
      const r = rooms.get(code);
      if (!r) { send(ws, { type: "error", message: "Room not found — check the code" }); return; }
      if (r.phase !== "lobby") { send(ws, { type: "error", message: "Game already in progress" }); return; }

      if (r.mode === "quad") {
        const openSeat = QUAD_SEATS.find(s => !r.seats[s]);
        if (!openSeat) { send(ws, { type: "error", message: "Room is full" }); return; }
        r.seats[openSeat] = ws; ws.room = r; ws.seat = openSeat;
        r.lastActivity = Date.now();
        const filled = QUAD_SEATS.filter(s => r.seats[s]).length;
        send(ws, { type: "assigned", mode: "quad", side: openSeat, roomId: r.id, filled, total: 4 });
        if (filled === 4) {
          QUAD_SEATS.filter(s => s !== "LA").forEach(s => send(r.seats[s], { type: "waitingForHost" }));
          send(r.seats.LA, { type: "chooseDuration" });
        } else {
          QUAD_SEATS.filter(s => r.seats[s] && s !== openSeat)
            .forEach(s => send(r.seats[s], { type: "seatUpdate", filled, total: 4 }));
        }
        updateLobbyRoster();
        return;
      }

      if (r.guest) { send(ws, { type: "error", message: "Room is full" }); return; }
      r.guest = ws; ws.room = r; ws.side = "right";
      r.lastActivity = Date.now();
      send(ws, { type: "assigned", mode: "duo", side: "right", roomId: r.id });
      if (r.autoStart) {
        startGame(r, 60);
      } else {
        send(ws, { type: "waitingForHost" });
        send(r.host, { type: "chooseDuration" });
      }
      updateLobbyRoster();
      break;
    }
    case "input": {
      if (!room || room.phase !== "playing") return;
      const seatKey = room.mode === "quad" ? ws.seat : ws.side;
      if (!seatKey) return;
      const inp = room.inputs[seatKey];
      inp.left = !!msg.left; inp.right = !!msg.right;
      inp.up = !!msg.up; inp.down = !!msg.down;
      break;
    }
    case "roll": {
      if (!room || room.phase !== "playing") return;
      const seatKey = room.mode === "quad" ? ws.seat : ws.side;
      if (!seatKey) return;
      const sl = room.mode === "quad" ? room.gs[seatKey] : (seatKey === "left" ? room.gs.L : room.gs.R);
      const airborne = sl && sl.y < H - GH - 1;
      if (!sl || sl.rollCooldown > 0 || !airborne) return; // off cooldown AND airborne
      const team = room.mode === "quad" ? sl.team : seatKey; // "left" | "right"
      sl.rollCooldown = ROLL_COOLDOWN_TICKS;
      sl.rollTimer = ROLL_TICKS;
      sl.rollDir = team === "left" ? 1 : -1; // left spins clockwise, right counter-clockwise
      break;
    }
    case "chooseDuration": {
      if (!room || room.phase !== "lobby") return;
      if (room.mode === "quad") {
        if (ws !== room.seats.LA || QUAD_SEATS.some(s => !room.seats[s])) return;
      } else {
        if (ws !== room.host || !room.guest) return;
      }
      startGame(room, msg.duration);
      break;
    }
    case "rematch": {
      if (!room || room.phase !== "ended") return;
      if (room.mode === "quad") {
        if (ws !== room.seats.LA) return;
        if (QUAD_SEATS.some(s => !room.seats[s])) { send(ws, { type: "error", message: "A player left" }); return; }
      } else {
        if (ws !== room.host) return;
        if (!room.guest) { send(ws, { type: "error", message: "Opponent left" }); return; }
      }
      startGame(room, msg.duration || room.duration);
      break;
    }
  }
}

function leaveRoom(ws) {
  const room = ws.room;
  if (!room) return;
  ws.room = null;

  if (room.mode === "quad") {
    const seat = ws.seat; ws.seat = null;
    if (!seat) return;

    if (seat !== "LA" && room.phase === "lobby") {
      // THE FIX: free the seat instead of leaving the room full forever
      room.seats[seat] = null;
      room.inputs[seat] = { left: false, right: false, up: false, down: false };
      const filled = QUAD_SEATS.filter(s => room.seats[s]).length;
      QUAD_SEATS.filter(s => room.seats[s])
        .forEach(s => send(room.seats[s], { type: "seatLeftLobby", filled, total: 4 }));
      updateLobbyRoster();
      return;
    }

    // Host (LA) left, or anyone left during/after a game: the room is over
    QUAD_SEATS.filter(s => room.seats[s] && s !== seat)
      .forEach(s => send(room.seats[s], { type: "opponentLeft", side: seat }));
    destroyRoom(room);
    updateLobbyRoster();
    return;
  }

  if (ws === room.guest && room.phase === "lobby") {
    // THE FIX: free the seat instead of leaving the room full forever
    room.guest = null;
    room.inputs = freshInputsDuo();
    send(room.host, { type: "opponentLeftLobby" });
    updateLobbyRoster();
    return;
  }

  // Host left, or anyone left during/after a game: the room is over
  const other = ws === room.host ? room.guest : room.host;
  if (other) send(other, { type: "opponentLeft", side: ws.side });
  destroyRoom(room);
  updateLobbyRoster();
}

// ── Server setup ─────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));
app.get("/healthz", (req, res) => res.json({ ok: true, rooms: rooms.size }));

// ── Records / hall of fame API ───────────────────────────────────────────────
// CORS-open so the client works both when served by this server and when run
// from a separate dev origin (e.g. the preview server) against it.
app.use("/api", (req, res, next) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});
app.post("/api/register", async (req, res) => {
  try {
    const token = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
    const r = await verifiedOnePassUser(token);
    if (!r) return res.status(404).json({ ok: false, error: "OnePass profile not found" });
    res.json({ ok: true, name: r.name, record: publicRecord(r), source: "onepass" });
  } catch (error) {
    const notConfigured = /not configured/i.test(error.message || "");
    if (notConfigured) console.error("OnePass register:", error.message);
    res.status(notConfigured ? 503 : 401).json({
      ok: false,
      error: notConfigured
        ? "OnePass Firebase Admin is not configured on this server"
        : "OnePass sign-in could not be verified"
    });
  }
});
app.get("/api/record", (req, res) => {
  res.json(publicRecord(findPlayerRecord(req.query.name)));
});
app.get("/api/halloffame", (req, res) => {
  const players = uniqueRecords()
    .map(publicRecord)
    .sort((a, b) => b.wins - a.wins || a.losses - b.losses || b.games - a.games || a.name.localeCompare(b.name))
    .slice(0, 50);
  res.json({ players });
});
app.get("/api/loony-online", async (req, res) => {
  if (!firestore) return res.json({ players: [] });
  try {
    res.json({ players: loonyOnlinePlayers });
  } catch (e) {
    console.error("Loony online list:", e.message);
    res.status(503).json({ players: [], error: "Loony Firebase users are not reachable" });
  }
});

const port = process.env.PORT || 2567;
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

wss.on("connection", (ws, req) => {
  try { req.socket.setNoDelay(true); } catch (e) {} // don't let Nagle batch tiny frames
  ws.isAlive = true;
  ws.clientId = nextClientId++;
  ws.lobbyPing = null;
  ws.room = null; ws.side = null; ws.seat = null;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    try { handleMessage(ws, msg); } catch (e) { console.error("handleMessage:", e); }
  });
  ws.on("close", () => { leaveLobby(ws); leaveRoom(ws); setLoonyPresence(ws, { online: false, inChat: false, clientId: null }); });
  ws.on("error", () => { leaveLobby(ws); leaveRoom(ws); setLoonyPresence(ws, { online: false, inChat: false, clientId: null }); });
});

// Detect dead connections (mobile locks, dropped wifi) so seats free up
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Expire idle rooms
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room) => {
    if (now - room.lastActivity > ROOM_IDLE_MS) {
      broadcast(room, { type: "error", message: "Room expired" });
      roomSockets(room).forEach(ws => { ws.room = null; ws.close(); });
      destroyRoom(room);
    }
  });
}, 60000);

// Load the record backend first (so the hall of fame is populated on boot),
// then start accepting connections either way.
initRecordStore().finally(() => {
  server.listen(port, () => {
    console.log("Slime Soccer server listening on http://localhost:" + port + " (ws path /ws)");
  });
});
