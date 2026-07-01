const http       = require('http');
const path       = require('path');
const express    = require('express');
const WebSocket  = require('ws');
const { SoccerRoom } = require('./SoccerRoom');

const PORT      = process.env.PORT || 2567;
const GAME_FILE = path.join(__dirname, '..', 'HalfCapSoccer', 'HalfCapSoccer.html');

const app = express();
app.get('/', (_req, res) => res.sendFile(GAME_FILE));

const httpServer = http.createServer(app);
const wss = new WebSocket.Server({ server: httpServer, path: '/ws' });

// ── Room registry ─────────────────────────────────────────────────────────────
const rooms = new Map(); // roomId -> SoccerRoom

function makeRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id = '';
  for (let i = 0; i < 6; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(id) ? makeRoomId() : id;
}

// ── Connection handling ───────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let myRoomId = null, mySide = null, myRoom = null;

  function send(data) {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
  }

  ws.on('message', (rawData) => {
    let msg;
    try { msg = JSON.parse(rawData.toString()); } catch { return; }

    if (msg.type === 'create') {
      myRoomId = makeRoomId();
      myRoom   = new SoccerRoom(myRoomId, () => rooms.delete(myRoomId));
      rooms.set(myRoomId, myRoom);
      mySide   = 'left';
      myRoom.addClient(ws, 'left');
      send({ type: 'assigned', side: 'left', roomId: myRoomId });
      console.log(`[${myRoomId}] room created`);

    } else if (msg.type === 'join') {
      const roomId = (msg.roomId || '').toUpperCase();
      const room   = rooms.get(roomId);
      if (!room || room.isFull()) {
        send({ type: 'error', message: 'Room not found or full' });
        return;
      }
      myRoomId = roomId; myRoom = room; mySide = 'right';
      myRoom.addClient(ws, 'right');
      send({ type: 'assigned', side: 'right', roomId: myRoomId });
      myRoom.sendTo('left',  { type: 'chooseDuration' });
      myRoom.sendTo('right', { type: 'waitingForHost' });
      console.log(`[${myRoomId}] right player joined`);

    } else if (msg.type === 'chooseDuration' && myRoom && mySide === 'left') {
      myRoom.startGame(Number(msg.duration) || 60);

    } else if (msg.type === 'rematch' && myRoom && mySide === 'left') {
      myRoom.startGame(Number(msg.duration) || myRoom.lastDuration);

    } else if (msg.type === 'input' && myRoom) {
      myRoom.setInput(mySide, msg);
    }
  });

  ws.on('close', () => {
    if (!myRoom) return;
    myRoom.removeClient(mySide);
    if (myRoom.isEmpty()) rooms.delete(myRoomId);
  });

  ws.on('error', (err) => console.error('ws error:', err.message));
});

httpServer.listen(PORT, () => {
  console.log(`\n⚽  Slime Soccer  →  http://localhost:${PORT}\n`);
});
