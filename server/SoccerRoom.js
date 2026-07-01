const WebSocket = require('ws');

// ── Physics constants (must match client) ─────────────────────────────────────
const W=1000, H=520, GH=80, SR=40, BR=12, GW=80, GHEIGHT=130;
const GRAV=0.6, SPEED=6, JUMP=-13, BDAMP=0.99, BBOUNCE=0.8, MAXSPEED=14;
const SIM_TICK = 1000/60;

// ── Helpers ───────────────────────────────────────────────────────────────────
function makeGs() {
  return {
    L:    { x:250,  y:H-GH,   vx:0, vy:0, grab:false, hasBall:false, glTime:0 },
    R:    { x:750,  y:H-GH,   vx:0, vy:0, grab:false, hasBall:false, glTime:0 },
    ball: { x:W/2,  y:H/2-60, vx:0, vy:0, owner:null, angle:0, angVel:0 }
  };
}

function resetPos(gs) {
  Object.assign(gs.L,    { x:250,  y:H-GH,   vx:0, vy:0, grab:false, hasBall:false, glTime:0 });
  Object.assign(gs.R,    { x:750,  y:H-GH,   vx:0, vy:0, grab:false, hasBall:false, glTime:0 });
  Object.assign(gs.ball, { x:W/2,  y:H/2-60, vx:0, vy:0, owner:null, angle:0, angVel:0 });
}

// ── Physics step ──────────────────────────────────────────────────────────────
function physicsStep(gs, score, inputs, room) {
  const li = inputs.left  || {};
  const ri = inputs.right || {};

  gs.L.vx = li.left ? -SPEED : li.right ? SPEED : 0;
  if (li.up  && gs.L.y >= H-GH-1 && !gs.L.grab) gs.L.vy = JUMP;
  gs.L.grab = !!li.down;

  gs.R.vx = ri.left ? -SPEED : ri.right ? SPEED : 0;
  if (ri.up  && gs.R.y >= H-GH-1 && !gs.R.grab) gs.R.vy = JUMP;
  gs.R.grab = !!ri.down;

  [[gs.L, 0], [gs.R, 1]].forEach(([sl, idx]) => {
    sl.vy += GRAV; sl.x += sl.vx; sl.y += sl.vy;
    if (sl.x < SR)   sl.x = SR;
    if (sl.x > W-SR) sl.x = W-SR;
    if (sl.y > H-GH) { sl.y = H-GH; sl.vy = 0; }

    const inOwn = (idx===0 && sl.x < GW) || (idx===1 && sl.x > W-GW);
    if (inOwn) {
      sl.glTime += 1/60;
      if (sl.glTime >= 1) {
        sl.glTime = 0;
        const scorer = idx===0 ? 'right' : 'left';
        score[scorer]++;
        room.broadcast({ type:'goal', scorer, score:{...score} });
        resetPos(gs);
      }
    } else {
      sl.glTime = 0;
    }
  });

  const b = gs.ball;
  if (b.owner) {
    const gr  = b.owner==='left' ? gs.L : gs.R;
    const dir = b.owner==='left' ? 1 : -1;
    b.angVel += (-gr.vx * 0.008 * dir); b.angVel *= 0.85; b.angle += b.angVel;

    if (b.owner === 'left') {
      if (b.angle < -Math.PI/2)     { b.angle = -Math.PI/2;    b.angVel = 0; }
      else if (b.angle > Math.PI/2) { b.angle =  Math.PI/2;    b.angVel = 0; }
    } else {
      while (b.angle < 0)           b.angle += Math.PI*2;
      while (b.angle > Math.PI*2)   b.angle -= Math.PI*2;
      if (b.angle < Math.PI/2)          { b.angle = Math.PI/2;     b.angVel = 0; }
      else if (b.angle > 3*Math.PI/2)   { b.angle = 3*Math.PI/2;   b.angVel = 0; }
    }

    const hd = SR + BR - 5;
    b.x = gr.x + Math.cos(b.angle)*hd; b.y = gr.y + Math.sin(b.angle)*hd;
    b.vx = gr.vx; b.vy = gr.vy;

    if (!gr.grab) {
      const sp = Math.abs(b.angVel)*20;
      b.vx = gr.vx*1.5 + Math.cos(b.angle)*(3+sp);
      b.vy = gr.vy-2   + Math.sin(b.angle)*sp*0.3;
      b.owner = null; b.angle = 0; b.angVel = 0; gr.hasBall = false;
    }
  } else {
    b.vy += GRAV; b.vx *= BDAMP; b.x += b.vx; b.y += b.vy;
  }

  if (b.x < BR)      { b.x = BR;       b.vx = -b.vx*BBOUNCE; }
  if (b.x > W-BR)    { b.x = W-BR;     b.vx = -b.vx*BBOUNCE; }
  if (b.y > H-GH-BR) { b.y = H-GH-BR;  b.vy = -b.vy*BBOUNCE; }
  if (b.y < BR)      { b.y = BR;        b.vy = -b.vy*BBOUNCE; }

  if (b.x <= BR && b.y > H-GH-GHEIGHT) {
    score.right++;
    room.broadcast({ type:'goal', scorer:'right', score:{...score} });
    resetPos(gs); return;
  }
  if (b.x >= W-BR && b.y > H-GH-GHEIGHT) {
    score.left++;
    room.broadcast({ type:'goal', scorer:'left', score:{...score} });
    resetPos(gs); return;
  }

  [[gs.L,'left',gs.R],[gs.R,'right',gs.L]].forEach(([sl,nm,other]) => {
    const dx = b.x-sl.x, dy = b.y-sl.y;
    const dist = Math.sqrt(dx*dx+dy*dy);
    if (dist < SR+BR) {
      if (b.owner && b.owner !== nm) {
        const sp = Math.hypot(sl.vx, sl.vy);
        if (sp > 2 || Math.abs(sl.vy) > 5) {
          const ang = Math.atan2(dy,dx);
          b.owner=null; b.angle=0; b.angVel=0; other.hasBall=false;
          b.vx = Math.cos(ang)*8+sl.vx; b.vy = Math.sin(ang)*8+sl.vy;
          room.broadcast({ type:'hit', x:b.x, y:b.y });
        }
      } else if (sl.grab && !b.owner) {
        b.owner=nm; b.angle=Math.atan2(dy,dx); b.angVel=0; sl.hasBall=true;
        room.broadcast({ type:'grab', side:nm });
      } else if (!b.owner) {
        const ang = Math.atan2(dy,dx);
        if (b.y < sl.y || Math.abs(ang) < Math.PI*0.5) {
          b.x = sl.x + Math.cos(ang)*(SR+BR);
          b.y = sl.y + Math.sin(ang)*(SR+BR);
          const sp = Math.hypot(b.vx,b.vy);
          b.vx = Math.cos(ang)*sp*1.5 + sl.vx*0.5;
          b.vy = Math.sin(ang)*sp*1.5 + sl.vy*0.5;
          const ns = Math.hypot(b.vx,b.vy);
          if (ns > MAXSPEED) { b.vx *= MAXSPEED/ns; b.vy *= MAXSPEED/ns; }
          room.broadcast({ type:'hit', x:b.x, y:b.y });
        }
      }
    }
  });
}

// ── Room ──────────────────────────────────────────────────────────────────────
class SoccerRoom {
  constructor(roomId, onEmpty) {
    this.roomId       = roomId;
    this.onEmpty      = onEmpty;
    this.clients      = {};
    this.inputs       = { left:{}, right:{} };
    this.gs           = makeGs();
    this.score        = { left:0, right:0 };
    this.timeLeft     = 60;
    this.lastDuration = 60;
    this.status       = 'waiting';
    this._simTimer    = null;
    this._clockTimer  = null;
  }

  addClient(ws, side) { this.clients[side] = ws; }

  removeClient(side) {
    delete this.clients[side];
    this._stopTimers();
    this.broadcast({ type:'opponentLeft', side });
    if (this.isEmpty()) this.onEmpty();
  }

  isFull()  { return !!(this.clients.left && this.clients.right); }
  isEmpty() { return !this.clients.left && !this.clients.right; }

  sendTo(side, data) {
    const ws = this.clients[side];
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
  }

  broadcast(data) {
    const msg = JSON.stringify(data);
    for (const ws of Object.values(this.clients)) {
      if (ws && ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
  }

  setInput(side, input) { this.inputs[side] = input; }

  startGame(duration) {
    this._stopTimers();
    this.lastDuration = duration;
    this.timeLeft     = duration;
    this.score        = { left:0, right:0 };
    this.gs           = makeGs();
    this.inputs       = { left:{}, right:{} };
    this.status       = 'playing';

    this.broadcast({ type:'gameStarted', duration, score: this.score });

    this._simTimer = setInterval(() => {
      if (this.status !== 'playing') return;
      physicsStep(this.gs, this.score, this.inputs, this);
      this.broadcast({
        type:  'state',
        L:    { x:this.gs.L.x,    y:this.gs.L.y,    vx:this.gs.L.vx,    vy:this.gs.L.vy,    grab:this.gs.L.grab,    hasBall:this.gs.L.hasBall },
        R:    { x:this.gs.R.x,    y:this.gs.R.y,     vx:this.gs.R.vx,    vy:this.gs.R.vy,    grab:this.gs.R.grab,    hasBall:this.gs.R.hasBall },
        ball: { x:this.gs.ball.x, y:this.gs.ball.y,  vx:this.gs.ball.vx, vy:this.gs.ball.vy,
                owner:this.gs.ball.owner, angle:this.gs.ball.angle, angVel:this.gs.ball.angVel },
        score:    this.score,
        timeLeft: this.timeLeft
      });
    }, SIM_TICK);

    this._clockTimer = setInterval(() => {
      if (this.status !== 'playing') return;
      this.timeLeft = Math.max(0, this.timeLeft - 1);
      if (this.timeLeft <= 0) {
        this.status = 'ended';
        this.broadcast({ type:'gameEnded', score: this.score });
        this._stopTimers();
      }
    }, 1000);
  }

  _stopTimers() {
    if (this._simTimer)   { clearInterval(this._simTimer);   this._simTimer   = null; }
    if (this._clockTimer) { clearInterval(this._clockTimer); this._clockTimer = null; }
    this.status = 'ended';
  }
}

module.exports = { SoccerRoom };
