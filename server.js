// 코너 슈터 3D 멀티플레이 서버
// - index.html(게임)을 보여주고
// - /ws 로 들어온 사람들을 "방 코드"별로 묶어서 서로의 상태(presence)를 전달해요.
// 게임 계산(적 이동, 점수 등)은 방장 브라우저가 해요. 서버는 중계만 해서 가볍고 단순해요.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 4;          // 방 하나에 최대 4명
const MAX_ROOMS = 300;          // 서버 하나가 동시에 열어 둘 방 수
const MAX_MSG = 48 * 1024;      // 메시지 하나의 최대 크기(바이트)
const RATE = 40;                // 한 사람이 1초에 보낼 수 있는 메시지 수(넘으면 무시)

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') {
    const proto = String(req.headers['x-forwarded-proto'] || 'http').split(',')[0] === 'https' ? 'wss' : 'ws';
    const tag = '<script>window.GAME_WS=' + JSON.stringify(proto + '://' + req.headers.host + '/ws') + ';</script>';
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(html.replace('<!--WS-->', tag));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });
const rooms = new Map();        // 방 코드 -> { peers: Map(id -> { ws, pres }) }
let nextId = 1;

const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const others = (room, except, obj) => {
  const data = JSON.stringify(obj);
  room.peers.forEach((p, id) => { if (id !== except && p.ws.readyState === 1) p.ws.send(data); });
};
const merge = (base, patch) => {
  const o = Object.assign({}, base);
  for (const k of Object.keys(patch)) { if (patch[k] === null) delete o[k]; else o[k] = patch[k]; }
  return o;
};

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  room.peers.delete(ws.pid);
  others(room, ws.pid, { t: 'left', peer: ws.pid });
  if (room.peers.size === 0) rooms.delete(ws.code);
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.tokens = RATE; ws.tick = Date.now();
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // 초당 메시지 수 제한
    const now = Date.now();
    ws.tokens = Math.min(RATE, ws.tokens + ((now - ws.tick) / 1000) * RATE); ws.tick = now;
    if (ws.tokens < 1) return;
    ws.tokens -= 1;

    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join' && !ws.room) {
      const code = String(m.room || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 12);
      if (code.length < 4) { ws.close(); return; }
      let room = rooms.get(code);
      if (!room) {
        if (rooms.size >= MAX_ROOMS) { send(ws, { t: 'full' }); ws.close(); return; }
        room = { peers: new Map() }; rooms.set(code, room);
      }
      if (room.peers.size >= MAX_PLAYERS) { send(ws, { t: 'full' }); ws.close(); return; }
      ws.pid = 'p' + (nextId++); ws.room = room; ws.code = code;
      room.peers.set(ws.pid, { ws, pres: {} });
      send(ws, { t: 'hello', peer: ws.pid });
      const peers = [];
      room.peers.forEach((p, id) => { if (id !== ws.pid) peers.push({ peer: id, presence: p.pres }); });
      send(ws, { t: 'snap', peers });
      others(room, ws.pid, { t: 'join', peer: ws.pid });
      return;
    }

    if (m.t === 'pres' && ws.room && m.p && typeof m.p === 'object') {
      const me = ws.room.peers.get(ws.pid);
      if (!me) return;
      me.pres = merge(me.pres, m.p);
      others(ws.room, ws.pid, { t: 'pres', peer: ws.pid, p: m.p });
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

// 끊어진 연결 정리 (25초마다 확인)
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false; ws.ping();
  });
}, 25000);

server.listen(PORT, '0.0.0.0', () => console.log('코너 슈터 서버 시작: 포트 ' + PORT));
