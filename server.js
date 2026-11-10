const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e5 });

app.use(express.static(path.join(__dirname, 'public')));

// إعدادات ICE: STUN مجاني + TURN اختياري من متغيرات البيئة
app.get('/config', (req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(','),
      username: process.env.TURN_USER,
      credential: process.env.TURN_PASS,
    });
  }
  res.json({ iceServers });
});

const waiting = [];            // طابور الانتظار
const partners = new Map();    // socket.id -> partner socket.id
const reportCount = new Map(); // ip -> عدد البلاغات
const banned = new Map();      // ip -> وقت انتهاء الحظر

const getIp = (s) =>
  (s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();

function leave(socket) {
  const i = waiting.indexOf(socket.id);
  if (i !== -1) waiting.splice(i, 1);
  const p = partners.get(socket.id);
  if (p) {
    partners.delete(socket.id);
    partners.delete(p);
    io.to(p).emit('partner-left');
  }
}

function broadcastCount() {
  io.emit('online', io.engine.clientsCount);
}

io.on('connection', (socket) => {
  const ip = getIp(socket);
  if (banned.has(ip) && banned.get(ip) > Date.now()) {
    socket.emit('banned');
    return socket.disconnect(true);
  }
  broadcastCount();

  socket.on('find', () => {
    leave(socket);
    while (waiting.length) {
      const otherId = waiting.shift();
      const other = io.sockets.sockets.get(otherId);
      if (!other || otherId === socket.id) continue;
      partners.set(socket.id, otherId);
      partners.set(otherId, socket.id);
      socket.emit('matched', { initiator: true });
      other.emit('matched', { initiator: false });
      return;
    }
    waiting.push(socket.id);
    socket.emit('waiting');
  });

  socket.on('signal', (data) => {
    const p = partners.get(socket.id);
    if (p) io.to(p).emit('signal', data);
  });

  socket.on('chat', (msg) => {
    const p = partners.get(socket.id);
    if (p && typeof msg === 'string') io.to(p).emit('chat', msg.slice(0, 500));
  });

  socket.on('report', () => {
    const p = partners.get(socket.id);
    const target = p && io.sockets.sockets.get(p);
    if (!target) return;
    const tip = getIp(target);
    const n = (reportCount.get(tip) || 0) + 1;
    reportCount.set(tip, n);
    if (n >= 3) {
      banned.set(tip, Date.now() + 60 * 60 * 1000); // حظر ساعة
      target.emit('banned');
      target.disconnect(true);
    }
  });

  socket.on('leave', () => leave(socket));

  socket.on('disconnect', () => {
    leave(socket);
    broadcastCount();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('omee tv running on port ' + PORT));
