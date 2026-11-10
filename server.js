const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_ANON_KEY;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e5 });

app.use(express.static(path.join(__dirname, 'public')));

app.get('/config', (req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(','),
      username: process.env.TURN_USER,
      credential: process.env.TURN_PASS,
    });
  }
  res.json({ iceServers, supabaseUrl: SB_URL, supabaseKey: SB_KEY });
});

const waiting = [];
const partners = new Map();
const reports = new Map(); // userId -> count
const banned = new Map();  // userId -> expiry

// التحقق من الحساب عند Supabase (يجب أن يكون البريد مفعّلاً)
async function verify(token) {
  if (!token || !SB_URL) return null;
  const r = await fetch(SB_URL + '/auth/v1/user', {
    headers: { apikey: SB_KEY, Authorization: 'Bearer ' + token },
  });
  if (!r.ok) return null;
  const u = await r.json();
  if (!u.email_confirmed_at) return null;
  const m = u.user_metadata || {};
  return {
    id: u.id,
    name: String(m.name || 'مستخدم').slice(0, 20),
    gender: m.gender === 'male' || m.gender === 'female' ? m.gender : 'other',
    country: String(m.country || '').slice(0, 2).toUpperCase(),
  };
}

io.use(async (socket, next) => {
  try {
    const u = await verify(socket.handshake.auth && socket.handshake.auth.token);
    if (!u) return next(new Error('auth'));
    if (banned.has(u.id) && banned.get(u.id) > Date.now()) return next(new Error('banned'));
    socket.user = u;
    socket.prefs = { g: 'any', c: 'any' };
    next();
  } catch (e) {
    next(new Error('auth'));
  }
});

function compatible(a, b) {
  return (
    a.id !== b.id &&
    a.user.id !== b.user.id &&
    (a.prefs.g === 'any' || a.prefs.g === b.user.gender) &&
    (b.prefs.g === 'any' || b.prefs.g === a.user.gender) &&
    (a.prefs.c === 'any' || a.prefs.c === b.user.country) &&
    (b.prefs.c === 'any' || b.prefs.c === a.user.country)
  );
}

function leave(socket) {
  const i = waiting.indexOf(socket);
  if (i !== -1) waiting.splice(i, 1);
  const p = partners.get(socket.id);
  if (p) {
    partners.delete(socket.id);
    partners.delete(p);
    io.to(p).emit('partner-left');
  }
}

const pub = (u) => ({ name: u.name, country: u.country, gender: u.gender });

io.on('connection', (socket) => {
  io.emit('online', io.engine.clientsCount);

  socket.on('find', (p) => {
    leave(socket);
    const g = p && ['any', 'male', 'female'].includes(p.g) ? p.g : 'any';
    const c = p && typeof p.c === 'string' && p.c.length <= 3 ? p.c.toUpperCase() : 'any';
    socket.prefs = { g, c: c === 'ANY' ? 'any' : c };

    const idx = waiting.findIndex((w) => w.connected && compatible(socket, w));
    if (idx !== -1) {
      const other = waiting.splice(idx, 1)[0];
      partners.set(socket.id, other.id);
      partners.set(other.id, socket.id);
      socket.emit('matched', { initiator: true, partner: pub(other.user) });
      other.emit('matched', { initiator: false, partner: pub(socket.user) });
      return;
    }
    waiting.push(socket);
    socket.emit('waiting');
  });

  socket.on('signal', (d) => {
    const p = partners.get(socket.id);
    if (p) io.to(p).emit('signal', d);
  });

  socket.on('chat', (msg) => {
    const p = partners.get(socket.id);
    if (p && typeof msg === 'string') io.to(p).emit('chat', msg.slice(0, 500));
  });

  socket.on('report', () => {
    const p = partners.get(socket.id);
    const target = p && io.sockets.sockets.get(p);
    if (!target) return;
    const n = (reports.get(target.user.id) || 0) + 1;
    reports.set(target.user.id, n);
    if (n >= 3) {
      banned.set(target.user.id, Date.now() + 60 * 60 * 1000);
      target.emit('banned');
      target.disconnect(true);
    }
  });

  socket.on('leave', () => leave(socket));
  socket.on('disconnect', () => {
    leave(socket);
    io.emit('online', io.engine.clientsCount);
  });
});

server.listen(process.env.PORT || 3000, () => console.log('omee tv running'));
