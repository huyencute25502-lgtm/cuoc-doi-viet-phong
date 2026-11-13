/**
 * Máy chủ phòng chơi "Cuộc Đời Việt" — chơi cùng bạn bè, xem ai sống lâu nhất.
 *
 * Chạy:  npm install && npm start   (mặc định cổng 8975, hoặc PORT trong env)
 * Giao thức: Socket.io (client tải thư viện từ chính server này:
 *   <script src="https://<host>/socket.io/socket.io.js"></script>)
 *
 * Luồng chơi:
 *   1. Người tạo bấm "Tạo phòng" -> gửi `create-room` { name }
 *      -> nhận `room-created` { code, link, state }, link mời dạng
 *         https://cuocdoiviet.netlify.app/?phong=CODE
 *   2. Bạn bè nhập mã (hoặc bấm link mời) -> gửi `join-room` { code, name }
 *      -> nhận `joined` { state } hoặc `join-error` { message }
 *   3. Trong lúc chơi, mỗi client gửi `progress` { age, job?, money? } mỗi khi
 *      lên 1 tuổi (server giới hạn 1 tin/giây/người, phát trạng thái phòng tối
 *      đa 1 lần/giây). job = nghề hiện tại, money = tiền (triệu đồng) — để
 *      bảng xếp hạng hiện đủ nghề + tiền. Client cũ chỉ gửi { age } vẫn
 *      chạy bình thường (job/money null -> bảng hiện '—').
 *   4. Khi một người chết, client gửi `died` { finalAge }.
 *      Khi mọi người trong phòng đều đã chết -> server phát `room-finished`
 *      kèm bảng xếp hạng cuối cùng (tuổi thọ giảm dần).
 *   5. Mất kết nối giữa chừng: người chơi giữ nguyên tuổi cuối, hiện "mất kết nối";
 *      vào lại cùng mã + cùng tên thì chơi tiếp từ chỗ cũ.
 *   6. v8 — chơi lại cùng phòng: sau khi phòng kết thúc, ai còn online bấm
 *      `rematch` (Sẵn sàng chơi lại) -> phòng sang trạng thái `waiting`, đếm
 *      ngược 30 giây; đủ người (mọi người còn online đều sẵn sàng) thì bắt đầu
 *      ngay, hết giờ thì bắt đầu với những ai đã sẵn sàng; ván mới giữ nguyên
 *      mã phòng + tên, tuổi mọi người về 0.
 *   7. v8 — thẻ đời + khoảnh khắc chung: client gửi `life-card` (nghề, tỉnh,
 *      cưới chưa, mấy con, chết vì sao...) khi có mốc lớn; server lưu vào người
 *      chơi, phát ngay cho cả phòng. Mốc lớn (cưới, sinh con, đỗ đại học, lên
 *      chức) thành 1 dòng trong nhật ký chung 5 dòng gần nhất của phòng + thông
 *      báo `room-moment` phát ngay, không chờ throttle.
 *   8. v8 — thách đấu: phòng có trường `target` (mặc định 74,7; người tạo chọn
 *      74,7 / 80 / 90 / 100 lúc tạo phòng). Ai sống tới target thì được ghi
 *      "Vượt mốc" ngay trong bảng + thông báo chung + ghi vào bảng tổng kết.
 *
 * Không cần đăng ký/đăng nhập. Phòng lưu trong bộ nhớ, tự xóa khi hết hạn.
 */

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 8975;
const GAME_URL = process.env.GAME_URL || 'https://cuocdoiviet.netlify.app/';

// ---- Giới hạn ----
const MAX_PLAYERS_PER_ROOM = 12;
const MAX_ROOMS = 200;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;      // phòng tối đa 6 giờ (1 đời chơi 30-90 phút)
const EMPTY_ROOM_TTL_MS = 30 * 60 * 1000;    // phòng không còn ai thì giữ 30 phút
const OFFLINE_KEEP_MS = 10 * 60 * 1000;      // người mất kết nối giữ chỗ 10 phút
const BROADCAST_MIN_GAP_MS = 1000;           // phát trạng thái phòng tối đa 1 lần/giây
const PROGRESS_MIN_GAP_MS = 1000;            // nhận progress tối đa 1 lần/giây/người
const LIFE_CARD_MIN_GAP_MS = 5000;           // nhận life-card tối đa 1 lần/5 giây/người (mốc lớn hiếm, chống spam)
// Đếm ngược sảnh chơi lại (v8-A): 30 giây mặc định. Chỉ để test nhanh mới
// chỉnh bằng biến môi trường, vd REMATCH_COUNTDOWN_MS=3000.
const REMATCH_COUNTDOWN_MS = Number(process.env.REMATCH_COUNTDOWN_MS || 30000);
// Mốc thách đấu (v8-D): người tạo phòng chọn 1 trong 4 mốc này, mặc định 74,7.
const ALLOWED_TARGETS = [74.7, 80, 90, 100];
const DEFAULT_TARGET = 74.7;
// Nhật ký chung của phòng (v8-C): giữ 5 mốc gần nhất.
const ROOM_LOG_MAX = 5;
// Các mốc lớn client được báo qua life-card (v8-C). Server tự soạn câu thông
// báo tiếng Việt từ loại mốc + tên + tuổi (không tin chữ client gửi kèm).
const MOMENT_TYPES = ['married', 'child', 'graduated', 'promoted'];

// Mã phòng 4 ký tự, bỏ các ký tự dễ nhầm (0/O, 1/I) để đọc to/nhắn tin không nhầm.
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const CODE_LEN = 4;

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

// rooms: Map<code, Room>
// Room = { code, createdAt, lastActivity, status: 'playing'|'finished'|'waiting',
//          target (mốc thách đấu, mặc định 74,7), log: [{name,age,type,text,at}]
//          (nhật ký chung 5 mốc gần nhất), rematch: null | { ready: Set<key>,
//          endsAt, timer } (sảnh chơi lại), players: Map<playerKey, Player>,
//          lastBroadcast }
// Player = { id (socket id hiện tại, '' nếu offline), name, age, alive,
//            finalAge, online, diedAt, lastSeen,
//            job: null | string (nghề hiện tại, client gửi qua progress),
//            money: null | number (tiền, triệu đồng, client gửi qua progress),
//            card: null | { job, province, married, children, age, cause, note }
//            (thẻ đời tóm tắt, client gửi qua life-card),
//            targetBeaten: false (đã sống qua mốc thách đấu của phòng chưa) }
const rooms = new Map();

function normName(name) {
  return String(name || '')
    .replace(/[<>&"']/g, '')
    .trim()
    .slice(0, 12);
}
function normCode(code) {
  return String(code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, CODE_LEN);
}
function makeCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < CODE_LEN; i++) {
      code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    }
  } while (rooms.has(code));
  return code;
}
function playerKey(name) {
  return normName(name).toLowerCase();
}
function publicState(room) {
  const players = [...room.players.values()]
    .map((p) => ({
      name: p.name,
      age: p.age,
      alive: p.alive,
      finalAge: p.finalAge,
      online: p.online,
      job: p.job || null,
      money: (typeof p.money === 'number' && Number.isFinite(p.money)) ? p.money : null,
      card: p.card || null,
      targetBeaten: !!p.targetBeaten,
    }))
    // Đang sống xếp trên (tuổi cao trước), đã chết xếp sau (tuổi thọ cao trước).
    .sort((a, b) => {
      if (a.alive !== b.alive) return a.alive ? -1 : 1;
      return (b.alive ? b.age : b.finalAge) - (a.alive ? a.age : a.finalAge);
    });
  const state = {
    code: room.code,
    status: room.status,
    target: room.target,
    log: (room.log || []).slice(),
    players,
    createdAt: room.createdAt,
  };
  // Đang ở sảnh chơi lại thì kèm luôn ai đã sẵn sàng + còn bao nhiêu giây.
  if (room.status === 'waiting' && room.rematch) {
    const readyNames = [...room.rematch.ready]
      .map((k) => room.players.get(k))
      .filter(Boolean)
      .map((p) => p.name);
    const waitingNames = [...room.players.values()]
      .filter((p) => p.online && !room.rematch.ready.has(playerKey(p.name)))
      .map((p) => p.name);
    state.rematch = {
      ready: readyNames,
      waitingFor: waitingNames,
      secondsLeft: Math.max(0, Math.ceil((room.rematch.endsAt - Date.now()) / 1000)),
    };
  }
  return state;
}
function rankingOf(room) {
  return [...room.players.values()]
    .filter((p) => !p.alive)
    .map((p) => ({
      name: p.name,
      finalAge: p.finalAge,
      diedAt: p.diedAt || 0,
      targetBeaten: !!p.targetBeaten,
    }))
    .sort((a, b) => b.finalAge - a.finalAge || a.diedAt - b.diedAt);
}
// "74.7" -> "74,7" (hiển thị kiểu Việt Nam).
function fmtTarget(t) {
  return String(t).replace('.', ',');
}
// Câu thông báo khoảnh khắc chung (v8-C): server tự soạn từ loại mốc + tên +
// tuổi thật trên server (không tin chữ client gửi kèm).
function momentText(type, name, age, target) {
  switch (type) {
    case 'married': return `${name} vừa cưới ở tuổi ${age}! 💍`;
    case 'child': return `${name} vừa đón em bé ở tuổi ${age}! 👶`;
    case 'graduated': return `${name} vừa đỗ đại học ở tuổi ${age}! 🎓`;
    case 'promoted': return `${name} vừa lên chức ở tuổi ${age}! 📈`;
    case 'passed-target': return `${name} đã sống qua ${fmtTarget(target)} tuổi! 👑`;
    default: return '';
  }
}
// Thêm 1 dòng vào nhật ký chung của phòng (giữ tối đa 5 mốc gần nhất).
function addLog(room, entry) {
  room.log.push(entry);
  if (room.log.length > ROOM_LOG_MAX) room.log.splice(0, room.log.length - ROOM_LOG_MAX);
}
// Phát 1 khoảnh khắc chung cho cả phòng NGAY (không chờ throttle 1 giây,
// giống room-finished) + ghi vào nhật ký chung.
function announceMoment(room, type, player) {
  const text = momentText(type, player.name, player.age, room.target);
  if (!text) return;
  const entry = { name: player.name, age: player.age, type, text, at: Date.now() };
  addLog(room, entry);
  io.to(room.code).emit('room-moment', entry);
}
// Rửa thẻ đời client gửi qua life-card (v8-B): chỉ giữ các trường đã biết,
// chữ bỏ ký tự <>&"' + cắt ngắn, số kẹp trong khoảng hợp lý. Thẻ rỗng
// (không còn trường nào) thì trả null -> server bỏ qua.
function cleanCard(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const s = (v, max) => String(v == null ? '' : v).replace(/[<>&"']/g, '').trim().slice(0, max);
  const card = {};
  if (raw.job != null) card.job = s(raw.job, 40);
  if (raw.province != null) card.province = s(raw.province, 40);
  if (raw.cause != null) card.cause = s(raw.cause, 60);
  if (raw.note != null) card.note = s(raw.note, 80);
  if (typeof raw.married === 'boolean') card.married = raw.married;
  if (Number.isFinite(Number(raw.children))) card.children = Math.max(0, Math.min(20, Math.floor(Number(raw.children))));
  if (Number.isFinite(Number(raw.age))) card.age = Math.max(0, Math.min(110, Math.floor(Number(raw.age))));
  for (const k of Object.keys(card)) {
    if (card[k] === '' || card[k] == null) delete card[k];
  }
  return Object.keys(card).length ? card : null;
}
function touch(room) {
  room.lastActivity = Date.now();
}
let broadcastTimer = null;
function broadcast(room) {
  const now = Date.now();
  if (now - (room.lastBroadcast || 0) < BROADCAST_MIN_GAP_MS) {
    // Đang dồn tin: hẹn phát sau, không phát chồng.
    if (!room._pendingBroadcast) {
      room._pendingBroadcast = true;
      setTimeout(() => {
        room._pendingBroadcast = false;
        room.lastBroadcast = Date.now();
        io.to(room.code).emit('room-state', publicState(room));
      }, BROADCAST_MIN_GAP_MS - (now - (room.lastBroadcast || 0)));
    }
    return;
  }
  room.lastBroadcast = now;
  io.to(room.code).emit('room-state', publicState(room));
}
function checkFinished(room) {
  // Chỉ phòng đang chơi mới kết thúc được. Phòng 'waiting' (sảnh chơi lại) hay
  // 'finished' rồi thì không kết thúc lại — tránh hẹn giờ dọn người offline
  // (10 phút) vô tình kéo phòng đang chờ chơi lại về 'finished'.
  if (room.status !== 'playing') return;
  const players = [...room.players.values()];
  if (players.length === 0) return;
  if (players.every((p) => !p.alive)) {
    room.status = 'finished';
    touch(room);
    io.to(room.code).emit('room-finished', {
      code: room.code,
      ranking: rankingOf(room),
      target: room.target,
    });
    // Phát trạng thái cuối NGAY (bỏ qua throttle 1 giây): bảng xếp hạng và
    // dòng "Đã kết thúc" phải đồng bộ với bảng tổng kết, không được chậm sau.
    room.lastBroadcast = Date.now();
    io.to(room.code).emit('room-state', publicState(room));
  }
}

// ---- Chơi lại cùng phòng (v8-A) ----
// Sảnh chơi lại: người đầu tiên bấm `rematch` mở đếm ngược 30 giây; mọi người
// còn online bấm Sẵn sàng thì bắt đầu ngay; hết giờ thì bắt đầu với những ai
// đã sẵn sàng. Ván mới giữ nguyên mã phòng + tên, tuổi mọi người về 0.
function startRematchCountdown(room) {
  room.status = 'waiting';
  const endsAt = Date.now() + REMATCH_COUNTDOWN_MS;
  const timer = setTimeout(() => {
    // Phòng có thể đã bị xóa (hết hạn) hoặc đã bắt đầu lại trước đó.
    if (rooms.get(room.code) !== room || room.status !== 'waiting' || !room.rematch) return;
    startRematch(room);
  }, REMATCH_COUNTDOWN_MS);
  room.rematch = { ready: new Set(), endsAt, timer };
  io.to(room.code).emit('rematch-countdown', { code: room.code, seconds: Math.round(REMATCH_COUNTDOWN_MS / 1000), endsAt });
  broadcast(room);
}
// Mọi người còn online đều đã sẵn sàng chưa?
function allOnlineReady(room) {
  const online = [...room.players.values()].filter((p) => p.online);
  return online.length > 0 && online.every((p) => room.rematch.ready.has(playerKey(p.name)));
}
// Hủy sảnh chơi lại khi không còn ai muốn chơi lại (mọi người đã rời phòng).
function maybeCancelRematch(room) {
  if (room.status !== 'waiting' || !room.rematch) return;
  if (room.rematch.ready.size > 0) return;
  clearTimeout(room.rematch.timer);
  room.rematch = null;
  room.status = 'finished';
  io.to(room.code).emit('rematch-state', { code: room.code, status: 'finished', ready: [], waitingFor: [], secondsLeft: 0 });
  broadcast(room);
}
// Bắt đầu ván mới: chỉ những ai đã bấm Sẵn sàng được chơi tiếp; ai chưa bấm
// thì rời khỏi phòng (nhận `rematch-skipped`, vào lại bằng mã phòng sau).
function startRematch(room) {
  if (!room.rematch) return;
  clearTimeout(room.rematch.timer);
  const readyKeys = [...room.rematch.ready].filter((k) => room.players.has(k));
  if (readyKeys.length === 0) {
    // Không còn ai: hủy sảnh, về lại bảng tổng kết cũ.
    room.rematch = null;
    room.status = 'finished';
    io.to(room.code).emit('rematch-state', { code: room.code, status: 'finished', ready: [], waitingFor: [], secondsLeft: 0 });
    broadcast(room);
    return;
  }
  for (const [key, p] of [...room.players.entries()]) {
    if (readyKeys.includes(key)) {
      // Chơi lại: tuổi về 0, sống lại, xóa thẻ đời + mốc cũ của ván trước.
      p.age = 0;
      p.alive = true;
      p.finalAge = null;
      p.diedAt = null;
      p.card = null;
      p.job = null;
      p.money = null;
      p.targetBeaten = false;
      p.lastSeen = Date.now();
    } else {
      // Chưa bấm Sẵn sàng: rời khỏi phòng + báo riêng cho người đó.
      room.players.delete(key);
      const s = p.id ? io.sockets.sockets.get(p.id) : null;
      if (s) s.leave(room.code);
      if (p.id) {
        io.to(p.id).emit('rematch-skipped', {
          message: 'Bạn chưa bấm Sẵn sàng nên không vào ván mới. Nhập lại mã phòng để chơi tiếp nhé.',
        });
      }
    }
  }
  room.rematch = null;
  room.log = [];
  room.status = 'playing';
  touch(room);
  io.to(room.code).emit('room-restarted', { code: room.code, state: publicState(room) });
  broadcast(room);
}

// Dọn phòng hết hạn mỗi phút.
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const empty = [...room.players.values()].every((p) => !p.online);
    if (
      now - room.createdAt > ROOM_TTL_MS ||
      (empty && now - room.lastActivity > EMPTY_ROOM_TTL_MS)
    ) {
      rooms.delete(code);
    }
  }
}, 60 * 1000);

io.on('connection', (socket) => {
  let currentRoom = null; // code
  let currentKey = null; // playerKey
  let lastProgressAt = 0;
  let lastLifeCardAt = 0;

  function myPlayer() {
    if (!currentRoom) return null;
    const room = rooms.get(currentRoom);
    if (!room || !currentKey) return null;
    return room.players.get(currentKey) || null;
  }

  socket.on('create-room', ({ name, target } = {}) => {
    const cleanName = normName(name);
    if (!cleanName) {
      socket.emit('join-error', { message: 'Bạn chưa nhập tên.' });
      return;
    }
    if (rooms.size >= MAX_ROOMS) {
      socket.emit('join-error', { message: 'Đang có quá nhiều phòng, thử lại sau ít phút.' });
      return;
    }
    // Mốc thách đấu (v8-D): chỉ nhận 1 trong 4 mốc, còn lại về mặc định 74,7.
    const t = Number(target);
    const roomTarget = ALLOWED_TARGETS.includes(t) ? t : DEFAULT_TARGET;
    const code = makeCode();
    const room = {
      code,
      createdAt: Date.now(),
      lastActivity: Date.now(),
      status: 'playing',
      target: roomTarget,
      log: [],
      rematch: null,
      players: new Map(),
      lastBroadcast: 0,
      _pendingBroadcast: false,
    };
    rooms.set(code, room);
    const key = playerKey(cleanName);
    room.players.set(key, {
      id: socket.id,
      name: cleanName,
      age: 0,
      alive: true,
      finalAge: null,
      online: true,
      diedAt: null,
      lastSeen: Date.now(),
      card: null,
      job: null,
      money: null,
      targetBeaten: false,
    });
    currentRoom = code;
    currentKey = key;
    socket.join(code);
    touch(room);
    socket.emit('room-created', {
      code,
      link: `${GAME_URL}?phong=${code}`,
      state: publicState(room),
    });
    broadcast(room);
  });

  socket.on('join-room', ({ code, name } = {}) => {
    const cleanCode = normCode(code);
    const cleanName = normName(name);
    if (!cleanName) {
      socket.emit('join-error', { message: 'Bạn chưa nhập tên.' });
      return;
    }
    const room = rooms.get(cleanCode);
    if (!room) {
      socket.emit('join-error', { message: `Không tìm thấy phòng mã ${cleanCode || '???'}. Kiểm tra lại mã nhé.` });
      return;
    }
    if (room.status === 'finished') {
      socket.emit('join-error', { message: 'Phòng này đã chơi xong rồi.' });
      return;
    }
    if (room.status === 'waiting') {
      // Phòng đang ở sảnh chơi lại: không cho vào giữa chừng (vào lại sau
      // khi ván mới bắt đầu, lúc đó phòng lại 'playing' như bình thường).
      socket.emit('join-error', { message: 'Phòng đang chờ chơi lại, thử lại sau ít phút nhé.' });
      return;
    }
    const key = playerKey(cleanName);
    const existing = room.players.get(key);
    if (existing) {
      // Tên này đã có người ĐANG ONLINE (socket khác) thì không cho chiếm
      // ghế — tránh trường hợp 2 người cùng tên, người sau đá người trước
      // ra khỏi bảng xếp hạng. Vào lại bằng cùng tên chỉ được khi người
      // đó đã ngắt kết nối (offline), lúc đó chơi tiếp từ chỗ cũ.
      if (existing.online && existing.id && existing.id !== socket.id) {
        socket.emit('join-error', { message: 'Tên này đang có người chơi rồi' });
        return;
      }
      existing.id = socket.id;
      existing.online = true;
      existing.lastSeen = Date.now();
    } else {
      if (room.players.size >= MAX_PLAYERS_PER_ROOM) {
        socket.emit('join-error', { message: 'Phòng đã đủ người (tối đa 12 người).' });
        return;
      }
      room.players.set(key, {
        id: socket.id,
        name: cleanName,
        age: 0,
        alive: true,
        finalAge: null,
        online: true,
        diedAt: null,
        lastSeen: Date.now(),
        card: null,
        job: null,
        money: null,
        targetBeaten: false,
      });
    }
    currentRoom = cleanCode;
    currentKey = key;
    socket.join(cleanCode);
    touch(room);
    socket.emit('joined', { state: publicState(room) });
    socket.to(cleanCode).emit('player-joined', { name: cleanName });
    broadcast(room);
  });

  // Client xin lại trạng thái phòng hiện tại (dùng khi vừa vào lại sau khi
  // mất mạng, hoặc khi sợ lỡ một bản phát trạng thái trước đó).
  socket.on('get-state', () => {
    const room = currentRoom ? rooms.get(currentRoom) : null;
    if (room) socket.emit('room-state', publicState(room));
  });

  socket.on('progress', ({ age, job, money } = {}) => {
    const player = myPlayer();
    if (!player || !player.alive) return;
    const now = Date.now();
    if (now - lastProgressAt < PROGRESS_MIN_GAP_MS) return;
    lastProgressAt = now;
    const a = Math.max(0, Math.min(110, Math.floor(Number(age) || 0)));
    // Tuổi chỉ tăng từng bước 1: chỉ chấp nhận đúng tuổi hiện tại + 1.
    // Tuổi bằng hoặc nhỏ hơn thì bỏ qua (không lùi tuổi); tuổi nhảy vọt
    // (ví dụ từ 5 lên 50) cũng bỏ qua — chống client báo tuổi gian lận.
    if (a !== player.age + 1) return;
    player.age = a;
    // Nghề + tiền để bảng xếp hạng hiện đủ: chỉ lưu khi đúng kiểu
    // (job là chuỗi, money là số hữu hạn), sai kiểu thì bỏ qua, không crash.
    if (typeof job === 'string' && job.trim()) player.job = job.trim().slice(0, 30);
    if (typeof money === 'number' && Number.isFinite(money)) player.money = money;
    player.lastSeen = now;
    const room = rooms.get(currentRoom);
    // Vượt mốc thách đấu (v8-D): tuổi chạm mốc lần đầu thì thông báo chung
    // cho cả phòng + ghi vào nhật ký chung (chỉ 1 lần mỗi ván).
    if (!player.targetBeaten && a >= room.target) {
      player.targetBeaten = true;
      announceMoment(room, 'passed-target', player);
    }
    touch(room);
    broadcast(room);
  });

  socket.on('died', ({ finalAge } = {}) => {
    const player = myPlayer();
    if (!player || !player.alive) return;
    // Tuổi lúc chết phải khớp tuổi server đang giữ: chỉ chấp nhận đúng tuổi
    // hiện tại, hoặc tuổi hiện tại + 1 (trừ hao 1 tin progress có thể đã bị
    // throttle nuốt ở tốc độ cao). Báo bừa (nhảy vọt, lùi tuổi, chữ linh
    // tinh) thì lấy đúng tuổi server — không cho báo tuổi chết gian lận.
    const sent = Math.floor(Number(finalAge));
    const a = Number.isFinite(sent) && (sent === player.age || sent === player.age + 1)
      ? Math.max(0, Math.min(110, sent))
      : player.age;
    player.alive = false;
    player.finalAge = a;
    player.age = a;
    player.diedAt = Date.now();
    player.lastSeen = Date.now();
    const room = rooms.get(currentRoom);
    touch(room);
    checkFinished(room); // chạy trước để lần phát trạng thái sau mang trạng thái cuối
    broadcast(room);
  });

  // ---- Chơi lại cùng phòng (v8-A) ----
  // Client bấm "Sẵn sàng chơi lại" sau khi phòng kết thúc. Người đầu tiên
  // bấm thì mở sảnh chơi lại + đếm ngược 30 giây; mọi người còn online đều
  // bấm thì bắt đầu ván mới ngay; hết giờ thì bắt đầu với ai đã sẵn sàng.
  socket.on('rematch', () => {
    const player = myPlayer();
    if (!player) return;
    const room = rooms.get(currentRoom);
    if (!room) return;
    if (room.status === 'playing') {
      socket.emit('rematch-error', { message: 'Phòng chưa chơi xong, chưa chơi lại được nhé.' });
      return;
    }
    if (room.status === 'finished') {
      startRematchCountdown(room);
    }
    if (room.status !== 'waiting' || !room.rematch) return;
    room.rematch.ready.add(currentKey);
    touch(room);
    // Phát trạng thái sảnh chơi lại cho cả phòng (ai đã sẵn sàng, còn chờ ai,
    // còn bao nhiêu giây) — phát ngay, không chờ throttle.
    const st = publicState(room);
    io.to(room.code).emit('rematch-state', {
      code: room.code,
      status: 'waiting',
      ready: st.rematch.ready,
      waitingFor: st.rematch.waitingFor,
      secondsLeft: st.rematch.secondsLeft,
    });
    broadcast(room);
    // Mọi người còn online đều sẵn sàng -> bắt đầu ván mới ngay.
    if (allOnlineReady(room)) startRematch(room);
  });

  // ---- Thẻ đời + khoảnh khắc chung (v8-B, v8-C) ----
  // Client gửi khi có mốc lớn (cưới, sinh con, đỗ đại học, lên chức, chết):
  // { job, province, married, children, age, cause, note, moment? }
  // moment (nếu có) phải là 1 trong MOMENT_TYPES; server tự soạn câu thông
  // báo từ loại mốc + tên + tuổi thật, không tin chữ client gửi kèm.
  socket.on('life-card', (raw = {}) => {
    const player = myPlayer();
    if (!player) return;
    const now = Date.now();
    if (now - lastLifeCardAt < LIFE_CARD_MIN_GAP_MS) return;
    const card = cleanCard(raw);
    if (!card) return; // thẻ rỗng / sai kiểu thì bỏ qua
    lastLifeCardAt = now;
    player.card = card;
    player.lastSeen = now;
    const room = rooms.get(currentRoom);
    if (!room) return;
    touch(room);
    // Thẻ đời phát ngay cho cả phòng (kèm theo trạng thái phòng sau đó).
    io.to(room.code).emit('life-card', { name: player.name, card });
    // Có mốc lớn + người chơi còn sống -> thông báo chung + ghi nhật ký.
    if (player.alive && typeof raw.moment === 'string' && MOMENT_TYPES.includes(raw.moment)) {
      announceMoment(room, raw.moment, player);
    }
    broadcast(room);
  });

  socket.on('leave-room', () => {
    const room = currentRoom ? rooms.get(currentRoom) : null;
    const player = myPlayer();
    if (room && player) {
      room.players.delete(currentKey);
      if (room.rematch) room.rematch.ready.delete(currentKey);
      socket.leave(currentRoom);
      socket.to(currentRoom).emit('player-left', { name: player.name });
      touch(room);
      maybeCancelRematch(room);
      broadcast(room);
    }
    currentRoom = null;
    currentKey = null;
  });

  socket.on('disconnect', () => {
    const room = currentRoom ? rooms.get(currentRoom) : null;
    const player = myPlayer();
    if (room && player) {
      player.online = false;
      player.id = '';
      player.lastSeen = Date.now();
      touch(room);
      // Giữ chỗ 10 phút cho người vào lại; quá hạn thì xóa khỏi phòng.
      setTimeout(() => {
        const r = rooms.get(currentRoom || '');
        if (!r) return;
        const p = r.players.get(currentKey || '');
        if (p && !p.online && Date.now() - p.lastSeen > OFFLINE_KEEP_MS) {
          r.players.delete(currentKey);
          if (r.rematch) r.rematch.ready.delete(currentKey);
          checkFinished(r);
          maybeCancelRematch(r);
          broadcast(r);
        }
      }, OFFLINE_KEEP_MS + 1000);
      broadcast(room);
    }
  });
});

// ---- Trang kiểm tra + trang test ----
app.get('/health', (req, res) => {
  let players = 0;
  for (const room of rooms.values()) players += room.players.size;
  res.json({ ok: true, rooms: rooms.size, players, uptimeSec: Math.floor(process.uptime()) });
});
app.get('/', (req, res) => {
  res.type('html').send(`<!DOCTYPE html><html lang="vi"><head><meta charset="utf-8"><title>Phòng chơi Cuộc Đời Việt</title></head>
<body style="font-family:sans-serif;max-width:640px;margin:40px auto;line-height:1.6">
<h1>🛖 Phòng chơi "Cuộc Đời Việt"</h1>
<p>Máy chủ phòng đang chạy. Tạo phòng trong game rồi chia sẻ mã 4 ký tự cho bạn bè cùng chơi, xem ai sống lâu nhất.</p>
<p>Phòng đang mở: <b>${rooms.size}</b> — <a href="/health">/health</a> — <a href="/test">trang test phòng</a></p>
</body></html>`);
});
app.get('/test', (req, res) => {
  res.sendFile(path.join(__dirname, 'test-client.html'));
});

server.listen(PORT, () => {
  console.log(`Phong Cuoc Doi Viet dang chay o cong ${PORT} (GAME_URL=${GAME_URL})`);
});
