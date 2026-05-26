const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

// ══════════════════════════════
// WORDS
// ══════════════════════════════
const WORDS = [
  'gol','chute','penalti','falta','escanteio','impedimento','lateral',
  'goleiro','zagueiro','meia','atacante','centroavante','volante','artilheiro',
  'camisa','chuteira','caneleira','luva','arbitro','bandeirinha',
  'driblar','cabecear','cruzar','cobrar','chutar','defender','finalizar',
  'trave','rede','gramado','arquibancada','torcida','estadio','campo',
  'copa','campeonato','liga','rodada','placar','resultado','tabela',
  'brasil','argentina','alemanha','espanha','portugal','franca','italia',
  'flamengo','corinthians','palmeiras','santos','gremio','cruzeiro',
  'bicicleta','chapeu','calcanhar','trivela','voleio','pressao',
  'rebaixamento','titulo','trofeu','recorde','campeao','selecionado',
  'dribling','offside','cartao','expulsao','marcacao','contra-ataque'
];

// ══════════════════════════════
// STATE
// ══════════════════════════════
// rooms: { [roomId]: Room }
// Room: {
//   id, code, isPrivate, name,
//   players: [{ id, name, score, misses }],
//   status: 'waiting' | 'playing' | 'finished',
//   goalLimit, currentWord, currentPlayerIdx,
//   currentTime, roundActive, prevWords,
//   timerTimeout
// }
const rooms = {};
const socketToRoom = {}; // socketId -> roomId

function generateCode() {
  return Math.random().toString(36).substring(2, 7).toUpperCase();
}

function pickWord(prevWords) {
  let w;
  do { w = WORDS[Math.floor(Math.random() * WORDS.length)]; }
  while (prevWords.includes(w));
  return w;
}

function normalize(str) {
  return str.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/-/g, '');
}

function publicRooms() {
  return Object.values(rooms)
    .filter(r => !r.isPrivate && r.status === 'waiting' && r.players.length < 2)
    .map(r => ({ id: r.id, name: r.name, host: r.players[0]?.name || '?', goalLimit: r.goalLimit }));
}

function broadcastLobby() {
  io.emit('lobby_update', publicRooms());
}

// ══════════════════════════════
// ROUND LOGIC (server-side)
// ══════════════════════════════
function startRound(room) {
  room.roundActive = true;
  const word = pickWord(room.prevWords);
  room.prevWords.push(word);
  if (room.prevWords.length > 5) room.prevWords.shift();
  room.currentWord = word;
  room.misses = [0, 0];

  const who = room.players[room.currentPlayerIdx];
  const timeMs = room.currentTime;

  io.to(room.id).emit('round_start', {
    word,
    currentPlayerId: who.id,
    currentPlayerName: who.name,
    timeMs,
    scores: room.players.map(p => p.score),
    playerIds: room.players.map(p => p.id),
  });

  // Server-side timeout
  clearTimeout(room.timerTimeout);
  room.timerTimeout = setTimeout(() => {
    if (!room.roundActive) return;
    room.roundActive = false;
    const loser = room.currentPlayerIdx;
    const winner = 1 - loser;
    scoreGoal(room, winner, 'tempo esgotado');
  }, timeMs + 300); // +300ms buffer for network
}

function scoreGoal(room, winnerIdx, reason) {
  room.players[winnerIdx].score++;
  const winner = room.players[winnerIdx];
  const scores = room.players.map(p => p.score);

  io.to(room.id).emit('goal', {
    scorerId: winner.id,
    scorerName: winner.name,
    reason,
    scores,
    playerIds: room.players.map(p => p.id),
  });

  if (winner.score >= room.goalLimit) {
    room.status = 'finished';
    setTimeout(() => {
      io.to(room.id).emit('champion', {
        winnerId: winner.id,
        winnerName: winner.name,
        scores,
      });
      // Clean up room after a delay
      setTimeout(() => deleteRoom(room.id), 30000);
    }, 2500);
  } else {
    // Next round after goal animation
    setTimeout(() => {
      if (room.status !== 'playing') return;
      room.currentPlayerIdx = Math.random() < 0.5 ? 0 : 1;
      room.currentTime = 5000; // reset per goal
      startRound(room);
    }, 4000);
  }
}

function deleteRoom(roomId) {
  const room = rooms[roomId];
  if (!room) return;
  room.players.forEach(p => { delete socketToRoom[p.id]; });
  delete rooms[roomId];
  broadcastLobby();
}

// ══════════════════════════════
// SOCKET EVENTS
// ══════════════════════════════
io.on('connection', (socket) => {
  console.log('connect', socket.id);

  // Send current lobby
  socket.emit('lobby_update', publicRooms());

  // ── CREATE ROOM ──
  socket.on('create_room', ({ playerName, isPrivate, roomName, goalLimit }) => {
    const roomId = generateCode() + Date.now().toString(36);
    const code = generateCode();
    const room = {
      id: roomId,
      code,
      isPrivate: !!isPrivate,
      name: roomName || `Sala de ${playerName}`,
      goalLimit: goalLimit || 3,
      players: [{ id: socket.id, name: playerName, score: 0 }],
      status: 'waiting',
      currentWord: '',
      currentPlayerIdx: 0,
      currentTime: 5000,
      roundActive: false,
      prevWords: [],
      misses: [0, 0],
      timerTimeout: null,
    };
    rooms[roomId] = room;
    socketToRoom[socket.id] = roomId;
    socket.join(roomId);
    socket.emit('room_created', { roomId, code, isPrivate: room.isPrivate, roomName: room.name });
    broadcastLobby();
  });

  // ── JOIN BY CODE ──
  socket.on('join_by_code', ({ playerName, code }) => {
    const room = Object.values(rooms).find(r => r.code === code.toUpperCase());
    if (!room) { socket.emit('error_msg', 'Código inválido ou sala não encontrada.'); return; }
    if (room.players.length >= 2) { socket.emit('error_msg', 'Sala cheia!'); return; }
    if (room.status !== 'waiting') { socket.emit('error_msg', 'Partida já em andamento.'); return; }
    joinRoom(socket, room, playerName);
  });

  // ── JOIN PUBLIC ──
  socket.on('join_public', ({ playerName, roomId }) => {
    const room = rooms[roomId];
    if (!room) { socket.emit('error_msg', 'Sala não encontrada.'); return; }
    if (room.players.length >= 2) { socket.emit('error_msg', 'Sala cheia!'); return; }
    if (room.status !== 'waiting') { socket.emit('error_msg', 'Partida já em andamento.'); return; }
    joinRoom(socket, room, playerName);
  });

  function joinRoom(socket, room, playerName) {
    room.players.push({ id: socket.id, name: playerName, score: 0 });
    socketToRoom[socket.id] = room.id;
    socket.join(room.id);
    // Both players joined — start!
    room.status = 'playing';
    io.to(room.id).emit('match_start', {
      roomId: room.id,
      players: room.players.map(p => ({ id: p.id, name: p.name })),
      goalLimit: room.goalLimit,
    });
    broadcastLobby();
    setTimeout(() => startRound(room), 1000);
  }

  // ── WORD SUBMIT ──
  socket.on('submit_word', ({ word }) => {
    const roomId = socketToRoom[socket.id];
    const room = rooms[roomId];
    if (!room || !room.roundActive) return;
    const currentPlayer = room.players[room.currentPlayerIdx];
    if (currentPlayer.id !== socket.id) return; // not your turn

    if (normalize(word) === normalize(room.currentWord)) {
      // Correct!
      clearTimeout(room.timerTimeout);
      room.roundActive = false;

      // Adjust time based on... we track on client; server just advances
      const newTime = Math.max(1200, room.currentTime - 350);
      room.currentTime = newTime;

      io.to(room.id).emit('word_correct', {
        playerId: socket.id,
        newTime,
      });

      setTimeout(() => {
        if (room.status !== 'playing') return;
        room.currentPlayerIdx = 1 - room.currentPlayerIdx;
        startRound(room);
      }, 800);

    } else {
      // Wrong
      room.misses[room.currentPlayerIdx] = (room.misses[room.currentPlayerIdx] || 0) + 1;
      io.to(room.id).emit('word_wrong', {
        playerId: socket.id,
        misses: room.misses[room.currentPlayerIdx],
      });

      if (room.misses[room.currentPlayerIdx] >= 3) {
        clearTimeout(room.timerTimeout);
        room.roundActive = false;
        const winner = 1 - room.currentPlayerIdx;
        scoreGoal(room, winner, 'erros demais');
      }
    }
  });

  // ── DISCONNECT ──
  socket.on('disconnect', () => {
    console.log('disconnect', socket.id);
    const roomId = socketToRoom[socket.id];
    if (!roomId) return;
    const room = rooms[roomId];
    if (!room) return;
    clearTimeout(room.timerTimeout);
    room.roundActive = false;
    io.to(roomId).emit('player_left', { name: room.players.find(p => p.id === socket.id)?.name || 'Jogador' });
    deleteRoom(roomId);
  });
});

// ── HEALTH CHECK ──
app.get('/', (req, res) => res.json({ status: 'ok', rooms: Object.keys(rooms).length }));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`🚀 Servidor rodando na porta ${PORT}`));
