const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);

// ─── Socket.IO — Robust connection settings ─────────────────────
// FIX #1: Proper ping/timeout config prevents random disconnects
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingTimeout: 60000,       // wait 60s for pong before declaring disconnect (was 5s default)
  pingInterval: 25000,      // send ping every 25s to keep connections alive
  upgradeTimeout: 30000,    // 30s to upgrade from polling → WebSocket
  connectTimeout: 45000,    // 45s connection timeout
  transports: ['websocket', 'polling'],
  allowUpgrades: true,
  maxHttpBufferSize: 1e6    // 1MB max message size
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ─── In-memory state ────────────────────────────────────────────
const rooms = {};             // roomId => Room object
const conductorTimers = {};   // roomId => grace-period timer
const participantTimers = {}; // socketId => removal timer

// ─── FIX #3: Unambiguous room ID (no 0/O, 1/I/L confusion) ─────
const ROOM_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // 32 safe chars
function generateRoomId() {
  let id = '';
  for (let i = 0; i < 6; i++) {
    id += ROOM_CHARS[Math.floor(Math.random() * ROOM_CHARS.length)];
  }
  return rooms[id] ? generateRoomId() : id; // retry on collision
}

function createRoom(conductorId, conductorName, maxBudget) {
  const roomId = generateRoomId();
  rooms[roomId] = {
    id: roomId,
    conductorId,
    conductorName,
    conductorDisconnected: false,
    maxBudget: parseFloat(maxBudget) || 0,
    participants: {},
    products: [],
    auction: null,
    auctionHistory: [],
    status: 'waiting',
    createdAt: Date.now()
  };
  return rooms[roomId];
}

// FIX #3b: Normalize room code — strip spaces, handle case
function getRoom(roomId) {
  if (!roomId) return null;
  const normalized = String(roomId).replace(/\s+/g, '').toUpperCase();
  return rooms[normalized] || null;
}

// ─── Socket.IO handlers ─────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[CONNECT] ${socket.id} | Active connections: ${io.engine.clientsCount}`);

  // ── Conductor creates a room ─────────────────────────────────
  socket.on('create_room', ({ conductorName, maxBudget }, cb) => {
    try {
      const room = createRoom(socket.id, conductorName, maxBudget);
      socket.join(room.id);
      socket.data.role = 'conductor';
      socket.data.roomId = room.id;
      socket.data.name = conductorName;
      console.log(`[ROOM CREATED] ${room.id} | Host: ${conductorName} | Budget: ₹${room.maxBudget || 'unlimited'}`);
      cb({ success: true, roomId: room.id, room: sanitizeRoom(room) });
    } catch (err) {
      console.error('[create_room error]', err);
      cb({ success: false, message: 'Server error creating room. Please try again.' });
    }
  });

  // ── FIX #2: Conductor rejoin after disconnect ────────────────
  socket.on('rejoin_conductor', ({ roomId, conductorName }, cb) => {
    try {
      const room = getRoom(roomId);
      if (!room) return cb({ success: false, message: 'Session no longer exists. Please create a new room.' });
      if (room.conductorName !== conductorName) return cb({ success: false, message: 'Conductor name does not match original session.' });

      // Cancel the grace-period destroy timer
      if (conductorTimers[room.id]) {
        clearTimeout(conductorTimers[room.id]);
        delete conductorTimers[room.id];
        console.log(`[CONDUCTOR REJOIN] ${room.id} — grace timer cancelled`);
      }

      room.conductorId = socket.id;
      room.conductorDisconnected = false;
      socket.join(room.id);
      socket.data.role = 'conductor';
      socket.data.roomId = room.id;
      socket.data.name = conductorName;

      console.log(`[CONDUCTOR REJOINED] ${conductorName} → room ${room.id}`);
      io.to(room.id).emit('conductor_reconnected', { message: 'Conductor reconnected successfully.' });
      cb({ success: true, room: sanitizeRoom(room) });
    } catch (err) {
      console.error('[rejoin_conductor error]', err);
      cb({ success: false, message: 'Server error during rejoin.' });
    }
  });

  // ── Participant joins a room ─────────────────────────────────
  socket.on('join_room', ({ roomId, participantName }, cb) => {
    try {
      const room = getRoom(roomId);
      if (!room) return cb({ success: false, message: 'Room not found. Please check the room code and try again.' });
      if (room.status === 'ended') return cb({ success: false, message: 'This auction session has ended.' });

      const cleanName = (participantName || '').trim();
      if (!cleanName) return cb({ success: false, message: 'Please enter your name.' });

      // Allow rejoin if same name is reconnecting
      const existing = Object.values(room.participants).find(
        p => p.name.toLowerCase() === cleanName.toLowerCase()
      );
      if (existing) {
        if (existing.connected) {
          return cb({ success: false, message: 'Name already taken in this room. Choose another name.' });
        }
        // Reconnecting — cancel removal timer and reuse slot
        if (participantTimers[existing.socketId]) {
          clearTimeout(participantTimers[existing.socketId]);
          delete participantTimers[existing.socketId];
        }
        delete room.participants[existing.socketId];
      }

      room.participants[socket.id] = {
        name: cleanName,
        socketId: socket.id,
        joinedAt: Date.now(),
        connected: true,
        spent: existing ? (existing.spent || 0) : 0,
        wonItems: existing ? (existing.wonItems || []) : []
      };

      socket.join(room.id);
      socket.data.role = 'participant';
      socket.data.roomId = room.id;
      socket.data.name = cleanName;

      const connectedCount = Object.values(room.participants).filter(p => p.connected).length;
      console.log(`[JOINED] ${cleanName} → room ${room.id} | Participants: ${connectedCount}`);

      io.to(room.id).emit('participant_joined', {
        participant: sanitizeParticipant(room.participants[socket.id], room.maxBudget),
        participants: sanitizeParticipants(room)
      });

      cb({ success: true, room: sanitizeRoom(room) });
    } catch (err) {
      console.error('[join_room error]', err);
      cb({ success: false, message: 'Server error. Please try again.' });
    }
  });

  // ── FIX #2: Participant rejoin after disconnect ───────────────
  socket.on('rejoin_participant', ({ roomId, participantName }, cb) => {
    try {
      const room = getRoom(roomId);
      if (!room) return cb({ success: false, message: 'Session no longer exists. Please rejoin the room.' });

      const cleanName = (participantName || '').trim();
      const existing = Object.values(room.participants).find(
        p => p.name.toLowerCase() === cleanName.toLowerCase()
      );

      if (existing) {
        if (participantTimers[existing.socketId]) {
          clearTimeout(participantTimers[existing.socketId]);
          delete participantTimers[existing.socketId];
        }
        delete room.participants[existing.socketId];
        room.participants[socket.id] = { ...existing, socketId: socket.id, connected: true };
      } else {
        // New slot (wasn't tracked before)
        room.participants[socket.id] = {
          name: cleanName, socketId: socket.id, joinedAt: Date.now(),
          connected: true, spent: 0, wonItems: []
        };
      }

      socket.join(room.id);
      socket.data.role = 'participant';
      socket.data.roomId = room.id;
      socket.data.name = cleanName;

      console.log(`[PARTICIPANT REJOINED] ${cleanName} → room ${room.id}`);
      io.to(room.id).emit('participant_joined', {
        participant: sanitizeParticipant(room.participants[socket.id], room.maxBudget),
        participants: sanitizeParticipants(room)
      });

      cb({ success: true, room: sanitizeRoom(room) });
    } catch (err) {
      console.error('[rejoin_participant error]', err);
      cb({ success: false, message: 'Server error during rejoin.' });
    }
  });

  // ── Add product to queue ─────────────────────────────────────
  socket.on('add_product', ({ name, description, startingBid, minIncrement }, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room) return cb({ success: false, message: 'Room not found.' });
      if (socket.id !== room.conductorId) return cb({ success: false, message: 'Only conductor can manage products.' });

      const product = {
        id: uuidv4(),
        name: (name || '').trim(),
        description: (description || '').trim(),
        startingBid: parseFloat(startingBid),
        minIncrement: parseFloat(minIncrement) || 1,
        addedAt: Date.now()
      };
      room.products.push(product);
      console.log(`[PRODUCT] Added "${product.name}" to room ${room.id}`);
      io.to(room.id).emit('products_updated', { products: room.products });
      cb({ success: true, product, products: room.products });
    } catch (err) {
      cb({ success: false, message: 'Server error.' });
    }
  });

  // ── Remove product ───────────────────────────────────────────
  socket.on('remove_product', ({ productId }, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room || socket.id !== room.conductorId) return cb({ success: false });
      room.products = room.products.filter(p => p.id !== productId);
      io.to(room.id).emit('products_updated', { products: room.products });
      cb({ success: true, products: room.products });
    } catch (err) { cb({ success: false }); }
  });

  // ── Reorder products ─────────────────────────────────────────
  socket.on('reorder_products', ({ productIds }, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room || socket.id !== room.conductorId) return cb({ success: false });
      const reordered = [];
      productIds.forEach(id => { const p = room.products.find(x => x.id === id); if (p) reordered.push(p); });
      room.products = reordered;
      io.to(room.id).emit('products_updated', { products: room.products });
      cb({ success: true });
    } catch (err) { cb({ success: false }); }
  });

  // ── Start auction ────────────────────────────────────────────
  socket.on('start_auction', ({ itemName, itemDescription, startingBid, minIncrement, fromQueueId }, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room) return cb({ success: false, message: 'Room not found.' });
      if (socket.id !== room.conductorId) return cb({ success: false, message: 'Only the conductor can start an auction.' });
      if (room.auction && room.auction.status === 'live') return cb({ success: false, message: 'An auction is already live.' });

      if (fromQueueId) {
        room.products = room.products.filter(p => p.id !== fromQueueId);
        io.to(room.id).emit('products_updated', { products: room.products });
      }

      const auction = {
        id: uuidv4(),
        itemName: (itemName || '').trim(),
        itemDescription: (itemDescription || '').trim(),
        startingBid: parseFloat(startingBid),
        minIncrement: parseFloat(minIncrement) || 1,
        currentBid: parseFloat(startingBid),
        currentWinner: null,
        bids: [],
        status: 'live',
        startedAt: Date.now(),
        endedAt: null
      };

      room.auction = auction;
      room.status = 'live';
      console.log(`[AUCTION START] "${auction.itemName}" @ ₹${auction.startingBid} | Room: ${room.id}`);

      io.to(room.id).emit('auction_started', { auction: sanitizeAuction(auction) });
      cb({ success: true, auction: sanitizeAuction(auction) });
    } catch (err) {
      console.error('[start_auction error]', err);
      cb({ success: false, message: 'Server error.' });
    }
  });

  // ── Place bid ────────────────────────────────────────────────
  socket.on('place_bid', ({ amount }, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room) return cb({ success: false, message: 'Room not found.' });
      if (!room.auction || room.auction.status !== 'live') return cb({ success: false, message: 'No active auction right now.' });

      const auction = room.auction;
      const bidAmount = parseFloat(amount);
      const participant = room.participants[socket.id];

      if (isNaN(bidAmount) || bidAmount <= 0) return cb({ success: false, message: 'Invalid bid amount.' });
      if (bidAmount <= auction.currentBid) {
        return cb({ success: false, message: `Bid must be higher than current bid ${formatRupee(auction.currentBid)}.` });
      }
      if (bidAmount < auction.currentBid + auction.minIncrement) {
        return cb({ success: false, message: `Min increment is ${formatRupee(auction.minIncrement)}. Bid at least ${formatRupee(auction.currentBid + auction.minIncrement)}.` });
      }

      // Budget check
      if (room.maxBudget > 0 && participant) {
        const remaining = room.maxBudget - (participant.spent || 0);
        if (bidAmount > remaining) {
          return cb({ success: false, message: `Bid exceeds your remaining budget of ${formatRupee(remaining)}.` });
        }
      }

      const bid = {
        id: uuidv4(),
        participantId: socket.id,
        participantName: socket.data.name,
        amount: bidAmount,
        timestamp: Date.now()
      };

      auction.bids.push(bid);
      auction.currentBid = bidAmount;
      auction.currentWinner = { name: socket.data.name, socketId: socket.id };

      console.log(`[BID] ${socket.data.name}: ₹${bidAmount} in room ${room.id}`);
      io.to(room.id).emit('bid_placed', { bid, auction: sanitizeAuction(auction) });
      cb({ success: true, bid, auction: sanitizeAuction(auction) });
    } catch (err) {
      console.error('[place_bid error]', err);
      cb({ success: false, message: 'Server error placing bid.' });
    }
  });

  // ── End auction ──────────────────────────────────────────────
  socket.on('end_auction', (_, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room) return cb({ success: false, message: 'Room not found.' });
      if (socket.id !== room.conductorId) return cb({ success: false, message: 'Only conductor can end auction.' });
      if (!room.auction || room.auction.status !== 'live') return cb({ success: false, message: 'No active auction to end.' });

      room.auction.status = 'ended';
      room.auction.endedAt = Date.now();
      room.status = 'waiting';
      room.auctionHistory.push({ ...room.auction });

      // Update winner's spending
      if (room.auction.currentWinner) {
        const ws = room.auction.currentWinner.socketId;
        if (room.participants[ws]) {
          room.participants[ws].spent = (room.participants[ws].spent || 0) + room.auction.currentBid;
          room.participants[ws].wonItems.push({ itemName: room.auction.itemName, amount: room.auction.currentBid });
        }
      }

      const result = sanitizeAuction(room.auction);
      const nextProduct = room.products.length > 0 ? room.products[0] : null;
      console.log(`[AUCTION END] "${room.auction.itemName}" → ${room.auction.currentWinner?.name || 'no winner'} @ ₹${room.auction.currentBid}`);

      io.to(room.id).emit('auction_ended', {
        auction: result,
        participants: sanitizeParticipants(room),
        nextProduct
      });

      cb({ success: true, auction: result });
      room.auction = null;
    } catch (err) {
      console.error('[end_auction error]', err);
      cb({ success: false, message: 'Server error.' });
    }
  });

  // ── Close room ───────────────────────────────────────────────
  socket.on('close_room', (_, cb) => {
    try {
      const room = getRoom(socket.data.roomId);
      if (!room || socket.id !== room.conductorId) return cb({ success: false });
      room.status = 'ended';
      io.to(room.id).emit('room_closed', { message: 'The auction session has been closed by the conductor.' });
      if (conductorTimers[room.id]) { clearTimeout(conductorTimers[room.id]); delete conductorTimers[room.id]; }
      // Clean up participant timers for this room
      Object.values(room.participants).forEach(p => {
        if (participantTimers[p.socketId]) { clearTimeout(participantTimers[p.socketId]); delete participantTimers[p.socketId]; }
      });
      delete rooms[room.id];
      cb({ success: true });
    } catch (err) { cb({ success: false }); }
  });

  // ── Get room state ───────────────────────────────────────────
  socket.on('get_room_state', (_, cb) => {
    const room = getRoom(socket.data.roomId);
    if (!room) return cb({ success: false });
    cb({ success: true, room: sanitizeRoom(room) });
  });

  // ── FIX #2: Disconnect — Grace periods instead of instant destroy
  socket.on('disconnect', (reason) => {
    const roomId = socket.data.roomId;
    const room = getRoom(roomId);
    console.log(`[DISCONNECT] ${socket.id} | reason: ${reason} | room: ${roomId || 'none'}`);
    if (!room) return;

    if (socket.data.role === 'conductor') {
      // DO NOT immediately destroy — give 30s for conductor to reconnect
      room.conductorDisconnected = true;
      room.conductorId = null; // block further conductor actions from stale socket

      console.log(`[CONDUCTOR DISCONNECT] Room ${roomId} — 30s grace period started`);
      io.to(roomId).emit('conductor_disconnected', {
        message: 'Conductor lost connection. Waiting to reconnect...',
        gracePeriodMs: 30000
      });

      conductorTimers[roomId] = setTimeout(() => {
        if (rooms[roomId] && rooms[roomId].conductorDisconnected) {
          console.log(`[GRACE EXPIRED] Room ${roomId} destroyed`);
          io.to(roomId).emit('room_closed', { message: 'Conductor failed to reconnect. Session ended.' });
          // Clean participant timers
          if (rooms[roomId]) {
            Object.values(rooms[roomId].participants).forEach(p => {
              if (participantTimers[p.socketId]) { clearTimeout(participantTimers[p.socketId]); delete participantTimers[p.socketId]; }
            });
          }
          delete rooms[roomId];
        }
        delete conductorTimers[roomId];
      }, 30000);

    } else if (socket.data.role === 'participant') {
      const name = socket.data.name;
      // Mark as disconnected but keep in room for 10s
      if (room.participants[socket.id]) {
        room.participants[socket.id].connected = false;
      }
      console.log(`[PARTICIPANT DISCONNECT] ${name} from ${roomId} — 10s grace`);

      participantTimers[socket.id] = setTimeout(() => {
        const r = rooms[roomId]; // re-fetch in case it changed
        if (r && r.participants[socket.id]) {
          delete r.participants[socket.id];
          io.to(roomId).emit('participant_left', {
            participantName: name,
            participants: sanitizeParticipants(r)
          });
          console.log(`[PARTICIPANT REMOVED] ${name} from ${roomId}`);
        }
        delete participantTimers[socket.id];
      }, 10000);
    }
  });
});

// ─── Helpers ────────────────────────────────────────────────────
function formatRupee(n) { return '₹' + Number(n).toLocaleString('en-IN'); }

function sanitizeAuction(a) {
  return {
    id: a.id, itemName: a.itemName, itemDescription: a.itemDescription,
    startingBid: a.startingBid, minIncrement: a.minIncrement,
    currentBid: a.currentBid, currentWinner: a.currentWinner,
    bids: a.bids, status: a.status, startedAt: a.startedAt, endedAt: a.endedAt
  };
}

function sanitizeParticipant(p, maxBudget) {
  return {
    name: p.name, socketId: p.socketId, joinedAt: p.joinedAt,
    connected: p.connected !== false,
    spent: p.spent || 0,
    remaining: maxBudget > 0 ? maxBudget - (p.spent || 0) : null,
    wonItems: p.wonItems || []
  };
}

function sanitizeParticipants(room) {
  return Object.values(room.participants).map(p => sanitizeParticipant(p, room.maxBudget));
}

function sanitizeRoom(room) {
  return {
    id: room.id, conductorName: room.conductorName, maxBudget: room.maxBudget,
    participants: sanitizeParticipants(room), products: room.products,
    auction: room.auction ? sanitizeAuction(room.auction) : null,
    auctionHistory: room.auctionHistory.map(sanitizeAuction),
    status: room.status, conductorDisconnected: room.conductorDisconnected || false
  };
}

// ─── Health check ────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    rooms: Object.keys(rooms).length,
    connections: io.engine.clientsCount,
    uptime: Math.floor(process.uptime()) + 's'
  });
});

// ─── Start server ────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`\n🔨 LiveBid Server — http://localhost:${PORT}`);
  console.log(`📊 Health: http://localhost:${PORT}/health\n`);
});
