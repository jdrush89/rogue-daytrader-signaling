const { WebSocketServer } = require("ws");
const http = require("http");

const port = process.env.PORT || 9000;

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Rogue Day Trader Signaling Server");
});

const wss = new WebSocketServer({ server });

// Room management
const rooms = new Map(); // roomCode -> Map<peerId, ws>
const peerRooms = new Map(); // ws -> { roomCode, peerId }

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      handleMessage(ws, msg);
    } catch (e) {
      console.error("Invalid message:", e.message);
    }
  });

  ws.on("close", () => {
    handleDisconnect(ws);
  });
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case "create_room": {
      const { roomCode, peerId } = msg;
      if (rooms.has(roomCode)) {
        ws.send(JSON.stringify({ type: "error", message: "Room already exists" }));
        return;
      }
      const room = new Map();
      room.set(peerId, ws);
      rooms.set(roomCode, room);
      peerRooms.set(ws, { roomCode, peerId });
      ws.send(JSON.stringify({ type: "room_created", roomCode }));
      console.log(`[Room ${roomCode}] Created by ${peerId}`);
      break;
    }
    case "join_room": {
      const { roomCode, peerId } = msg;
      const room = rooms.get(roomCode);
      if (!room) {
        ws.send(JSON.stringify({ type: "error", message: "Room not found" }));
        return;
      }
      room.set(peerId, ws);
      peerRooms.set(ws, { roomCode, peerId });
      // Notify all existing peers in room
      for (const [existingId, existingWs] of room) {
        if (existingId !== peerId) {
          existingWs.send(JSON.stringify({ type: "peer_joined", peerId }));
        }
      }
      ws.send(JSON.stringify({ type: "room_joined", roomCode, peers: Array.from(room.keys()).filter(id => id !== peerId) }));
      console.log(`[Room ${roomCode}] ${peerId} joined (${room.size} peers)`);
      break;
    }
    case "signal": {
      // Relay WebRTC signaling data to a specific peer
      const { targetPeerId, signalData, fromPeerId } = msg;
      const info = peerRooms.get(ws);
      if (!info) return;
      const room = rooms.get(info.roomCode);
      if (!room) return;
      const targetWs = room.get(targetPeerId);
      if (targetWs && targetWs.readyState === 1) {
        targetWs.send(JSON.stringify({ type: "signal", fromPeerId: fromPeerId || info.peerId, signalData }));
      }
      break;
    }
    case "broadcast": {
      // Broadcast a message to all peers in the room (except sender)
      const info = peerRooms.get(ws);
      if (!info) return;
      const room = rooms.get(info.roomCode);
      if (!room) return;
      for (const [id, peerWs] of room) {
        if (id !== info.peerId && peerWs.readyState === 1) {
          peerWs.send(JSON.stringify({ type: "broadcast", fromPeerId: info.peerId, data: msg.data }));
        }
      }
      break;
    }
    case "relay": {
      // Send game data to a specific peer
      const { targetPeerId, data } = msg;
      const info = peerRooms.get(ws);
      if (!info) return;
      const room = rooms.get(info.roomCode);
      if (!room) return;
      const targetWs = room.get(targetPeerId);
      if (targetWs && targetWs.readyState === 1) {
        targetWs.send(JSON.stringify({ type: "relay", fromPeerId: info.peerId, data }));
      }
      break;
    }
  }
}

function handleDisconnect(ws) {
  const info = peerRooms.get(ws);
  if (!info) return;
  const { roomCode, peerId } = info;
  const room = rooms.get(roomCode);
  if (room) {
    room.delete(peerId);
    // Notify remaining peers
    for (const [, peerWs] of room) {
      if (peerWs.readyState === 1) {
        peerWs.send(JSON.stringify({ type: "peer_left", peerId }));
      }
    }
    if (room.size === 0) {
      rooms.delete(roomCode);
      console.log(`[Room ${roomCode}] Deleted (empty)`);
    } else {
      console.log(`[Room ${roomCode}] ${peerId} left (${room.size} remaining)`);
    }
  }
  peerRooms.delete(ws);
}

// Heartbeat to detect dead connections
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(interval));

server.listen(port, () => {
  console.log(`Signaling server running on port ${port}`);
});
