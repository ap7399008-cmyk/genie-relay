import { createServer } from "http";
import { Server } from "socket.io";

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  pingTimeout: 120000,
  pingInterval: 25000,
});

const pairCodeToRoom = new Map();
const rooms = new Map();

function getRoom(roomId) {
  let r = rooms.get(roomId);
  if (!r) {
    r = { parent: null, child: null, lastLocation: null, gallery: [], notifications: [], commandQueue: [], childLastSeen: 0, lastBattery: null };
    rooms.set(roomId, r);
  }
  return r;
}

function generatePairCode() {
  const chars = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
  let code;
  do {
    code = "";
    for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  } while (pairCodeToRoom.has(code));
  return code;
}

setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms.entries()) {
    if (!room.parent && !room.child && now - room.childLastSeen > 3600000) {
      rooms.delete(roomId);
      for (const [code, rid] of pairCodeToRoom.entries()) {
        if (rid === roomId) { pairCodeToRoom.delete(code); break; }
      }
    }
  }
}, 300000);

io.on("connection", (socket) => {
  console.log(`[relay] connected ${socket.id}`);

  socket.on("parent:create", () => {
    const code = generatePairCode();
    const roomId = `room_${code}`;
    pairCodeToRoom.set(code, roomId);
    const room = getRoom(roomId);
    room.parent = socket.id;
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = "parent";
    socket.emit("parent:created", { code, roomId });
    console.log(`[relay] parent created room ${roomId} code ${code}`);
  });

  socket.on("child:join", ({ code }) => {
    if (!code) return socket.emit("child:join-error", { error: "No code" });
    const roomId = pairCodeToRoom.get(code.toUpperCase());
    if (!roomId) return socket.emit("child:join-error", { error: "Invalid code" });
    const room = getRoom(roomId);
    if (room.child && room.child !== socket.id) return socket.emit("child:join-error", { error: "Already paired" });
    room.child = socket.id;
    room.childLastSeen = Date.now();
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = "child";
    socket.to(roomId).emit("child:connected", { childId: socket.id });
    socket.emit("child:joined", { roomId });
    if (room.commandQueue.length > 0) {
      socket.emit("commands:replay", { commands: room.commandQueue });
      room.commandQueue = [];
    }
    console.log(`[relay] child joined room ${roomId}`);
  });

  socket.on("parent:sync", () => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = getRoom(roomId);
    socket.emit("parent:state", {
      childOnline: room.child !== null,
      childLastSeen: room.childLastSeen,
      lastLocation: room.lastLocation,
      gallery: room.gallery.slice(0, 50),
      notifications: room.notifications.slice(0, 50),
      pendingCommands: room.commandQueue.length,
      lastBattery: room.lastBattery,
    });
  });

  socket.on("parent:command", ({ type, payload }) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = getRoom(roomId);
    const cmd = { id: `${Date.now()}_${Math.random().toString(36).slice(2,8)}`, type, payload, ts: Date.now() };
    if (room.child) {
      io.to(room.child).emit("command", cmd);
    } else {
      room.commandQueue.push(cmd);
      socket.emit("command:queued", { pending: room.commandQueue.length });
    }
  });

  socket.on("data", ({ type, payload }) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = getRoom(roomId);
    if (type === "location") room.lastLocation = { ...payload, ts: Date.now() };
    else if (type === "gallery") { room.gallery.unshift({ ...payload, ts: Date.now() }); if (room.gallery.length > 50) room.gallery = room.gallery.slice(0, 50); }
    else if (type === "notification") { room.notifications.unshift({ ...payload, ts: Date.now() }); if (room.notifications.length > 50) room.notifications = room.notifications.slice(0, 50); }
    else if (type === "battery") room.lastBattery = { ...payload, ts: Date.now() };
    socket.to(roomId).emit("data", { type, payload, from: socket.id });
  });

  socket.on("binary", (data) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    socket.to(roomId).emit("binary", data);
  });

  socket.on("webrtc:offer", ({ to, sdp }) => { if (to) io.to(to).emit("webrtc:offer", { from: socket.id, sdp }); });
  socket.on("webrtc:answer", ({ to, sdp }) => { if (to) io.to(to).emit("webrtc:answer", { from: socket.id, sdp }); });
  socket.on("webrtc:ice", ({ to, candidate }) => { if (to) io.to(to).emit("webrtc:ice", { from: socket.id, candidate }); });

  socket.on("media:request", ({ to, kinds }) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = getRoom(roomId);
    if (room.child) io.to(room.child).emit("media:request", { from: socket.id, kinds });
    else room.commandQueue.push({ id: `${Date.now()}_media`, type: "media:request", payload: { kinds }, ts: Date.now() });
  });

  socket.on("child:heartbeat", () => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    getRoom(roomId).childLastSeen = Date.now();
  });

  socket.on("family:unpair", () => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    socket.to(roomId).emit("family:unpaired", {});
    const room = getRoom(roomId);
    if (socket.data.role === "parent") {
      rooms.delete(roomId);
      for (const [code, rid] of pairCodeToRoom.entries()) { if (rid === roomId) { pairCodeToRoom.delete(code); break; } }
    } else { room.child = null; }
  });

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    if (room.parent === socket.id) room.parent = null;
    if (room.child === socket.id) { room.child = null; room.childLastSeen = Date.now(); }
    socket.to(roomId).emit("presence", { role: socket.data.role, online: false, ts: Date.now() });
    console.log(`[relay] disconnected ${socket.id}`);
  });
});

httpServer.on("request", (req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", service: "GENIE Relay Server", rooms: rooms.size, uptime: process.uptime() }));
  } else { res.writeHead(404); res.end("Not found"); }
});

const PORT = process.env.PORT || 8080;
httpServer.listen(PORT, () => console.log(`[relay] GENIE Relay Server on port ${PORT}`));

process.on("SIGTERM", () => { io.close(); httpServer.close(() => process.exit(0)); });
process.on("SIGINT", () => { io.close(); httpServer.close(() => process.exit(0)); });
