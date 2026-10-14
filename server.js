// 0 1.2 SOJOL Socket.IO backend
// IMPORTANT: Socket.IO requires a persistent Node.js host (e.g. Render/Railway).
// Vercel Functions do not provide a reliable persistent Socket.IO server.
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "*";

app.use(express.json());
app.get("/", (_req, res) => res.json({ ok: true, service: "SOJOL Socket.IO backend", status: "running" }));
app.get("/health", (_req, res) => res.json({ ok: true }));

const io = new Server(server, {
  cors: { origin: FRONTEND_ORIGIN === "*" ? "*" : FRONTEND_ORIGIN.split(",").map(s => s.trim()), methods: ["GET", "POST"] }
});

// Temporary in-memory pairing. Restarting server clears codes/sessions.
const codes = new Map();   // code -> { room, expiresAt, used }
const peers = new Map();   // room -> { laptopId, mobileId }

function newCode() {
  let code;
  do { code = String(crypto.randomInt(0, 1000000)).padStart(6, "0"); } while (codes.has(code));
  return code;
}
function cleanup() {
  const now = Date.now();
  for (const [code, item] of codes) if (item.expiresAt < now || item.used) codes.delete(code);
}
setInterval(cleanup, 30_000).unref();

io.on("connection", socket => {
  socket.emit("backend:ready", { ok: true, message: "Connected to SOJOL backend" });

  socket.on("pair:create", (ack = () => {}) => {
    const code = newCode();
    const room = crypto.randomUUID();
    codes.set(code, { room, expiresAt: Date.now() + 5 * 60_000, used: false });
    peers.set(room, { laptopId: socket.id, mobileId: null });
    socket.join(room);
    socket.data.role = "laptop";
    socket.data.room = room;
    ack({ ok: true, code, expiresInSeconds: 300 });
    io.to(room).emit("pair:status", { connected: false, message: "Waiting for mobile device" });
  });

  socket.on("pair:join", ({ code } = {}, ack = () => {}) => {
    const normalized = String(code || "").trim();
    const entry = codes.get(normalized);
    if (!/^\d{6}$/.test(normalized) || !entry || entry.used || entry.expiresAt < Date.now()) {
      return ack({ ok: false, error: "Code invalid or expired. Generate a new code on laptop." });
    }
    entry.used = true;
    socket.join(entry.room);
    socket.data.role = "mobile";
    socket.data.room = entry.room;
    const p = peers.get(entry.room) || {};
    p.mobileId = socket.id;
    peers.set(entry.room, p);
    ack({ ok: true, room: entry.room, status: "Connected" });
    io.to(entry.room).emit("pair:status", { connected: true, message: "Connected" });
    io.to(p.laptopId).emit("pair:mobile-joined", { connected: true });
  });

  // WebRTC signaling only. Actual video flows peer-to-peer, not through this server.
  for (const eventName of ["webrtc:offer", "webrtc:answer", "webrtc:ice"]) {
    socket.on(eventName, payload => {
      const room = socket.data.room;
      if (!room || !peers.has(room)) return;
      socket.to(room).emit(eventName, payload);
    });
  }

  socket.on("disconnect", () => {
    const room = socket.data.room;
    if (!room) return;
    const p = peers.get(room);
    if (p && (p.laptopId === socket.id || p.mobileId === socket.id)) {
      io.to(room).emit("pair:status", { connected: false, message: "Disconnected" });
      if (p.laptopId === socket.id && p.mobileId) io.to(p.mobileId).emit("peer:disconnected");
      if (p.mobileId === socket.id && p.laptopId) io.to(p.laptopId).emit("peer:disconnected");
    }
  });
});

server.listen(PORT, () => console.log(`SOJOL Socket.IO backend listening on ${PORT}`));
