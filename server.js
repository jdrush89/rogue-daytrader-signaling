const { PeerServer } = require("peer");

const port = process.env.PORT || 9000;

const server = PeerServer({
  port,
  path: "/",
  allow_discovery: false,
  corsOptions: {
    origin: "*",
  },
});

server.on("connection", (client) => {
  console.log(`[PeerJS] Client connected: ${client.getId()}`);
});

server.on("disconnect", (client) => {
  console.log(`[PeerJS] Client disconnected: ${client.getId()}`);
});

console.log(`PeerJS server running on port ${port}`);
