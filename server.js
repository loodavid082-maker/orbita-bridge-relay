const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 8080);
const RELAY_TOKEN = process.env.RELAY_TOKEN;
if (!RELAY_TOKEN) throw new Error("RELAY_TOKEN is required");

const bridgeSockets = new Map();

function authorized(req) {
  const value = req.headers.authorization || "";
  return value === "Bearer " + RELAY_TOKEN;
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {"content-type":"application/json"});
    return res.end(JSON.stringify({ok:true, service:"orbita-bridge-relay", version:"0.1.0"}));
  }
  if (req.url === "/bridge/status") {
    if (!authorized(req)) {
      res.writeHead(401); return res.end("Unauthorized");
    }
    res.writeHead(200, {"content-type":"application/json"});
    return res.end(JSON.stringify({
      ok:true,
      connected: bridgeSockets.size > 0,
      bridge_ids: [...bridgeSockets.keys()]
    }));
  }
  res.writeHead(404); res.end("Not Found");
});

const wss = new WebSocketServer({server, path:"/ws/bridge"});

wss.on("connection", (ws, req) => {
  if (!authorized(req)) return ws.close(1008, "Unauthorized");

  const bridgeId = crypto.randomUUID();
  bridgeSockets.set(bridgeId, ws);

  ws.send(JSON.stringify({type:"relay.hello", bridge_id:bridgeId}));

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    // Relay never executes commands. It only transports framed messages.
    if (msg.type === "bridge.event") {
      // Placeholder for the authenticated ChatGPT/MCP ingress.
      // No public command-to-bridge path is exposed in v0.1.
    }
  });

  ws.on("close", () => bridgeSockets.delete(bridgeId));
  ws.on("error", () => bridgeSockets.delete(bridgeId));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("ORBİTA BRIDGE RELAY listening on " + PORT);
});
