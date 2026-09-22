const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 8080);
const RELAY_TOKEN = process.env.RELAY_TOKEN;
if (!RELAY_TOKEN) throw new Error("RELAY_TOKEN is required");

const bridgeSockets = new Map();
const pending = new Map();

function authorized(req) {
  const value = req.headers.authorization || "";
  return value === "Bearer " + RELAY_TOKEN;
}

function json(res, status, body) {
  res.writeHead(status, {"content-type":"application/json"});
  res.end(JSON.stringify(body));
}

function rpcResult(id, result) {
  return {jsonrpc:"2.0", id, result};
}

function mcpTools() {
  return [
    {
      name:"orbita_bridge_status",
      description:"Read-only. Returns whether an Orbita Localhost Bridge is currently connected to the relay.",
      inputSchema:{type:"object",properties:{},additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_observe",
      description:"Read-only control request. Ask the connected Orbita Bridge to observe its fixed localhost Orbita target. Never executes shell, writes files, changes DB, deploys, or mutates Git.",
      inputSchema:{type:"object",properties:{},additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_diagnose",
      description:"Read-only control request. Ask the connected Orbita Bridge for diagnosis evidence. No code mutation.",
      inputSchema:{type:"object",properties:{},additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_propose",
      description:"Read-only control request. Ask the connected Orbita Bridge to prepare a proposal. Proposal does not authorize execution.",
      inputSchema:{type:"object",properties:{},additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    }
  ];
}

function sendBridgeRequest(action, args={}) {
  const first = bridgeSockets.entries().next();
  if (first.done) return Promise.resolve({ok:false,code:"BRIDGE_OFFLINE"});
  const [bridgeId, ws] = first.value;
  const requestId = crypto.randomUUID();
  return new Promise(resolve => {
    const timer = setTimeout(() => {
      pending.delete(requestId);
      resolve({ok:false,code:"BRIDGE_TIMEOUT",bridge_id:bridgeId,request_id:requestId});
    }, 10000);
    pending.set(requestId,{resolve,timer,bridgeId});
    ws.send(JSON.stringify({type:"relay.request",request_id:requestId,action,args}));
  });
}

async function handleMcp(body) {
  const id = body && Object.prototype.hasOwnProperty.call(body,"id") ? body.id : null;
  const method = body && body.method;
  if (method === "initialize") {
    return rpcResult(id,{
      protocolVersion:(body.params && body.params.protocolVersion) || "2025-06-18",
      capabilities:{tools:{listChanged:false}},
      serverInfo:{name:"orbita-bridge",version:"1.0.0"},
      instructions:"PRO-FIRST / BUSINESS-READY. Observe, diagnose, and propose are read-only. NO APPROVAL = NO CODE CHANGE. Relay never executes shell, writes files, changes databases, deploys, or mutates Git."
    });
  }
  if (method === "notifications/initialized") return null;
  if (method === "ping") return rpcResult(id,{});
  if (method === "tools/list") return rpcResult(id,{tools:mcpTools()});
  if (method === "tools/call") {
    const name = body.params && body.params.name;
    let result;
    if (name === "orbita_bridge_status") {
      result={ok:true,connected:bridgeSockets.size>0,bridge_ids:[...bridgeSockets.keys()]};
    } else if (name === "orbita_observe") {
      result=await sendBridgeRequest("observe",{});
    } else if (name === "orbita_diagnose") {
      result=await sendBridgeRequest("diagnose",{});
    } else if (name === "orbita_propose") {
      result=await sendBridgeRequest("propose",{});
    } else {
      return {jsonrpc:"2.0",id,error:{code:-32601,message:"Unknown tool"}};
    }
    return rpcResult(id,{content:[{type:"text",text:JSON.stringify(result)}],structuredContent:result,isError:result.ok===false});
  }
  return {jsonrpc:"2.0",id,error:{code:-32601,message:"Method not found"}};
}

const server = http.createServer(async (req,res) => {
  const path = new URL(req.url,"http://relay.local").pathname;
  if (path === "/health") return json(res,200,{ok:true,service:"orbita-bridge-relay",version:"1.0.0",mcp:true});
  if (path === "/bridge/status") {
    if (!authorized(req)) return json(res,401,{ok:false,error:"Unauthorized"});
    return json(res,200,{ok:true,connected:bridgeSockets.size>0,bridge_ids:[...bridgeSockets.keys()]});
  }
  if (path === "/mcp") {
    if (req.method !== "POST") return json(res,405,{error:"Method Not Allowed"});
    if (!authorized(req)) return json(res,401,{error:"Unauthorized"});
    let raw="";
    req.on("data",chunk => { raw += chunk; if (raw.length > 1024*1024) req.destroy(); });
    req.on("end",async () => {
      let body;
      try { body=JSON.parse(raw || "{}"); } catch { return json(res,400,{error:"Invalid JSON"}); }
      try {
        const out=await handleMcp(body);
        if (out === null) { res.writeHead(202); return res.end(); }
        return json(res,200,out);
      } catch (e) {
        return json(res,500,{jsonrpc:"2.0",id:body && body.id,error:{code:-32603,message:"Internal error"}});
      }
    });
    return;
  }
  return json(res,404,{error:"Not Found"});
});

const wss = new WebSocketServer({server,path:"/ws/bridge"});
wss.on("connection",(ws,req) => {
  if (!authorized(req)) return ws.close(1008,"Unauthorized");
  const bridgeId=crypto.randomUUID();
  bridgeSockets.set(bridgeId,ws);
  ws.send(JSON.stringify({type:"relay.hello",bridge_id:bridgeId}));
  ws.on("message",raw => {
    let msg; try { msg=JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "relay.response" && msg.request_id) {
      const p=pending.get(msg.request_id);
      if (!p) return;
      clearTimeout(p.timer); pending.delete(msg.request_id);
      p.resolve({ok:msg.ok!==false,bridge_id:p.bridgeId,request_id:msg.request_id,result:msg.result,error:msg.error});
    }
  });
  const cleanup=() => {
    bridgeSockets.delete(bridgeId);
    for (const [id,p] of pending) if (p.bridgeId===bridgeId) {
      clearTimeout(p.timer); pending.delete(id); p.resolve({ok:false,code:"BRIDGE_DISCONNECTED",bridge_id:bridgeId,request_id:id});
    }
  };
  ws.on("close",cleanup); ws.on("error",cleanup);
});

server.listen(PORT,"0.0.0.0",()=>console.log("ORBITA BRIDGE RELAY V1.0 listening on "+PORT));
