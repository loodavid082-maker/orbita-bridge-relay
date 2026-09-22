const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 8080);
const RELAY_TOKEN = process.env.REAY_TOKEN || process.env.RELAY_TOKEN;
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

const digestSchema={type:"string",pattern:"^[0-9a-fA-F]{64}$"};
const idSchema={type:"string",minLength:1,maxLength:200};

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
    },
    {
      name:"orbita_approve",
      description:"Records approval for an existing proposal through the Local Bridge. Approval does not grant execution authorization and does not execute a plan.",
      inputSchema:{type:"object",properties:{proposal_id:idSchema},required:["proposal_id"],additionalProperties:false},
      annotations:{readOnlyHint:false,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_execution_request",
      description:"Controlled execution gate request. Binds an already-approved proposal to an exact SHA-256 plan digest. Does not authorize or execute it.",
      inputSchema:{type:"object",properties:{proposal_id:idSchema,plan_digest:digestSchema},required:["proposal_id","plan_digest"],additionalProperties:false},
      annotations:{readOnlyHint:false,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_execution_authorize",
      description:"Controlled authorization step for an existing execution request bound to the exact proposal and plan digest. Does not itself execute a plan.",
      inputSchema:{type:"object",properties:{proposal_id:idSchema,plan_digest:digestSchema},required:["proposal_id","plan_digest"],additionalProperties:false},
      annotations:{readOnlyHint:false,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_execution_plan",
      description:"Creates a governed execution manifest through the Local Bridge. Allowed operations remain enforced by the Local Bridge; Relay grants no filesystem, shell, database, Git, or deployment authority.",
      inputSchema:{type:"object",properties:{
        proposal_id:idSchema,
        plan_digest:digestSchema,
        operations:{type:"array",items:{type:"string"},minItems:1},
        files:{type:"array",items:{type:"string"}},
        tests:{type:"array",items:{type:"string"}},
        write_contents:{type:"object"},
        expected_hashes:{type:"object"},
        rollback_enabled:{type:"boolean"},
        search_text:{type:"string"}
      },required:["proposal_id","plan_digest","operations"],additionalProperties:false},
      annotations:{readOnlyHint:false,destructiveHint:false,idempotentHint:false,openWorldHint:false}
    },
    {
      name:"orbita_execution_run",
      description:"Runs one already-created governed manifest by manifest_id. Relay accepts no command, shell, executable, path, host, port, or deployment target.",
      inputSchema:{type:"object",properties:{manifest_id:idSchema},required:["manifest_id"],additionalProperties:false},
      annotations:{readOnlyHint:false,destructiveHint:true,idempotentHint:false,openWorldHint:false}
    },
    {
      name:"orbita_verify",
      description:"Read-only. Returns verification evidence for one existing governed manifest by manifest_id. Relay accepts no path, URL, host, port, command, shell, or execution target.",
      inputSchema:{type:"object",properties:{manifest_id:idSchema},required:["manifest_id"],additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    },
    {
      name:"orbita_audit",
      description:"Read-only. Returns audit evidence for one existing governed manifest by manifest_id. Relay accepts no path, URL, host, port, command, shell, or execution target.",
      inputSchema:{type:"object",properties:{manifest_id:idSchema},required:["manifest_id"],additionalProperties:false},
      annotations:{readOnlyHint:true,destructiveHint:false,idempotentHint:true,openWorldHint:false}
    }
  ];
}

function sendBridgeRequest(action, payload={}) {
  const first = bridgeSockets.entries().next();
  if (first.done) return Promise.resolve({ok:false,code:"BRIDGE_OFFLINE"});
  const [bridgeId, ws] = first.value;
  const requestId = crypto.randomUUID();
  return new Promise(resolve => {
    const timer = setTimeout(() => {
      pending.delete(requestId);
      resolve({ok:false,code:"BRIDGE_TIMEOUT",bridge_id:bridgeId,request_id:requestId});
    }, 35000);
    pending.set(requestId,{resolve,timer,bridgeId});
    ws.send(JSON.stringify({type:"relay.request",request_id:requestId,action,payload}));
  });
}

async function handleMcp(body) {
  const id = body && Object.prototype.hasOwnProperty.call(body,"id") ? body.id : null;
  const method = body && body.method;
  if (method === "initialize") {
    return rpcResult(id,{
      protocolVersion:(body.params && body.params.protocolVersion) || "2025-06-18",
      capabilities:{tools:{listChanged:false}},
      serverInfo:{name:"orbita-bridge",version:"1.1.0"},
      instructions:"PRO-FIRST / BUSINESS-READY. Observe, diagnose, and propose are read-only. Controlled execution requires approved proposal, exact SHA-256 plan digest, explicit authorization, manifest, verification, and audit. NO APPROVAL = NO CODE CHANGE. Relay never executes arbitrary shell or commands and grants no direct filesystem, database, Git, or deployment authority."
    });
  }
  if (method === "notifications/initialized") return null;
  if (method === "ping") return rpcResult(id,{});
  if (method === "tools/list") return rpcResult(id,{tools:mcpTools()});
  if (method === "tools/call") {
    const name = body.params && body.params.name;
    const args = body.params && body.params.arguments && typeof body.params.arguments === "object"
      ? body.params.arguments : {};
    let result;
    if (name === "orbita_bridge_status") {
      result={ok:true,connected:bridgeSockets.size>0,bridge_ids:[...bridgeSockets.keys()]};
    } else if (name === "orbita_observe") {
      result=await sendBridgeRequest("observe",{});
    } else if (name === "orbita_diagnose") {
      result=await sendBridgeRequest("diagnose",{});
    } else if (name === "orbita_propose") {
      result=await sendBridgeRequest("propose",{});
    } else if (name === "orbita_approve") {
      result=await sendBridgeRequest("approval",args);
    } else if (name === "orbita_execution_request") {
      result=await sendBridgeRequest("execution.request",args);
    } else if (name === "orbita_execution_authorize") {
      result=await sendBridgeRequest("execution.authorize",args);
    } else if (name === "orbita_execution_plan") {
      result=await sendBridgeRequest("execution.plan",args);
    } else if (name === "orbita_execution_run") {
      result=await sendBridgeRequest("execution.run",args);
    } else if (name === "orbita_verify") {
      result=await sendBridgeRequest("execution.verify",args);
    } else if (name === "orbita_audit") {
      result=await sendBridgeRequest("execution.audit",args);
    } else {
      return {jsonrpc:"2.0",id,error:{code:-32601,message:"Unknown tool"}};
    }
    return rpcResult(id,{content:[{type:"text",text:JSON.stringify(result)}],structuredContent:result,isError:result.ok===false});
  }
  return {jsonrpc:"2.0",id,error:{code:-32601,message:"Method not found"}};
}

const server = http.createServer(async (req,res) => {
  const path = new URL(req.url,"http://relay.local").pathname;
  if (path === "/health") return json(res,200,{ok:true,service:"orbita-bridge-relay",version:"1.1.0",mcp:true});
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

server.listen(PORT,"0.0.0.0",()=>console.log("ORBITA BRIDGE RELAY V1.1 listening on "+PORT));
