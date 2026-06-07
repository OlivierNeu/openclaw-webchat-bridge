// DEV PROBE: agents.files.* + artifacts.* shape capture (the remote-fetch
// mechanism for agent-produced files). Run with the bridge STOPPED.
//   node --env-file=.env dev-probe-files.mjs
import { loadConfig } from "./dist/config.js";
import { OpenClawConnection } from "./dist/providers/openclaw/openclaw-client.js";
import { buildSessionKey } from "./dist/providers/openclaw/session-keys.js";

const cfg = loadConfig();
const conn = await OpenClawConnection.connect(cfg.openclawGatewayUrl, cfg.openclawToken, cfg.deviceIdentity);
const agentId = cfg.agentId;
const canonical = cfg.canonical;
const knownPath = "/home/node/.openclaw/media/outbound/fruits---f998f47f-6ef5-4fbd-9e38-38468e06f10d.md";
const knownName = "fruits---f998f47f-6ef5-4fbd-9e38-38468e06f10d.md";

async function tryRpc(method, params) {
  try {
    const res = await conn.request(method, params);
    const body = res?.payload ?? res?.result ?? res;
    const redacted = JSON.stringify(body, (k, v) => (typeof v === "string" && v.length > 160 ? `<str ${v.length}>` : v), 2);
    console.log(`\n### ${method}(${JSON.stringify(params)}) OK`);
    console.log(redacted.slice(0, 1800));
    return body;
  } catch (e) {
    console.log(`\n### ${method}(${JSON.stringify(params)}) ERR: ${e?.message ?? e}`);
    return null;
  }
}

console.log("agentId:", agentId, "canonical:", canonical);
await tryRpc("agents.files.list", { agentId });
await tryRpc("agents.files.list", { agentId, dir: "media/outbound" });
await tryRpc("agents.files.list", { agentId, path: "media/outbound" });
await tryRpc("agents.files.get", { agentId, path: knownPath });
await tryRpc("agents.files.get", { agentId, name: knownName });
await tryRpc("agents.files.get", { agentId, path: "media/outbound/" + knownName });
await tryRpc("artifacts.list", { agentId });
conn.close();
process.exit(0);
