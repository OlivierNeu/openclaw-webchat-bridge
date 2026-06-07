import { loadConfig } from "./dist/config.js";
import { OpenClawConnection } from "./dist/providers/openclaw/openclaw-client.js";
import { buildSessionKey } from "./dist/providers/openclaw/session-keys.js";
const cfg = loadConfig();
const conn = await OpenClawConnection.connect(cfg.openclawGatewayUrl, cfg.openclawToken, cfg.deviceIdentity);
const sk = buildSessionKey("jx7f3yr4ctbr8fzp0kswjxx9f1883d6e", cfg.agentId, cfg.canonical);
function shape(v){return JSON.stringify(v,(k,val)=>typeof val==="string"&&val.length>140?`<str ${val.length}>`:val,1);}
async function probe(m,p){try{const r=await conn.request(m,p);const b=r?.payload??r?.result??r;console.log(`\n##### ${m} OK #####`);console.log(shape(b).slice(0,3000));}catch(e){console.log(`\n##### ${m} ERR: ${e?.message??e} #####`);}}
await probe("sessions.describe", { key: sk });
await probe("config.schema.lookup", { path: "agents" });
await probe("models.list", {});
conn.close();process.exit(0);
