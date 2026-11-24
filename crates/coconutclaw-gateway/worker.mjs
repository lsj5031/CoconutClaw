import { BotLedger, fetch, initSync } from "./build/worker/coconutclaw_gateway.js";
import module from "./build/worker/coconutclaw_gateway_bg.wasm";

initSync({ module });

export { BotLedger };
export default { fetch };
