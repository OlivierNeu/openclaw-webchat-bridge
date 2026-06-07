// Inbound HTTP endpoint: Convex -> bridge.
//
// `convex/bridge.ts` dispatch POSTs a pending user turn to `POST /send`. The
// request shape and auth are DICTATED by convex/bridge.ts (source of truth):
//   headers: { Authorization: <BRIDGE_SHARED_SECRET> }   // raw, NO "Bearer "
//   body:    { chatId, openclawChatId, text, clientMessageId, attachments }
//
// On a valid request we:
//   1. resolve (or lazily create) the per-session OpenClaw connection + run
//      manager for `openclawChatId`,
//   2. patch verboseLevel=full once per connection (sticky server-side),
//   3. chat.send with an idempotencyKey derived from clientMessageId,
//   4. learn the ack runId and beginTurn() so the normalizer admits this run.
//
// SECURITY: the shared secret is compared in CONSTANT TIME; the body is size-
// limited before parsing. We never echo gateway/filesystem detail to the caller.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { timingSafeEqual } from "node:crypto";

import type { BridgeConfig } from "./config.js";
import { idempotencyKey } from "./providers/openclaw/openclaw-client.js";
import { classifyGatewayError } from "./core/dispatch-errors.js";
import type { ConvexWriter, SessionMetaReport } from "./convex-writer.js";
import type { SessionRegistry, BridgeSession } from "./session.js";

/** Per-chat OpenClaw knob intent (reasoning level / model). Non-secret. */
interface SessionSettings {
  thinkingLevel?: string | null;
  model?: string | null;
}

interface SendBody {
  chatId: string;
  openclawChatId: string | null;
  text: string;
  clientMessageId: string;
  /** The user message id for this turn (excluded from re-hydration history). */
  messageId: string | null;
  /** The user's reasoning/model overrides, re-applied before chat.send. */
  sessionSettings: SessionSettings | null;
  attachments?: unknown;
}

/** Inbound body for the immediate knob write-back (`POST /patch`). */
interface PatchBody {
  chatId: string;
  openclawChatId: string | null;
  thinkingLevel: string | null;
  model: string | null;
}

/** Inbound body for a session reset (`POST /reset`). */
interface ResetBody {
  chatId: string;
  openclawChatId: string | null;
}

/** Constant-time string compare that does not leak length via early return. */
function constantTimeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) {
    // Still run a comparison to avoid trivially leaking the length difference.
    timingSafeEqual(ab, ab);
    return false;
  }
  return timingSafeEqual(ab, bb);
}

/** Read the request body up to `maxBytes`, rejecting anything larger. */
function readBody(req: IncomingMessage, maxBytes: number): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        reject(new Error("payload too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}

function parseSendBody(raw: string): SendBody | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) {
    return null;
  }
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.chatId !== "string" || typeof obj.text !== "string") {
    return null;
  }
  if (typeof obj.clientMessageId !== "string") {
    return null;
  }
  return {
    chatId: obj.chatId,
    openclawChatId: typeof obj.openclawChatId === "string" ? obj.openclawChatId : null,
    text: obj.text,
    clientMessageId: obj.clientMessageId,
    messageId: typeof obj.messageId === "string" ? obj.messageId : null,
    sessionSettings: parseSessionSettings(obj.sessionSettings),
    attachments: obj.attachments,
  };
}

/** Defensive parse of the session-reset body. Exported for tests. */
export function parseResetBody(raw: string): ResetBody | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.chatId !== "string") return null;
  return {
    chatId: obj.chatId,
    openclawChatId: typeof obj.openclawChatId === "string" ? obj.openclawChatId : null,
  };
}

/** Defensive parse of the (optional) per-chat knob intent. Exported for tests. */
export function parseSessionSettings(raw: unknown): SessionSettings | null {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  const str = (v: unknown): string | null =>
    typeof v === "string" && v.length > 0 ? v : null;
  const settings: SessionSettings = {
    thinkingLevel: str(o.thinkingLevel),
    model: str(o.model),
  };
  return settings.thinkingLevel || settings.model ? settings : null;
}

/** Defensive parse of the immediate write-back body. Exported for tests. */
export function parsePatchBody(raw: string): PatchBody | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.chatId !== "string") return null;
  const str = (v: unknown): string | null =>
    typeof v === "string" && v.length > 0 ? v : null;
  const thinkingLevel = str(obj.thinkingLevel);
  const model = str(obj.model);
  // At least one knob must be present, else there is nothing to patch.
  if (!thinkingLevel && !model) return null;
  return {
    chatId: obj.chatId,
    openclawChatId: typeof obj.openclawChatId === "string" ? obj.openclawChatId : null,
    thinkingLevel,
    model,
  };
}

function extractRunId(response: {
  payload?: Record<string, unknown>;
  runId?: unknown;
}): string | null {
  const payload = response.payload;
  if (payload && typeof payload.runId === "string" && payload.runId) {
    return payload.runId;
  }
  if (typeof response.runId === "string" && response.runId) {
    return response.runId;
  }
  return null;
}

/**
 * Perform the send against OpenClaw and begin the assistant turn.
 *
 * Mirrors backend/app/main.py `_send_chat_message` + `_handle_send`:
 * verboseLevel=full once per connection, then chat.send, then note_run_started.
 */
/**
 * Extract the header-strip session meta from a `sessions.describe` session row.
 * Defensive about shapes (agentRuntime may be a string or `{id}`; thinkingLevels
 * may be strings or `{id,label}`; fresh sessions omit token counts). The
 * "reasoning level" shown is the per-session OVERRIDE if set, else the agent
 * default (so the chip's "inherited" badge is correct). Non-secret labels only.
 */
function parseSessionMeta(
  sess: Record<string, unknown>,
  availableModels?: { id: string; label: string }[],
): SessionMetaReport {
  const str = (v: unknown): string | undefined =>
    typeof v === "string" && v.length > 0 ? v : undefined;
  const num = (v: unknown): number | undefined =>
    typeof v === "number" ? v : undefined;

  const runtime = sess.agentRuntime;
  const agentRuntime =
    typeof runtime === "string"
      ? runtime
      : str((runtime as { id?: unknown } | null)?.id);

  let thinkingLevels: { id: string; label: string }[] | undefined;
  if (Array.isArray(sess.thinkingLevels)) {
    thinkingLevels = sess.thinkingLevels
      .map((t): { id: string; label: string } => {
        if (typeof t === "string") return { id: t, label: t };
        const o = t as { id?: unknown; label?: unknown };
        const id = typeof o?.id === "string" ? o.id : "";
        const label = typeof o?.label === "string" ? o.label : id;
        return { id, label };
      })
      .filter((t) => t.id.length > 0);
  }

  const thinkingDefault = str(sess.thinkingDefault);
  return {
    model: str(sess.model),
    modelProvider: str(sess.modelProvider),
    agentRuntime,
    // Effective reasoning level: per-session override, else the agent default.
    thinkingLevel: str(sess.thinkingLevel) ?? thinkingDefault,
    thinkingDefault,
    thinkingLevels,
    availableModels:
      availableModels && availableModels.length > 0 ? availableModels : undefined,
    verboseLevel: str(sess.verboseLevel),
    totalTokens: num(sess.totalTokens),
    contextTokens: num(sess.contextTokens),
    estimatedCostUsd: num(sess.estimatedCostUsd),
  };
}

/**
 * Fetch `models.list` ONCE per connection (cached on `conn.availableModels`) and
 * return the deduped {id,label} list for the header's model picker. The gateway
 * may list the same id under several providers (e.g. gpt-5.5 under openai AND
 * openai-codex) — we dedupe by id (first label wins). Non-fatal: any failure
 * caches `[]` so we do not retry every turn.
 */
/**
 * Dedupe a raw `models.list` payload into {id,label}. The gateway may list the
 * same id under several providers (e.g. gpt-5.5 under openai AND openai-codex);
 * we keep the first occurrence (its name wins). Empty/invalid ids are dropped.
 * Pure (no I/O) so it is unit-testable. Exported for tests.
 */
export function dedupeModels(list: unknown): { id: string; label: string }[] {
  const out: { id: string; label: string }[] = [];
  const seen = new Set<string>();
  if (Array.isArray(list)) {
    for (const m of list) {
      const o = m as { id?: unknown; name?: unknown };
      const id = typeof o?.id === "string" ? o.id : "";
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const label = typeof o?.name === "string" && o.name.length > 0 ? o.name : id;
      out.push({ id, label });
    }
  }
  return out;
}

async function ensureAvailableModels(
  conn: BridgeSession["connection"],
): Promise<{ id: string; label: string }[]> {
  if (conn.availableModels !== null) return conn.availableModels;
  try {
    const resp = await conn.request("models.list", {}, 8_000);
    const list = (resp.payload as { models?: unknown } | undefined)?.models;
    conn.availableModels = dedupeModels(list);
  } catch (err) {
    console.error("[models.list] skipped (non-fatal):", (err as Error)?.message ?? err);
    conn.availableModels = [];
  }
  return conn.availableModels;
}

/**
 * Apply the user's per-chat knob intent to the gateway via `sessions.patch`.
 * Idempotent (patching to the current value is a no-op server-side). Used by BOTH
 * the immediate write-back (`/patch`) and the per-turn re-apply in `performSend`
 * (so a reset/rolled session keeps the user's reasoning/model). Non-fatal: a patch
 * failure is logged and the turn proceeds with whatever the session already had.
 */
async function applySessionSettings(
  conn: BridgeSession["connection"],
  sessionKey: string,
  settings: SessionSettings | null,
): Promise<void> {
  if (!settings) return;
  try {
    if (settings.thinkingLevel) {
      await conn.request(
        "sessions.patch",
        { key: sessionKey, thinkingLevel: settings.thinkingLevel },
        10_000,
      );
    }
    if (settings.model) {
      await conn.request(
        "sessions.patch",
        { key: sessionKey, model: settings.model },
        10_000,
      );
    }
  } catch (err) {
    console.error(
      "[sessionSettings] patch skipped (non-fatal):",
      (err as Error)?.message ?? err,
    );
  }
}

async function performSend(
  session: BridgeSession,
  body: SendBody,
  writer: ConvexWriter,
): Promise<void> {
  const conn = session.connection;
  const sessionKey = session.sessionKey;
  if (!conn.verboseFullApplied) {
    await conn.request(
      "sessions.patch",
      { key: sessionKey, verboseLevel: "full" },
      10_000,
    );
    conn.verboseFullApplied = true;
  }

  // RE-APPLY the user's per-chat knob intent (reasoning/model) BEFORE the describe
  // below, so a reset/rolled session keeps the user's choice AND the meta we mirror
  // reflects it within THIS turn (not the next). Idempotent + non-fatal.
  await applySessionSettings(conn, sessionKey, body.sessionSettings);

  // SESSION RE-HYDRATION (docs/SESSION_CONTINUITY_DESIGN.md). OpenClaw sessions are
  // ephemeral (daily/idle reset, pruning); our webchat displays the FULL thread.
  // If the gateway session is FRESH/rolled (no session row, or `systemSent` is
  // false — verified: it flips true after the first turn, false on reset) it no
  // longer holds the conversation the user still sees. Detect that and PREPEND our
  // stored prior turns so the model's context matches the display. The visible
  // message in Convex stays `body.text` (we only enrich what the gateway sees), so
  // re-hydration never leaks into the UI. NON-FATAL: any failure falls back to the
  // bare message — re-hydration must never break a send.
  let message = body.text;
  try {
    const desc = await conn.request("sessions.describe", { key: sessionKey }, 8_000);
    const sess = (desc.payload as { session?: Record<string, unknown> } | undefined)
      ?.session;

    // (a) Mirror LIVE session meta onto the chat for the header strip (model +
    // reasoning chips + context meter). Fire-and-forget — never blocks/fails the
    // send. NOTE: this `describe` runs BEFORE the turn's reply, so the meter
    // reflects the session as of the LAST COMPLETED turn (a one-turn lag). A v2
    // could re-describe after finalize for during-turn accuracy.
    if (sess) {
      const models = await ensureAvailableModels(conn);
      void writer
        .reportSessionMeta(body.chatId, parseSessionMeta(sess, models))
        .catch((e) =>
          console.error("[sessionMeta] skipped (non-fatal):", (e as Error)?.message ?? e),
        );
    }

    // (b) Re-hydration on a fresh/rolled session (systemSent flips true after the
    // first turn, false on reset; absent session row -> also fresh).
    const freshSession = !sess || sess.systemSent === false;
    if (freshSession) {
      const ctx = await writer.getRehydrationContext(body.chatId, body.messageId);
      if (ctx.history) {
        message = `${ctx.history}\n\n${body.text}`;
        // Decision log (no PHI — counts + chatId only): fires on a fresh session
        // WITH prior history, not on warm turns or empty chats.
        console.error(
          `[rehydrate] chat=${body.chatId} fresh session -> prepended ${ctx.turnCount} prior turn(s)`,
        );
      }
    }
  } catch (err) {
    console.error("[rehydrate] skipped (non-fatal):", (err as Error)?.message ?? err);
  }

  const params: Record<string, unknown> = {
    sessionKey,
    message,
    idempotencyKey: await idempotencyKey(sessionKey, body.clientMessageId),
  };
  if (Array.isArray(body.attachments) && body.attachments.length > 0) {
    params.attachments = body.attachments;
  }
  const now = session.clock();
  // Reset the normalizer for this turn BEFORE the ack so frames arriving before
  // the ack are admitted on sessionKey alone (ownRunIds empty), then seed.
  const response = await conn.request("chat.send", params, 20_000);
  const ackRunId = extractRunId(response);
  await session.runManager.beginTurn(now, ackRunId);
}

/**
 * Immediate knob write-back (`POST /patch`). Applies the user's reasoning/model
 * choice via `sessions.patch`, then re-describes and reports the CONFIRMED live
 * `sessionMeta` back to Convex — so the header chip reflects the gateway's actual
 * state, not an optimistic guess. The describe reflects the patch immediately
 * (verified live, 6.1: a patch is visible in the very next describe). Does NOT
 * begin a turn (no chat.send): patching a knob must never look like a message.
 */
async function performPatch(
  session: BridgeSession,
  body: PatchBody,
  writer: ConvexWriter,
): Promise<void> {
  const conn = session.connection;
  const sessionKey = session.sessionKey;

  await applySessionSettings(conn, sessionKey, {
    thinkingLevel: body.thinkingLevel,
    model: body.model,
  });

  // Confirm + mirror the live state so the chip converges to the truth.
  try {
    const desc = await conn.request("sessions.describe", { key: sessionKey }, 8_000);
    const sess = (desc.payload as { session?: Record<string, unknown> } | undefined)
      ?.session;
    if (sess) {
      const models = await ensureAvailableModels(conn);
      await writer.reportSessionMeta(body.chatId, parseSessionMeta(sess, models));
    }
  } catch (err) {
    console.error("[patch] describe/report skipped (non-fatal):", (err as Error)?.message ?? err);
  }
}

/**
 * Reset the OpenClaw session (`sessions.reset`). Called after a message DELETE in
 * Convex so the gateway's session context stops diverging from the (now-truncated)
 * webchat: a reset flips `systemSent` to false, so the NEXT turn re-hydrates from
 * the truncated Convex state (docs/SESSION_CONTINUITY_DESIGN.md). Without this, a
 * warm session would keep deleted turns in the model's context — the user would
 * see a truncated thread while the model still reasons over what they removed.
 * We also clear `verboseFullApplied` so the next send re-applies verboseLevel.
 */
async function performReset(session: BridgeSession): Promise<void> {
  const conn = session.connection;
  await conn.request("sessions.reset", { key: session.sessionKey }, 10_000);
  conn.verboseFullApplied = false;
}

export interface BridgeServerDeps {
  config: BridgeConfig;
  registry: SessionRegistry;
}

/**
 * Create (but do not start) the inbound HTTP server. Call `.listen(port)`.
 *
 * Routes:
 *   GET  /health  -> liveness probe (no auth)
 *   POST /send    -> authenticated turn dispatch from Convex
 *   POST /patch   -> authenticated knob write-back (reasoning/model) from Convex
 */
export function createBridgeServer(deps: BridgeServerDeps): Server {
  const { config, registry } = deps;
  return createServer((req: IncomingMessage, res: ServerResponse) => {
    void handle(req, res).catch((err: unknown) => {
      // Never leave the dispatcher hanging; never leak gateway detail.
      console.error("bridge server error:", (err as Error)?.message ?? err);
      if (!res.headersSent) {
        sendJson(res, 500, { ok: false, error: "internal error" });
      }
    });
  });

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    if (req.method === "GET" && req.url === "/health") {
      sendJson(res, 200, { status: "ok" });
      return;
    }
    if (
      req.method !== "POST" ||
      (req.url !== "/send" && req.url !== "/patch" && req.url !== "/reset")
    ) {
      sendJson(res, 404, { ok: false, error: "not found" });
      return;
    }

    // Auth: convex/bridge.ts sends the secret RAW in Authorization (no Bearer).
    const provided = req.headers["authorization"];
    if (typeof provided !== "string" || !constantTimeEqual(provided, config.bridgeSharedSecret)) {
      sendJson(res, 401, { ok: false, error: "unauthorized" });
      return;
    }

    let raw: string;
    try {
      raw = await readBody(req, config.maxBodyBytes);
    } catch {
      sendJson(res, 413, { ok: false, error: "payload too large" });
      return;
    }

    if (req.url === "/patch") {
      const patch = parsePatchBody(raw);
      if (patch === null) {
        sendJson(res, 400, { ok: false, error: "invalid body" });
        return;
      }
      try {
        const session = await registry.acquire(patch.chatId, patch.openclawChatId);
        await performPatch(session, patch, registry.getWriter());
        sendJson(res, 200, { ok: true });
      } catch (err) {
        console.error("bridge /patch failed:", (err as Error)?.message ?? err);
        sendJson(res, 502, { ok: false, error: "upstream patch failed" });
      }
      return;
    }

    if (req.url === "/reset") {
      const reset = parseResetBody(raw);
      if (reset === null) {
        sendJson(res, 400, { ok: false, error: "invalid body" });
        return;
      }
      try {
        const session = await registry.acquire(reset.chatId, reset.openclawChatId);
        await performReset(session);
        sendJson(res, 200, { ok: true });
      } catch (err) {
        console.error("bridge /reset failed:", (err as Error)?.message ?? err);
        sendJson(res, 502, { ok: false, error: "upstream reset failed" });
      }
      return;
    }

    const body = parseSendBody(raw);
    if (body === null) {
      sendJson(res, 400, { ok: false, error: "invalid body" });
      return;
    }

    try {
      const session = await registry.acquire(body.chatId, body.openclawChatId);
      await performSend(session, body, registry.getWriter());
      sendJson(res, 200, { ok: true });
    } catch (err) {
      // A per-send upstream failure is reported but does not crash the bridge.
      // Classify into a stable, non-PHI code: the RAW message stays in this log
      // only; only `error.code` crosses to Convex (the platform forbids shipping
      // raw message text). Convex maps the code to the user/admin surfaces.
      const code = classifyGatewayError(err);
      console.error(`bridge /send failed [${code}]:`, (err as Error)?.message ?? err);
      sendJson(res, 502, { ok: false, error: { code } });
    }
  }
}
