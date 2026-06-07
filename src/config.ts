// Bridge configuration, loaded from the environment and validated fail-fast.
//
// SECURITY (load-bearing): every secret here lives ONLY in the bridge process
// environment. Gateway tokens, the Ed25519 device identity, the Convex ingest
// secret and the shared secret Convex uses to call us are NEVER stored in a
// Convex table or sent to the browser (see convex/schema.ts design invariants).
//
// We fail fast on a missing required variable: a half-configured bridge that
// silently drops sends or can't authenticate is worse than a process that
// refuses to start with a clear message.

/** Ed25519 device identity used to sign the OpenClaw connect challenge. */
export interface DeviceIdentity {
  id: string;
  publicKey: string;
  /** PEM-encoded PKCS#8 Ed25519 private key. */
  privateKey: string;
}

export interface BridgeConfig {
  // --- OpenClaw Gateway ------------------------------------------------------
  /** Gateway URL (ws:// wss:// http:// https://; normalized to ws/wss). */
  openclawGatewayUrl: string;
  /** Bearer token presented in the connect request's auth.token. */
  openclawToken: string;
  /** Ed25519 device identity (id + publicKey + PEM privateKey). */
  deviceIdentity: DeviceIdentity;
  /** Agent id segment of the gateway session key (e.g. "main"). */
  agentId: string;
  /** Canonical operator segment of the gateway session key (e.g. "olivier"). */
  canonical: string;
  /**
   * Base directory the gateway writes outbound media into. The normalizer only
   * ever surfaces paths under `<dir>/...`; we read bytes from here to upload
   * into Convex storage. Mirrors OPENCLAW_MEDIA_OUTBOUND_DIR in backend/app.
   */
  mediaOutboundDir: string;
  /**
   * Safety cap on a single outbound attachment. Bytes are STREAMED to a Convex
   * upload URL (no base64, no full buffer, no 20MB httpAction ceiling), so this
   * is just a guard against absurd files — raise OPENCLAW_MEDIA_MAX_MB freely.
   * Files above it are skipped (logged) rather than shipped.
   */
  mediaMaxBytes: number;

  // --- Convex ----------------------------------------------------------------
  /**
   * Convex HTTP actions base URL (the `.site` origin, NOT the `.cloud` query
   * origin). The bridge POSTs normalized events to an authenticated ingest
   * httpAction there which runs the internal `stream.*` mutations.
   */
  convexHttpActionsUrl: string;
  /**
   * Shared secret the bridge presents to the Convex ingest httpAction
   * (`Authorization: Bearer <secret>`). Must equal the value set on the
   * deployment with `npx convex env set BRIDGE_INGEST_SECRET ...`.
   */
  convexIngestSecret: string;

  // --- Inbound (Convex -> bridge) -------------------------------------------
  /**
   * Secret Convex's `bridge.dispatch` presents to OUR `POST /send` endpoint
   * (it sends it raw in the `Authorization` header, no "Bearer " prefix — see
   * convex/bridge.ts). Constant-time compared in server.ts.
   */
  bridgeSharedSecret: string;
  /** Port the bridge HTTP server listens on. */
  port: number;
  /** Max request body size (bytes) accepted by `POST /send`. */
  maxBodyBytes: number;
}

class ConfigError extends Error {}

function requireEnv(name: string): string {
  const value = (process.env[name] ?? "").trim();
  if (!value) {
    throw new ConfigError(`Missing required environment variable: ${name}`);
  }
  return value;
}

function optionalEnv(name: string, fallback: string): string {
  const value = (process.env[name] ?? "").trim();
  return value || fallback;
}

function parseIntEnv(name: string, fallback: number): number {
  const raw = (process.env[name] ?? "").trim();
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new ConfigError(`Invalid ${name}: expected a positive integer`);
  }
  return parsed;
}

/**
 * Resolve the device identity from either an inline JSON env var or a path to a
 * JSON file. Exactly one must be present.
 */
function loadDeviceIdentity(): DeviceIdentity {
  const inline = (process.env.OPENCLAW_DEVICE_IDENTITY ?? "").trim();
  if (!inline) {
    throw new ConfigError(
      "Missing required environment variable: OPENCLAW_DEVICE_IDENTITY (JSON)",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(inline);
  } catch (err) {
    throw new ConfigError(
      `OPENCLAW_DEVICE_IDENTITY is not valid JSON: ${(err as Error).message}`,
    );
  }
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    typeof (parsed as Record<string, unknown>).id !== "string" ||
    typeof (parsed as Record<string, unknown>).publicKey !== "string" ||
    typeof (parsed as Record<string, unknown>).privateKey !== "string"
  ) {
    throw new ConfigError(
      "OPENCLAW_DEVICE_IDENTITY must be {id, publicKey, privateKey}",
    );
  }
  // Validated above: id/publicKey/privateKey are all present strings.
  const obj = parsed as DeviceIdentity;
  return { id: obj.id, publicKey: obj.publicKey, privateKey: obj.privateKey };
}

/**
 * Load and validate the bridge configuration. Throws a ConfigError listing the
 * first missing/invalid variable. Call once at startup so misconfiguration is
 * surfaced immediately rather than on the first send.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): BridgeConfig {
  // Bind process.env for the require helpers (they read the live env).
  const prev = process.env;
  process.env = env;
  try {
    return {
      openclawGatewayUrl: requireEnv("OPENCLAW_GATEWAY_URL"),
      openclawToken: requireEnv("OPENCLAW_TOKEN"),
      deviceIdentity: loadDeviceIdentity(),
      agentId: optionalEnv("OPENCLAW_AGENT_ID", "main"),
      canonical: requireEnv("OPENCLAW_CANONICAL"),
      mediaOutboundDir: optionalEnv(
        "OPENCLAW_MEDIA_OUTBOUND_DIR",
        "/home/node/.openclaw/media/outbound",
      ),
      mediaMaxBytes: parseIntEnv("OPENCLAW_MEDIA_MAX_MB", 1024) * 1024 * 1024,
      convexHttpActionsUrl: requireEnv("CONVEX_HTTP_ACTIONS_URL"),
      convexIngestSecret: requireEnv("BRIDGE_INGEST_SECRET"),
      bridgeSharedSecret: requireEnv("BRIDGE_SHARED_SECRET"),
      port: parseIntEnv("BRIDGE_PORT", 8787),
      maxBodyBytes: parseIntEnv("BRIDGE_MAX_BODY_BYTES", 1_048_576),
    };
  } finally {
    process.env = prev;
  }
}

export { ConfigError };
