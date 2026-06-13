// Route-level tests for the compat surface: the /capabilities response shape
// is PINNED (retro-compatible legacy fields + the additive compat manifest)
// and /health stays additive. Runs the REAL HTTP server (createBridgeServer)
// with an empty SessionRegistry — GET routes never open a gateway socket — and
// covers the live-target projection through the pure buildCapabilityTargets.

import { readFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import type { Server } from "node:http";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import type { BridgeConfig } from "../src/config.js";
import type { ConvexWriter } from "../src/convex-writer.js";
import { HealthRegistry } from "../src/core/health.js";
import { SessionRegistry } from "../src/session.js";
import { buildCapabilityTargets, createBridgeServer } from "../src/server.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PKG = JSON.parse(
  readFileSync(resolve(__dirname, "../package.json"), "utf-8"),
) as { version: string };

const CONFIG: BridgeConfig = {
  openclawGatewayUrl: "ws://gateway.example.org:18789",
  openclawToken: "test-token",
  deviceIdentity: { id: "device-test", publicKey: "pk", privateKey: "sk" },
  instanceName: "primary",
  mediaOutboundDir: "/tmp/media-outbound",
  mediaMaxBytes: 1024,
  convexHttpActionsUrl: "http://convex.example.org",
  convexIngestSecret: "ingest-secret",
  bridgeSharedSecret: "shared-secret",
  port: 0,
  maxBodyBytes: 4096,
};

describe("GET /capabilities + /health (compat surface)", () => {
  let server: Server;
  let baseUrl: string;
  let health: HealthRegistry;

  beforeAll(async () => {
    // GET routes never call the writer; a structural stub is enough.
    const registry = new SessionRegistry(CONFIG, {} as ConvexWriter);
    health = new HealthRegistry(1000, () => 2000);
    health.recordOk({
      key: "u-alice",
      canonical: "u-alice",
      agentId: "main",
      gatewayHost: "gateway.example.org:18789",
      instanceName: "primary",
    });
    server = createBridgeServer({ config: CONFIG, registry, health });
    await new Promise<void>((res) => server.listen(0, res));
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await new Promise<void>((res) => server.close(() => res()));
  });

  test("/capabilities keeps the legacy fields VERBATIM (retro-compat)", async () => {
    const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as Record<
      string,
      unknown
    >;
    expect(body.instanceName).toBe("primary");
    // The pre-compat static descriptor, byte-for-byte.
    expect(body.capabilities).toEqual({
      kind: "openclaw",
      agentDiscovery: true,
      abort: false,
      history: false,
      attachments: true,
      media: true,
      streaming: "both",
    });
  });

  test("/capabilities response shape is pinned (top-level key set)", async () => {
    const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as Record<
      string,
      unknown
    >;
    expect(Object.keys(body).sort()).toEqual([
      "bridgeVersion",
      "capabilities",
      "compat",
      "instanceName",
      "protocolVersion",
      "targets",
    ]);
  });

  test("/capabilities carries the compat manifest + versions", async () => {
    const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as {
      bridgeVersion: string;
      protocolVersion: number;
      compat: {
        bridgeVersion: string;
        protocolVersion: number;
        providers: Record<
          string,
          { supportedRange: unknown; validatedVersions: string[] }
        >;
      };
      targets: unknown[];
    };
    expect(body.bridgeVersion).toBe(PKG.version);
    expect(body.protocolVersion).toBe(2);
    expect(body.compat.bridgeVersion).toBe(PKG.version);
    expect(body.compat.protocolVersion).toBe(2);
    expect(body.compat.providers.openclaw!.supportedRange).toEqual({
      min: "2026.5.19",
      maxValidated: "2026.6.5",
    });
    expect(body.compat.providers.openclaw!.validatedVersions).toEqual([
      "2026.5.19",
      "2026.6.1",
      "2026.6.5",
    ]);
    expect(body.compat.providers.hermes).toEqual({
      supportedRange: null,
      validatedVersions: [],
      capabilities: {},
    });
    // No live gateway session in this test -> no targets.
    expect(body.targets).toEqual([]);
  });

  test("/health stays additive: legacy fields intact + compat fields added", async () => {
    const body = (await (await fetch(`${baseUrl}/health`)).json()) as {
      status: string;
      startedAt: number;
      now: number;
      bridgeVersion: string;
      protocolVersion: number;
      targets: Array<Record<string, unknown>>;
    };
    // Legacy snapshot fields, unchanged (the Convex poller's contract).
    expect(body.status).toBe("ok");
    expect(body.startedAt).toBe(1000);
    expect(body.now).toBe(2000);
    const target = body.targets[0]!;
    expect(target.key).toBe("u-alice");
    expect(target.canonical).toBe("u-alice");
    expect(target.agentId).toBe("main");
    expect(target.state).toBe("connected");
    expect(target.okCount).toBe(1);
    // Additive compat fields.
    expect(body.bridgeVersion).toBe(PKG.version);
    expect(body.protocolVersion).toBe(2);
    // No live session for this target -> the version is honestly unknown.
    expect(target.gatewayVersion).toBeNull();
  });
});

describe("buildCapabilityTargets (live-session projection)", () => {
  const LIVE = (gatewayVersion: string | null, canonical = "u-alice") => ({
    canonical,
    agentId: "main",
    gatewayVersion,
  });

  test("a validated live version resolves its full capability row", () => {
    const targets = buildCapabilityTargets([LIVE("2026.6.5")], "primary");
    expect(targets).toHaveLength(1);
    const t = targets[0]!;
    expect(t.key).toBe("u-alice");
    expect(t.instanceName).toBe("primary");
    expect(t.provider).toBe("openclaw");
    expect(t.agentId).toBe("main");
    expect(t.gatewayVersion).toBe("2026.6.5");
    expect(Object.values(t.capabilities).every((v) => v === true)).toBe(true);
    // The flag is OMITTED (not false) within the validated range.
    expect(t).not.toHaveProperty("versionBeyondValidated");
  });

  test("a null gateway version applies the conservative floor", () => {
    const t = buildCapabilityTargets([LIVE(null)], null)[0]!;
    expect(t.gatewayVersion).toBeNull();
    expect(t.instanceName).toBeNull();
    expect(t.capabilities.knobThinkingLevel).toBe(true);
    expect(t.capabilities.knobFastMode).toBe(false);
    expect(t.capabilities.inboundAttachments).toBe(false);
    expect(t).not.toHaveProperty("versionBeyondValidated");
  });

  test("a version beyond maxValidated sets the flag", () => {
    const t = buildCapabilityTargets([LIVE("2026.9.9")], "primary")[0]!;
    expect(t.versionBeyondValidated).toBe(true);
    expect(Object.values(t.capabilities).every((v) => v === true)).toBe(true);
  });

  test("dedupes by canonical (bounded like /health), last live session wins", () => {
    const targets = buildCapabilityTargets(
      [LIVE("2026.6.1"), LIVE("2026.6.5")],
      "primary",
    );
    expect(targets).toHaveLength(1);
    expect(targets[0]!.gatewayVersion).toBe("2026.6.5");
  });

  test("distinct canonicals yield distinct targets", () => {
    const targets = buildCapabilityTargets(
      [LIVE("2026.6.5", "u-alice"), LIVE("2026.6.5", "u-bob")],
      "primary",
    );
    expect(targets.map((t) => t.key).sort()).toEqual(["u-alice", "u-bob"]);
  });

  // BUG-1: no live chat session at the compat poll must NOT make a supported
  // gateway resolve to "unknown version". The served instance gets a fallback
  // target from the last gateway version seen on any connection (discovery).
  test("no live session + fallback version: synthetic served-instance target", () => {
    const targets = buildCapabilityTargets([], "primary", "2026.6.5");
    expect(targets).toHaveLength(1);
    const t = targets[0]!;
    expect(t.instanceName).toBe("primary");
    expect(t.key).toBe("primary");
    expect(t.gatewayVersion).toBe("2026.6.5");
    // Full 6.5 row -> agentFiles/configDefaults resolve TRUE (no longer gated).
    expect(t.capabilities.agentFiles).toBe(true);
    expect(t.capabilities.configDefaults).toBe(true);
  });

  test("no live session + NO fallback version: stays empty (honest unknown)", () => {
    expect(buildCapabilityTargets([], "primary", null)).toEqual([]);
  });

  test("a live session for the served instance SUPPRESSES the fallback", () => {
    // The live target is more specific; the synthetic one must not duplicate it.
    const targets = buildCapabilityTargets([LIVE("2026.6.1")], "primary", "2026.6.5");
    expect(targets).toHaveLength(1);
    expect(targets[0]!.key).toBe("u-alice");
    expect(targets[0]!.gatewayVersion).toBe("2026.6.1");
  });

  test("fallback version beyond maxValidated flags the synthetic target", () => {
    const t = buildCapabilityTargets([], "primary", "2026.9.9")[0]!;
    expect(t.versionBeyondValidated).toBe(true);
  });
});
