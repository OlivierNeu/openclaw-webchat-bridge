import { describe, expect, test } from "vitest";
import { HealthRegistry, gatewayHostOf, type TargetRef } from "../src/core/health.js";

const REF: TargetRef = {
  key: "olivier",
  canonical: "olivier",
  agentId: "main",
  gatewayHost: "192.168.1.49:18789",
};

describe("gatewayHostOf", () => {
  test("extracts host:port from a ws/wss/http url (no token)", () => {
    expect(gatewayHostOf("ws://192.168.1.49:18789")).toBe("192.168.1.49:18789");
    expect(gatewayHostOf("wss://gateway.lacneu.com")).toBe("gateway.lacneu.com");
  });
  test("degrades gracefully on a non-url", () => {
    expect(gatewayHostOf("not a url")).toBe("not a url");
  });
});

describe("HealthRegistry", () => {
  test("starts idle (no work attempted yet)", () => {
    const h = new HealthRegistry(1000, () => 2000);
    const snap = h.snapshot();
    expect(snap.status).toBe("ok");
    expect(snap.startedAt).toBe(1000);
    expect(snap.targets).toHaveLength(0); // nothing recorded -> no targets
  });

  test("recordOk -> connected with lastOkAt + counters", () => {
    let t = 5000;
    const h = new HealthRegistry(1000, () => t);
    h.recordOk(REF);
    const target = h.snapshot().targets[0]!;
    expect(target.state).toBe("connected");
    expect(target.lastOkAt).toBe(5000);
    expect(target.lastError).toBeNull();
    expect(target.okCount).toBe(1);
    expect(target.agentId).toBe("main"); // the REAL env agent, not a body claim
    expect(target.gatewayHost).toBe("192.168.1.49:18789");
  });

  test("recordError -> error with the curated code + when", () => {
    let t = 7000;
    const h = new HealthRegistry(1000, () => t);
    h.recordError(REF, "AGENT_NOT_FOUND");
    const target = h.snapshot().targets[0]!;
    expect(target.state).toBe("error");
    expect(target.lastError).toEqual({ code: "AGENT_NOT_FOUND", at: 7000 });
    expect(target.errorCount).toBe(1);
  });

  test("a later OK clears the state to connected (recovery), keeping history counts", () => {
    let t = 1;
    const h = new HealthRegistry(0, () => t);
    h.recordError(REF, "GATEWAY_TIMEOUT");
    t = 2;
    h.recordOk(REF);
    const target = h.snapshot().targets[0]!;
    expect(target.state).toBe("connected");
    expect(target.lastOkAt).toBe(2);
    expect(target.lastError).toEqual({ code: "GATEWAY_TIMEOUT", at: 1 }); // history kept
    expect(target.attempts).toBe(2);
    expect(target.okCount).toBe(1);
    expect(target.errorCount).toBe(1);
  });

  test("one target per key (mono-tenant collapses to a single row)", () => {
    const h = new HealthRegistry(0, () => 1);
    h.recordError(REF, "X");
    h.recordError(REF, "Y");
    h.recordOk(REF);
    expect(h.snapshot().targets).toHaveLength(1);
    expect(h.snapshot().targets[0]!.attempts).toBe(3);
  });
});
