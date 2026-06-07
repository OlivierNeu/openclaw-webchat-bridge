/**
 * End-to-end run-manager tests.
 *
 * Replays REAL OpenClaw frame scenarios (the canonical fixtures shared with the
 * Python suite and the normalizer tests) through RunManager + a FAKE
 * ConvexWriter, asserting the exact internal stream mutations a correct bridge
 * must call, IN ORDER. This pins the run-manager -> convex-writer seam (the part
 * the offline gate actually exercises) without any live Convex or socket.
 *
 * The fixtures are read VERBATIM from backend/tests/fixtures/openclaw_frames.json
 * (the single source of truth), so these scenarios stay in lockstep with the
 * normalizer's behavior.
 */

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { RunManager } from "../src/providers/openclaw/run-manager.js";
import type {
  ConvexWriter,
  FinalizeStatus,
  ToolPart,
} from "../src/convex-writer.js";
import { BASE_RECV_TIMEOUT } from "../src/providers/openclaw/normalizer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const FIXTURES_PATH = resolve(
  __dirname,
  "./fixtures/openclaw_frames.json",
);
const FIXTURES = JSON.parse(readFileSync(FIXTURES_PATH, "utf-8")) as {
  session_key: string;
  run_id: string;
  scenarios: Record<string, { description: string; frames: unknown[] }>;
};

const SESSION_KEY = FIXTURES.session_key;
const OWN_RUN = FIXTURES.run_id;
const CHAT_ID = "chat_test_1";
const MESSAGE_ID = "msg_test_1";

/** A recorded writer call: [method, ...args]. */
type Call =
  | ["startAssistant", string, string | null]
  | ["appendDelta", string, string]
  | ["setSnapshot", string, string]
  | ["addToolPart", string, ToolPart]
  | ["addMedia", string, { filename: string; path: string }]
  | ["finalize", string, FinalizeStatus, string, string | null];

/**
 * Records every writer call in order. startAssistant returns a stub message id;
 * deltas are NOT coalesced (the live HttpConvexWriter coalesces; the seam under
 * test is the run-manager's call ordering, so the fake records each call).
 */
class FakeWriter implements ConvexWriter {
  readonly calls: Call[] = [];

  async startAssistant(chatId: string, runId: string | null): Promise<string> {
    this.calls.push(["startAssistant", chatId, runId]);
    return MESSAGE_ID;
  }
  async appendDelta(messageId: string, text: string): Promise<void> {
    this.calls.push(["appendDelta", messageId, text]);
  }
  async setSnapshot(messageId: string, text: string): Promise<void> {
    this.calls.push(["setSnapshot", messageId, text]);
  }
  async addToolPart(messageId: string, part: ToolPart): Promise<void> {
    this.calls.push(["addToolPart", messageId, part]);
  }
  async addMedia(
    messageId: string,
    media: { filename: string; path: string; mimeType?: string },
  ): Promise<void> {
    this.calls.push([
      "addMedia",
      messageId,
      { filename: media.filename, path: media.path },
    ]);
  }
  async finalize(
    messageId: string,
    status: FinalizeStatus,
    text: string,
    error: string | null,
  ): Promise<void> {
    this.calls.push(["finalize", messageId, status, text, error]);
  }
  async getRehydrationContext(): Promise<{
    history: string | null;
    turnCount: number;
  }> {
    // Read-only seam; the RunManager tests never re-hydrate.
    return { history: null, turnCount: 0 };
  }
  async reportSessionMeta(): Promise<void> {
    // Fire-and-forget seam; the RunManager tests don't assert session meta.
  }
}

class Clock {
  now = 1000.0;
  tick(seconds = 0.01): number {
    this.now += seconds;
    return this.now;
  }
}

function frames(scenario: string): unknown[] {
  const s = FIXTURES.scenarios[scenario];
  if (!s) {
    throw new Error(`unknown scenario: ${scenario}`);
  }
  return s.frames;
}

/**
 * Drive a scenario end-to-end through a fresh RunManager + FakeWriter, mirroring
 * the real session loop: beginTurn (with the ack runId), feed every frame, then
 * (optionally) advance the clock past every grace and tick once so a pending
 * turn finalizes.
 */
async function drive(
  scenario: string,
  opts: { seedRun?: string | null; advanceToFinalize?: boolean } = {},
): Promise<{ writer: FakeWriter; manager: RunManager }> {
  const seedRun = opts.seedRun === undefined ? OWN_RUN : opts.seedRun;
  const advanceToFinalize = opts.advanceToFinalize ?? false;
  const writer = new FakeWriter();
  const manager = new RunManager(CHAT_ID, SESSION_KEY, writer);
  const clock = new Clock();

  await manager.beginTurn(clock.now, seedRun);
  for (const frame of frames(scenario)) {
    await manager.feed(frame, clock.tick());
  }
  if (advanceToFinalize && !manager.isFinalized) {
    clock.tick(BASE_RECV_TIMEOUT + 1);
    await manager.tick(clock.now);
  }
  return { writer, manager };
}

describe("run-manager -> convex-writer mapping", () => {
  it("chat final content: startAssistant, snapshots, finalize(complete)", async () => {
    const { writer, manager } = await drive("chat-final-content");
    expect(manager.isFinalized).toBe(true);
    expect(writer.calls).toEqual([
      ["startAssistant", CHAT_ID, OWN_RUN],
      ["setSnapshot", MESSAGE_ID, "Bon"],
      ["setSnapshot", MESSAGE_ID, "Bonjour !"],
      ["finalize", MESSAGE_ID, "complete", "Bonjour !", null],
    ]);
  });

  it("chat final content string finalizes complete with the text", async () => {
    const { writer, manager } = await drive("chat-final-content-string");
    expect(manager.isFinalized).toBe(true);
    // First op is always the streaming-message creation.
    expect(writer.calls[0]).toEqual(["startAssistant", CHAT_ID, OWN_RUN]);
    const final = writer.calls[writer.calls.length - 1];
    expect(final).toEqual([
      "finalize",
      MESSAGE_ID,
      "complete",
      "Réponse en texte simple.",
      null,
    ]);
  });

  it("legacy agent deltas accumulate as appendDelta then finalize", async () => {
    const { writer, manager } = await drive("agent-assistant-delta-legacy", {
      advanceToFinalize: true,
    });
    expect(manager.isFinalized).toBe(true);
    expect(writer.calls[0]).toEqual(["startAssistant", CHAT_ID, OWN_RUN]);
    const deltas = writer.calls
      .filter((c) => c[0] === "appendDelta")
      .map((c) => (c as ["appendDelta", string, string])[2]);
    expect(deltas).toEqual(["Hello ", "world"]);
    const final = writer.calls[writer.calls.length - 1] as [
      "finalize",
      string,
      FinalizeStatus,
      string,
      string | null,
    ];
    expect(final[0]).toBe("finalize");
    expect(final[2]).toBe("complete");
    expect(final[3]).toBe("Hello world");
  });

  it("tool message: addToolPart precedes the finalize", async () => {
    const { writer, manager } = await drive("tool-message-visible");
    expect(manager.isFinalized).toBe(true);
    const tool = writer.calls.find((c) => c[0] === "addToolPart") as
      | ["addToolPart", string, ToolPart]
      | undefined;
    expect(tool).toBeDefined();
    expect(tool![2]).toMatchObject({ kind: "tool", name: "message", phase: "start" });
    const final = writer.calls[writer.calls.length - 1] as [
      "finalize",
      string,
      FinalizeStatus,
      string,
      string | null,
    ];
    expect(final[0]).toBe("finalize");
    expect(final[2]).toBe("complete");
    expect(final[3]).toBe("Réponse visible complète.");
  });

  it("mediaUrls: addMedia for each filtered item, no path leak in finalize", async () => {
    const { writer, manager } = await drive("mediaurls-list", {
      advanceToFinalize: true,
    });
    expect(manager.isFinalized).toBe(true);
    const media = writer.calls.filter((c) => c[0] === "addMedia") as Array<
      ["addMedia", string, { filename: string; path: string }]
    >;
    // Same filtering as the normalizer: dup collapsed; empty/int/https/../inbound
    // rejected. Only a.pdf + c.pdf survive.
    expect(media.map((c) => c[2].filename)).toEqual(["a.pdf", "c.pdf"]);
    expect(media.map((c) => c[2].path)).toEqual([
      "/home/node/.openclaw/media/outbound/a.pdf",
      "/home/node/.openclaw/media/outbound/c.pdf",
    ]);
    const final = writer.calls[writer.calls.length - 1] as [
      "finalize",
      string,
      FinalizeStatus,
      string,
      string | null,
    ];
    expect(final[0]).toBe("finalize");
  });

  it("lifecycle error: finalize(error) with partial text + error string", async () => {
    const { writer, manager } = await drive("lifecycle-error");
    expect(manager.isFinalized).toBe(true);
    const final = writer.calls[writer.calls.length - 1] as [
      "finalize",
      string,
      FinalizeStatus,
      string,
      string | null,
    ];
    expect(final[0]).toBe("finalize");
    expect(final[2]).toBe("error"); // status mapped from the terminal run.status
    expect(final[3]).toBe("moitié"); // partial content preserved
    expect(String(final[4] ?? "")).toContain("Context overflow");
  });

  it("isolation: a foreign-session turn writes nothing but the streaming message", async () => {
    // No own frames are admitted, so only startAssistant fired; nothing else.
    const { writer } = await drive("isolation-foreign-session", {
      advanceToFinalize: false,
    });
    expect(writer.calls).toEqual([["startAssistant", CHAT_ID, OWN_RUN]]);
  });

  it("exactly one finalize is emitted per turn", async () => {
    const { writer } = await drive("duplicate-final");
    const finals = writer.calls.filter((c) => c[0] === "finalize");
    expect(finals.length).toBe(1);
  });
});
