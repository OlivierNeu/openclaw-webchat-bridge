// RunManager — the OpenClaw-SPECIFIC half of the old monolith: it drives the
// proven Normalizer (begin_turn/feed/tick/next_timeout) and forwards the
// normalized events it emits to the provider-agnostic core `TurnSink`. The
// Normalizer is the only vendor coupling, which is exactly why this class lives
// under providers/openclaw/ and the sink lives in core/ (docs/BRIDGE_ARCHITECTURE
// §2.1). In P2 this driver is absorbed into the OpenClaw `adapter.ts`; for the P1
// refactor it keeps the same public surface session.ts + the tests already use,
// so behavior is byte-identical (the 23 normalizer + 8 run-manager tests pin it).
//
// One RunManager handles one OpenClaw session (one chat).

import { Normalizer } from "./normalizer.js";
import { TurnSink } from "../../core/turn-sink.js";
import type { ConvexWriter } from "../../convex-writer.js";

/**
 * Drives one OpenClaw session's normalized stream into Convex (via TurnSink).
 *
 * Lifecycle per user turn:
 *   1. beginTurn(): reset normalizer state, seed ownRunIds from the chat.send ack
 *      runId (foreign-run isolation), and have the sink create the streaming
 *      assistant message (startAssistant).
 *   2. feed each inbound gateway frame; tick on the normalizer's timeout.
 *   3. the normalizer emits the terminal [message.final, run.status] pair, which
 *      the sink translates into a single writer.finalize().
 */
export class RunManager {
  private readonly normalizer: Normalizer;
  private readonly sink: TurnSink;
  // DIAGNOSTIC (streaming-lag investigation): per-turn tally of the raw frame
  // SHAPES the gateway delivered (type/event/state/has-delta/has-message), plus
  // a one-time 300-char sample per shape. Bounded: one line per NEW shape + one
  // summary per turn — this is what separates "the gateway never sent deltas"
  // from "the normalizer dropped them".
  private frameTally = new Map<string, number>();
  private frameSampled = new Set<string>();
  private tallyDumped = false;

  constructor(chatId: string, sessionKey: string, writer: ConvexWriter) {
    this.normalizer = new Normalizer(sessionKey);
    this.sink = new TurnSink(chatId, writer);
  }

  private tallyFrame(frame: unknown): void {
    if (typeof frame !== "object" || frame === null) return;
    const f = frame as Record<string, unknown>;
    const payload =
      typeof f.payload === "object" && f.payload !== null
        ? (f.payload as Record<string, unknown>)
        : {};
    const key = [
      String(f.type ?? "?"),
      String(f.event ?? "-"),
      typeof payload.state === "string" ? payload.state : "-",
      typeof payload.deltaText === "string" && payload.deltaText ? "delta" : "-",
      payload.message !== undefined ? "msg" : "-",
    ].join("/");
    this.frameTally.set(key, (this.frameTally.get(key) ?? 0) + 1);
    if (!this.frameSampled.has(key)) {
      this.frameSampled.add(key);
      let sample = "";
      try {
        sample = JSON.stringify(frame).slice(0, 300);
      } catch {
        sample = "<unserializable>";
      }
      console.log(`[frames] first shape ${key}: ${sample}`);
    }
  }

  private dumpTallyOnce(): void {
    if (this.tallyDumped || !this.normalizer.finalized) return;
    this.tallyDumped = true;
    const parts = [...this.frameTally.entries()]
      .map(([k, n]) => `${k}=${n}`)
      .join(" | ");
    console.log(`[frames] turn summary: ${parts || "(no frames)"}`);
  }

  /** Seconds until the normalizer's nearest deadline (null = idle). */
  nextTimeout(now: number): number | null {
    return this.normalizer.nextTimeout(now);
  }

  get isFinalized(): boolean {
    return this.normalizer.finalized;
  }

  /**
   * Start a new assistant turn. Seeds ownRunIds from the chat.send ack runId
   * (foreign-run isolation) BEFORE the sink creates the streaming message, so
   * ordering matches the pre-refactor monolith. Call before feeding any frames.
   */
  async beginTurn(now: number, ackRunId: string | null): Promise<void> {
    this.normalizer.beginTurn(now);
    this.frameTally.clear();
    this.frameSampled.clear();
    this.tallyDumped = false;
    if (ackRunId) {
      this.normalizer.noteRunStarted(ackRunId, now);
    }
    await this.sink.beginTurn(ackRunId);
  }

  /** Feed one raw gateway frame; apply the resulting events to Convex. */
  async feed(frame: unknown, now: number): Promise<void> {
    if (!this.sink.active) {
      return;
    }
    this.tallyFrame(frame);
    await this.sink.apply(this.normalizer.feed(frame, now));
    this.dumpTallyOnce();
  }

  /** Resolve expired normalizer deadlines; apply any emitted events. */
  async tick(now: number): Promise<void> {
    if (!this.sink.active) {
      return;
    }
    await this.sink.apply(this.normalizer.tick(now));
  }

  /**
   * Force-finalize the active turn (e.g. on socket close or a send error). The
   * normalizer emits its terminal pair; the sink flushes it to Convex.
   */
  async endTurn(
    now: number,
    status = "final",
    error: string | null = null,
  ): Promise<void> {
    if (!this.sink.active) {
      return;
    }
    await this.sink.apply(this.normalizer.endTurn(now, status, error));
  }
}
