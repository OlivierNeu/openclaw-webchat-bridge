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

  constructor(chatId: string, sessionKey: string, writer: ConvexWriter) {
    this.normalizer = new Normalizer(sessionKey);
    this.sink = new TurnSink(chatId, writer);
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
    await this.sink.apply(this.normalizer.feed(frame, now));
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
