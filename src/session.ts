// Per-session lifecycle: one OpenClaw connection + one RunManager + the inbound
// frame-consumer loop that drives the normalizer's receive-loop timing.
//
// A "session" maps a Convex chat (chatId) to an OpenClaw session key and the
// persistent operator WebSocket that serves it. The registry lazily creates a
// session on first send and reuses it for subsequent turns on the same chat.
//
// The consumer loop mirrors backend/app/main.py `_openclaw_to_browser`: a single
// pending `frames()` read across iterations so a frame dequeued exactly when the
// timeout fires is never dropped; on timeout we `tick()` the normalizer so an
// armed grace always finalizes (never a hung "thinking" UI).

import { OpenClawConnection } from "./providers/openclaw/openclaw-client.js";
import { RunManager } from "./providers/openclaw/run-manager.js";
import type { ConvexWriter } from "./convex-writer.js";
import type { BridgeConfig } from "./config.js";
import { buildSessionKey } from "./providers/openclaw/session-keys.js";

/** A monotonic clock in SECONDS (matches the normalizer's time unit). */
export type Clock = () => number;

const defaultClock: Clock = () => Date.now() / 1000;

export interface BridgeSession {
  readonly chatId: string;
  readonly sessionKey: string;
  readonly connection: OpenClawConnection;
  readonly runManager: RunManager;
  readonly clock: Clock;
}

class Session implements BridgeSession {
  readonly chatId: string;
  readonly sessionKey: string;
  readonly connection: OpenClawConnection;
  readonly runManager: RunManager;
  readonly clock: Clock;
  private consumerStarted = false;

  constructor(
    chatId: string,
    sessionKey: string,
    connection: OpenClawConnection,
    writer: ConvexWriter,
    clock: Clock,
  ) {
    this.chatId = chatId;
    this.sessionKey = sessionKey;
    this.connection = connection;
    this.runManager = new RunManager(chatId, sessionKey, writer);
    this.clock = clock;
  }

  /**
   * Start the single inbound consumer loop for this session's connection.
   * Idempotent. Resolves when the connection closes (frames() terminates).
   */
  startConsumer(): void {
    if (this.consumerStarted) {
      return;
    }
    this.consumerStarted = true;
    void this.consume();
  }

  private async consume(): Promise<void> {
    const iterator = this.connection.frames();
    // Maintain a single pending read so a frame is never lost on a tick timeout.
    let nextFrame = iterator.next();
    while (true) {
      const timeoutSec = this.runManager.nextTimeout(this.clock());
      const timeoutMs = timeoutSec === null ? null : Math.max(0, timeoutSec * 1000);
      const winner = await raceWithTimeout(nextFrame, timeoutMs);
      const now = this.clock();
      if (winner.kind === "frame") {
        if (winner.done) {
          // Connection closed. If a turn was mid-flight, finalize it as aborted
          // so the UI never stays stuck on a "streaming" message.
          if (!this.runManager.isFinalized) {
            try {
              await this.runManager.endTurn(now, "aborted");
            } catch (err) {
              console.error("session close finalize error:", (err as Error)?.message ?? err);
            }
          }
          break;
        }
        nextFrame = iterator.next();
        try {
          await this.runManager.feed(winner.value, now);
        } catch (err) {
          console.error("session feed error:", (err as Error)?.message ?? err);
        }
      } else {
        // timeout: resolve any expired normalizer deadline (may finalize).
        try {
          await this.runManager.tick(now);
        } catch (err) {
          console.error("session tick error:", (err as Error)?.message ?? err);
        }
      }
    }
  }

  close(): void {
    this.connection.close();
  }
}

type RaceResult<T> =
  | { kind: "frame"; done: false; value: T }
  | { kind: "frame"; done: true; value: undefined }
  | { kind: "timeout" };

/**
 * Await the pending iterator read, but give up after `timeoutMs` (null = wait
 * forever). On timeout the original `nextFrame` promise is left pending so the
 * next iteration re-awaits it — no frame is dropped.
 */
function raceWithTimeout<T>(
  nextFrame: Promise<IteratorResult<T>>,
  timeoutMs: number | null,
): Promise<RaceResult<T>> {
  const framePromise = nextFrame.then(
    (r): RaceResult<T> =>
      r.done
        ? { kind: "frame", done: true, value: undefined }
        : { kind: "frame", done: false, value: r.value },
  );
  if (timeoutMs === null) {
    return framePromise;
  }
  const timeoutPromise = new Promise<RaceResult<T>>((resolve) => {
    const timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
    if (typeof timer.unref === "function") {
      timer.unref();
    }
  });
  return Promise.race([framePromise, timeoutPromise]);
}

/**
 * Owns the live sessions. `acquire` returns the session for a chat, creating
 * (and connecting) it on first use. Routing uses `openclawChatId` to build the
 * gateway session key; when absent we fall back to the Convex chatId.
 */
export class SessionRegistry {
  private readonly sessions = new Map<string, Session>();
  private readonly inflight = new Map<string, Promise<Session>>();

  constructor(
    private readonly config: BridgeConfig,
    private readonly writer: ConvexWriter,
    private readonly clock: Clock = defaultClock,
  ) {}

  /** The Convex writer (read seam for session re-hydration in performSend). */
  getWriter(): ConvexWriter {
    return this.writer;
  }

  async acquire(chatId: string, openclawChatId: string | null): Promise<BridgeSession> {
    const existing = this.sessions.get(chatId);
    if (existing && !existing.connection.isClosed) {
      return existing;
    }
    // A closed/missing session: drop it and (re)connect, deduping concurrent
    // acquisitions for the same chat.
    if (existing) {
      this.sessions.delete(chatId);
    }
    const pending = this.inflight.get(chatId);
    if (pending) {
      return pending;
    }
    const promise = this.create(chatId, openclawChatId).finally(() => {
      this.inflight.delete(chatId);
    });
    this.inflight.set(chatId, promise);
    return promise;
  }

  private async create(chatId: string, openclawChatId: string | null): Promise<Session> {
    const routingId = openclawChatId ?? chatId;
    const sessionKey = buildSessionKey(routingId, this.config.agentId, this.config.canonical);
    const connection = await OpenClawConnection.connect(
      this.config.openclawGatewayUrl,
      this.config.openclawToken,
      this.config.deviceIdentity,
    );
    const session = new Session(chatId, sessionKey, connection, this.writer, this.clock);
    session.startConsumer();
    this.sessions.set(chatId, session);
    return session;
  }

  /** Cleanly close every live session (graceful shutdown). */
  closeAll(): void {
    for (const session of this.sessions.values()) {
      session.close();
    }
    this.sessions.clear();
  }
}
