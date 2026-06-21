/**
 * Optional transactional-outbox helper (ADR-0029): remove the producer **dual write**.
 *
 * A plain producer does two things that must both happen or neither — commit the business
 * row (`INSERT INTO orders …`) and publish the message to the broker. They are two
 * independent systems; a crash between them leaves the order saved with no message, or a
 * message sent for an order that rolled back. The outbox removes that dual write: the
 * encoded envelope is persisted **into the same database, in the same transaction** as the
 * business data — so it commits or rolls back atomically with it — and a separate
 * {@link OutboxRelay} publishes the durable rows afterwards. No distributed transaction;
 * exactly-once *handoff* into the broker, then at-least-once on the wire as always.
 *
 * The Node mirror of the PHP `BabelQueue\Outbox` reference. It keeps the two ADR-0029
 * invariants:
 * - **Frozen envelope (GR-1):** the store holds the {@link EnvelopeCodec.encode}-encoded
 *   string **verbatim**; the relay publishes those exact bytes — it never decodes, rebuilds
 *   or re-encodes the envelope, so `trace_id` is preserved end-to-end (GR-4) and the body is
 *   byte-identical before store and after relay (GR-5). The outbox's own bookkeeping (id,
 *   status, attempts) lives *around* the envelope, never *on* the wire.
 * - **Zero-dep core (GR-7):** {@link OutboxStore} and {@link OutboxTransport} are interfaces
 *   the caller binds to their own DB / transport. The core ships only the in-memory
 *   {@link InMemoryOutboxStore} reference — no DB driver, no transport.
 *
 * @module
 */

import { EnvelopeCodec, type Envelope } from "./codec.js";

/**
 * One pending row read back from an {@link OutboxStore} for the {@link OutboxRelay} to
 * publish. It pairs the store's own bookkeeping (`id`, `attempts`) with the verbatim,
 * frozen wire envelope ({@link OutboxRecord.body}) and the queue it should go to.
 *
 * `body` is the exact {@link EnvelopeCodec.encode} string that was handed to
 * {@link OutboxStore.save} — the relay publishes these bytes unchanged (GR-1/GR-5), so
 * `trace_id` is preserved end-to-end (GR-4) without the relay ever decoding the envelope.
 */
export interface OutboxRecord {
  /** The outbox row id (the store's own primary key, **not** `meta.id`). */
  id: string;
  /** The frozen, encoded envelope JSON, byte-for-byte as stored. */
  body: string;
  /** The logical queue the relay should publish to. */
  queue: string;
  /** How many times the relay has already tried to publish this row. */
  attempts: number;
}

/** Summary of one {@link OutboxRelay.flush} / {@link OutboxRelay.drain} pass. */
export interface OutboxRelayResult {
  /** Rows the transport accepted and the store marked published. */
  published: number;
  /** Rows whose publish rejected; left pending for a later retry. */
  failed: number;
}

/**
 * The persistence seam for the transactional outbox — the durable "outbox" table that an
 * {@link Outbox} writer fills and an {@link OutboxRelay} drains. The core defines it and
 * binds to **no** DB driver (GR-7); a concrete adapter (the caller's, or an example) binds
 * it to a real connection. The reference {@link InMemoryOutboxStore} is for tests / demos.
 *
 * **The transaction boundary is the CALLER'S.** The core never opens, commits or rolls
 * back anything: {@link OutboxStore.save} is invoked from *inside* a transaction the caller
 * already began (around its own `INSERT INTO orders …`), and the caller commits both
 * together. That is the whole point of the pattern.
 *
 * Every method is async so a production store can be DB- or network-backed; the in-memory
 * reference resolves synchronously.
 */
export interface OutboxStore {
  /**
   * Persist one encoded envelope into the outbox, **within the transaction the caller has
   * already opened** around its business write. Resolves with the new row's outbox id (the
   * store's own primary key — NOT `meta.id`), which the caller may keep for correlation.
   * The body is stored verbatim; do not re-encode or mutate it.
   *
   * @param encoded The {@link EnvelopeCodec.encode} output (UTF-8 JSON), stored verbatim.
   * @param queue   The logical target queue, captured for the relay.
   */
  save(encoded: string, queue: string): Promise<string>;

  /**
   * Reserve up to `limit` rows that are pending publish, **oldest first**, so a relay can
   * forward them. Implementations SHOULD lock/claim the rows they return (e.g.
   * `SELECT … FOR UPDATE SKIP LOCKED`, or a `picked_at` claim) so two concurrent relays do
   * not both publish the same row; at-least-once still tolerates a rare double send. The
   * claim/lock is the adapter's job — the in-memory reference does not implement it.
   *
   * @param limit Maximum rows to return (a positive batch size).
   * @returns Pending rows, oldest first; empty when the outbox is drained.
   */
  fetchUnpublished(limit: number): Promise<OutboxRecord[]>;

  /**
   * Mark the given outbox rows as successfully published (so they are never relayed again).
   * Called by the relay only **after** the transport accepted the message.
   *
   * @param ids Outbox row ids previously returned by {@link OutboxStore.fetchUnpublished}.
   */
  markPublished(ids: string[]): Promise<void>;

  /**
   * Record a failed publish attempt for one row: increment its attempt counter and store
   * the last error, leaving it **pending** so a later relay pass retries it (at-least-once).
   * The store MAY move a row that exceeds a max-attempts threshold to a terminal/parked
   * state, but that policy is the adapter's, not the core's.
   *
   * @param id    The outbox row id.
   * @param error A short, human-readable failure reason (never secrets).
   */
  markFailed(id: string, error: string): Promise<void>;
}

/**
 * The publish-only seam the {@link OutboxRelay} forwards rows through — the Node counterpart
 * of the PHP `Transport` contract and the same shape the `redrive` / `otel.publish` helpers
 * use. The core defines it; an adapter binds it to a real broker (GR-7). `body` is the
 * stored, frozen envelope and is published **verbatim**.
 */
export interface OutboxTransport {
  /**
   * Publish an already-encoded envelope `body` to `queue`. Resolves on success; **rejects**
   * to signal a failed publish (the relay catches it, marks the row failed and leaves it
   * pending). The arg order mirrors the PHP `Transport::publish($body, $queue)`.
   */
  publish(body: string, queue: string): Promise<void>;
}

/** Sleep `ms` milliseconds. Injectable so tests stay instant (pass a no-op). */
export type Sleeper = (ms: number) => Promise<void>;

/** Tuning for the {@link OutboxRelay}. All optional; the defaults mirror the PHP reference. */
export interface OutboxRelayOptions {
  /** How many rows to reserve and publish per {@link OutboxRelay.flush} (default 100). */
  batchSize?: number;
  /** Base backoff added per prior attempt, in milliseconds (default 50). */
  backoffStepMs?: number;
  /** Upper bound on a single backoff sleep, in milliseconds (default 5000). */
  backoffCapMs?: number;
  /** Sleep implementation; defaults to a real timer. Inject a no-op in tests. */
  sleeper?: Sleeper;
}

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_BACKOFF_STEP_MS = 50;
const DEFAULT_BACKOFF_CAP_MS = 5000;
/** Hard safety ceiling on {@link OutboxRelay.drain} passes when the caller passes 0. */
const DEFAULT_DRAIN_CEILING = 10_000;

/** The default real sleeper: a promise that resolves after `ms` (skipped when `ms <= 0`). */
const realSleeper: Sleeper = (ms: number): Promise<void> =>
  ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();

/**
 * The **write side** of the transactional outbox: turn a BabelQueue envelope into a stored
 * outbox row, so the message is persisted *atomically with the business data* and a separate
 * {@link OutboxRelay} publishes it later.
 *
 * Usage — the caller owns the transaction boundary (this is the whole point):
 *
 * ```ts
 * await db.transaction(async (tx) => {
 *   await tx.insertOrder(order);                 // the business write
 *   const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id }, { queue: "orders" });
 *   await outbox.write(env);                      // same connection, same tx
 * });                                             // both commit, or neither
 * ```
 *
 * Because both writes share one transaction, a crash can never leave the business row
 * committed without its message — they commit or roll back together. This helper only
 * encodes via the frozen {@link EnvelopeCodec} (GR-1) and delegates persistence to the
 * injected {@link OutboxStore}; it does **not** begin/commit anything.
 */
export class Outbox {
  constructor(private readonly store: OutboxStore) {}

  /**
   * Encode the envelope (frozen codec, bytes unchanged) and persist it via the store, inside
   * the transaction the caller has already opened. Resolves with the new outbox row id (for
   * the caller's own correlation, if wanted).
   *
   * @param envelope A canonical envelope from {@link EnvelopeCodec.make} / `fromMessage`.
   */
  write(envelope: Envelope): Promise<string> {
    const queue = queueOf(envelope);
    return this.store.save(EnvelopeCodec.encode(envelope), queue);
  }
}

/**
 * The logical queue a message targets: its `meta.queue`, falling back to `"default"`.
 * Captured at write time so the relay can publish to the right queue without decoding the
 * body.
 */
function queueOf(envelope: Envelope): string {
  const queue = envelope.meta?.queue;
  return typeof queue === "string" && queue !== "" ? queue : "default";
}

/**
 * The **read/publish side** of the transactional outbox: drain pending rows the
 * {@link Outbox} writer committed and forward each onto the broker through the
 * {@link OutboxTransport} seam, marking every row published or failed.
 *
 * Run it on a short interval (a worker loop, a scheduled command) *after* the business
 * transaction commits. It only ever reads already-durable rows, so it never invents work.
 *
 * **Semantics — at-least-once handoff:**
 * - A row is marked **published only after** {@link OutboxTransport.publish} resolves; if the
 *   process dies between publish and {@link OutboxStore.markPublished}, the row stays pending
 *   and is published **again** on the next pass. That is at-least-once: a downstream consumer
 *   must dedupe on the canonical `meta.id` (the `Wrap` idempotency helper is exactly that
 *   guard — ADR-0022, the consumer-side mirror of this producer-side helper).
 * - A publish that **rejects** is caught, {@link OutboxStore.markFailed} records the error and
 *   bumps the attempt count, and the row stays pending for a later retry. One poison row never
 *   blocks the rest of the batch.
 * - **`trace_id` is preserved end-to-end** (GR-4): the relay publishes the stored bytes
 *   *verbatim* — it never decodes, rebuilds or re-encodes the envelope — so the body that
 *   reaches the broker is byte-identical to what was stored (GR-1/GR-5).
 *
 * **Backoff:** between a failed publish and continuing the same pass, the relay sleeps for a
 * bounded, linearly-growing delay (capped), to avoid hammering a broker that is briefly down.
 * The {@link Sleeper} is injectable so tests stay instant.
 */
export class OutboxRelay {
  private readonly batchSize: number;
  private readonly backoffStepMs: number;
  private readonly backoffCapMs: number;
  private readonly sleeper: Sleeper;

  constructor(
    private readonly transport: OutboxTransport,
    private readonly store: OutboxStore,
    options: OutboxRelayOptions = {},
  ) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.backoffStepMs = options.backoffStepMs ?? DEFAULT_BACKOFF_STEP_MS;
    this.backoffCapMs = options.backoffCapMs ?? DEFAULT_BACKOFF_CAP_MS;
    this.sleeper = options.sleeper ?? realSleeper;
  }

  /**
   * Publish one batch of pending rows. Each row the transport accepts is marked published;
   * each that rejects is marked failed (with a backoff before continuing) and left pending.
   * Resolves with a per-pass tally. Call it repeatedly (a loop / cron) to drain the outbox;
   * {@link OutboxRelay.drain} loops until it is empty.
   */
  async flush(): Promise<OutboxRelayResult> {
    const records = await this.store.fetchUnpublished(this.batchSize);

    const publishedIds: string[] = [];
    let failed = 0;

    for (const record of records) {
      try {
        await this.transport.publish(record.body, record.queue);
        publishedIds.push(record.id);
      } catch (err) {
        await this.store.markFailed(record.id, reason(err));
        failed += 1;
        await this.sleeper(this.backoffFor(record.attempts));
      }
    }

    if (publishedIds.length > 0) {
      await this.store.markPublished(publishedIds);
    }

    return { published: publishedIds.length, failed };
  }

  /**
   * Drain the outbox by repeatedly calling {@link OutboxRelay.flush} while each pass keeps
   * making progress (publishes at least one row), then resolve with the cumulative tally. The
   * loop stops as soon as a pass publishes nothing — the outbox is empty, or only currently
   * failing rows remain (left pending for a future `drain` once the broker recovers).
   * `maxPasses` is a hard safety ceiling so a degenerate store can never spin forever
   * (0 / omitted = a generous internal default).
   */
  async drain(maxPasses = 0): Promise<OutboxRelayResult> {
    const ceiling = maxPasses > 0 ? maxPasses : DEFAULT_DRAIN_CEILING;
    let published = 0;
    let failed = 0;

    for (let pass = 0; pass < ceiling; pass += 1) {
      const result = await this.flush();
      published += result.published;
      failed += result.failed;

      // No progress this pass → drained, or only failing rows remain. Stop.
      if (result.published === 0) {
        break;
      }
    }

    return { published, failed };
  }

  /**
   * The backoff (ms) for a row that has already failed `priorAttempts` times: a linear step
   * per attempt, capped. Kept simple and deterministic so the budget is obvious.
   */
  private backoffFor(priorAttempts: number): number {
    const delay = this.backoffStepMs * Math.max(1, priorAttempts + 1);
    return Math.min(delay, this.backoffCapMs);
  }
}

/** A short, safe failure reason from a thrown/rejected value (name + message, no stack). */
function reason(err: unknown): string {
  if (err instanceof Error) {
    return `${err.name}: ${err.message}`;
  }
  return String(err);
}

/**
 * Process-local reference {@link OutboxStore} backed by a Map — for tests and single-process
 * demos. It has **no real transaction**: {@link InMemoryOutboxStore.save} just appends, so it
 * cannot deliver the atomic-with-the-business-write guarantee a production store gives. Use a
 * database-backed adapter in production.
 *
 * It still faithfully models the relay contract: rows are pending until
 * {@link InMemoryOutboxStore.markPublished}, {@link InMemoryOutboxStore.fetchUnpublished}
 * returns them oldest-first, and {@link InMemoryOutboxStore.markFailed} bumps the attempt
 * count and stores the last error while leaving the row pending for retry.
 */
export class InMemoryOutboxStore implements OutboxStore {
  private readonly rows = new Map<
    string,
    { body: string; queue: string; attempts: number; published: boolean; error: string }
  >();

  private sequence = 0;

  save(encoded: string, queue: string): Promise<string> {
    this.sequence += 1;
    // A non-numeric id keeps it an honest string key (mirrors the PHP reference).
    const id = `ob-${this.sequence}`;
    this.rows.set(id, { body: encoded, queue, attempts: 0, published: false, error: "" });
    return Promise.resolve(id);
  }

  fetchUnpublished(limit: number): Promise<OutboxRecord[]> {
    const records: OutboxRecord[] = [];
    // Map preserves insertion order, so this is oldest-first.
    for (const [id, row] of this.rows) {
      if (row.published) {
        continue;
      }
      records.push({ id, body: row.body, queue: row.queue, attempts: row.attempts });
      if (records.length >= limit) {
        break;
      }
    }
    return Promise.resolve(records);
  }

  markPublished(ids: string[]): Promise<void> {
    for (const id of ids) {
      const row = this.rows.get(id);
      if (row) {
        row.published = true;
      }
    }
    return Promise.resolve();
  }

  markFailed(id: string, error: string): Promise<void> {
    const row = this.rows.get(id);
    if (row) {
      row.attempts += 1;
      row.error = error;
    }
    return Promise.resolve();
  }

  /** Test/inspection helper: the number of rows still pending publish. */
  pendingCount(): number {
    let pending = 0;
    for (const row of this.rows.values()) {
      if (!row.published) {
        pending += 1;
      }
    }
    return pending;
  }

  /** Test/inspection helper: the recorded attempt count for one row (0 if unknown). */
  attemptsOf(id: string): number {
    return this.rows.get(id)?.attempts ?? 0;
  }

  /** Test/inspection helper: the last recorded error for one row ("" if none). */
  lastErrorOf(id: string): string {
    return this.rows.get(id)?.error ?? "";
  }
}
