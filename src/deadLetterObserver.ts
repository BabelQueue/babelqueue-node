/**
 * The dead-letter observer seam (ADR-0039 section 5.1).
 *
 * The zero-dependency core owns only these types. A component that writes dead-letter records
 * (a transport consumer in a `babelqueue-node-adapters` package; the Node core has no consume
 * runtime of its own, ADR-0039 section 5.2) takes a {@link DeadLetterObserver} as an optional
 * consumer option and calls it once per dead-letter the SDK itself carried out, after the write
 * (or the native dead-letter call) is confirmed. The `@babelqueue/core/otel` module provides an
 * observer that counts them (`createDeadLetterObserver`).
 *
 * Rules for the caller of the observer (ADR-0039 section 5.1):
 *
 * - Call it only after a confirmed BabelQueue DLQ write, or after an SDK-initiated native
 *   dead-letter that actually dead-lettered. Never for a failed write, a native no-op (SQS), a
 *   broker-autonomous dead-letter, or a dropped message.
 * - Isolate it: an error thrown, or a promise rejected, by the observer is caught and logged and
 *   never changes the settle outcome (no ack, nack or retry is skipped or added because of it).
 */

/** One dead-letter the SDK completed. */
export interface DeadLetterEvent {
  /** The source queue/topic the message was consumed from (`meta.queue`, else the consumer's configured queue); `""` when unknown. */
  readonly queue: string;
  /** The DLQ the record was written to; `""` for a native dead-letter (the broker's own `$DeadLetterQueue`). */
  readonly deadLetterQueue: string;
  /** The `dead_letter.reason` the SDK wrote; `""` for a raw body or a native dead-letter. */
  readonly reason: string;
}

/** Called once per dead-letter the SDK itself completed. See the module notes for the calling rules. */
export type DeadLetterObserver = (event: DeadLetterEvent) => void | Promise<void>;
