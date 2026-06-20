/**
 * A message that can be produced as a polyglot envelope. Implement it on your
 * own classes/objects so {@link EnvelopeCodec.fromMessage} can build the canonical
 * envelope without ever leaking a language-specific class name onto the wire.
 */
export interface PolyglotMessage {
  /** The stable URN that identifies this message across languages. */
  getBabelUrn(): string;
  /** The pure-JSON payload (no class instances). */
  toPayload(): Record<string, unknown>;
}

/**
 * Optionally implemented alongside {@link PolyglotMessage} to continue an existing
 * distributed trace instead of minting a fresh one.
 */
export interface HasTraceId {
  /** The trace id to reuse, or null/undefined to mint a new one. */
  getBabelTraceId(): string | null | undefined;
}

/**
 * Out-of-band per-message transport metadata that rides **beside** the frozen wire
 * envelope, never inside it (GR-1: the envelope stays `schema_version: 1`). It is a
 * plain string→string map — the seam the core and the `babelqueue-node-adapters`
 * transports (bullmq/redis/rabbitmq/sqs) agree on for metadata that must not become an
 * envelope field.
 *
 * The first rider is the W3C `traceparent` (and `tracestate`) header for true cross-hop
 * span parent-child linkage (ADR-0028) — see `@babelqueue/core/otel`. It is the Node
 * counterpart of the Go `HeaderPublisher.PublishWithHeaders` / `ReceivedMessage.Headers`
 * seam and the same shape used by the replay-bypass marker (ADR-0027).
 *
 * An adapter carries it on its transport's native per-message metadata channel (e.g.
 * AMQP message headers, SQS `MessageAttributes`, a Redis transport-owned frame), merging
 * it beside the contract `bq-*`/`x-*` headers without clobbering them. A transport that
 * has no such channel simply does not carry it, and propagation degrades to the v0.1
 * `trace_id` correlation with no error — exactly as the Go side degrades.
 */
export type HeaderCarrier = Record<string, string>;
