/**
 * BabelQueue — Polyglot Queues, Simplified.
 *
 * The framework-agnostic Node/TypeScript core: the canonical wire-envelope codec,
 * contracts and dead-letter helpers. Zero runtime dependencies.
 *
 * ```ts
 * import { EnvelopeCodec } from "@babelqueue/core";
 *
 * const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1042 });
 * const body = EnvelopeCodec.encode(env); // publish body to Redis / RabbitMQ / ...
 * ```
 *
 * Full spec: https://babelqueue.com
 */

export { EnvelopeCodec, SCHEMA_VERSION, SOURCE_LANG } from "./codec.js";
export type {
  DeadLetter,
  Envelope,
  IncomingEnvelope,
  MakeOptions,
  Meta,
} from "./codec.js";

export type { HasTraceId, HeaderCarrier, PolyglotMessage } from "./contracts.js";

export { annotate } from "./deadLetter.js";
export type { AnnotateOptions } from "./deadLetter.js";
export * as deadLetter from "./deadLetter.js";

export { UnknownUrnStrategy } from "./routing.js";

export { BabelQueueError, UnknownUrnError, InvalidPayloadError } from "./errors.js";

export { Wrap, InMemoryStore } from "./idempotency.js";
export type { Handler, Store } from "./idempotency.js";

export * as schema from "./schema.js";
export type { SchemaProvider, SchemaNode } from "./schema.js";

export { redrive, resetForRedrive } from "./redrive.js";
export type {
  RedriveIO,
  RedriveItem,
  RedriveMessage,
  RedriveOptions,
  RedriveResult,
} from "./redrive.js";

export { HEADER_REPLAY_BYPASS, isReplay, bypassExternalEffects } from "./replay.js";
