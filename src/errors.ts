/** Base error for all BabelQueue failures. */
export class BabelQueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BabelQueueError";
  }
}

/** Raised when no handler is mapped for a message URN. */
export class UnknownUrnError extends BabelQueueError {
  constructor(urn: string) {
    super(`No handler is mapped for the message URN "${urn}".`);
    this.name = "UnknownUrnError";
  }
}

/**
 * Raised when a message's `data` does not match the JSON Schema registered for its URN
 * (ADR-0024). The consumer-side {@link schema.wrap} throws it so the adapter redelivers
 * (and eventually dead-letters) a poison message.
 */
export class InvalidPayloadError extends BabelQueueError {
  constructor(
    readonly urn: string,
    readonly violation: string,
  ) {
    super(`Message data for "${urn}" does not match its URN schema: ${violation}.`);
    this.name = "InvalidPayloadError";
  }
}

/**
 * Raised when a producer supplies an envelope key that must never be written: a
 * non-canonical key (message-envelope.md section 10), a collision with a core `meta` key,
 * or an alias key such as a top-level `urn` next to `job` (ADR-0031). Thrown by
 * {@link EnvelopeCodec.make} and {@link EnvelopeCodec.encode}; decode never throws it.
 */
export class InvalidEnvelopeKeyError extends BabelQueueError {
  constructor(
    /** The offending key as an RFC 6901 pointer, e.g. `/meta/ts`. */
    readonly pointer: string,
    reason: string,
  ) {
    super(`BabelQueue: cannot encode envelope key ${pointer}: ${reason}.`);
    this.name = "InvalidEnvelopeKeyError";
  }
}
