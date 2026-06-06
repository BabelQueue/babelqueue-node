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
