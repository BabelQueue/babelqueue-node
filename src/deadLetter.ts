import type { DeadLetter, Envelope } from "./codec.js";
import { SOURCE_LANG } from "./codec.js";

/** Options for {@link annotate}. */
export interface AnnotateOptions {
  /** Defaults to the envelope's current `attempts`. */
  attempts?: number;
  /** A human-readable error message (JSON `null` when omitted). */
  error?: string | null;
  /** The originating error type/class name (JSON `null` when omitted). */
  exception?: string | null;
}

/**
 * Return a copy of the envelope with a `dead_letter` block attached, recording
 * why and where it failed. The original envelope is preserved unchanged inside
 * the result, so any-language consumers can still read it.
 */
export function annotate(
  envelope: Envelope,
  reason: string,
  originalQueue: string,
  options: AnnotateOptions = {},
): Envelope {
  const deadLetter: DeadLetter = {
    reason,
    error: options.error ?? null,
    exception: options.exception ?? null,
    failed_at: Date.now(),
    original_queue: originalQueue,
    attempts: options.attempts ?? envelope.attempts ?? 0,
    lang: SOURCE_LANG,
  };

  return { ...envelope, dead_letter: deadLetter };
}
