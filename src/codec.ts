import { randomUUID } from "node:crypto";

import type { HasTraceId, PolyglotMessage } from "./contracts.js";
import { BabelQueueError } from "./errors.js";

/** The wire envelope schema version this core implements (versioned independently of the package version). */
export const SCHEMA_VERSION = 1;

/** Stamped into `meta.lang` for envelopes produced by this core. */
export const SOURCE_LANG = "node";

/** Immutable per-message metadata. */
export interface Meta {
  id: string;
  queue: string;
  lang: string;
  schema_version: number;
  /** Unix milliseconds, UTC. */
  created_at: number;
}

/** The additive block appended to an envelope when a message is dead-lettered. */
export interface DeadLetter {
  reason: string;
  error: string | null;
  exception: string | null;
  /** Unix milliseconds, UTC. */
  failed_at: number;
  original_queue: string;
  attempts: number;
  lang: string;
}

/**
 * The canonical BabelQueue wire message: a strict, language-neutral JSON shape
 * that every SDK produces and consumes identically. The property order here is
 * significant — it matches the other cores so {@link EnvelopeCodec.encode} is
 * byte-for-byte identical across the insertion-order languages (PHP/Python).
 */
export interface Envelope {
  /** The message URN (never a class name). */
  job: string;
  /** Correlation id, preserved across every hop. */
  trace_id: string;
  /** The pure-JSON payload. */
  data: Record<string, unknown>;
  meta: Meta;
  /** Top-level transport retry counter. */
  attempts: number;
  /** Present only once the message has been dead-lettered. */
  dead_letter?: DeadLetter;
}

/**
 * A decoded, not-yet-validated envelope. Fields are loosely typed because they
 * come off the wire; `urn` is accepted as an inbound alias for `job`. Narrow it
 * with {@link EnvelopeCodec.accepts} before trusting the contents.
 */
export interface IncomingEnvelope {
  job?: string;
  /** Inbound alias for `job`. */
  urn?: string;
  trace_id?: string;
  data?: unknown;
  meta?: unknown;
  attempts?: unknown;
  dead_letter?: unknown;
}

/** Options for {@link EnvelopeCodec.make}. */
export interface MakeOptions {
  /** Logical queue name recorded in `meta.queue` (default `"default"`). */
  queue?: string;
  /** Reuse an existing trace id (trace continuation) instead of minting one. */
  traceId?: string;
}

/** Options for {@link EnvelopeCodec.decode}. */
export interface DecodeOptions {
  /**
   * Receives one human-readable warning per non-canonical key found on the wire
   * (message-envelope.md §10). The message always names the key as an RFC 6901
   * pointer (e.g. `/meta/max_retries`). Called for every occurrence. Defaults to
   * `process.emitWarning` with the code {@link FORBIDDEN_KEY_WARNING_CODE}, emitted
   * once per key per process so a high-volume legacy producer cannot flood stderr.
   */
  onWarning?: (message: string) => void;
}

/** Options for {@link EnvelopeCodec.encode}. */
export interface EncodeOptions {
  /**
   * Receives one human-readable warning per non-canonical key the caller put on the
   * envelope (message-envelope.md §10); the key is left out of the output. Called
   * for every occurrence. Defaults to `process.emitWarning` with the code
   * {@link FORBIDDEN_KEY_WARNING_CODE}, emitted once per key per process.
   */
  onWarning?: (message: string) => void;
}

/** The `process.emitWarning` code used for dropped non-canonical keys. */
export const FORBIDDEN_KEY_WARNING_CODE = "BABELQUEUE_FORBIDDEN_KEY";

/** Non-canonical top-level keys (message-envelope.md §10). */
const FORBIDDEN_TOP_LEVEL_KEYS = ["timestamp"] as const;

/** Non-canonical `meta` keys (message-envelope.md §10). */
const FORBIDDEN_META_KEYS = ["max_retries", "attempts", "source", "ts"] as const;

/** Default-channel warnings already emitted in this process (one per key and direction). */
const emittedWarnings = new Set<string>();

function defaultWarning(message: string): void {
  if (emittedWarnings.has(message)) return;
  emittedWarnings.add(message);
  if (typeof process !== "undefined" && typeof process.emitWarning === "function") {
    process.emitWarning(message, { code: FORBIDDEN_KEY_WARNING_CODE });
  }
}

/**
 * Remove every non-canonical key from `envelope` in place, reporting each one via
 * `warn` (when given). Returns the same object for chaining.
 */
function stripForbiddenKeys<T extends object>(
  envelope: T,
  warn?: (message: string) => void,
  message: (pointer: string) => string = forbiddenKeyMessage,
): T {
  const record = envelope as Record<string, unknown>;
  for (const key of FORBIDDEN_TOP_LEVEL_KEYS) {
    if (Object.prototype.hasOwnProperty.call(record, key)) {
      delete record[key];
      warn?.(message(`/${key}`));
    }
  }
  const meta = record.meta;
  if (meta !== null && typeof meta === "object" && !Array.isArray(meta)) {
    const metaRecord = meta as Record<string, unknown>;
    for (const key of FORBIDDEN_META_KEYS) {
      if (Object.prototype.hasOwnProperty.call(metaRecord, key)) {
        delete metaRecord[key];
        warn?.(message(`/meta/${key}`));
      }
    }
  }
  return envelope;
}

function hasForbiddenKeys(envelope: object): boolean {
  const record = envelope as Record<string, unknown>;
  if (FORBIDDEN_TOP_LEVEL_KEYS.some((k) => Object.prototype.hasOwnProperty.call(record, k))) {
    return true;
  }
  const meta = record.meta;
  return (
    meta !== null &&
    typeof meta === "object" &&
    FORBIDDEN_META_KEYS.some((k) => Object.prototype.hasOwnProperty.call(meta, k))
  );
}

function forbiddenKeyMessage(pointer: string): string {
  return `BabelQueue: dropped non-canonical envelope key ${pointer} (message-envelope.md §10); it is never re-emitted.`;
}

function forbiddenEncodeKeyMessage(pointer: string): string {
  return `BabelQueue: not encoding non-canonical envelope key ${pointer} (message-envelope.md §10); remove it from the envelope.`;
}

/**
 * Builds, encodes and decodes the canonical envelope — the single Node/TypeScript
 * implementation of the wire format.
 */
export const EnvelopeCodec = {
  SCHEMA_VERSION,
  SOURCE_LANG,

  /**
   * Build the canonical envelope for a `(urn, data)` pair. Mints a fresh trace id
   * unless `options.traceId` is given, starts `attempts` at 0, and stamps `meta`.
   * Throws {@link BabelQueueError} when the URN is blank.
   */
  make(
    urn: string,
    data: Record<string, unknown>,
    options: MakeOptions = {},
  ): Envelope {
    const resolvedUrn = (urn ?? "").trim();
    if (resolvedUrn === "") {
      throw new BabelQueueError(
        "A polyglot message must expose a stable, non-empty URN so consumers can identify it without any class name.",
      );
    }

    const traceId = (options.traceId ?? "").trim() || randomUUID();

    return {
      job: resolvedUrn,
      trace_id: traceId,
      data: { ...data },
      meta: {
        id: randomUUID(),
        queue: options.queue ?? "default",
        lang: SOURCE_LANG,
        schema_version: SCHEMA_VERSION,
        created_at: Date.now(),
      },
      attempts: 0,
    };
  },

  /**
   * Build the envelope from a {@link PolyglotMessage}. If the message also
   * implements {@link HasTraceId} and returns a non-empty value, that trace id is
   * reused.
   */
  fromMessage(
    message: PolyglotMessage & Partial<HasTraceId>,
    queue = "default",
  ): Envelope {
    const traceId =
      typeof message.getBabelTraceId === "function"
        ? (message.getBabelTraceId() ?? undefined)
        : undefined;

    return EnvelopeCodec.make(message.getBabelUrn(), message.toPayload(), {
      queue,
      traceId,
    });
  },

  /**
   * Encode the envelope as compact UTF-8 JSON. `JSON.stringify` already emits the
   * canonical form — no spaces, and slashes/unicode/HTML left unescaped — matching
   * the other SDK cores. Non-canonical keys (message-envelope.md §10) are never
   * emitted: if the caller put one on the envelope it is left out of the output and
   * a warning names it (the caller's object is not modified).
   */
  encode(envelope: Envelope, options: EncodeOptions = {}): string {
    if (hasForbiddenKeys(envelope)) {
      const copy = { ...envelope, meta: { ...envelope.meta } };
      const warn = options?.onWarning ?? defaultWarning;
      return JSON.stringify(stripForbiddenKeys(copy, warn, forbiddenEncodeKeyMessage));
    }
    return JSON.stringify(envelope);
  },

  /**
   * Parse a raw JSON body. Returns `{}` for malformed or non-object input (call
   * {@link EnvelopeCodec.accepts} before trusting it). Resolves the `urn` inbound
   * alias into `job`. Unknown keys are kept (forward compatibility); the
   * non-canonical keys of message-envelope.md §10 (`timestamp`, `meta.max_retries`,
   * `meta.attempts`, `meta.source`, `meta.ts`) are dropped with a warning — decode
   * still succeeds so older producers keep working.
   */
  decode(raw: string, options: DecodeOptions = {}): IncomingEnvelope {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return {};
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const envelope = stripForbiddenKeys(
      parsed as IncomingEnvelope,
      options.onWarning ?? defaultWarning,
    );
    if (!envelope.job && typeof envelope.urn === "string") {
      envelope.job = envelope.urn;
    }
    return envelope;
  },

  /** The message URN — canonical `job`, with `urn` accepted as an alias. */
  urn(envelope: IncomingEnvelope): string {
    const value = envelope?.job ?? envelope?.urn ?? "";
    return typeof value === "string" ? value.trim() : "";
  },

  /**
   * Whether a consumer should accept this envelope. Rejects a missing URN, an
   * unsupported `meta.schema_version`, a non-object `data`, a non-integer
   * `attempts`, or a blank `trace_id` — the consumer-side counterpart to the
   * producer JSON Schema. Acts as a type guard that narrows to {@link Envelope}.
   */
  accepts(envelope: IncomingEnvelope): envelope is Envelope {
    if (EnvelopeCodec.urn(envelope) === "") {
      return false;
    }

    const meta = envelope.meta;
    if (
      meta === null ||
      typeof meta !== "object" ||
      (meta as Meta).schema_version !== SCHEMA_VERSION
    ) {
      return false;
    }

    const data = envelope.data;
    if (data === null || typeof data !== "object" || Array.isArray(data)) {
      return false;
    }

    const attempts = envelope.attempts;
    if (typeof attempts !== "number" || !Number.isInteger(attempts)) {
      return false;
    }

    const traceId = envelope.trace_id;
    if (typeof traceId !== "string" || traceId.trim() === "") {
      return false;
    }

    return true;
  },
} as const;
