import { randomUUID } from "node:crypto";

import type { HasTraceId, PolyglotMessage } from "./contracts.js";
import { BabelQueueError, InvalidEnvelopeKeyError } from "./errors.js";

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
  /**
   * Producer extension keys (ADR-0031): flat vendor keys and the recommended
   * `meta.ext.<vendor>` block. Unknown keys read off the wire are preserved here.
   */
  [extension: string]: unknown;
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
  /**
   * Producer extension entries written into `meta` next to the core keys (ADR-0031 (b)),
   * e.g. `{ ext: { acme: { tier: "gold" } } }`. Throws {@link InvalidEnvelopeKeyError} when a
   * key collides with a core `meta` key (`id`, `queue`, `lang`, `schema_version`,
   * `created_at`) or is a non-canonical key (message-envelope.md section 10); the core never
   * silently overwrites or drops a key. The map is copied shallowly (like `data`): nested
   * values are shared with the envelope, so do not mutate them after `make`.
   */
  meta?: Record<string, unknown>;
}

/** Options for {@link EnvelopeCodec.decode}. */
export interface DecodeOptions {
  /**
   * Receives one human-readable warning per non-canonical key found on the wire
   * (message-envelope.md §10) and per `urn` alias dropped because `job` is present
   * (ADR-0031 (c)). The message always names the key as an RFC 6901
   * pointer (e.g. `/meta/max_retries`, `/urn`). Called for every occurrence. Defaults to
   * `process.emitWarning` with the code {@link FORBIDDEN_KEY_WARNING_CODE} (or
   * {@link NON_STRING_JOB_WARNING_CODE} for an ignored non-string `job`), emitted
   * once per key per process so a high-volume legacy producer cannot flood stderr.
   */
  onWarning?: (message: string) => void;
}

/**
 * Options for {@link EnvelopeCodec.encode}.
 *
 * @deprecated `encode` no longer warns and drops non-canonical keys; it throws
 * {@link InvalidEnvelopeKeyError} (ADR-0031 (b)). `onWarning` is accepted for source
 * compatibility and is never called.
 */
export interface EncodeOptions {
  /** @deprecated Never called; `encode` throws instead of warning. */
  onWarning?: (message: string) => void;
}

/** The `process.emitWarning` code used for keys dropped on decode. */
export const FORBIDDEN_KEY_WARNING_CODE = "BABELQUEUE_FORBIDDEN_KEY";

/** The `process.emitWarning` code used when a non-string `job` is ignored on decode (ADR-0032 section 6). */
export const NON_STRING_JOB_WARNING_CODE = "BABELQUEUE_NON_STRING_JOB";

/** Non-canonical top-level keys (message-envelope.md §10). */
const FORBIDDEN_TOP_LEVEL_KEYS = ["timestamp"] as const;

/** Non-canonical `meta` keys (message-envelope.md §10). */
const FORBIDDEN_META_KEYS = ["max_retries", "attempts", "source", "ts"] as const;

/** Default-channel warnings already emitted in this process (one per key and direction). */
const emittedWarnings = new Set<string>();

function defaultWarning(message: string, code: string = FORBIDDEN_KEY_WARNING_CODE): void {
  if (emittedWarnings.has(message)) return;
  emittedWarnings.add(message);
  if (typeof process !== "undefined" && typeof process.emitWarning === "function") {
    process.emitWarning(message, { code });
  }
}

/**
 * Remove every non-canonical key from `envelope` in place, reporting each one via
 * `warn`. Returns the same object for chaining.
 */
function stripForbiddenKeys<T extends object>(
  envelope: T,
  warn: (message: string) => void,
): T {
  const record = envelope as Record<string, unknown>;
  for (const key of FORBIDDEN_TOP_LEVEL_KEYS) {
    if (Object.prototype.hasOwnProperty.call(record, key)) {
      delete record[key];
      warn(forbiddenKeyMessage(`/${key}`));
    }
  }
  const meta = record.meta;
  if (meta !== null && typeof meta === "object" && !Array.isArray(meta)) {
    const metaRecord = meta as Record<string, unknown>;
    for (const key of FORBIDDEN_META_KEYS) {
      if (Object.prototype.hasOwnProperty.call(metaRecord, key)) {
        delete metaRecord[key];
        warn(forbiddenKeyMessage(`/meta/${key}`));
      }
    }
  }
  return envelope;
}

/** The core `meta` keys a producer can never override (message-envelope.md section 3). */
const CORE_META_KEYS = ["id", "queue", "lang", "schema_version", "created_at"] as const;

/** Whether one UTF-16 code unit is in the closed ASCII whitespace set of ADR-0031 (c): space, \t, \n, \v, \f, \r. */
function isAsciiWhitespaceCode(code: number): boolean {
  return code === 0x20 || (code >= 0x09 && code <= 0x0d);
}

/**
 * Cuts surrounding ASCII whitespace only (ADR-0032 section 5); U+00A0, U+2003 and U+3000 stay part of
 * the value. Never `trim()`, which also cuts Unicode spaces. A two-ended index scan: linear in the
 * input, unlike an edge-anchored regex, which backtracks quadratically on long whitespace runs.
 */
function trimAscii(value: string): string {
  let start = 0;
  let end = value.length;
  while (start < end && isAsciiWhitespaceCode(value.charCodeAt(start))) {
    start++;
  }
  while (end > start && isAsciiWhitespaceCode(value.charCodeAt(end - 1))) {
    end--;
  }
  return start === 0 && end === value.length ? value : value.slice(start, end);
}

/** Whether the value is empty or only ASCII whitespace (same closed set as {@link trimAscii}). */
function isAsciiBlank(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    if (!isAsciiWhitespaceCode(value.charCodeAt(i))) {
      return false;
    }
  }
  return true;
}

function hasOwn(record: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(record, key);
}

/**
 * Throw {@link InvalidEnvelopeKeyError} for the first non-canonical key found on an
 * envelope about to be encoded (message-envelope.md section 10): top-level `timestamp`
 * and `urn` (the inbound alias is never written), `meta.max_retries|attempts|source|ts`.
 */
function assertEncodable(envelope: object): void {
  const record = envelope as Record<string, unknown>;
  for (const key of FORBIDDEN_TOP_LEVEL_KEYS) {
    if (hasOwn(record, key)) {
      throw new InvalidEnvelopeKeyError(`/${key}`, "non-canonical envelope key (message-envelope.md section 10)");
    }
  }
  if (hasOwn(record, "urn")) {
    throw new InvalidEnvelopeKeyError("/urn", "the inbound alias of `job` is never written (message-envelope.md section 10)");
  }
  const meta = record.meta;
  if (meta !== null && typeof meta === "object" && !Array.isArray(meta)) {
    for (const key of FORBIDDEN_META_KEYS) {
      if (hasOwn(meta, key)) {
        throw new InvalidEnvelopeKeyError(`/meta/${key}`, "non-canonical envelope key (message-envelope.md section 10)");
      }
    }
  }
}

/** Validate producer `meta` entries and return a shallow copy (ADR-0031 (b)). */
function checkProducerMeta(meta: Record<string, unknown>): Record<string, unknown> {
  if (meta === null || typeof meta !== "object" || Array.isArray(meta)) {
    throw new BabelQueueError("MakeOptions.meta must be an object of extension entries.");
  }
  for (const key of Object.keys(meta)) {
    if ((CORE_META_KEYS as readonly string[]).includes(key)) {
      throw new InvalidEnvelopeKeyError(`/meta/${key}`, "collides with a core meta key");
    }
    if ((FORBIDDEN_META_KEYS as readonly string[]).includes(key)) {
      throw new InvalidEnvelopeKeyError(`/meta/${key}`, "non-canonical envelope key (message-envelope.md section 10)");
    }
  }
  return { ...meta };
}

function nonStringJobMessage(): string {
  return "BabelQueue: ignoring /job: it is not a string (ADR-0032 section 6); a valid urn alias is used instead, if present.";
}

function forbiddenKeyMessage(pointer: string): string {
  return `BabelQueue: dropped non-canonical envelope key ${pointer} (message-envelope.md §10); it is never re-emitted.`;
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
   * `options.meta` entries are written into `meta` next to the core keys (ADR-0031).
   * Throws {@link BabelQueueError} when the URN is blank, and
   * {@link InvalidEnvelopeKeyError} when `options.meta` collides with a core `meta` key or
   * carries a non-canonical key.
   */
  make(
    urn: string,
    data: Record<string, unknown>,
    options: MakeOptions = {},
  ): Envelope {
    const resolvedUrn = trimAscii(urn ?? "");
    if (resolvedUrn === "") {
      throw new BabelQueueError(
        "A polyglot message must expose a stable, non-empty URN so consumers can identify it without any class name.",
      );
    }

    const traceId = trimAscii(options.traceId ?? "") || randomUUID();

    const extension = options.meta === undefined ? {} : checkProducerMeta(options.meta);

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
        ...extension,
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
   * written: if the caller put one on the envelope (top-level `timestamp` or `urn`,
   * `meta.max_retries|attempts|source|ts`) this throws {@link InvalidEnvelopeKeyError}
   * and produces no output (ADR-0031 (b)). Decode, by contrast, still drops them with a
   * warning. The second parameter is deprecated and ignored.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars -- deprecated, kept for source compatibility
  encode(envelope: Envelope, _options?: EncodeOptions): string {
    assertEncodable(envelope);
    return JSON.stringify(envelope);
  },

  /**
   * Parse a raw JSON body. Returns `{}` for malformed or non-object input (call
   * {@link EnvelopeCodec.accepts} before trusting it). Resolves the `urn` inbound
   * alias into `job` (ADR-0031 (c)): a missing, empty or ASCII-whitespace-only `job`
   * (space, `\t`, `\n`, `\v`, `\f`, `\r`) counts as absent and the URN is read from `urn`;
   * if `job` is present, `job` wins and a `urn` next to it is dropped with a warning
   * naming `/urn`. A `job` that is not a string (null, number, boolean, object, array) is
   * ignored with a warning and the URN is read from a valid `urn` alias (ADR-0032 section 6);
   * with no valid identity at all the result fails {@link EnvelopeCodec.accepts}. The `urn` key is never kept on the decoded envelope. Unknown keys are
   * kept (forward compatibility); the non-canonical keys of message-envelope.md §10
   * (`timestamp`, `meta.max_retries`, `meta.attempts`, `meta.source`, `meta.ts`) are
   * dropped with a warning — decode still succeeds so older producers keep working.
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

    const warn = options.onWarning ?? defaultWarning;
    const envelope = stripForbiddenKeys(parsed as IncomingEnvelope, warn);
    // ADR-0032 section 6: a `job` that is not a string (null, number, boolean, object, array)
    // is ignored with a warning, so a valid `urn` alias can stand in for it.
    const rawJob: unknown = envelope.job;
    const jobIsNonString = rawJob !== undefined && typeof rawJob !== "string";
    if (jobIsNonString) {
      const warnJob = options.onWarning ?? ((message: string) => defaultWarning(message, NON_STRING_JOB_WARNING_CODE));
      warnJob(nonStringJobMessage());
    }
    if (hasOwn(envelope, "urn")) {
      const jobAbsent =
        jobIsNonString || rawJob === undefined || (typeof rawJob === "string" && isAsciiBlank(rawJob));
      if (!jobAbsent) {
        delete envelope.urn;
        warn(forbiddenKeyMessage("/urn"));
      } else if (typeof envelope.urn === "string") {
        envelope.job = envelope.urn;
        delete envelope.urn;
      } else {
        // No usable `job` and a `urn` that cannot stand in for it (number, null, object ...):
        // the key is still never kept (ADR-0031 (c)), and dropping it is announced.
        delete envelope.urn;
        warn(forbiddenKeyMessage("/urn"));
      }
    }
    return envelope;
  },

  /**
   * The message URN — canonical `job`, with `urn` accepted as an alias. Surrounding ASCII
   * whitespace is cut and a blank value is `""`; only the closed ASCII set (space, `\t`,
   * `\n`, `\v`, `\f`, `\r`) counts, never Unicode spaces such as U+2003.
   */
  urn(envelope: IncomingEnvelope): string {
    const value = envelope?.job ?? envelope?.urn ?? "";
    if (typeof value !== "string" || isAsciiBlank(value)) {
      return "";
    }
    return trimAscii(value);
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
    if (typeof traceId !== "string" || isAsciiBlank(traceId)) {
      return false;
    }

    return true;
  },
} as const;
