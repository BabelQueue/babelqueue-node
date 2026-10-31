/**
 * Optional OpenTelemetry tracing — the Node mirror of `babelqueue-go/otel`.
 *
 * Cross-hop trace propagation works at two layered levels:
 *
 *   - **`trace_id` ↔ OTel trace id (ADR-0025, v0.1):** the envelope's `trace_id` (a UUID)
 *     maps 1:1 to a 32-hex OTel trace id, so every hop that shares a `trace_id` shares one
 *     trace — correlation and per-hop timing with zero wire/transport change.
 *   - **W3C `traceparent` transport header (ADR-0028, v0.2):** the producer also injects the
 *     active span context as a `traceparent` (and `tracestate`) header onto an out-of-band
 *     {@link HeaderCarrier} that rides **beside** the frozen envelope, never inside it (GR-1).
 *     The consumer reads it and starts its span as a true **child** of the producer span —
 *     real cross-hop parent→child linkage and per-hop span timing, not just a shared trace.
 *     When no `traceparent` header is present it falls back to the v0.1 `trace_id` behaviour,
 *     so enabling propagation is a strict, backward-compatible upgrade — never a regression.
 *
 * The wire envelope is untouched (GR-1) and the zero-dependency core never imports
 * OpenTelemetry: this module pulls `@opentelemetry/api` as an **optional peer dependency**
 * (GR-7) and is reached only via the `@babelqueue/core/otel` subpath, so importing the core
 * itself stays dependency-free. The W3C `traceparent` parse/format is implemented here
 * directly against the frozen W3C Trace Context format rather than pulling
 * `@opentelemetry/core`, keeping the optional dependency at the API only.
 *
 * ```ts
 * import { trace } from "@opentelemetry/api";
 * import { publish, wrapHandler } from "@babelqueue/core/otel";
 * import type { HeaderCarrier } from "@babelqueue/core";
 *
 * const tracer = trace.getTracer("orders");
 *
 * // PRODUCER: the adapter passes a headers carrier; the active span's traceparent is
 * // written into it, and the adapter carries it on its transport's metadata channel.
 * const headers: HeaderCarrier = {};
 * await publish(tracer, "urn:babel:orders:created", { order_id: 1 },
 *   (env) => myTransport.send(env, headers), { headers });
 *
 * // CONSUMER: the adapter surfaces the delivered message's headers; wrapHandler reads the
 * // traceparent and starts the span as a child of the producer span.
 * const traced = wrapHandler(tracer, async (env) => { ... },
 *   () => deliveredMessage.headers);
 * ```
 *
 * **Adapter wiring is a documented follow-up.** The Node transports/brokers live in a
 * separate repo (`babelqueue-node-adapters`: bullmq/redis/rabbitmq/sqs). This core delivers
 * the v0.2 *mechanism* — the {@link HeaderCarrier} seam plus the `traceparent` inject/extract.
 * Carrying the headers on each transport's native per-message metadata channel (AMQP headers,
 * SQS `MessageAttributes`, a Redis transport-owned frame), beside the contract `bq-*`/`x-*`
 * headers, is the per-adapter rollout — the same seam ADR-0027 and the broker bindings roll
 * out per SDK. Until an adapter wires it, propagation degrades to the v0.1 `trace_id`
 * correlation with no error.
 */

import { createHash } from "node:crypto";

import {
  SpanKind,
  SpanStatusCode,
  TraceFlags,
  context as otelContext,
  createTraceState,
  isSpanContextValid,
  trace,
  type Attributes,
  type Context,
  type Counter,
  type Histogram,
  type Meter,
  type Span,
  type SpanContext,
  type Tracer,
} from "@opentelemetry/api";

import { EnvelopeCodec, type Envelope, type MakeOptions } from "./codec.js";
import type { HeaderCarrier } from "./contracts.js";
import type { DeadLetterEvent, DeadLetterObserver } from "./deadLetterObserver.js";
import type { Handler } from "./idempotency.js";

/**
 * The OpenTelemetry semantic-conventions release the span and metric shapes follow
 * (ADR-0038 section 1). A later bump is a code + doc change in a MINOR plus a conformance update.
 */
export const SEMCONV_VERSION = "1.44.0";

/**
 * The semantic-conventions schema URL for {@link SEMCONV_VERSION}. The caller creates its tracer
 * and meter, so it passes this value when it does: `trace.getTracer(name, version, { schemaUrl })`
 * and `metrics.getMeter(name, version, { schemaUrl })`. The SDK never wraps or replaces a
 * caller-supplied tracer or meter (ADR-0038 section 1).
 */
export const SEMCONV_SCHEMA_URL = `https://opentelemetry.io/schemas/${SEMCONV_VERSION}`;

/** The `messaging.system` value used when no `system` option is given (deprecated default, ADR-0038 section 5). */
const DEFAULT_SYSTEM = "babelqueue";
const INVALID_TRACE_ID = "00000000000000000000000000000000";
const INVALID_SPAN_ID = "0000000000000000";

/**
 * The out-of-band transport-header keys that carry W3C Trace Context across a hop
 * (ADR-0028). They ride on a {@link HeaderCarrier} beside the frozen envelope — the same
 * seam as the replay-bypass marker (ADR-0027) — so a consumer can start its span as a true
 * child of the producer span. The names are the W3C standard, so a babelqueue `traceparent`
 * interoperates with any OTel SDK or W3C-compliant peer.
 */
export const HEADER_TRACEPARENT = "traceparent";
export const HEADER_TRACESTATE = "tracestate";

// W3C Trace Context `traceparent` is a frozen, fixed-width format:
//   `<version>-<32-hex trace id>-<16-hex parent span id>-<2-hex flags>`
// This regex mirrors `@opentelemetry/core`'s parser exactly (version != ff, non-zero
// trace/span ids, optional trailing parts rejected for version 00), so acceptance matches
// the standard propagator without taking a dependency on it.
const VERSION_PART = "(?!ff)[\\da-f]{2}";
const TRACE_ID_PART = "(?![0]{32})[\\da-f]{32}";
const PARENT_ID_PART = "(?![0]{16})[\\da-f]{16}";
const FLAGS_PART = "[\\da-f]{2}";
const TRACEPARENT_REGEX = new RegExp(
  `^\\s?(${VERSION_PART})-(${TRACE_ID_PART})-(${PARENT_ID_PART})-(${FLAGS_PART})(-.*)?\\s?$`,
);

/* -------------------------------------------------------------------------- */
/*  trace_id ↔ OTel trace id (v0.1, ADR-0025 Option 1)                        */
/* -------------------------------------------------------------------------- */

/**
 * Map an envelope `trace_id` to a deterministic 32-hex OTel trace id: a UUID maps to its
 * hex bytes; any other string is hashed (SHA-256, first 16 bytes). The inverse of {@link uuidOf}
 * for the UUID case.
 */
export function traceIdOf(traceId: string): string {
  return decodedTraceId(traceId) ?? hashedTraceId(traceId);
}

/** The 32-hex form of a `trace_id` the mapping decodes directly (hyphens optional, any case), else `undefined`. */
function decodedTraceId(traceId: string): string | undefined {
  const hex = traceId.replace(/-/g, "").toLowerCase();
  return /^[0-9a-f]{32}$/.test(hex) && hex !== INVALID_TRACE_ID ? hex : undefined;
}

function hashedTraceId(traceId: string): string {
  return createHash("sha256").update(traceId).digest("hex").slice(0, 32);
}

/** Hashes (32 hex) of the distinct non-UUID values already warned about; bounded so a producer of random values cannot grow it. */
const warnedTraceIds = new Set<string>();
const WARNED_TRACE_IDS_LIMIT = 1024;

/** The `process.emitWarning` code used when the OTel mapping hashes a non-UUID `trace_id` (ADR-0038 section 7). */
export const NON_UUID_TRACE_ID_WARNING_CODE = "BABELQUEUE_NON_UUID_TRACE_ID";

/**
 * {@link traceIdOf} for the consumer span's parent, which also warns (once per distinct value, at
 * most {@link WARNED_TRACE_IDS_LIMIT} values) when the value is hashed instead of decoded
 * (message-envelope.md section 5.1). The value is preserved on the envelope either way; the
 * warning only reports that the mapping fell back to the hash.
 */
function traceIdForParent(traceId: string): string {
  const decoded = decodedTraceId(traceId);
  if (decoded !== undefined) {
    return decoded;
  }
  const hashed = hashedTraceId(traceId);
  // Keyed by the 32-hex hash, not the raw value: the set stays small however long the trace_id is.
  if (!warnedTraceIds.has(hashed) && warnedTraceIds.size < WARNED_TRACE_IDS_LIMIT) {
    warnedTraceIds.add(hashed);
    if (typeof process !== "undefined" && typeof process.emitWarning === "function") {
      const shown = traceId.length > 64 ? `${traceId.slice(0, 64)}...` : traceId;
      process.emitWarning(
        `BabelQueue: trace_id ${JSON.stringify(shown)} is not a UUID; the OpenTelemetry trace id is the SHA-256 hash of its UTF-8 bytes (first 16 bytes). The value itself is forwarded unchanged.`,
        { code: NON_UUID_TRACE_ID_WARNING_CODE },
      );
    }
  }
  return hashed;
}

/**
 * Format a 32-hex OTel trace id as a canonical UUID string — the form a producer stamps into
 * the message's `trace_id` so a consumer can recover the same trace id via {@link traceIdOf}.
 */
export function uuidOf(traceIdHex: string): string {
  const h = traceIdHex.replace(/-/g, "").toLowerCase().padStart(32, "0").slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/** Deterministic, non-zero 16-hex span id so the remote parent context is valid. */
function spanIdOf(traceId: string): string {
  const sid = createHash("sha256").update(`babelqueue-span:${traceId}`).digest("hex").slice(0, 16);
  return sid === INVALID_SPAN_ID ? "0000000000000001" : sid;
}

/** A context carrying a remote parent in the `trace_id`-derived trace (v0.1 fallback). */
function parentContext(traceId: string): Context {
  return trace.setSpanContext(otelContext.active(), {
    traceId: traceIdForParent(traceId),
    spanId: spanIdOf(traceId),
    traceFlags: TraceFlags.SAMPLED,
    isRemote: true,
  });
}

/* -------------------------------------------------------------------------- */
/*  W3C traceparent inject/extract (v0.2, ADR-0028 — the cross-hop span link) */
/* -------------------------------------------------------------------------- */

/**
 * Format a {@link SpanContext} as a W3C `traceparent` header string, or `undefined` when the
 * context is not valid. Identical wire form to `@opentelemetry/core`'s propagator.
 */
function formatTraceparent(sc: SpanContext): string | undefined {
  if (!isSpanContextValid(sc)) {
    return undefined;
  }
  const flags = `0${Number(sc.traceFlags || TraceFlags.NONE).toString(16)}`.slice(-2);
  return `00-${sc.traceId}-${sc.spanId}-${flags}`;
}

/**
 * Parse a W3C `traceparent` header into a remote {@link SpanContext}, or `undefined` when the
 * header is missing or malformed. Mirrors the standard propagator's acceptance rules, so a
 * garbage value never hijacks the trace — the caller falls back to the v0.1 parent.
 */
function parseTraceparent(traceparent: string | undefined, tracestate?: string): SpanContext | undefined {
  if (!traceparent) {
    return undefined;
  }
  const match = TRACEPARENT_REGEX.exec(traceparent);
  if (!match) {
    return undefined;
  }
  // Per the W3C spec, only reject trailing parts when the version is the known "00".
  if (match[1] === "00" && match[5]) {
    return undefined;
  }
  const sc: SpanContext = {
    traceId: match[2],
    spanId: match[3],
    traceFlags: parseInt(match[4], 16),
    isRemote: true,
  };
  if (tracestate) {
    sc.traceState = createTraceState(tracestate);
  }
  return sc;
}

/**
 * Write the active span context in `ctx` (default: the current active context) as a W3C
 * `traceparent` (and `tracestate`, when present) into `headers`, returning the same carrier.
 *
 * This is the **producer half** of cross-hop linkage (ADR-0028): the resulting carrier is
 * handed to a transport adapter, which carries it beside the frozen envelope so the consumer
 * can reconstruct the remote parent. When `ctx` carries no valid span context the carrier is
 * returned unchanged — a no-trace publish stays header-free (no regression).
 */
export function injectTraceparent(
  headers: HeaderCarrier = {},
  ctx: Context = otelContext.active(),
): HeaderCarrier {
  const sc = trace.getSpanContext(ctx);
  if (!sc) {
    return headers;
  }
  const traceparent = formatTraceparent(sc);
  if (!traceparent) {
    return headers;
  }
  headers[HEADER_TRACEPARENT] = traceparent;
  const serialized = sc.traceState?.serialize();
  if (serialized) {
    headers[HEADER_TRACESTATE] = serialized;
  }
  return headers;
}

/**
 * Build a {@link Context} carrying the remote parent extracted from a delivered message's
 * out-of-band `headers`, or `undefined` when there is no valid `traceparent`.
 *
 * This is the **consumer half** of cross-hop linkage (ADR-0028): a span started from the
 * returned context is a child of the producer's span (remote parent), preserving per-hop span
 * timing and real parent→child links. Returning `undefined` signals the caller to fall back to
 * the v0.1 `trace_id`-derived parent — so a malformed, empty, or absent header never regresses
 * a message produced by a pre-0028 (or header-blind) producer.
 */
export function remoteParentFromHeaders(
  headers: HeaderCarrier | null | undefined,
  base: Context = otelContext.active(),
): Context | undefined {
  if (!headers) {
    return undefined;
  }
  const sc = parseTraceparent(headers[HEADER_TRACEPARENT], headers[HEADER_TRACESTATE]);
  if (!sc) {
    return undefined;
  }
  return trace.setSpanContext(base, sc);
}

/* -------------------------------------------------------------------------- */
/*  Producer / consumer wrappers                                              */
/* -------------------------------------------------------------------------- */

/** Options shared by the producer and consumer wrappers (ADR-0038 section 2, ADR-0039 section 3). */
export interface OtelOptions {
  /**
   * The `messaging.system` value: `"redis"`, `"rabbitmq"`, `"aws_sqs"`, `"servicebus"`, `"pulsar"`,
   * `"kafka"` or `"activemq"` (ADR-0038 section 3). A value outside that set is written as given.
   * Absent or empty means `"babelqueue"`, which is **deprecated**: pass the broker so spans and
   * metrics group by it in APM messaging views.
   */
  system?: string;
  /**
   * The OpenTelemetry `Meter` the standard metric set is recorded on (ADR-0039). With no meter
   * the wrapper records no metrics. Create it with {@link SEMCONV_SCHEMA_URL} if you want the
   * schema URL attached.
   */
  meter?: Meter;
}

/** Options for {@link wrapHandler}. */
export interface WrapHandlerOptions extends OtelOptions {
  /**
   * The `messaging.consumer.group.name` value (intended for Kafka; written whenever given). It is
   * a span attribute only, never a metric attribute (ADR-0039 section 2).
   */
  consumerGroup?: string;
}

function systemOf(system: string | undefined): string {
  return system === undefined || system === "" ? DEFAULT_SYSTEM : system;
}

/** Whether `value` is empty or only ASCII whitespace (the closed set of ADR-0032: space, \t, \n, \v, \f, \r). */
function isBlank(value: string): boolean {
  return /^[ \t\n\v\f\r]*$/.test(value);
}

/** The destination to report: `queue` when it is a non-blank string, else `undefined`. */
function destinationOf(queue: unknown): string | undefined {
  return typeof queue === "string" && !isBlank(queue) ? queue : undefined;
}

/** `<operation> <destination>`, or the bare operation name when there is no destination. */
function spanNameOf(operation: string, destination: string | undefined): string {
  return destination === undefined ? operation : `${operation} ${destination}`;
}

/**
 * The `error.type` value of a failure (ADR-0038 section 6): the constructor name, else the `name`
 * property, else `_OTHER` (a thrown non-error value).
 */
function errorTypeOf(err: unknown): string {
  if (err !== null && typeof err === "object") {
    const constructorName = (err as { constructor?: { name?: unknown } }).constructor?.name;
    if (typeof constructorName === "string" && constructorName !== "" && constructorName !== "Object") {
      return constructorName;
    }
    const name = (err as { name?: unknown }).name;
    if (typeof name === "string" && name !== "") {
      return name;
    }
  }
  return "_OTHER";
}

/** Record a failure on `span`: the exception event, status Error, and `error.type` (ADR-0038 section 6). */
function recordFailure(span: Span, err: unknown): string {
  const errorType = errorTypeOf(err);
  span.recordException(err instanceof Error ? err : String(err));
  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: err instanceof Error ? err.message : String(err),
  });
  span.setAttribute("error.type", errorType);
  return errorType;
}

function consumeAttributes(
  env: Envelope,
  system: string,
  destination: string | undefined,
  consumerGroup: string | undefined,
): Attributes {
  const attempts = Number.isInteger(env.attempts) ? env.attempts : 0;
  const schemaVersion = Number.isInteger(env.meta?.schema_version)
    ? env.meta.schema_version
    : EnvelopeCodec.SCHEMA_VERSION;
  const attributes: Attributes = {
    "messaging.system": system,
    "messaging.operation.type": "process",
    "messaging.operation.name": "process",
    "babelqueue.urn": EnvelopeCodec.urn(env),
    "babelqueue.schema_version": schemaVersion,
    "babelqueue.attempts": attempts,
    // Deprecated keys, dual-emitted for one MINOR (ADR-0038 section 5).
    "messaging.operation": "process",
    "messaging.babelqueue.attempts": attempts,
  };
  if (destination !== undefined) {
    attributes["messaging.destination.name"] = destination;
  }
  const messageId = env.meta?.id;
  if (typeof messageId === "string" && messageId !== "") {
    attributes["messaging.message.id"] = messageId;
  }
  if (typeof env.trace_id === "string") {
    attributes["messaging.message.conversation_id"] = env.trace_id;
  }
  if (consumerGroup !== undefined) {
    attributes["messaging.consumer.group.name"] = consumerGroup;
  }
  return attributes;
}

/* -------------------------------------------------------------------------- */
/*  Metrics (ADR-0039)                                                        */
/* -------------------------------------------------------------------------- */

/** The semconv advisory bucket boundaries of `messaging.process.duration`, in seconds. */
const PROCESS_DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

interface ConsumerInstruments {
  processDuration: Histogram;
  consumed: Counter;
  retries: Counter;
}

interface ProducerInstruments {
  sent: Counter;
}

// One set of instruments per meter, created on first use (ADR-0039 section 3: once, not per message).
const consumerInstruments = new WeakMap<Meter, ConsumerInstruments>();
const producerInstruments = new WeakMap<Meter, ProducerInstruments>();
const deadLetterCounters = new WeakMap<Meter, Counter>();

function consumerInstrumentsOf(meter: Meter): ConsumerInstruments {
  let instruments = consumerInstruments.get(meter);
  if (instruments === undefined) {
    instruments = {
      processDuration: meter.createHistogram("messaging.process.duration", {
        description: "Duration of processing operations.",
        unit: "s",
        advice: { explicitBucketBoundaries: PROCESS_DURATION_BUCKETS },
      }),
      consumed: meter.createCounter("messaging.client.consumed.messages", {
        description: "Number of messages that were delivered to the application.",
        unit: "{message}",
      }),
      retries: meter.createCounter("babelqueue.retries", {
        description: "Number of deliveries that were redeliveries (top-level attempts > 0).",
        unit: "{message}",
      }),
    };
    consumerInstruments.set(meter, instruments);
  }
  return instruments;
}

function producerInstrumentsOf(meter: Meter): ProducerInstruments {
  let instruments = producerInstruments.get(meter);
  if (instruments === undefined) {
    instruments = {
      sent: meter.createCounter("messaging.client.sent.messages", {
        description: "Number of messages producer attempted to send to the broker.",
        unit: "{message}",
      }),
    };
    producerInstruments.set(meter, instruments);
  }
  return instruments;
}

function deadLetterCounterOf(meter: Meter): Counter {
  let counter = deadLetterCounters.get(meter);
  if (counter === undefined) {
    counter = meter.createCounter("babelqueue.dlq.messages", {
      description: "Number of messages the SDK dead-lettered.",
      unit: "{message}",
    });
    deadLetterCounters.set(meter, counter);
  }
  return counter;
}

/**
 * The metric attribute set (ADR-0039 section 2): system, operation name, the destination when
 * known, and `error.type` on failure. Nothing else is ever a metric attribute.
 */
function metricAttributes(
  system: string,
  operationName: string,
  destination: string | undefined,
  errorType?: string,
): Attributes {
  const attributes: Attributes = {
    "messaging.system": system,
    "messaging.operation.name": operationName,
  };
  if (destination !== undefined) {
    attributes["messaging.destination.name"] = destination;
  }
  if (errorType !== undefined) {
    attributes["error.type"] = errorType;
  }
  return attributes;
}

/**
 * A {@link DeadLetterObserver} that increments `babelqueue.dlq.messages` (ADR-0039 section 5.1):
 * `messaging.system` is the `system` option, `messaging.destination.name` is the event's source
 * queue (when known) and `messaging.operation.name` is `process`. With no `meter` it records
 * nothing. The observer never throws: a failing meter cannot change how a message is settled.
 *
 * Wiring it into a transport's dead-letter write point is the transport package's job.
 */
export function createDeadLetterObserver(options: OtelOptions = {}): DeadLetterObserver {
  const { meter } = options;
  if (meter === undefined) {
    return () => undefined;
  }
  const system = systemOf(options.system);
  return (event: DeadLetterEvent): void => {
    try {
      deadLetterCounterOf(meter).add(1, metricAttributes(system, "process", destinationOf(event.queue)));
    } catch {
      // Metrics must never change the settle outcome (ADR-0039 section 5.1, isolation).
    }
  };
}

/** Resolve instruments without letting a failing meter change the wrapped call: `undefined` means "record nothing". */
function instrumentsOrUndefined<T>(meter: Meter | undefined, resolve: (meter: Meter) => T): T | undefined {
  if (meter === undefined) {
    return undefined;
  }
  try {
    return resolve(meter);
  } catch {
    return undefined;
  }
}

/** Never lets a failing meter change the wrapped call's outcome. */
function safely(record: () => void): void {
  try {
    record();
  } catch {
    // swallow: metrics observe, they never alter settlement (ADR-0039 context)
  }
}

/**
 * Resolve a delivered message's out-of-band headers for the consumer span's parent. Accepts a
 * plain carrier or a (sync/async) getter the adapter wires to its transport, so reading headers
 * never forces a particular delivery shape.
 */
type DeliveredHeaders =
  | HeaderCarrier
  | null
  | undefined
  | (() => HeaderCarrier | null | undefined | Promise<HeaderCarrier | null | undefined>);

async function resolveHeaders(source: DeliveredHeaders): Promise<HeaderCarrier | null | undefined> {
  return typeof source === "function" ? source() : source;
}

/**
 * Wrap a consume handler to emit a CONSUMER span `process <queue>` per message, recording the
 * handler's error/status. The handler receives the full {@link Envelope} as before.
 *
 * The span follows the OpenTelemetry messaging semantic conventions v1.44.0 (ADR-0038): the
 * destination is the delivered envelope's `meta.queue` (no destination, and a bare `process`
 * name, when it is absent or blank), `messaging.system` is `options.system`, the BabelQueue
 * identity is in `babelqueue.*`, and a failing handler sets status Error plus `error.type`. The
 * deprecated `messaging.operation` and `messaging.babelqueue.attempts` keys are dual-emitted for
 * one MINOR.
 *
 * With `options.meter`, it also records `messaging.client.consumed.messages`, `babelqueue.retries`
 * (when `attempts > 0`) and `messaging.process.duration` (ADR-0039). The handler's result, or the
 * error it throws, is passed through unchanged.
 *
 * **Parent selection (ADR-0028):** when the adapter supplies the delivered message's
 * out-of-band headers via `headers` and they carry a valid W3C `traceparent`, the span is
 * started as a true **child** of the producer span — real cross-hop parent→child linkage with
 * per-hop span timing. When no `traceparent` is present (no `headers` argument, an empty
 * carrier, or a malformed value) it falls back to the v0.1 behaviour: a remote parent derived
 * from the envelope's `trace_id` (ADR-0025 Option 1), which shares the trace but not the exact
 * span edge. So enabling `traceparent` propagation is a strict, backward-compatible upgrade.
 *
 * `headers` may be a plain carrier or a getter `() => carrier` the adapter wires to its
 * transport (it is read once per delivery, after the envelope is in hand).
 */
export function wrapHandler(
  tracer: Tracer,
  handler: Handler,
  headers?: DeliveredHeaders,
  options: WrapHandlerOptions = {},
): (env: Envelope) => Promise<void> {
  const system = systemOf(options.system);
  const consumerGroup = options.consumerGroup === undefined || options.consumerGroup === "" ? undefined : options.consumerGroup;
  const instruments = instrumentsOrUndefined(options.meter, consumerInstrumentsOf);
  return async (env: Envelope): Promise<void> => {
    const delivered = await resolveHeaders(headers);
    const ctx = remoteParentFromHeaders(delivered) ?? parentContext(env.trace_id);
    const destination = destinationOf(env.meta?.queue);
    const attempts = Number.isInteger(env.attempts) ? env.attempts : 0;
    if (instruments !== undefined) {
      const attributes = metricAttributes(system, "process", destination);
      safely(() => {
        instruments.consumed.add(1, attributes);
        if (attempts > 0) {
          instruments.retries.add(1, attributes);
        }
      });
    }
    const started = performance.now();
    return tracer.startActiveSpan(
      spanNameOf("process", destination),
      { kind: SpanKind.CONSUMER, attributes: consumeAttributes(env, system, destination, consumerGroup) },
      ctx,
      async (span) => {
        let errorType: string | undefined;
        try {
          await handler(env);
        } catch (err) {
          errorType = recordFailure(span, err);
          throw err;
        } finally {
          span.end();
          if (instruments !== undefined) {
            const seconds = Math.max(0, (performance.now() - started) / 1000);
            safely(() => instruments.processDuration.record(seconds, metricAttributes(system, "process", destination, errorType)));
          }
        }
      },
    );
  };
}

/** Options for {@link publish}. */
export interface PublishOptions extends MakeOptions, OtelOptions {
  /**
   * An out-of-band {@link HeaderCarrier} to receive the active producer span's W3C
   * `traceparent` (and `tracestate`). The adapter passes a carrier here and carries the same
   * object on its transport's metadata channel inside `send`, so the consumer can start its
   * span as a true child of this producer span (ADR-0028). When omitted, only the v0.1
   * `trace_id` propagation applies — a strict, backward-compatible default.
   */
  headers?: HeaderCarrier;
}

/**
 * Run a publish under a PRODUCER span `send <queue>` (ADR-0038), propagating the trace
 * downstream two ways:
 *
 *   - **W3C `traceparent` (ADR-0028, v0.2):** when `options.headers` is supplied, the active
 *     span context is injected into that carrier as a `traceparent` (and `tracestate`) header.
 *     The adapter's `send` carries it beside the frozen envelope on the transport's metadata
 *     channel, so a consumer can start its span as a true **child** of this producer span. The
 *     header rides out of band, never in the envelope (GR-1); a transport that can't carry
 *     headers simply doesn't — no error, no regression.
 *   - **`trace_id` (ADR-0025, v0.1):** it also stamps the active trace's id into the built
 *     envelope's `trace_id`, so even a consumer that ignores the header — or a transport that
 *     drops it — recovers the same trace (correlation without the exact span edge).
 *
 * The destination is the queue the message is published to: `options.queue`, or `default` when
 * none is given — what `make` writes into `meta.queue`. A blank queue means no destination and a
 * bare `send` name. `options.system` is the `messaging.system` value (absent: the deprecated
 * `babelqueue`). With `options.meter`, `messaging.client.sent.messages` is incremented once per
 * call, success or failure (ADR-0039). The URN is only in `babelqueue.urn`, never in the span name.
 *
 * `send` performs the real transport write and its result is returned; a failure is rethrown
 * unchanged after the span records it.
 */
export function publish<R>(
  tracer: Tracer,
  urn: string,
  data: Record<string, unknown>,
  send: (envelope: Envelope) => R | Promise<R>,
  options: PublishOptions = {},
): Promise<R> {
  const { headers, system: systemOption, meter, ...makeOptions } = options;
  const system = systemOf(systemOption);
  const destination = destinationOf(makeOptions.queue ?? "default");
  const instruments = instrumentsOrUndefined(meter, producerInstrumentsOf);
  const urnAttribute = EnvelopeCodec.urn({ job: urn }) || urn;
  const attributes: Attributes = {
    "messaging.system": system,
    "messaging.operation.type": "send",
    "messaging.operation.name": "send",
    "babelqueue.urn": urnAttribute,
    "babelqueue.schema_version": EnvelopeCodec.SCHEMA_VERSION,
    // Deprecated key, dual-emitted for one MINOR (ADR-0038 section 5).
    "messaging.operation": "publish",
  };
  if (destination !== undefined) {
    attributes["messaging.destination.name"] = destination;
  }
  return tracer.startActiveSpan(
    spanNameOf("send", destination),
    { kind: SpanKind.PRODUCER, attributes },
    async (span) => {
      let errorType: string | undefined;
      try {
        const traceId = uuidOf(span.spanContext().traceId);
        if (headers) {
          // Inject from a context built explicitly from *this* producer span — not the ambient
          // active context, which is unreliable unless a global ContextManager is registered.
          injectTraceparent(headers, trace.setSpan(otelContext.active(), span));
        }
        const envelope = EnvelopeCodec.make(urn, data, { ...makeOptions, traceId });
        span.setAttribute("messaging.message.id", envelope.meta.id);
        span.setAttribute("messaging.message.conversation_id", envelope.trace_id);
        return await send(envelope);
      } catch (err) {
        errorType = recordFailure(span, err);
        throw err;
      } finally {
        span.end();
        if (instruments !== undefined) {
          safely(() => instruments.sent.add(1, metricAttributes(system, "send", destination, errorType)));
        }
      }
    },
  );
}
