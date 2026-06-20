# Changelog

All notable changes to `@babelqueue/core` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OpenTelemetry v0.2 — W3C `traceparent` cross-hop span linkage (ADR-0028).** The
  `@babelqueue/core/otel` module now propagates the active span as a W3C `traceparent`
  (and `tracestate`) **transport header** so a consumer span is a true **child** of the
  producer span — not just a shared `trace_id` trace.
  - New `HeaderCarrier` type (a `Record<string, string>` out-of-band header map) — the
    seam the core and the `babelqueue-node-adapters` transports agree on. It rides
    **beside** the frozen envelope, never inside it (`schema_version` stays **1**, GR-1).
  - `publish(...)` takes an optional `options.headers` carrier and writes the producer
    span's `traceparent` into it; `wrapHandler(tracer, handler, headers?)` reads a
    delivered message's headers (a carrier or a sync/async getter) and starts the consumer
    span as a remote-parent child. **No header ⇒ v0.1 `trace_id` fallback** (no regression);
    fully opt-in and backward compatible.
  - New low-level exports `injectTraceparent`, `remoteParentFromHeaders`,
    `HEADER_TRACEPARENT`, `HEADER_TRACESTATE`. The W3C parse/format is implemented against
    the frozen Trace Context format, so the optional dependency stays at `@opentelemetry/api`
    only — the core itself remains zero-runtime-dependency (GR-7).
  - **Per-adapter transport wiring** (bullmq/redis/rabbitmq/sqs in the separate
    `babelqueue-node-adapters` repo) is a documented follow-up; until wired, propagation
    degrades to v0.1 `trace_id` correlation with no error.

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable**: breaking changes require a MAJOR,
following the deprecation policy. The wire envelope is unchanged
(`schema_version: 1`). Full reference at [babelqueue.com](https://babelqueue.com).

### Internal
- CI adds **ESLint** (`@eslint/js` + `typescript-eslint`) and a **c8 coverage gate**
  (`npm run coverage`, ≥90% lines/functions, ≥85% branches) as separate jobs;
  `tsc --strict` typecheck already ran. `npm run lint` for local use.
- **GR-8 latency benchmark** (`test/overhead.test.ts`) — asserts the envelope
  encode/decode path adds **≤2%** over plain-JSON serialization vs a conservative
  750µs broker round-trip.

## [0.1.0] - 2026-06-06

### Added
- `EnvelopeCodec` — builds (`make`, `fromMessage`), encodes and decodes the
  canonical `{job, trace_id, data, meta, attempts}` envelope (`schema_version` 1).
  The single Node/TypeScript implementation of the wire format.
- `EnvelopeCodec.encode` emits compact UTF-8 JSON (slashes/unicode unescaped) —
  the canonical wire form shared by every SDK.
- `EnvelopeCodec.urn()` — resolve the URN (`job`, accepting `urn` as an alias).
- `EnvelopeCodec.accepts()` — consumer-side validation (rejects empty URN,
  unsupported `meta.schema_version`, non-object `data`, non-integer `attempts`,
  blank `trace_id`); acts as a TypeScript type guard narrowing to `Envelope`.
- `make` options `queue` and `traceId` (trace continuation).
- `annotate` / `deadLetter.annotate` — additive `dead_letter` block builder.
- Contracts `PolyglotMessage` / `HasTraceId`.
- `UnknownUrnStrategy` (`FAIL` / `DELETE` / `RELEASE` / `DEAD_LETTER`);
  `BabelQueueError` / `UnknownUrnError`.
- Shipped as a dual **ESM + CommonJS** package with bundled type declarations.
- Shared cross-SDK **conformance suite** under `test/conformance/` (vendored from
  the canonical `conformance/` set) plus a runner.

### Notes
- Pre-1.0: the public API may change before the `1.0.0` tag.
- **Zero runtime dependencies**; Node `>=18`.

[Unreleased]: https://github.com/BabelQueue/babelqueue-node/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/BabelQueue/babelqueue-node/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/BabelQueue/babelqueue-node/releases/tag/v0.1.0
