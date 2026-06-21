# Changelog

All notable changes to `@babelqueue/core` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.7.0] - 2026-06-21

### Added
- **Runtime GDPR field encryption — the SDK-enforcement half of `x-gdpr-sensitive`
  (ADR-0030).** The registry only *declares* and *audits* which `data` fields are
  personal/sensitive; this core now *enforces* it on the wire. A producer encrypts each
  marked leaf before publish, a consumer decrypts it after decode — PII rides the wire as
  ciphertext while the envelope stays frozen. The Node mirror of the Go reference; purely
  additive, opt-in and validation-neutral.
  - New `gdpr.protect(data, schema, cipher)` / `gdpr.unprotect(...)` standalone helpers —
    they rewrite each `x-gdpr-sensitive` leaf **in place**: `protect` canonically
    JSON-encodes the value then replaces it with the cipher's **ciphertext string**;
    `unprotect` is the byte-for-byte inverse (numbers restore to numbers, objects to
    objects). They walk **nested objects** (`profile.full_name`) and **array items**
    (`addresses[].line`); an absent marked field is skipped (not an error); a non-sensitive
    field is never touched. A wrong-key / tampered / non-ciphertext value throws the typed
    `DecryptError` so the consumer fails the message (retry / dead-letter).
  - New `Cipher` interface — `encrypt(bytes): string` / `decrypt(string): bytes`, both
    **synchronous** — the caller-provided seam onto a KMS / Vault / HSM / tokenisation
    service, so the core pulls **no** crypto dependency (GR-7).
  - New `AesGcmCipher` reference cipher built **only** on `node:crypto` — AES-GCM with a
    random 12-byte IV prepended and base64 output (`base64(iv || ciphertext || tag)`);
    16/24/32-byte keys select AES-128/192/256-GCM. The caller owns the key (no key
    management). GCM authenticates, so a wrong key or tampered input throws rather than
    returning corrupt plaintext. New typed errors `DecryptError`, `InvalidKeySizeError`,
    `MalformedCiphertextError`.
  - New `schema.sensitivePaths(schema)` + `SensitivePath` type — parses the
    `x-gdpr-sensitive` keyword (boolean `true` or non-empty string category) and walks
    nested objects, array items and the root mark, in sorted path order. Parsing is
    **validation-neutral** — the keyword never makes a value valid or invalid, so annotating
    a schema is non-breaking.
  - The envelope stays **frozen** (GR-1): only `data` *values* change, a ciphertext is a
    JSON string so `data` stays pure JSON (GR-3), `meta.schema_version` stays **1** and
    `trace_id` is preserved (GR-4). An SDK without the key still carries the envelope.
    Validate **cleartext** — before `protect`, after `unprotect`. Entirely opt-in and
    backward compatible.

## [1.6.0] - 2026-06-21

### Added
- **Transactional outbox — an optional producer-side helper that removes the dual
  write (ADR-0029).** A plain producer must commit its business row **and** publish to
  the broker — two systems that disagree on a crash. The outbox persists the encoded
  envelope into the **same DB transaction** as the business write, and a separate relay
  publishes the durable rows afterwards: no distributed transaction, exactly-once
  *handoff*, then at-least-once on the wire as always.
  - New `Outbox` writer — `write(envelope)` encodes via the frozen `EnvelopeCodec` and
    delegates to the store. **The caller owns the transaction boundary**; the helper
    never begins/commits anything (GR-7).
  - New `OutboxStore` interface (`save`, `fetchUnpublished` oldest-first, `markPublished`,
    `markFailed`) — the persistence seam the caller binds to their own DB; the core ships
    no DB driver. New `OutboxTransport` interface (`publish(body, queue)`) — the
    publish-only seam the relay forwards through, bound to the caller's broker.
  - New `OutboxRelay` — `flush()` publishes one batch (marking each row published only
    **after** the transport resolves; a rejecting publish → `markFailed` + bounded linear
    backoff, the row stays pending, the batch continues) and `drain(maxPasses?)` loops
    until no progress, with a safety ceiling. The sleeper is injectable so tests are
    instant.
  - New `InMemoryOutboxStore` reference store (tests / single-process demos; no real
    transaction). New types `OutboxRecord`, `OutboxRelayResult`, `OutboxRelayOptions`,
    `Sleeper`.
  - The relay publishes the **stored bytes verbatim** — never decoding, rebuilding or
    re-encoding the envelope — so `trace_id` is preserved end-to-end (GR-4) and the body
    is byte-identical before store and after relay (GR-1/GR-5). `schema_version` stays
    **1**: the outbox's bookkeeping lives *around* the envelope, never *on* the wire.
    Entirely opt-in and backward compatible.

## [1.5.0] - 2026-06-21

### Added
- **Replay-bypass — an out-of-band side-effect guard for DLQ replay (ADR-0027).** A
  deliberate `redrive` re-runs the handler and re-fires its external side-effects (a second
  charge, a duplicate email); idempotency stops an *accidental* duplicate, not the *intended*
  reprocess. This closes that gap.
  - New `redrive(io, dlq, { bypass: true })` option stamps a `bq-replay-bypass` **transport
    header** on each replayed message; the per-item result gains a `bypassed` flag. It takes
    effect only when the `RedriveIO` implements the new optional `publishWithHeaders(queue,
    body, headers)` capability — otherwise `bypass` is a no-op and the message is still
    redriven (`bypassed: false`), exactly like the Go reference.
  - New `isReplay(headers)` + `bypassExternalEffects(headers, effect)` consume-side guard
    (plus the `HEADER_REPLAY_BYPASS` constant): a handler wraps its external, non-idempotent
    side so a replay skips it while the idempotent core still runs.
  - The marker rides **beside** the frozen envelope on the `HeaderCarrier` seam, never inside
    it (`schema_version` stays **1**, GR-1; `trace_id` preserved, GR-4) — the same out-of-band
    channel as the `traceparent` header. Fully opt-in and backward compatible: a header-less
    message behaves exactly as before. Per-adapter transport wiring (the
    `babelqueue-node-adapters` repo) is the documented follow-up, like ADR-0028's.
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
