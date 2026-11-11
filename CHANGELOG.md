# Changelog

All notable changes to `@babelqueue/core` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **`schema`: payload-validator parity with the other SDKs (ADR-0024; conformance 1.10.0 `payload_schema_keywords`).**
  `type` may now be an array (`["string","null"]`; a value is valid when it matches any listed type, and
  `integer` accepts `1.0` and `1e2`); `const` / `enum` compare numbers by mathematical value (`1` = `1.0` = `1e0`,
  a `bigint` included), recursively in arrays and objects (object key order is irrelevant), while a boolean or a
  string never equals a number; `maxLength` is enforced in Unicode code points. A keyword outside the supported
  subset still never changes a verdict, but registering a schema in a `MapProvider` now reports one warning per
  occurrence: `{ urn, pointer, keyword }` through the new `onWarning` option (default: `process.emitWarning` with
  the code `BABELQUEUE_UNSUPPORTED_SCHEMA_KEYWORD`, once per message) and on `MapProvider.warnings`; the pure
  `unsupportedKeywords(schema)` helper is exported too. Annotation keywords (`$schema`, `$id`, `title`,
  `description`, `examples`, `default`, `$comment`, `x-*`) never warn. No wire or envelope change
  (`schema_version` stays 1).

### Changed
- Vendored conformance suite bumped to 1.10.0; the runner executes the new `payload_schema_keywords` section.

## [1.14.0] - 2026-11-01

### Changed — dashboard-breaking (`@babelqueue/core/otel`, ADR-0038)
- **Span names:** `publish <urn>` is now `send <queue>` and `process <urn>` is now `process <queue>`.
  With no destination (a blank `meta.queue` on consume) the name is the bare `send` / `process`. The
  URN is never in the span name; it is the `babelqueue.urn` attribute.
- **Producer destination:** `messaging.destination.name` on the producer span is now the queue the
  message is published to (`queue` option, else `default`), no longer the URN. Update saved queries.
- **`messaging.system`:** with the new `system` option it is the broker (`redis`, `rabbitmq`,
  `aws_sqs`, `servicebus`, `pulsar`, `kafka`, `activemq`; any other value is written as given).
  **Without `system` it stays `babelqueue`, and that default is deprecated** — pass `system`.
- `messaging.operation.type` / `messaging.operation.name` (`send` or `process`) are written; the
  `babelqueue.urn`, `babelqueue.schema_version` and `babelqueue.attempts` attributes carry the
  BabelQueue identity. `messaging.message.id` is written only when `meta.id` is present, and on a
  producer span as soon as the envelope is built (also when the send then fails). The consumer span
  no longer writes empty strings for a missing destination or id.
- A failing publish or handler now also sets `error.type` (the error's constructor name, else its
  `name`, else `_OTHER`); status Error and the recorded exception are unchanged, and the error is
  rethrown as before.
- **Deprecated keys, dual-emitted for this MINOR only:** `messaging.operation` (`publish` /
  `process`) and `messaging.babelqueue.attempts`. The next MINOR removes both.

### Added
- `publish(...)` options: `system`, `meter`. `wrapHandler(tracer, handler, headers?, options?)`
  options: `system`, `consumerGroup` (`messaging.consumer.group.name`), `meter`. Existing calls keep
  working unchanged.
- `SEMCONV_VERSION` (`"1.44.0"`) and `SEMCONV_SCHEMA_URL` (`https://opentelemetry.io/schemas/1.44.0`):
  pass the schema URL when you create your tracer or meter (`getTracer(name, version, { schemaUrl })`).
- Standard metrics (ADR-0039), recorded only when a `meter` is passed (`@opentelemetry/api` stays an
  optional peer dependency; no new runtime dependency): `messaging.process.duration` (histogram, `s`,
  advisory buckets), `messaging.client.sent.messages`, `messaging.client.consumed.messages` and
  `babelqueue.retries` (counters, `{message}`). Attributes are `messaging.system`,
  `messaging.operation.name`, `messaging.destination.name` (when known) and `error.type` (on failure
  of the duration and sent metrics); never the URN, message id, `trace_id` or attempts.
- Dead-letter observer seam (ADR-0039 section 5.1): the `DeadLetterObserver` and `DeadLetterEvent`
  types in the core (`@babelqueue/core`), and `createDeadLetterObserver({ system, meter })` in
  `@babelqueue/core/otel`, which counts `babelqueue.dlq.messages`. The Node core has no consume
  runtime; wiring the observer into the transport packages is the node-adapters' job.
- A non-UUID `trace_id` is forwarded unchanged; the OTel parent mapping (SHA-256 hash, as before) now
  emits a `BABELQUEUE_NON_UUID_TRACE_ID` process warning once per distinct value
  (message-envelope.md section 5.1). `traceIdOf` itself is unchanged.
- Conformance runner covers the v1.9.0 `span-shape`, `trace-id` and `metrics` sections (plus the six
  new `traceparent` cases); the vendored suite is now 1.9.0.
- `classifyBody(raw)` and the `BodyVerdict` type (ADR-0036 decision 4, ADR-0032 section 6): a strict,
  never-throwing classifier that returns `{ verdict: "ok" }` for a body that decodes into an envelope
  with a valid identity and `{ verdict: "poison", cause }` otherwise (not one JSON object, a wrongly
  typed known member such as `attempts: "3"` or `data: []`, or no valid `job` / `urn`). It lets a
  consumer park a malformed body on the poison route. **`EnvelopeCodec.decode` is unchanged**: it
  stays lenient about member types and keeps its signature and behaviour.
  A body nested deeper than 10000 levels is poison; the depth limit differs per SDK (Go 10000, PHP 511, Java 256,
  .NET 64, Python interpreter-dependent), recorded in ADR-0036 decision 4 "SDK note".
- Conformance runner covers the v1.7.0 core section `identity_verdict` (97 cases, all passing); it was
  the last section not run by this package.

## [1.13.0] - 2026-10-24

### Added
- `retryBackoffDelay(options, n, random?)` and `retryBackoffBound(options, n)`: the shared retry
  backoff formula (ADR-0034 §9). `d(n) = min(max, base * multiplier^(n-1))` with
  `n = attempts + 1`; `retryJitter: "full"` draws `uniform(0, d(n))`. Defaults (`retryBackoff` 0,
  multiplier 1, max 0 = uncapped, jitter `none`) give a zero delay, so nothing changes unless a
  consumer opts in. Pure helper, no wire change (`schema_version` stays 1).
- `buildPoisonRecord(body, deadLetterJson)`: the DLQ record of a poison body per ADR-0035. A strict
  JSON object without a top-level `dead_letter` is spliced at byte level (no value is re-encoded);
  anything else is wrapped as `{"raw", "raw_encoding"?, "dead_letter"}`, with invalid UTF-8 written as
  base64 plus `"raw_encoding": "base64"` (never U+FFFD). Pure helper, no wire change. The conformance
  runner covers the v1.6.0 `poison_dlq` section with it.
- Conformance runner covers the v1.6.0 `retry_backoff` section; the sections that belong to
  transport packages (`redis_layout`, `redis_delay`, `kafka_poison`, `backoff`) are reported as absent.

### Changed
- `buildPoisonRecord` / `poisonRecord()`: the `dead_letter` argument is now validated as a single JSON object and rejected otherwise. (`TypeError`); removed the unused `isLossyNumberToken`.
- CI pins `fetch.sh` by sha256 (`BQ_FETCH_SH_SHA256`) next to `BQ_CONFORMANCE_VERSION` (now 1.6.0).

## [1.12.0] - 2026-10-19

### Changed
- Decode: a non-string `job` (`null`, number, boolean, object, array) is now ignored with a warning
  and a valid string `urn` takes over; with neither, the message is not accepted (ADR-0032 §6). The
  default-channel warning starts with `BabelQueue:` and carries its own code,
  `NON_STRING_JOB_WARNING_CODE` (`"BABELQUEUE_NON_STRING_JOB"`), now exported.
- Trim and blank checks in `make`, `accepts` and `urn` use the closed ASCII set (space, `\t`, `\n`,
  `\v`, `\f`, `\r`) in linear time; Unicode whitespace is no longer trimmed (ADR-0032).

### Added
- Conformance runner covers the v1.5.0 sections (`trim`, `job_type`, `traceparent`) and writes a
  `results.json` (ADR-0032 §2), validated in CI and before publish.

## [1.11.0] - 2026-10-17

Implements ADR-0031 (producer `meta` extension keys); conformance suite v1.4.0. The envelope stays at `schema_version: 1`.

### Added

- `MakeOptions.meta` — producer extension keys (flat keys or a `meta.ext.<vendor>` block), written into
  `meta` and preserved across hops. A collision with a core `meta` key (`id`, `queue`, `lang`,
  `schema_version`, `created_at`) or a forbidden key throws.
- `InvalidEnvelopeKeyError` (extends `BabelQueueError`) with the offending RFC 6901 `pointer`.
- `Meta` has an index signature (`[extension: string]: unknown`), so extension keys type-check; existing
  code keeps compiling.
- Conformance runner: `extension_write`, `encode_reject` (the Go-only `reject-repeated-key` case is
  skipped), `expect_preserved`, `expect_absent_after_reencode`, `apply_dead_letter` (through `annotate`) and
  `expect_dead_letter`, with unit tests that prove each field is evaluated.

### Changed

- **`encode` throws `InvalidEnvelopeKeyError`** on a non-canonical key (top-level `timestamp` / `urn`,
  `meta.max_retries` / `attempts` / `source` / `ts`) instead of warning and dropping it. `decode` still
  warns and drops. `process.emitWarning` / `onWarning` is now used for decode only. The error message is
  `BabelQueue: cannot encode envelope key <pointer>: <reason>.` (for example `... /meta/ts: non-canonical
  envelope key (message-envelope.md section 10).`); `error.pointer` holds the RFC 6901 pointer.
- `decode` falls back to `urn` when `job` is missing, empty or only ASCII whitespace
  (`[ \t\n\v\f\r]`, never Unicode spaces); the `urn` key is not kept. When `job` and `urn` are both
  present, `job` wins and `urn` is dropped with a warning naming `/urn`. A `urn` that is not a string and has no
  usable `job` to stand in for (number, `null`, object ...) is dropped with the same warning.
- `EnvelopeCodec.urn()` / `accepts()` use the same closed ASCII whitespace set instead of `trim()`.

### Deprecated

- `EncodeOptions` / `encode(envelope, { onWarning })` — the option is accepted but never called, since
  `encode` no longer warns.

## [1.10.0] - 2026-10-12

### Added

- `RedriveIO.rewind?(from)` — optional capability (the Node counterpart of Go's `Rewinder`). Whatever stops a
  `redrive()` run (the restore write back to the DLQ failed, the redrive write failed, the `select`
  predicate threw, the ack failed) the IO is asked to hand back every reserved, unacknowledged message (an
  offset-committing broker such as Kafka seeks the consumer back, so a later cumulative commit cannot skip
  any of them; the whole batch is reserved first). A message whose restore failed is never acked. `from`
  may already be acknowledged (restored after a failed redrive write) and must then not be redelivered.
  When several steps failed (publish, restore, rewind) all errors are thrown together as one flat
  `AggregateError`; a lone error is thrown as is. A `RedriveIO` without `rewind` is unchanged, including
  the error thrown when a redrive write and its restore both fail (the restore error, as before).
  Additive, no wire change.

### Changed

- Tests only: the vendored conformance suite is now 1.3.0 (exact-case envelope keys, last-wins duplicates: new `invalid-wrong-case-job`, `wrong-case-keys-roundtrip` and `duplicate-keys-last-wins` fixtures). No production change; the Node core already behaved this way.

## [1.9.0] - 2026-10-11

### Added
- **DLQ redrive carries the Kafka record key.** `RedriveIO` gains an optional `publishFrom(queue, body, from)`
  capability (the Node counterpart of Go's `KeyedRepublisher`); `redrive` uses it for the redrive write and
  the restore write back to the DLQ, so a keyed transport can re-publish the consumed record's key
  byte-for-byte (null stays null, empty stays empty) per contract §6.5 "Record key on republish". A
  `RedriveIO` without it behaves exactly as before. No wire change (`schema_version` stays 1).

## [1.8.0] - 2026-10-03

### Fixed
- **`minLength` now counts Unicode code points**, not UTF-16 code units. An astral
  character (e.g. an emoji) counts once, so `"😀"` no longer satisfies `minLength: 2`.
  Verdicts now match the other SDK cores (conformance `payload_schema_unicode`).
- **Non-canonical envelope keys are dropped on decode, with a warning** (message-envelope.md
  §10, policy K-15): `timestamp`, `meta.max_retries`, `meta.attempts`, `meta.source` and
  `meta.ts`. Decode still succeeds so older producers keep working; the keys are no longer
  carried through and re-emitted on retry/relay. `encode` never emits them either: a key the
  caller set on the envelope object is left out of the output (the object itself is not
  modified) and a warning names it. All other unknown keys are still preserved.
  K-15's *reject on encode* is planned for R1-E0, when `encode` will throw instead of
  dropping, so treat these warnings as errors-to-be.
- **The default warning channel is rate-limited:** `process.emitWarning` fires once per key
  (and direction) per process, so a high-volume legacy producer cannot flood stderr. An
  explicit `onWarning` callback still receives every occurrence.

### Added
- `EnvelopeCodec.decode(raw, { onWarning })` — an optional `DecodeOptions.onWarning`
  callback that receives one message per dropped key, naming it as a JSON pointer
  (e.g. `/meta/max_retries`). Without it the warning goes to `process.emitWarning` with
  the code `FORBIDDEN_KEY_WARNING_CODE` (`"BABELQUEUE_FORBIDDEN_KEY"`).
- `EnvelopeCodec.encode(envelope, { onWarning })` — the same optional callback
  (`EncodeOptions`) for keys left out on encode.
- Conformance runners for the new shared sections `roundtrip`, `data_shape`,
  `forbidden_keys` and `payload_schema_unicode`.

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
