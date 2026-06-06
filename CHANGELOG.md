# Changelog

All notable changes to `@babelqueue/core` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-node/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/BabelQueue/babelqueue-node/releases/tag/v0.1.0
