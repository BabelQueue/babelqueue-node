# BabelQueue for Node.js

[![CI](https://github.com/BabelQueue/babelqueue-node/actions/workflows/ci.yml/badge.svg)](https://github.com/BabelQueue/babelqueue-node/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@babelqueue/core.svg)](https://www.npmjs.com/package/@babelqueue/core)
[![node](https://img.shields.io/node/v/@babelqueue/core.svg)](https://www.npmjs.com/package/@babelqueue/core)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Polyglot Queues, Simplified.** Read and write the canonical BabelQueue message
> envelope from Node.js — so your Node services exchange messages with Laravel,
> Symfony, Python, Go and .NET over one strict JSON format, on the broker you
> already run.

This is the framework-agnostic **Node/TypeScript core**: the wire-envelope codec,
contracts and dead-letter helpers — **zero runtime dependencies**, shipped as a
dual **ESM + CommonJS** package with bundled types. The full standard is documented
at **[babelqueue.com](https://babelqueue.com)**.

## Installation

```bash
npm install @babelqueue/core
```

Requires Node `>=18`.

## Usage

```ts
import { EnvelopeCodec } from "@babelqueue/core";

// Produce — build the canonical envelope and publish the JSON to your broker.
const env = EnvelopeCodec.make(
  "urn:babel:orders:created",
  { order_id: 1042 },
  { queue: "orders" },
);
const body = EnvelopeCodec.encode(env); // compact UTF-8 JSON string
// await redis.rpush("queues:orders", body);
//   /  channel.sendToQueue("orders", Buffer.from(body));

// Consume — decode a message produced by ANY BabelQueue SDK.
const incoming = EnvelopeCodec.decode(body);
if (EnvelopeCodec.accepts(incoming)) {
  // `incoming` is now narrowed to a fully-typed Envelope
  switch (EnvelopeCodec.urn(incoming)) {
    case "urn:babel:orders:created":
      console.log(incoming.data.order_id, incoming.trace_id);
      break;
  }
}
```

CommonJS works too:

```js
const { EnvelopeCodec } = require("@babelqueue/core");
```

The envelope is identical to every other SDK's:

```json
{
  "job": "urn:babel:orders:created",
  "trace_id": "…",
  "data": { "order_id": 1042 },
  "meta": { "id": "…", "queue": "orders", "lang": "node", "schema_version": 1, "created_at": 1749132727000 },
  "attempts": 0
}
```

### Typed messages (optional)

```ts
import { EnvelopeCodec, type PolyglotMessage } from "@babelqueue/core";

class OrderCreated implements PolyglotMessage {
  constructor(private readonly orderId: number) {}
  getBabelUrn() {
    return "urn:babel:orders:created";
  }
  toPayload() {
    return { order_id: this.orderId };
  }
}

const env = EnvelopeCodec.fromMessage(new OrderCreated(1042), "orders");
```

Continue an existing trace by adding `getBabelTraceId(): string | null` (see
`HasTraceId`), or pass `{ traceId }` to `EnvelopeCodec.make`.

### Dead-letter

```ts
import { annotate, EnvelopeCodec } from "@babelqueue/core";

const dlq = annotate(env, "failed", "orders", { attempts: 3, error: "boom" });
// publish EnvelopeCodec.encode(dlq) to the "orders.dlq" queue
```

`annotate` returns a copy — the original envelope is preserved unchanged inside
the dead-lettered message, so any-language consumers can still read it.

## What this core is (and isn't)

It enforces the **contract**: the envelope shape, URN identity, trace propagation,
schema-version gating and the dead-letter block. It is intentionally **not** a
worker/runtime — broker wiring, acks and retry loops stay in your own code (or a
future thin adapter), exactly as with the other SDK cores.

`UnknownUrnStrategy` (`FAIL`, `DELETE`, `RELEASE`, `DEAD_LETTER`) is provided for
adapters to act on.

## Conformance

This core passes the shared **cross-SDK conformance suite** (vendored under
[`test/conformance/`](test/conformance)) — the same fixtures every BabelQueue SDK
must satisfy, so a Node producer and, say, a Laravel consumer agree byte-for-byte.

```bash
npm test
```

## License

[MIT](LICENSE) © Muhammet Şafak
