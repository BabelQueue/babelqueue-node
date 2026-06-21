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

### Replay-bypass (optional)

A deliberate replay off the DLQ ([`redrive`](src/redrive.ts)) re-runs the handler,
re-firing its external side-effects — a second charge, a duplicate email. With
`bypass`, `redrive` stamps a `bq-replay-bypass` **transport header** on each replayed
message; a handler reads the delivered headers and skips the effects that already ran,
while the idempotent core still runs (ADR-0027).

```ts
import { redrive, isReplay, bypassExternalEffects } from "@babelqueue/core";

// PRODUCER: redrive with bypass (the IO must carry headers — publishWithHeaders).
await redrive(io, "orders.dlq", { bypass: true });

// CONSUMER: the adapter surfaces the delivered message's out-of-band headers.
async function onMessage(env, headers) {
  saveOrder(env);                                    // idempotent core — always runs
  await bypassExternalEffects(headers, async () => { // skipped when isReplay(headers)
    await sendConfirmationEmail(env);
  });
}
```

The marker rides **beside** the frozen envelope, never inside it (`schema_version`
stays **1**, GR-1) — the same out-of-band `HeaderCarrier` seam as the `traceparent`
header. It takes effect only when the `RedriveIO` implements `publishWithHeaders`;
otherwise `bypass` is a no-op (`bypassed: false`) and the message is still redriven.

### Transactional outbox (optional)

A plain producer does two things that must both happen or neither — commit the
business row and publish the message — across two systems that can disagree on a
crash (the **dual write**). The outbox removes it: `outbox.write(env)` persists the
**encoded envelope into the same DB transaction** as your business write, and a
separate `OutboxRelay` publishes the durable rows afterwards (ADR-0029).

```ts
import { Outbox, OutboxRelay, InMemoryOutboxStore, EnvelopeCodec } from "@babelqueue/core";

// 1) WRITE — the caller owns the transaction boundary (this is the whole point).
const outbox = new Outbox(store); // your OutboxStore, bound to your DB
await db.transaction(async (tx) => {
  await tx.insertOrder(order);                          // the business write
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id }, { queue: "orders" });
  await outbox.write(env);                              // same connection, same tx
});                                                     // both commit, or neither

// 2) RELAY — drain the durable rows onto the broker (a worker loop / cron).
const relay = new OutboxRelay(transport, store);       // your OutboxTransport
await relay.drain();                                   // publishes verbatim, marks published
```

The store and the transport are **interfaces you bind** to your own DB and broker —
the core ships no DB driver (GR-7) and only an `InMemoryOutboxStore` reference for
tests/demos. The relay publishes the **stored bytes verbatim** — it never decodes,
rebuilds or re-encodes the envelope — so `trace_id` is preserved end-to-end (GR-4)
and the body is byte-identical before store and after relay (GR-1/GR-5). It is
**at-least-once handoff**: a crash between publish and mark-published re-publishes the
row, so consumers must stay idempotent (the `Wrap` helper is the consumer-side mirror).

Implement `OutboxStore` over your DB (`save`, `fetchUnpublished` oldest-first — your
adapter SHOULD claim/lock rows so two relays don't double-publish — `markPublished`,
`markFailed`) and `OutboxTransport` (`publish(body, queue)`) over your broker.

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
