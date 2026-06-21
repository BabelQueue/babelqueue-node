import assert from "node:assert/strict";
import { test } from "node:test";

import {
  EnvelopeCodec,
  InMemoryOutboxStore,
  Outbox,
  OutboxRelay,
  type OutboxTransport,
} from "../src/index.js";

// A fake transport over an array, with an optional rejection trigger keyed by queue.
function fakeTransport(): OutboxTransport & {
  readonly sent: Array<{ body: string; queue: string }>;
  failQueue?: string;
} {
  const sent: Array<{ body: string; queue: string }> = [];
  const transport: OutboxTransport & {
    readonly sent: Array<{ body: string; queue: string }>;
    failQueue?: string;
  } = {
    sent,
    async publish(body: string, queue: string): Promise<void> {
      if (transport.failQueue && queue === transport.failQueue) {
        throw new Error(`publish refused for ${queue}`);
      }
      sent.push({ body, queue });
    },
  };
  return transport;
}

// A no-op sleeper records the requested delays so backoff growth/capping is assertable,
// without ever waiting.
function recordingSleeper(): { fn: (ms: number) => Promise<void>; delays: number[] } {
  const delays: number[] = [];
  return {
    delays,
    fn: (ms: number): Promise<void> => {
      delays.push(ms);
      return Promise.resolve();
    },
  };
}

test("write stores the encoded envelope byte-for-byte and returns a row id", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1042 }, { queue: "orders" });
  const expected = EnvelopeCodec.encode(env);

  const id = await outbox.write(env);

  assert.equal(typeof id, "string");
  assert.equal(store.pendingCount(), 1);

  const [record] = await store.fetchUnpublished(10);
  assert.equal(record.id, id);
  assert.equal(record.queue, "orders");
  // GR-1: the stored body is the exact codec output — not decoded/rebuilt.
  assert.equal(record.body, expected);
});

test("write falls back to the 'default' queue when meta.queue is absent", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  // EnvelopeCodec.make already defaults meta.queue to 'default'; assert the relay captures it.
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 });

  await outbox.write(env);
  const [record] = await store.fetchUnpublished(10);
  assert.equal(record.queue, "default");
});

test("flush publishes the stored bytes verbatim and marks them published", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport = fakeTransport();
  const relay = new OutboxRelay(transport, store, { sleeper: recordingSleeper().fn });

  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 7 }, { queue: "orders" });
  const expected = EnvelopeCodec.encode(env);
  await outbox.write(env);

  const res = await relay.flush();

  assert.deepEqual([res.published, res.failed], [1, 0]);
  assert.equal(transport.sent.length, 1);
  // GR-1/GR-5: what reaches the transport is byte-identical to what was stored.
  assert.equal(transport.sent[0].body, expected);
  assert.equal(transport.sent[0].queue, "orders");
  // GR-4: trace_id survives the relay untouched (the body was never decoded/re-encoded).
  assert.equal(EnvelopeCodec.decode(transport.sent[0].body).trace_id, env.trace_id);
  assert.equal(store.pendingCount(), 0, "published row is no longer pending");
});

test("a rejecting publish marks the row failed, leaves it pending, and the batch continues", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport = fakeTransport();
  transport.failQueue = "orders"; // the 'orders' row rejects; the 'emails' row still goes
  const relay = new OutboxRelay(transport, store, { sleeper: recordingSleeper().fn });

  const failId = await outbox.write(
    EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" }),
  );
  await outbox.write(EnvelopeCodec.make("urn:babel:emails:welcome", { to: "a@b.c" }, { queue: "emails" }));

  const res = await relay.flush();

  assert.deepEqual([res.published, res.failed], [1, 1], "one published, one failed");
  assert.equal(transport.sent.length, 1, "only the good row reached the transport");
  assert.equal(transport.sent[0].queue, "emails");

  // The poison row is still pending, with its attempt bumped and the error recorded.
  assert.equal(store.pendingCount(), 1);
  assert.equal(store.attemptsOf(failId), 1);
  assert.match(store.lastErrorOf(failId), /publish refused for orders/);
});

test("drain loops flush until the outbox is empty", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport = fakeTransport();
  // batchSize 1 forces multiple passes so drain's loop is exercised.
  const relay = new OutboxRelay(transport, store, { batchSize: 1, sleeper: recordingSleeper().fn });

  for (let i = 0; i < 3; i += 1) {
    await outbox.write(EnvelopeCodec.make("urn:babel:orders:created", { order_id: i }, { queue: "orders" }));
  }

  const res = await relay.drain();

  assert.deepEqual([res.published, res.failed], [3, 0]);
  assert.equal(transport.sent.length, 3);
  assert.equal(store.pendingCount(), 0);
});

test("drain stops when only failing rows remain (no progress)", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport = fakeTransport();
  transport.failQueue = "orders"; // every row rejects → no progress → drain returns
  const relay = new OutboxRelay(transport, store, { batchSize: 5, sleeper: recordingSleeper().fn });

  await outbox.write(EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" }));

  const res = await relay.drain();

  assert.deepEqual([res.published, res.failed], [0, 1]);
  assert.equal(store.pendingCount(), 1, "the failing row stays pending for a later drain");
});

test("backoff grows linearly per prior attempt and caps", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport = fakeTransport();
  transport.failQueue = "orders";
  const sleeper = recordingSleeper();
  const relay = new OutboxRelay(transport, store, {
    batchSize: 5,
    backoffStepMs: 50,
    backoffCapMs: 120, // low cap so the growth hits the ceiling quickly
    sleeper: sleeper.fn,
  });

  const id = await outbox.write(
    EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" }),
  );

  // Each flush re-fails the same row; attempts climb 0 → 1 → 2 → 3, so backoff = 50, 100, 120(cap), 120(cap).
  await relay.flush(); // prior attempts 0 → 50
  await relay.flush(); // prior attempts 1 → 100
  await relay.flush(); // prior attempts 2 → 150 capped to 120
  await relay.flush(); // prior attempts 3 → 200 capped to 120

  assert.equal(store.attemptsOf(id), 4);
  assert.deepEqual(sleeper.delays, [50, 100, 120, 120]);
});

test("flush on an empty outbox is a no-op", async () => {
  const store = new InMemoryOutboxStore();
  const transport = fakeTransport();
  const relay = new OutboxRelay(transport, store, { sleeper: recordingSleeper().fn });

  const res = await relay.flush();

  assert.deepEqual([res.published, res.failed], [0, 0]);
  assert.equal(transport.sent.length, 0);
});

test("a non-Error rejection is recorded with a safe string reason", async () => {
  const store = new InMemoryOutboxStore();
  const outbox = new Outbox(store);
  const transport: OutboxTransport = {
    publish(): Promise<void> {
      return Promise.reject("string failure"); // not an Error instance
    },
  };
  const relay = new OutboxRelay(transport, store, { sleeper: recordingSleeper().fn });

  const id = await outbox.write(EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" }));
  const res = await relay.flush();

  assert.deepEqual([res.published, res.failed], [0, 1]);
  assert.equal(store.lastErrorOf(id), "string failure");
});
