import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, InMemoryStore, Wrap, type Envelope } from "../src/index.js";

function envWithId(id: string): Envelope {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 7 });
  env.meta.id = id;
  return env;
}

test("runs the handler on first delivery and remembers it", async () => {
  const store = new InMemoryStore();
  let calls = 0;
  const handler = Wrap(store, () => {
    calls += 1;
  });

  await handler(envWithId("m1"));

  assert.equal(calls, 1);
  assert.equal(store.seen("m1"), true);
});

test("skips the handler on a redelivery of the same id", async () => {
  const store = new InMemoryStore();
  let calls = 0;
  const handler = Wrap(store, () => {
    calls += 1;
  });

  await handler(envWithId("m1"));
  await handler(envWithId("m1")); // redelivery → skipped

  assert.equal(calls, 1);
});

test("runs the handler again for a different id", async () => {
  const store = new InMemoryStore();
  let calls = 0;
  const handler = Wrap(store, () => {
    calls += 1;
  });

  await handler(envWithId("m1"));
  await handler(envWithId("m2"));

  assert.equal(calls, 2);
});

test("does not remember an id when the handler throws", async () => {
  const store = new InMemoryStore();
  let calls = 0;
  const handler = Wrap(store, () => {
    calls += 1;
    throw new Error("boom");
  });

  await assert.rejects(() => Promise.resolve(handler(envWithId("m1"))), /boom/);
  assert.equal(store.seen("m1"), false);

  // A redelivery runs the handler again — retry works.
  await assert.rejects(() => Promise.resolve(handler(envWithId("m1"))), /boom/);
  assert.equal(calls, 2);
});

test("runs the handler when the message has no usable id", async () => {
  const store = new InMemoryStore();
  let calls = 0;
  const handler = Wrap(store, () => {
    calls += 1;
  });

  await handler(envWithId("")); // empty id → cannot dedupe → runs
  await handler(envWithId("")); // still runs

  assert.equal(calls, 2);
});

test("forget removes a remembered id", () => {
  const store = new InMemoryStore();
  store.remember("m1");
  assert.equal(store.seen("m1"), true);

  store.forget("m1");
  assert.equal(store.seen("m1"), false);
});
