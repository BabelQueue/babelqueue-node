import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, type Envelope } from "../src/codec.js";
import type { HeaderCarrier } from "../src/contracts.js";
import {
  HEADER_REPLAY_BYPASS,
  bypassExternalEffects,
  isReplay,
} from "../src/replay.js";
import {
  redrive,
  type RedriveIO,
  type RedriveMessage,
} from "../src/redrive.js";

/** A message reserved with the out-of-band headers it was published with. */
interface HeldMessage {
  body: string;
  headers: HeaderCarrier;
}

/**
 * memoryIO is a tiny RedriveIO that also implements the optional publishWithHeaders capability, so
 * a redriven message carries the headers it was stamped with (a `HeaderPublisher` in Go terms).
 */
function memoryIO(): RedriveIO & { queue(name: string): HeldMessage[] } {
  const queues = new Map<string, HeldMessage[]>();
  const q = (name: string): HeldMessage[] => {
    let arr = queues.get(name);
    if (!arr) {
      arr = [];
      queues.set(name, arr);
    }
    return arr;
  };
  return {
    queue: q,
    async pop(queue: string): Promise<RedriveMessage | null> {
      const arr = q(queue);
      if (arr.length === 0) {
        return null;
      }
      const held = arr.shift() as HeldMessage;
      return { body: held.body, async ack(): Promise<void> {} };
    },
    async publish(queue: string, body: string): Promise<void> {
      q(queue).push({ body, headers: {} });
    },
    async publishWithHeaders(queue: string, body: string, headers: HeaderCarrier): Promise<void> {
      q(queue).push({ body, headers });
    },
  };
}

/** A RedriveIO with NO header capability (omits publishWithHeaders): bypass must be a no-op. */
function plainIO(): RedriveIO & { queue(name: string): string[] } {
  const queues = new Map<string, string[]>();
  const q = (name: string): string[] => {
    let arr = queues.get(name);
    if (!arr) {
      arr = [];
      queues.set(name, arr);
    }
    return arr;
  };
  return {
    queue: q,
    async pop(queue: string): Promise<RedriveMessage | null> {
      const arr = q(queue);
      if (arr.length === 0) {
        return null;
      }
      const body = arr.shift() as string;
      return { body, async ack(): Promise<void> {} };
    },
    async publish(queue: string, body: string): Promise<void> {
      q(queue).push(body);
    },
  };
}

function deadLetteredBody(urn: string, originalQueue: string): string {
  const base = EnvelopeCodec.make(urn, { order_id: 1 }, { queue: originalQueue });
  const dl: Envelope = {
    ...base,
    attempts: 3,
    dead_letter: {
      reason: "failed",
      error: "boom",
      exception: "Error",
      failed_at: 1,
      original_queue: originalQueue,
      attempts: 3,
      lang: "node",
    },
  };
  return EnvelopeCodec.encode(dl);
}

test("isReplay defaults false; bypassExternalEffects runs the effect when not a replay", async () => {
  assert.equal(isReplay(undefined), false);
  assert.equal(isReplay(null), false);
  assert.equal(isReplay({}), false);

  let ran = false;
  const out = await bypassExternalEffects({}, async () => {
    ran = true;
    return "result";
  });
  assert.equal(ran, true, "effect must run when not a replay");
  assert.equal(out, "result");
});

test("a delivered replay header makes isReplay true and skips the external effect", async () => {
  const headers: HeaderCarrier = { [HEADER_REPLAY_BYPASS]: "1" };
  assert.equal(isReplay(headers), true);

  let ran = false;
  const out = await bypassExternalEffects(headers, async () => {
    ran = true;
    return "result";
  });
  assert.equal(ran, false, "the effect must be skipped on a replay");
  assert.equal(out, undefined);
});

test("redrive with bypass stamps the header and the consumer skips the side-effect", async () => {
  const io = memoryIO();
  io.queue("orders.dlq").push({ body: deadLetteredBody("urn:babel:orders:created", "orders"), headers: {} });

  const res = await redrive(io, "orders.dlq", { bypass: true });
  assert.equal(res.redriven, 1);
  assert.equal(res.items[0].bypassed, true, "the item must be flagged bypassed");

  // The redriven message carries the marker on its out-of-band headers...
  const delivered = io.queue("orders")[0];
  assert.equal(delivered.headers[HEADER_REPLAY_BYPASS], "1", "redriven message carries the header");

  // ...and a handler that reads those headers treats the delivery as a replay and skips effects.
  const env = EnvelopeCodec.decode(delivered.body);
  assert.equal(isReplay(delivered.headers), true, "the handler should see this as a replay");
  let emailed = false;
  await bypassExternalEffects(delivered.headers, async () => {
    emailed = true;
  });
  assert.equal(emailed, false, "the external side-effect must be skipped on a bypassed replay");
  // the idempotent core (decoded envelope) is still fully usable and traceable
  assert.equal(env.dead_letter, undefined);
  assert.equal(env.attempts, 0);
});

test("the bq-replay-bypass marker rides the transport seam, NOT the encoded envelope (GR-1)", async () => {
  const io = memoryIO();
  const original = deadLetteredBody("urn:babel:orders:created", "orders");
  const traceId = EnvelopeCodec.decode(original).trace_id;
  io.queue("orders.dlq").push({ body: original, headers: {} });

  await redrive(io, "orders.dlq", { bypass: true });

  const delivered = io.queue("orders")[0];
  // The header is out of band — exactly like traceparent: present on headers, absent from the body.
  assert.equal(delivered.headers[HEADER_REPLAY_BYPASS], "1");
  assert.ok(!delivered.body.includes(HEADER_REPLAY_BYPASS), "marker must not appear in the encoded envelope");
  // The frozen envelope is unchanged: schema_version stays 1 and trace_id is preserved (GR-4).
  const env = EnvelopeCodec.decode(delivered.body);
  assert.ok(EnvelopeCodec.accepts(env), "the redriven body is still a valid schema_version 1 envelope");
  assert.equal(env.meta.schema_version, 1);
  assert.equal(env.trace_id, traceId);
});

test("a normal redrive (no bypass) carries no marker and the handler is unaffected", async () => {
  const io = memoryIO();
  io.queue("orders.dlq").push({ body: deadLetteredBody("urn:babel:orders:created", "orders"), headers: {} });

  const res = await redrive(io, "orders.dlq");
  assert.equal(res.redriven, 1);
  assert.equal(res.items[0].bypassed, false);

  const delivered = io.queue("orders")[0];
  assert.equal(delivered.headers[HEADER_REPLAY_BYPASS], undefined);
  assert.equal(isReplay(delivered.headers), false);
  let emailed = false;
  await bypassExternalEffects(delivered.headers, async () => {
    emailed = true;
  });
  assert.equal(emailed, true, "a normal delivery fires the side-effect");
});

test("bypass is a no-op when the RedriveIO cannot carry headers", async () => {
  const io = plainIO();
  io.queue("dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));

  const res = await redrive(io, "dlq", { bypass: true });
  assert.equal(res.redriven, 1);
  assert.equal(res.items[0].bypassed, false, "bypass must be a no-op without publishWithHeaders");
  assert.equal(io.queue("orders").length, 1, "the message is still redriven, just header-less");
});
