import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, type Envelope } from "../src/codec.js";
import {
  redrive,
  resetForRedrive,
  type RedriveIO,
  type RedriveMessage,
} from "../src/redrive.js";

// memoryIO is a tiny in-memory RedriveIO over a Map of queues, with a queue() accessor for assertions.
function memoryIO(): RedriveIO & { queue(name: string): string[]; failOn?: string } {
  const queues = new Map<string, string[]>();
  const q = (name: string): string[] => {
    let arr = queues.get(name);
    if (!arr) {
      arr = [];
      queues.set(name, arr);
    }
    return arr;
  };
  const io: RedriveIO & { queue(name: string): string[]; failOn?: string } = {
    queue: q,
    async pop(queue: string): Promise<RedriveMessage | null> {
      const arr = q(queue);
      if (arr.length === 0) {
        return null;
      }
      const body = arr.shift() as string;
      return {
        body,
        async ack(): Promise<void> {},
      };
    },
    async publish(queue: string, body: string): Promise<void> {
      if (io.failOn && queue === io.failOn) {
        throw new Error(`publish refused for ${queue}`);
      }
      q(queue).push(body);
    },
  };
  return io;
}

function deadLetteredBody(urn: string, originalQueue: string, data: Record<string, unknown> = {}): string {
  const base = EnvelopeCodec.make(urn, data, { queue: originalQueue });
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

test("resetForRedrive strips dead_letter and resets attempts, without mutating the input", () => {
  const base = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" });
  const input: Envelope = { ...base, attempts: 3, dead_letter: { reason: "failed", error: null, exception: null, failed_at: 1, original_queue: "orders", attempts: 3, lang: "node" } };

  const out = resetForRedrive(input);
  assert.equal(out.dead_letter, undefined);
  assert.equal(out.attempts, 0);
  assert.equal(out.trace_id, input.trace_id);
  assert.equal(out.job, input.job);
  // input untouched
  assert.ok(input.dead_letter);
  assert.equal(input.attempts, 3);
});

test("redrive sends a message back to its source queue, reset and traceable", async () => {
  const io = memoryIO();
  io.queue("orders.dlq").push(deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 1 }));
  const traceId = EnvelopeCodec.decode(io.queue("orders.dlq")[0]).trace_id;

  const res = await redrive(io, "orders.dlq");
  assert.deepEqual([res.redriven, res.skipped], [1, 0]);

  assert.equal(io.queue("orders.dlq").length, 0, "DLQ should be drained");
  assert.equal(io.queue("orders").length, 1);
  const back = EnvelopeCodec.decode(io.queue("orders")[0]);
  assert.equal(back.dead_letter, undefined);
  assert.equal(back.attempts, 0);
  assert.equal(back.trace_id, traceId);
});

test("redrive routes to a sandbox queue without touching the source", async () => {
  const io = memoryIO();
  io.queue("orders.dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));

  const res = await redrive(io, "orders.dlq", { toQueue: "sandbox" });
  assert.equal(res.redriven, 1);
  assert.equal(io.queue("orders").length, 0);
  assert.equal(io.queue("sandbox").length, 1);
});

test("dryRun reports the plan and leaves the DLQ unchanged", async () => {
  const io = memoryIO();
  io.queue("orders.dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));

  const res = await redrive(io, "orders.dlq", { dryRun: true });
  assert.deepEqual([res.redriven, res.skipped], [0, 1]);
  assert.equal(res.items[0].to, "orders");
  assert.equal(res.items[0].redriven, false);
  assert.equal(io.queue("orders").length, 0, "source untouched");
  assert.equal(io.queue("orders.dlq").length, 1, "DLQ unchanged");
  assert.ok(EnvelopeCodec.decode(io.queue("orders.dlq")[0]).dead_letter, "dead_letter intact");
});

test("select redrives only the matching messages, restoring the rest", async () => {
  const io = memoryIO();
  io.queue("dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));
  io.queue("dlq").push(deadLetteredBody("urn:babel:emails:welcome", "emails"));

  const res = await redrive(io, "dlq", {
    select: (e) => EnvelopeCodec.urn(e) === "urn:babel:orders:created",
  });
  assert.deepEqual([res.redriven, res.skipped], [1, 1]);
  assert.equal(io.queue("orders").length, 1);
  assert.equal(io.queue("emails").length, 0);
  assert.equal(io.queue("dlq").length, 1, "unselected restored to the DLQ");
});

test("max caps how many messages are pulled", async () => {
  const io = memoryIO();
  for (let i = 0; i < 3; i++) {
    io.queue("dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));
  }
  const res = await redrive(io, "dlq", { max: 2 });
  assert.equal(res.redriven, 2);
  assert.equal(io.queue("dlq").length, 1);
});

test("a publish failure restores the message to the DLQ and re-throws", async () => {
  const io = memoryIO();
  io.queue("dlq").push(deadLetteredBody("urn:babel:orders:created", "orders"));
  io.failOn = "orders";

  await assert.rejects(redrive(io, "dlq"), /publish refused/);
  assert.equal(io.queue("dlq").length, 1, "message restored to the DLQ");
  assert.equal(io.queue("orders").length, 0);
});

test("an undecodable body is restored, not lost", async () => {
  const io = memoryIO();
  io.queue("dlq").push("not-json{{{");

  const res = await redrive(io, "dlq");
  assert.deepEqual([res.redriven, res.skipped], [0, 1]);
  assert.equal(io.queue("dlq").length, 1);
  assert.equal(io.queue("dlq")[0], "not-json{{{");
});

// --- Record key preservation (contract §6.5 "Record key on republish") ---------

type KeyedMessage = RedriveMessage & { key: Buffer | null };
interface KeyedRecord {
  body: string;
  key: Buffer | null;
}

// keyedIO is an in-memory RedriveIO with the optional publishFrom capability: a reserved message
// carries a key, and publishFrom re-publishes that key (null stays null, empty stays empty).
function keyedIO(): RedriveIO & { records(name: string): KeyedRecord[] } {
  const queues = new Map<string, KeyedRecord[]>();
  const q = (name: string): KeyedRecord[] => {
    let arr = queues.get(name);
    if (!arr) {
      arr = [];
      queues.set(name, arr);
    }
    return arr;
  };
  return {
    records: q,
    async pop(queue: string): Promise<KeyedMessage | null> {
      const rec = q(queue).shift();
      if (!rec) {
        return null;
      }
      return { body: rec.body, key: rec.key, async ack(): Promise<void> {} };
    },
    async publish(queue: string, body: string): Promise<void> {
      q(queue).push({ body, key: null });
    },
    async publishFrom(queue: string, body: string, from: RedriveMessage): Promise<void> {
      const key = (from as KeyedMessage).key;
      q(queue).push({ body, key: key === null || key === undefined ? null : Buffer.from(key) });
    },
  };
}

test("redrive carries the DLQ record key byte-for-byte: keyed, empty and null", async () => {
  const io = keyedIO();
  io.records("orders.dlq").push(
    { body: deadLetteredBody("urn:babel:orders:created", "orders", { n: 1 }), key: Buffer.from("tenant-7") },
    { body: deadLetteredBody("urn:babel:orders:created", "orders", { n: 2 }), key: Buffer.alloc(0) },
    { body: deadLetteredBody("urn:babel:orders:created", "orders", { n: 3 }), key: null },
  );

  const res = await redrive(io, "orders.dlq");
  assert.equal(res.redriven, 3);

  const out = io.records("orders");
  assert.equal(out.length, 3);
  assert.deepEqual(out[0]!.key, Buffer.from("tenant-7"));
  assert.ok(out[1]!.key !== null && out[1]!.key.length === 0, "empty key stays empty");
  assert.equal(out[2]!.key, null, "null key stays null");
});

test("redrive restore writes (dry run, unselected, undecodable) keep the key", async () => {
  const io = keyedIO();
  const key = Buffer.from([0x00, 0xff, 0x10]);
  io.records("orders.dlq").push(
    { body: deadLetteredBody("urn:babel:orders:created", "orders"), key },
    { body: "not-json", key: Buffer.alloc(0) },
  );

  const res = await redrive(io, "orders.dlq", { dryRun: true });
  assert.equal(res.skipped, 2);
  assert.equal(io.records("orders").length, 0);

  const back = io.records("orders.dlq");
  assert.equal(back.length, 2);
  assert.deepEqual(back[0]!.key, key);
  assert.ok(back[1]!.key !== null && back[1]!.key.length === 0);
});

test("a publish failure restores the DLQ message with its key", async () => {
  const io = keyedIO();
  const key = Buffer.from("k1");
  io.records("orders.dlq").push({ body: deadLetteredBody("urn:babel:orders:created", "orders"), key });
  const original = io.publishFrom!.bind(io);
  io.publishFrom = async (queue, body, from) => {
    if (queue === "orders") {
      throw new Error("refused");
    }
    return original(queue, body, from);
  };

  await assert.rejects(() => redrive(io, "orders.dlq"), /refused/);
  const back = io.records("orders.dlq");
  assert.equal(back.length, 1);
  assert.deepEqual(back[0]!.key, key);
});

test("a RedriveIO without publishFrom behaves exactly as before (plain publish)", async () => {
  const io = memoryIO();
  assert.equal(io.publishFrom, undefined);
  io.queue("orders.dlq").push(deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 1 }));
  const res = await redrive(io, "orders.dlq");
  assert.equal(res.redriven, 1);
  assert.equal(io.queue("orders").length, 1);
});
