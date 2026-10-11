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

// rewindingIO is a RedriveIO whose writes to `failQueue` are refused and which records every ack and
// every rewind, with an optional failing rewind.
function rewindingIO(failQueue: string | null, rewindError?: Error): RedriveIO & {
  pending: string[];
  acked: string[];
  rewound: string[];
} {
  const io: RedriveIO & { pending: string[]; acked: string[]; rewound: string[] } = {
    pending: [],
    acked: [],
    rewound: [],
    async pop(): Promise<RedriveMessage | null> {
      const body = io.pending.shift();
      if (body === undefined) {
        return null;
      }
      return {
        body,
        async ack(): Promise<void> {
          io.acked.push(body);
        },
      };
    },
    async publish(queue: string): Promise<void> {
      if (failQueue === null || queue === failQueue) {
        throw new Error(`write to ${queue} refused`);
      }
    },
    async rewind(from: RedriveMessage): Promise<void> {
      io.rewound.push(from.body);
      if (rewindError) {
        throw rewindError;
      }
    },
  };
  return io;
}

test("a failed restore of an undecodable body rewinds and never acks", async () => {
  const io = rewindingIO("orders.dlq");
  io.pending.push("not an envelope");
  await assert.rejects(redrive(io, "orders.dlq"), /write to orders\.dlq refused/);
  assert.deepEqual(io.rewound, ["not an envelope"]);
  assert.deepEqual(io.acked, []);
});

test("a failed restore of an unselected message rewinds and never acks", async () => {
  const io = rewindingIO("orders.dlq");
  const body = deadLetteredBody("urn:babel:orders:created", "orders");
  io.pending.push(body);
  await assert.rejects(redrive(io, "orders.dlq", { select: () => false }), /write to orders\.dlq refused/);
  assert.deepEqual(io.rewound, [body]);
  assert.deepEqual(io.acked, []);
});

test("a failed restore of a dry-run message rewinds and never acks", async () => {
  const io = rewindingIO("orders.dlq");
  const body = deadLetteredBody("urn:babel:orders:created", "orders");
  io.pending.push(body);
  await assert.rejects(redrive(io, "orders.dlq", { dryRun: true }), /write to orders\.dlq refused/);
  assert.deepEqual(io.rewound, [body]);
  assert.deepEqual(io.acked, []);
});

test("a failed redrive write followed by a failed restore rewinds and reports both errors", async () => {
  const io = rewindingIO(null); // every write is refused, the work queue and the DLQ alike
  const body = deadLetteredBody("urn:babel:orders:created", "orders");
  io.pending.push(body);
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof AggregateError);
    assert.deepEqual(
      err.errors.map((e: Error) => e.message),
      ["write to orders refused", "write to orders.dlq refused"],
    );
    return true;
  });
  assert.deepEqual(io.rewound, [body]);
  assert.deepEqual(io.acked, []);
});

test("a failed rewind is reported together with the restore error, still without an ack", async () => {
  const seekFailure = new Error("seek failed");
  const io = rewindingIO("orders.dlq", seekFailure);
  io.pending.push("not an envelope");
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof AggregateError);
    assert.equal(err.errors.length, 2);
    assert.equal(err.errors[0].message, "write to orders.dlq refused");
    assert.equal(err.errors[1], seekFailure);
    return true;
  });
  assert.deepEqual(io.acked, []);
});

test("a failed publish, restore and rewind are reported as one flat list", async () => {
  const seekFailure = new Error("seek failed");
  const io = rewindingIO(null, seekFailure);
  io.pending.push(deadLetteredBody("urn:babel:orders:created", "orders"));
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof AggregateError);
    assert.deepEqual(
      err.errors.map((e: Error) => e.message),
      ["write to orders refused", "write to orders.dlq refused", "seek failed"],
    );
    return true;
  });
  assert.deepEqual(io.acked, []);
});

test("a fully successful run never rewinds", async () => {
  const io = rewindingIO("never-written");
  io.pending.push("not an envelope");
  io.pending.push(deadLetteredBody("urn:babel:orders:created", "orders"));
  const res = await redrive(io, "orders.dlq");
  assert.equal(res.redriven, 1);
  assert.deepEqual(io.rewound, [], "every write succeeded, so nothing is rewound");
});

test("a failed redrive write with a successful restore still rewinds the rest of the batch", async () => {
  const io = rewindingIO("orders"); // the work queue refuses, the DLQ restore succeeds
  const first = deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 1 });
  const second = deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 2 });
  io.pending.push(first, second);
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof Error);
    assert.equal(err.message, "write to orders refused");
    assert.ok(!(err instanceof AggregateError), "a lone publish error is thrown as is");
    return true;
  });
  // The first message was restored and acknowledged; the run stopped, so the rewind hands back the rest. The
  // IO is told which message was the acknowledged one so it can leave that one alone.
  assert.deepEqual(io.acked, [first]);
  assert.deepEqual(io.rewound, [first]);
  assert.deepEqual(io.pending, [], "redrive reserved the whole batch up front, the second message included");
});

test("a failed rewind after a successful restore is reported together with the publish error", async () => {
  const seekFailure = new Error("seek failed");
  const io = rewindingIO("orders", seekFailure);
  const first = deadLetteredBody("urn:babel:orders:created", "orders");
  io.pending.push(first);
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof AggregateError);
    assert.deepEqual(
      err.errors.map((e: Error) => e.message),
      ["write to orders refused", "seek failed"],
    );
    return true;
  });
  assert.deepEqual(io.acked, [first], "the restored message was acknowledged");
});

test("a RedriveIO without rewind keeps leaving a failed restore unacknowledged", async () => {
  const io = rewindingIO("orders.dlq");
  delete (io as { rewind?: unknown }).rewind;
  io.pending.push("not an envelope");
  await assert.rejects(redrive(io, "orders.dlq"), /write to orders\.dlq refused/);
  assert.deepEqual(io.acked, []);
});

test("a RedriveIO without rewind rethrows the restore error as is after a failed publish and restore", async () => {
  const io = rewindingIO(null); // the work queue and the DLQ both refuse
  delete (io as { rewind?: unknown }).rewind;
  io.pending.push(deadLetteredBody("urn:babel:orders:created", "orders"));
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => {
    assert.ok(err instanceof Error);
    assert.ok(!(err instanceof AggregateError), "the restore error keeps its type");
    assert.equal(err.message, "write to orders.dlq refused");
    return true;
  });
  assert.deepEqual(io.acked, []);
});

test("a throwing select predicate rewinds the message and the rest of the batch", async () => {
  const io = rewindingIO("never-written");
  const first = deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 1 });
  const second = deadLetteredBody("urn:babel:orders:created", "orders", { order_id: 2 });
  io.pending.push(first, second);
  const boom = new Error("select exploded");
  await assert.rejects(
    redrive(io, "orders.dlq", {
      select: () => {
        throw boom;
      },
    }),
    (err: unknown) => err === boom,
  );
  assert.deepEqual(io.rewound, [first], "rewound from the message that stopped the run");
  assert.deepEqual(io.acked, []);
});

test("a failed ack rewinds the message", async () => {
  const io = rewindingIO("never-written");
  const body = deadLetteredBody("urn:babel:orders:created", "orders");
  io.pending.push(body);
  const ackFailure = new Error("commit timed out");
  const pop = io.pop.bind(io);
  io.pop = async (dlq: string) => {
    const message = await pop(dlq);
    return message && {
      ...message,
      async ack(): Promise<void> {
        throw ackFailure;
      },
    };
  };
  await assert.rejects(redrive(io, "orders.dlq"), (err: unknown) => err === ackFailure);
  assert.deepEqual(io.rewound, [body]);
});
