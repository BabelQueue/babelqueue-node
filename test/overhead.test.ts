import assert from 'node:assert/strict';
import { test } from 'node:test';

import { EnvelopeCodec } from '../src/index.js';

// GR-8 budget: the envelope encode/decode path must add no more than 2% over plain
// JSON serialization (the baseline a publisher already pays), measured against a
// conservative broker round-trip. Pure CPU — no broker — so the gate is stable in
// CI. Same methodology + reference as every other SDK.
//
// Conservative networked broker round-trip (ns): local loopback Redis measures
// ~300µs; production brokers are slower, so 750µs is conservative.
const REFERENCE_BROKER_ROUNDTRIP_NS = 750_000;

const DATA = { order_id: 1042, amount: 99.9, currency: 'USD', note: 'café ☕' };

function nsPerOp(fn: () => void): number {
  for (let i = 0; i < 5_000; i++) fn(); // warm up
  const iterations = 50_000;
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) fn();
  return Number(process.hrtime.bigint() - start) / iterations;
}

test('codec overhead is within the 2% GR-8 budget', () => {
  const envelope = (): void => {
    EnvelopeCodec.decode(
      EnvelopeCodec.encode(EnvelopeCodec.make('urn:babel:orders:created', DATA, { queue: 'orders' })),
    );
  };
  const bare = (): void => {
    JSON.parse(JSON.stringify(DATA));
  };

  const marginal = Math.max(0, nsPerOp(envelope) - nsPerOp(bare));
  const overhead = (marginal / REFERENCE_BROKER_ROUNDTRIP_NS) * 100;

  assert.ok(
    overhead <= 2.0,
    `codec overhead ${overhead.toFixed(2)}% exceeds the 2% GR-8 budget (marginal ${marginal.toFixed(0)} ns)`,
  );
});
