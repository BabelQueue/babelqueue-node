import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { InvalidPayloadError } from "../src/index.js";
import type { Envelope } from "../src/index.js";
import { check, MapProvider, validate, validateSchema, wrap } from "../src/schema.js";
import type { SchemaNode, SchemaProvider } from "../src/schema.js";

const ORDERS =
  '{"type":"object","required":["order_id"],' +
  '"properties":{"order_id":{"type":"integer"}},"additionalProperties":false}';

const provider = (): SchemaProvider => MapProvider.fromJson({ "urn:babel:orders:created": ORDERS });

function envelope(urn: string, data: Record<string, unknown>): Envelope {
  return {
    job: urn,
    trace_id: "trace-1",
    data,
    meta: { id: "m1", queue: "orders", lang: "node", schema_version: 1, created_at: 0 },
    attempts: 0,
  };
}

test("validateSchema enforces object/required/types/additionalProperties", () => {
  const s = JSON.parse(ORDERS) as SchemaNode;
  assert.equal(validateSchema(s, { order_id: 7 }), null);
  assert.notEqual(validateSchema(s, {}), null);
  assert.notEqual(validateSchema(s, { order_id: "x" }), null);
  assert.notEqual(validateSchema(s, { order_id: 7, extra: 1 }), null);
});

test("validateSchema scalar parity (bool != integer, enum, minimum, array items)", () => {
  const cases: Array<[string, unknown, boolean]> = [
    ['{"type":"boolean"}', true, true],
    ['{"type":"boolean"}', "x", false],
    ['{"type":"null"}', null, true],
    ['{"type":"null"}', 1, false],
    ['{"type":"number","minimum":0.5}', 0.6, true],
    ['{"type":"number","minimum":0.5}', 0.4, false],
    ['{"type":"integer"}', 1, true],
    ['{"type":"integer"}', 1.5, false],
    ['{"type":"integer"}', true, false],
    ['{"enum":["a","b"]}', "b", true],
    ['{"enum":["a","b"]}', "c", false],
    ['{"type":"array","items":{"type":"string"}}', ["a"], true],
    ['{"type":"array","items":{"type":"string"}}', ["a", 1], false],
  ];
  for (const [src, value, valid] of cases) {
    const s = JSON.parse(src) as SchemaNode;
    assert.equal(validateSchema(s, value) === null, valid, `${src} / ${JSON.stringify(value)}`);
  }
});

test("check: valid, invalid, and unregistered (opt-in)", async () => {
  const p = provider();
  assert.equal(await check(p, "urn:babel:orders:created", { order_id: 1 }), null);
  assert.equal(await check(p, "urn:babel:unknown", { x: 1 }), null);
  assert.notEqual(await check(p, "urn:babel:orders:created", {}), null);
});

test("validate throws InvalidPayloadError on invalid data", async () => {
  await assert.rejects(
    () => validate(provider(), "urn:babel:orders:created", { order_id: "x" }),
    InvalidPayloadError,
  );
});

test("wrap runs on valid, throws + skips on invalid, runs for an unregistered urn", async () => {
  const p = provider();
  let calls = 0;
  const handler = wrap(p, async () => {
    calls += 1;
  });

  await handler(envelope("urn:babel:orders:created", { order_id: 1 }));
  assert.equal(calls, 1);

  await assert.rejects(
    async () => {
      await handler(envelope("urn:babel:orders:created", {}));
    },
    InvalidPayloadError,
  );
  assert.equal(calls, 1);

  await handler(envelope("urn:babel:unknown", { anything: true }));
  assert.equal(calls, 2);
});

test("payload conformance: agrees with the shared cross-SDK cases", () => {
  const suite = new URL("./conformance/", import.meta.url);
  const manifest = JSON.parse(
    readFileSync(fileURLToPath(new URL("manifest.json", suite)), "utf8"),
  ) as {
    payload_schema?: {
      schema: SchemaNode;
      cases: Array<{ name: string; valid: boolean; data: Record<string, unknown> }>;
    };
  };
  const section = manifest.payload_schema;
  if (!section) {
    throw new Error("manifest has no payload_schema section");
  }
  assert.ok(section.cases.length > 0);
  for (const c of section.cases) {
    const isValid: boolean = validateSchema(section.schema, c.data) === null;
    assert.equal(isValid, c.valid, `case ${c.name}`);
  }
});
