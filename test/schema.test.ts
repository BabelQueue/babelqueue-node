import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { InvalidPayloadError } from "../src/index.js";
import type { Envelope } from "../src/index.js";
import {
  check,
  MapProvider,
  UNSUPPORTED_KEYWORD_WARNING_CODE,
  unsupportedKeywords,
  validate,
  validateSchema,
  wrap,
} from "../src/schema.js";
import type { RegisteredSchemaWarning, SchemaNode, SchemaProvider } from "../src/schema.js";

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

test("payload conformance: minLength counts Unicode code points", () => {
  const suite = new URL("./conformance/", import.meta.url);
  const manifest = JSON.parse(
    readFileSync(fileURLToPath(new URL("manifest.json", suite)), "utf8"),
  ) as {
    payload_schema_unicode?: {
      schema: SchemaNode;
      cases: Array<{ name: string; valid: boolean; data: Record<string, unknown> }>;
    };
  };
  const section = manifest.payload_schema_unicode;
  if (!section) {
    throw new Error("manifest has no payload_schema_unicode section");
  }
  assert.ok(section.cases.length > 0);
  for (const c of section.cases) {
    const isValid: boolean = validateSchema(section.schema, c.data) === null;
    assert.equal(isValid, c.valid, `case ${c.name}`);
  }
});

const node = (src: string): SchemaNode => JSON.parse(src) as SchemaNode;

test("union types: a value is valid when it matches any listed type", () => {
  const nullable = node('{"type":["string","null"]}');
  assert.equal(validateSchema(nullable, "abc"), null);
  assert.equal(validateSchema(nullable, null), null);
  assert.notEqual(validateSchema(nullable, 5), null);
  assert.notEqual(validateSchema(nullable, false), null);
  assert.notEqual(validateSchema(nullable, {}), null);

  const integerOrNull = node('{"type":["integer","null"],"minimum":0}');
  assert.equal(validateSchema(integerOrNull, 1.0), null);
  assert.equal(validateSchema(integerOrNull, JSON.parse("1e2")), null);
  assert.equal(validateSchema(integerOrNull, 10n), null);
  assert.equal(validateSchema(integerOrNull, null), null);
  assert.notEqual(validateSchema(integerOrNull, 1.5), null);
  assert.notEqual(validateSchema(integerOrNull, -1), null, "minimum still applies inside a union");
  assert.notEqual(validateSchema(integerOrNull, true), null);
  assert.notEqual(validateSchema(node('{"type":["integer","boolean"]}'), "1"), null);
});

test("union types keep the per-type constraints of the matched branch", () => {
  const s = node('{"type":["string","null"],"minLength":2,"maxLength":3}');
  assert.notEqual(validateSchema(s, "a"), null);
  assert.notEqual(validateSchema(s, "abcd"), null);
  assert.equal(validateSchema(s, "abc"), null);
  assert.equal(validateSchema(s, null), null);

  const obj = node('{"type":["object","null"],"required":["a"]}');
  assert.notEqual(validateSchema(obj, {}), null);
  assert.equal(validateSchema(obj, { a: 1 }), null);
  assert.equal(validateSchema(obj, null), null);

  const nested = node('{"type":"object","properties":{"list":{"type":"array","items":{"type":["string","number"]}}}}');
  assert.equal(validateSchema(nested, { list: ["a", 2, 3.5] }), null);
  assert.notEqual(validateSchema(nested, { list: ["a", true] }), null);
});

test("maxLength counts Unicode code points", () => {
  const s = node('{"type":"string","maxLength":3}');
  assert.equal(validateSchema(s, "abc"), null);
  assert.equal(validateSchema(s, "ğüş"), null);
  assert.equal(validateSchema(s, "😀😀😀"), null, "3 code points, 6 UTF-16 units");
  assert.notEqual(validateSchema(s, "😀😀😀😀"), null);
  assert.notEqual(validateSchema(node('{"type":"string","maxLength":1}'), "e\u0301"), null, "decomposed é is 2 code points");
});

test("const/enum compare numbers by value, recursively, and keep types apart", () => {
  const one = node('{"const":1}');
  for (const v of [1, 1.0, JSON.parse("1e0"), JSON.parse("1.0"), 1n]) {
    assert.equal(validateSchema(one, v), null, String(v));
  }
  for (const v of [true, "1", null, 1.5, undefined]) {
    assert.notEqual(validateSchema(one, v), null, String(v));
  }
  assert.equal(validateSchema(node('{"const":2.5}'), JSON.parse("2.50")), null);
  assert.equal(validateSchema(node('{"const":1.0}'), 1), null);
  assert.notEqual(validateSchema(node('{"const":true}'), 1), null);
  assert.notEqual(validateSchema(node('{"const":false}'), 0), null);
  assert.notEqual(validateSchema(node('{"const":null}'), 0), null);
  assert.notEqual(validateSchema(node('{"const":null}'), ""), null);

  const e = node('{"enum":[1,2.5,"x",null,{"n":[1]}]}');
  for (const v of [1, 2.5, "x", null, { n: [1.0] }, { n: [1n] }]) {
    assert.equal(validateSchema(e, v), null, JSON.stringify(v, (_k, x: unknown) => (typeof x === "bigint" ? Number(x) : x)));
  }
  for (const v of [3, "1", true, { n: [true] }, { n: [1, 2] }, { n: ["1"] }]) {
    assert.notEqual(validateSchema(e, v), null, JSON.stringify(v));
  }
  assert.notEqual(validateSchema(node('{"enum":[true,null,0]}'), 1), null);
  assert.equal(validateSchema(node('{"enum":[true,null,0]}'), 0.0), null);
});

test("const objects ignore key order, arrays keep order, extra keys differ", () => {
  const c = node('{"const":{"a":1,"b":[2,{"c":3}]}}');
  assert.equal(validateSchema(c, { b: [2.0, { c: 3.0 }], a: 1.0 }), null);
  assert.notEqual(validateSchema(node('{"const":{"a":[1,2]}}'), { a: [2, 1] }), null);
  assert.notEqual(validateSchema(node('{"const":{"a":[1]}}'), { a: [1], b: 1 }), null);
  assert.notEqual(validateSchema(node('{"const":{"a":[1]}}'), { a: [1, 2] }), null);
});

test("unsupportedKeywords: one warning per occurrence, with escaped pointers, never inside unsupported values", () => {
  const s = node(`{
    "type":"object","minProperties":1,
    "properties":{
      "email":{"type":"string","pattern":"^.+@.+$","format":"email"},
      "a/b":{"type":"string","pattern":"x"},
      "m~n":{"type":"string","format":"x"},
      "list":{"type":"array","items":{"type":"integer","multipleOf":2}}
    },
    "additionalProperties":{"type":"string","pattern":"^x"},
    "allOf":[{"type":"integer","pattern":"x"}],
    "definitions":{"s":{"type":"string","format":"x"}}
  }`);
  const got = unsupportedKeywords(s).map((w) => `${w.pointer}|${w.keyword}`).sort();
  assert.deepEqual(got, [
    "/additionalProperties|pattern",
    "/properties/a~1b|pattern",
    "/properties/email|format",
    "/properties/email|pattern",
    "/properties/list/items|multipleOf",
    "/properties/m~0n|format",
    "|allOf",
    "|definitions",
    "|minProperties",
  ].sort());
  // An unsupported keyword never changes the verdict.
  assert.equal(validateSchema(node('{"type":"string","pattern":"^a$"}'), "zzz"), null);
});

test("unsupportedKeywords: annotations, x-* and property names named like keywords never warn", () => {
  const s = node(`{
    "$schema":"http://json-schema.org/draft-07/schema#","$id":"urn:x","title":"T","description":"d",
    "examples":[1],"default":{},"$comment":"c","x-owner":"billing","type":"object",
    "properties":{
      "title":{"type":"string"},"pattern":{"type":"string"},"x-gdpr-sensitive":{"type":"string"},
      "email":{"type":"string","x-gdpr-sensitive":true,"x-custom":1,"default":"a"}
    }
  }`);
  assert.deepEqual(unsupportedKeywords(s), []);
  assert.deepEqual(
    unsupportedKeywords(node('{"type":"object","required":["a"],"properties":{"a":{"type":["number","null"],"minimum":0,"minLength":1,"maxLength":5,"enum":[1],"const":1}},"additionalProperties":false}')),
    [],
  );
});

test("MapProvider reports unsupported keywords at registration through onWarning and .warnings", () => {
  const seen: Array<{ warning: RegisteredSchemaWarning; message: string }> = [];
  const p = MapProvider.fromJson(
    { "urn:a": '{"type":"object","properties":{"e":{"type":"string","format":"email"}}}', "urn:b": ORDERS },
    { onWarning: (warning, message) => seen.push({ warning, message }) },
  );
  assert.deepEqual(seen.map((s) => s.warning), [{ urn: "urn:a", pointer: "/properties/e", keyword: "format" }]);
  assert.match(seen[0].message, /"format".*"\/properties\/e".*urn:a/);
  assert.deepEqual(p.warnings, [{ urn: "urn:a", pointer: "/properties/e", keyword: "format" }]);
  assert.deepEqual(MapProvider.fromJson({ "urn:b": ORDERS }, { onWarning: () => assert.fail("no warning expected") }).warnings, []);
});

test("MapProvider default warning channel is process.emitWarning, once per message", async () => {
  const emitted: Array<{ message: string; code?: string }> = [];
  const onWarning = (w: Error & { code?: string }): void => {
    if (w.code === UNSUPPORTED_KEYWORD_WARNING_CODE) emitted.push({ message: w.message, code: w.code });
  };
  process.on("warning", onWarning);
  try {
    const raw = { "urn:default-channel": '{"type":"object","minProperties":3}' };
    MapProvider.fromJson(raw);
    MapProvider.fromJson(raw);
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off("warning", onWarning);
  }
  assert.equal(emitted.length, 1);
  assert.match(emitted[0].message, /minProperties/);
});
