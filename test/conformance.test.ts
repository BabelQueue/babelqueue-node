import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { EnvelopeCodec, FORBIDDEN_KEY_WARNING_CODE } from "../src/index.js";
import type { Envelope } from "../src/index.js";

interface ConformanceCase {
  name: string;
  file: string;
  valid: boolean;
  reason?: string;
  expect?: {
    urn: string;
    data?: Record<string, unknown>;
    attempts: number;
    lang: string;
    schema_version: number;
    dead_letter?: { reason?: string; original_queue?: string };
  };
}

interface Manifest {
  schema_version: number;
  cases: ConformanceCase[];
}

const suite = new URL("./conformance/", import.meta.url);
const readJson = (relative: string): string =>
  readFileSync(fileURLToPath(new URL(relative, suite)), "utf8");

const manifest = JSON.parse(readJson("manifest.json")) as Manifest;

test("conformance manifest matches the core schema version", () => {
  assert.equal(manifest.schema_version, EnvelopeCodec.SCHEMA_VERSION);
  assert.ok(manifest.cases.length > 0, "manifest has no cases");
});

// The shared cross-SDK suite — the same fixtures every BabelQueue SDK must
// satisfy. Per-message fields (meta.id, trace_id, meta.created_at) are
// intrinsically unique and are checked for presence, not value.
for (const testCase of manifest.cases) {
  test(`conformance: ${testCase.name}`, () => {
    const body = readJson(testCase.file);
    const env = EnvelopeCodec.decode(body);

    if (!testCase.valid) {
      assert.ok(
        !EnvelopeCodec.accepts(env),
        `invalid fixture must be rejected (${testCase.reason ?? ""})`,
      );
      return;
    }

    if (!EnvelopeCodec.accepts(env)) {
      assert.fail("valid fixture must be accepted");
    }

    const expected = testCase.expect!;
    assert.equal(EnvelopeCodec.urn(env), expected.urn);
    assert.equal(env.attempts, expected.attempts);
    assert.equal(env.meta.lang, expected.lang);
    assert.equal(env.meta.schema_version, expected.schema_version);
    if (expected.data) {
      assert.deepEqual(env.data, expected.data);
    }

    assert.ok(
      env.trace_id && env.meta.id && env.meta.created_at,
      "per-message fields must be present",
    );

    if (expected.dead_letter) {
      assert.ok(env.dead_letter, "expected a dead_letter block");
      if (expected.dead_letter.reason !== undefined) {
        assert.equal(env.dead_letter.reason, expected.dead_letter.reason);
      }
      if (expected.dead_letter.original_queue !== undefined) {
        assert.equal(
          env.dead_letter.original_queue,
          expected.dead_letter.original_queue,
        );
      }
    }
  });
}

// --- Behaviour conformance (roundtrip / data_shape / forbidden_keys) --------

interface BehaviourManifest {
  roundtrip?: {
    cases: Array<{
      name: string;
      file: string;
      expect_attempts: number;
      expect_preserved: Record<string, unknown>;
      producer_schema_valid?: boolean;
    }>;
  };
  data_shape?: {
    cases: Array<
      | {
          name: string;
          mode: "encode";
          urn: string;
          queue: string;
          data: Record<string, unknown>;
          expect_encoded_data_json: string;
        }
      | { name: string; mode: "decode"; file: string; valid: boolean; reason?: string }
    >;
  };
  forbidden_keys?: {
    cases: Array<{
      name: string;
      file: string;
      forbidden_key: string;
      expect: "warn";
      expect_absent_after_reencode: string[];
    }>;
  };
}

const behaviour = JSON.parse(readJson("manifest.json")) as BehaviourManifest;

const MISSING = Symbol("missing");

/** Resolve an RFC 6901 JSON pointer; returns {@link MISSING} when absent. */
function resolvePointer(doc: unknown, pointer: string): unknown {
  if (pointer === "") {
    return doc;
  }
  let current: unknown = doc;
  for (const raw of pointer.slice(1).split("/")) {
    const token = raw.replace(/~1/g, "/").replace(/~0/g, "~");
    if (Array.isArray(current)) {
      const index = Number(token);
      if (!/^(0|[1-9]\d*)$/.test(token) || index >= current.length) {
        return MISSING;
      }
      current = current[index];
    } else if (current !== null && typeof current === "object") {
      if (!Object.prototype.hasOwnProperty.call(current, token)) {
        return MISSING;
      }
      current = (current as Record<string, unknown>)[token];
    } else {
      return MISSING;
    }
  }
  return current;
}

/** The raw JSON text of the top-level `data` value, whitespace removed. */
function rawDataJson(encoded: string): string {
  const reparsed = JSON.parse(encoded) as { data: unknown };
  // JSON.stringify of a parsed value is compact; `{}` and `[]` stay distinct.
  return JSON.stringify(reparsed.data);
}

test("conformance: behaviour sections are present", () => {
  assert.ok(behaviour.roundtrip && behaviour.roundtrip.cases.length > 0);
  assert.ok(behaviour.data_shape && behaviour.data_shape.cases.length > 0);
  assert.ok(behaviour.forbidden_keys && behaviour.forbidden_keys.cases.length > 0);
});

for (const c of behaviour.roundtrip?.cases ?? []) {
  test(`conformance roundtrip: ${c.name}`, () => {
    const warnings: string[] = [];
    const env = EnvelopeCodec.decode(readJson(c.file), {
      onWarning: (m) => warnings.push(m),
    });
    if (!EnvelopeCodec.accepts(env)) {
      assert.fail("roundtrip fixture must be accepted");
    }
    env.attempts += 1;
    const out = JSON.parse(EnvelopeCodec.encode(env)) as unknown;

    assert.equal(resolvePointer(out, "/attempts"), c.expect_attempts);
    for (const [pointer, expected] of Object.entries(c.expect_preserved)) {
      const actual = resolvePointer(out, pointer);
      assert.notEqual(actual, MISSING, `${pointer} must be preserved`);
      assert.deepStrictEqual(actual, expected, `${pointer} must round-trip unchanged`);
    }
    assert.deepEqual(warnings, [], "a canonical fixture must not warn");
  });
}

for (const c of behaviour.data_shape?.cases ?? []) {
  test(`conformance data_shape: ${c.name}`, () => {
    if (c.mode === "encode") {
      const env = EnvelopeCodec.make(c.urn, c.data, { queue: c.queue });
      const expected = JSON.stringify(JSON.parse(c.expect_encoded_data_json));
      assert.equal(rawDataJson(EnvelopeCodec.encode(env)), expected);
      return;
    }
    const verdict = EnvelopeCodec.accepts(EnvelopeCodec.decode(readJson(c.file)));
    assert.equal(verdict, c.valid, c.reason ?? "");
  });
}

for (const c of behaviour.forbidden_keys?.cases ?? []) {
  test(`conformance forbidden_keys: ${c.name}`, () => {
    assert.equal(c.expect, "warn");
    const warnings: string[] = [];
    const env = EnvelopeCodec.decode(readJson(c.file), {
      onWarning: (m) => warnings.push(m),
    });
    if (!EnvelopeCodec.accepts(env)) {
      assert.fail("a fixture carrying a forbidden key must still decode");
    }
    assert.ok(
      warnings.some((w) => w.includes(c.forbidden_key)),
      `expected a warning naming ${c.forbidden_key}, got ${JSON.stringify(warnings)}`,
    );

    const out = JSON.parse(EnvelopeCodec.encode(env)) as unknown;
    for (const pointer of c.expect_absent_after_reencode) {
      assert.equal(resolvePointer(out, pointer), MISSING, `${pointer} must not be re-emitted`);
    }
    assert.equal(typeof resolvePointer(out, "/attempts"), "number");
  });
}

async function captureProcessWarnings(fn: () => void): Promise<Error[]> {
  const seen: Error[] = [];
  const listener = (w: Error): void => {
    seen.push(w);
  };
  process.on("warning", listener);
  try {
    fn();
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off("warning", listener);
  }
  return seen;
}

test("forbidden keys: the default warning path uses process.emitWarning, once per key", async () => {
  const seen = await captureProcessWarnings(() => {
    for (let i = 0; i < 3; i += 1) {
      const env = EnvelopeCodec.decode(readJson("fixtures/forbidden-key-meta-ts.json"));
      assert.ok(EnvelopeCodec.accepts(env));
    }
  });
  const hits = seen.filter((w) => w.message.includes("/meta/ts"));
  assert.equal(hits.length, 1, "a process warning naming /meta/ts must be emitted exactly once");
  assert.equal((hits[0] as Error & { code?: string }).code, FORBIDDEN_KEY_WARNING_CODE);
});

test("forbidden keys: an explicit onWarning sees every occurrence", () => {
  const warnings: string[] = [];
  for (let i = 0; i < 3; i += 1) {
    EnvelopeCodec.decode(readJson("fixtures/forbidden-key-meta-ts.json"), {
      onWarning: (m) => warnings.push(m),
    });
  }
  assert.equal(warnings.filter((w) => w.includes("/meta/ts")).length, 3);
});

test("forbidden keys: encode warns about each key it leaves out", async () => {
  const dirty = (): Envelope => {
    const env = EnvelopeCodec.make("urn:babel:orders:created", { id: 1 });
    (env as unknown as Record<string, unknown>).timestamp = 1;
    (env.meta as unknown as Record<string, unknown>).source = "php";
    return env;
  };
  const warnings: string[] = [];
  EnvelopeCodec.encode(dirty(), { onWarning: (m) => warnings.push(m) });
  assert.equal(warnings.length, 2);
  assert.ok(warnings.some((w) => w.includes("/timestamp")));
  assert.ok(warnings.some((w) => w.includes("/meta/source")));
  assert.ok(warnings.every((w) => w.includes("not encoding")));

  const seen = await captureProcessWarnings(() => {
    EnvelopeCodec.encode(dirty());
    EnvelopeCodec.encode(dirty());
  });
  const hits = seen.filter((w) => w.message.includes("not encoding"));
  assert.equal(hits.length, 2, "default channel: once per key per process");
  assert.ok(hits.every((w) => (w as Error & { code?: string }).code === FORBIDDEN_KEY_WARNING_CODE));

  const clean: string[] = [];
  EnvelopeCodec.encode(EnvelopeCodec.make("urn:x:y", {}), { onWarning: (m) => clean.push(m) });
  assert.deepEqual(clean, []);
});

test("forbidden keys: encode never emits them, even when set by the caller", () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { id: 1 });
  const dirty = env as unknown as Record<string, unknown>;
  dirty.timestamp = 123;
  (dirty.meta as Record<string, unknown>).max_retries = 3;
  (dirty.meta as Record<string, unknown>).attempts = 2;
  (dirty.meta as Record<string, unknown>).source = "php";
  (dirty.meta as Record<string, unknown>).ts = 1;
  dirty.extra_top = "kept";

  const out = JSON.parse(EnvelopeCodec.encode(env, { onWarning: () => {} })) as Record<string, unknown>;
  assert.equal(resolvePointer(out, "/timestamp"), MISSING);
  for (const key of ["max_retries", "attempts", "source", "ts"]) {
    assert.equal(resolvePointer(out, `/meta/${key}`), MISSING, key);
  }
  assert.equal(out.extra_top, "kept");
  assert.equal(out.attempts, 0);
  // The caller's object is not mutated by encode.
  assert.equal((dirty.meta as Record<string, unknown>).ts, 1);
});
