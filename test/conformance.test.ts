import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import {
  BabelQueueError,
  EnvelopeCodec,
  FORBIDDEN_KEY_WARNING_CODE,
  InvalidEnvelopeKeyError,
  annotate,
} from "../src/index.js";
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

// --- Behaviour conformance (roundtrip / data_shape / forbidden_keys / extension_write / encode_reject) ---

interface RoundtripCase {
  name: string;
  file: string;
  expect_attempts: number;
  expect_preserved: Record<string, unknown>;
  producer_schema_valid?: boolean;
  expect_absent_after_reencode?: string[];
  apply_dead_letter?: { reason: string; original_queue: string };
  expect_dead_letter?: Record<string, unknown>;
}

interface ForbiddenKeysCase {
  name: string;
  file: string;
  forbidden_key: string;
  expect: "warn";
  expect_absent_after_reencode: string[];
  expect_preserved?: Record<string, unknown>;
}

interface ExtensionWriteCase {
  name: string;
  urn: string;
  queue: string;
  data: Record<string, unknown>;
  meta: Record<string, unknown>;
  expect_envelope: Record<string, unknown>;
}

interface EncodeRejectCase {
  name: string;
  urn: string;
  queue: string;
  data: Record<string, unknown>;
  meta?: Record<string, unknown>;
  meta_entries?: Array<[string, unknown]>;
  extras_top?: Record<string, unknown>;
  extras_meta?: Record<string, unknown>;
  offending_key?: string;
  expect: "reject";
}

interface BehaviourManifest {
  roundtrip?: { cases: RoundtripCase[] };
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
  forbidden_keys?: { cases: ForbiddenKeysCase[] };
  extension_write?: { cases: ExtensionWriteCase[] };
  encode_reject?: { cases: EncodeRejectCase[] };
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

// The runner checks return a list of violations (empty = pass) instead of asserting
// inline, so the "runner really evaluates this field" tests below can feed them
// synthetic cases and see the violation reported.

/** Every pointer must resolve to the given value (JSON type distinction). */
function checkPreserved(out: unknown, expected: Record<string, unknown> | undefined): string[] {
  const violations: string[] = [];
  for (const [pointer, want] of Object.entries(expected ?? {})) {
    const actual = resolvePointer(out, pointer);
    if (actual === MISSING) {
      violations.push(`${pointer} must be preserved but is missing`);
    } else if (!isDeepStrictEqual(actual, want)) {
      violations.push(`${pointer} must round-trip unchanged: want ${JSON.stringify(want)}, got ${JSON.stringify(actual)}`);
    }
  }
  return violations;
}

/** No pointer may be present. */
function checkAbsent(out: unknown, pointers: string[] | undefined): string[] {
  return (pointers ?? [])
    .filter((pointer) => resolvePointer(out, pointer) !== MISSING)
    .map((pointer) => `${pointer} must not be re-emitted`);
}

/** The `roundtrip` section runner for one case; `body` is the fixture text. */
function runRoundtrip(c: RoundtripCase, body: string): string[] {
  const violations: string[] = [];
  const warnings: string[] = [];
  const env = EnvelopeCodec.decode(body, { onWarning: (m) => warnings.push(m) });
  if (!EnvelopeCodec.accepts(env)) {
    return ["roundtrip fixture must be accepted"];
  }
  env.attempts += 1;
  let message: Envelope = env;
  if (c.apply_dead_letter) {
    // The SDK's own DLQ helper (the path the worker takes on retry exhaustion).
    message = annotate(env, c.apply_dead_letter.reason, c.apply_dead_letter.original_queue, {
      error: "conformance",
      exception: "ConformanceError",
    });
  }
  const out = JSON.parse(EnvelopeCodec.encode(message)) as unknown;

  if (resolvePointer(out, "/attempts") !== c.expect_attempts) {
    violations.push(`/attempts must be ${c.expect_attempts}`);
  }
  violations.push(...checkPreserved(out, c.expect_preserved));
  violations.push(...checkAbsent(out, c.expect_absent_after_reencode));
  if (c.expect_dead_letter) {
    const block = resolvePointer(out, "/dead_letter");
    if (block === MISSING) {
      violations.push("/dead_letter block must be present");
    } else {
      violations.push(
        ...checkPreserved(
          block,
          Object.fromEntries(Object.entries(c.expect_dead_letter).map(([k, v]) => [`/${k}`, v])),
        ).map((v) => `dead_letter: ${v}`),
      );
    }
  }
  if (warnings.length > 0) {
    violations.push(`a canonical fixture must not warn, got ${JSON.stringify(warnings)}`);
  }
  return violations;
}

/** The `forbidden_keys` section runner for one case; `body` is the fixture text. */
function runForbiddenKeys(c: ForbiddenKeysCase, body: string): string[] {
  const violations: string[] = [];
  const warnings: string[] = [];
  const env = EnvelopeCodec.decode(body, { onWarning: (m) => warnings.push(m) });
  if (!EnvelopeCodec.accepts(env)) {
    return ["a fixture carrying a forbidden key must still decode"];
  }
  if (!warnings.some((w) => w.includes(c.forbidden_key))) {
    violations.push(`expected a warning naming ${c.forbidden_key}, got ${JSON.stringify(warnings)}`);
  }
  const out = JSON.parse(EnvelopeCodec.encode(env)) as unknown;
  violations.push(...checkAbsent(out, c.expect_absent_after_reencode));
  violations.push(...checkPreserved(out, c.expect_preserved));
  if (typeof resolvePointer(out, "/attempts") !== "number") {
    violations.push("/attempts must be a number");
  }
  return violations;
}

/** The `extension_write` section runner for one case. */
function runExtensionWrite(c: ExtensionWriteCase, encode: (e: Envelope) => string = EnvelopeCodec.encode): string[] {
  const env = EnvelopeCodec.make(c.urn, c.data, { queue: c.queue, meta: c.meta });
  const first = encode(env);
  const violations = checkPreserved(JSON.parse(first), c.expect_envelope);

  // Re-emit chain: decode -> attempts + 1 -> encode; extension keys survive the hop.
  const warnings: string[] = [];
  const hop = EnvelopeCodec.decode(first, { onWarning: (m) => warnings.push(m) });
  if (warnings.length > 0) {
    // `make` output is canonical, so decoding it must not warn (same rule as `roundtrip`).
    violations.push(`a canonical message must not warn on decode, got ${JSON.stringify(warnings)}`);
  }
  if (!EnvelopeCodec.accepts(hop)) {
    return [...violations, "the encoded message must be accepted"];
  }
  hop.attempts += 1;
  const second = JSON.parse(EnvelopeCodec.encode(hop)) as unknown;
  violations.push(...checkPreserved(second, c.expect_envelope).map((v) => `after re-emit: ${v}`));
  if (resolvePointer(second, "/attempts") !== 1) {
    violations.push("after re-emit: /attempts must be 1");
  }
  return violations;
}

/** The `encode_reject` runner for one case: build + encode MUST fail and yield no JSON. */
function runEncodeReject(c: EncodeRejectCase): string[] {
  let produced: string | undefined;
  let failure: unknown;
  try {
    const env = EnvelopeCodec.make(c.urn, c.data, { queue: c.queue, meta: c.meta });
    Object.assign(env, c.extras_top ?? {});
    Object.assign(env.meta, c.extras_meta ?? {});
    produced = EnvelopeCodec.encode(env);
  } catch (error) {
    failure = error;
  }
  if (produced !== undefined) {
    return [`encode must reject, but produced ${produced}`];
  }
  if (!(failure instanceof InvalidEnvelopeKeyError)) {
    return [`expected InvalidEnvelopeKeyError, got ${String(failure)}`];
  }
  if (c.offending_key !== undefined && !failure.message.includes(c.offending_key)) {
    return [`the error must name ${c.offending_key}, got: ${failure.message}`];
  }
  return [];
}

test("conformance: behaviour sections are present", () => {
  assert.ok(behaviour.roundtrip && behaviour.roundtrip.cases.length > 0);
  assert.ok(behaviour.data_shape && behaviour.data_shape.cases.length > 0);
  assert.ok(behaviour.forbidden_keys && behaviour.forbidden_keys.cases.length > 0);
  assert.ok(behaviour.extension_write && behaviour.extension_write.cases.length > 0);
  assert.ok(behaviour.encode_reject && behaviour.encode_reject.cases.length > 0);
});

for (const c of behaviour.roundtrip?.cases ?? []) {
  test(`conformance roundtrip: ${c.name}`, () => {
    assert.deepEqual(runRoundtrip(c, readJson(c.file)), []);
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
    assert.deepEqual(runForbiddenKeys(c, readJson(c.file)), []);
  });
}

for (const c of behaviour.extension_write?.cases ?? []) {
  test(`conformance extension_write: ${c.name}`, () => {
    assert.deepEqual(runExtensionWrite(c), []);
  });
}

for (const c of behaviour.encode_reject?.cases ?? []) {
  // `meta_entries` is a repeated key, which one `MakeOptions.meta` map cannot express.
  test(`conformance encode_reject: ${c.name}`, { skip: c.meta_entries !== undefined }, () => {
    assert.equal(c.expect, "reject");
    assert.deepEqual(runEncodeReject(c), []);
  });
}

// --- The runner itself: synthetic cases prove each optional field is evaluated -----

const canonicalBody = JSON.stringify({
  job: "urn:acme:a",
  trace_id: "t-1",
  data: { n: 1 },
  meta: { id: "m-1", queue: "q", lang: "php", schema_version: 1, created_at: 1, vendor_flag: true },
  attempts: 0,
  extra_top: 1,
});

test("runner: expect_absent_after_reencode reports a pointer that is still present", () => {
  const base: RoundtripCase = {
    name: "synthetic",
    file: "",
    expect_attempts: 1,
    expect_preserved: { "/job": "urn:acme:a" },
  };
  assert.deepEqual(runRoundtrip(base, canonicalBody), []);
  const violations = runRoundtrip(
    { ...base, expect_absent_after_reencode: ["/meta/vendor_flag", "/nope"] },
    canonicalBody,
  );
  assert.deepEqual(violations, ["/meta/vendor_flag must not be re-emitted"]);
});

test("runner: expect_preserved reports a differing value (roundtrip and forbidden_keys)", () => {
  const base: RoundtripCase = {
    name: "synthetic",
    file: "",
    expect_attempts: 1,
    expect_preserved: { "/meta/vendor_flag": 1, "/gone": 0 },
  };
  const violations = runRoundtrip(base, canonicalBody);
  assert.equal(violations.length, 2, JSON.stringify(violations));
  assert.ok(violations[0]?.startsWith("/meta/vendor_flag must round-trip unchanged"));
  assert.ok(violations[1]?.startsWith("/gone must be preserved"));

  const dirty = JSON.stringify({ ...JSON.parse(canonicalBody), timestamp: 5 });
  const forbidden: ForbiddenKeysCase = {
    name: "synthetic",
    file: "",
    forbidden_key: "/timestamp",
    expect: "warn",
    expect_absent_after_reencode: ["/timestamp"],
    expect_preserved: { "/job": "urn:acme:other" },
  };
  const found = runForbiddenKeys(forbidden, dirty);
  assert.equal(found.length, 1, JSON.stringify(found));
  assert.ok(found[0]?.startsWith("/job must round-trip unchanged"));
  assert.deepEqual(runForbiddenKeys({ ...forbidden, expect_preserved: { "/job": "urn:acme:a" } }, dirty), []);
});

test("runner: apply_dead_letter goes through annotate and expect_dead_letter is evaluated", () => {
  const base: RoundtripCase = {
    name: "synthetic",
    file: "",
    expect_attempts: 1,
    expect_preserved: { "/meta/vendor_flag": true, "/extra_top": 1 },
    apply_dead_letter: { reason: "failed", original_queue: "orders" },
    expect_dead_letter: { reason: "failed", original_queue: "orders" },
  };
  assert.deepEqual(runRoundtrip(base, canonicalBody), []);
  const wrong = runRoundtrip({ ...base, expect_dead_letter: { reason: "other" } }, canonicalBody);
  assert.equal(wrong.length, 1, JSON.stringify(wrong));
  assert.ok(wrong[0]?.startsWith("dead_letter: /reason must round-trip unchanged"));
  // Without apply_dead_letter there is no block, and expecting one is a violation.
  const plain: RoundtripCase = { ...base };
  delete plain.apply_dead_letter;
  assert.deepEqual(runRoundtrip(plain, canonicalBody), ["/dead_letter block must be present"]);
});

test("runner: encode_reject and extension_write really fail on a wrong expectation", () => {
  const accepted: EncodeRejectCase = {
    name: "synthetic",
    urn: "urn:acme:a",
    queue: "q",
    data: {},
    meta: { vendor_flag: true },
    expect: "reject",
  };
  assert.equal(runEncodeReject(accepted).length, 1, "a clean message must not count as rejected");
  assert.equal(
    runEncodeReject({ ...accepted, meta: { ts: 1 }, offending_key: "/meta/other" }).length,
    1,
    "the offending key must be named",
  );
  const write: ExtensionWriteCase = {
    name: "synthetic",
    urn: "urn:acme:a",
    queue: "q",
    data: {},
    meta: { vendor_flag: true },
    expect_envelope: { "/meta/vendor_flag": true },
  };
  assert.deepEqual(runExtensionWrite(write), []);
  // An encoder that leaks a forbidden key makes the decode hop warn; the runner must report it.
  const leaky = (e: Envelope): string => {
    const raw = JSON.parse(EnvelopeCodec.encode(e)) as { meta: Record<string, unknown> };
    raw.meta.ts = 1;
    return JSON.stringify(raw);
  };
  const leaked = runExtensionWrite(write, leaky);
  assert.equal(leaked.length, 1, JSON.stringify(leaked));
  assert.ok(leaked[0]?.startsWith("a canonical message must not warn on decode"));
  assert.ok(runExtensionWrite({ ...write, expect_envelope: { "/meta/vendor_flag": 1 } }).length >= 2);
});

// --- Producer meta (ADR-0031 (b)) and decode identity (ADR-0031 (c)) ---------------

test("make: meta carries extension keys, rejects forbidden and core keys", () => {
  assert.throws(() => EnvelopeCodec.make("urn:a:b", {}, { meta: { ts: 1 } }), InvalidEnvelopeKeyError);
  assert.throws(() => EnvelopeCodec.make("urn:a:b", {}, { meta: { id: "x" } }), /\/meta\/id/);
  assert.throws(() => EnvelopeCodec.make("urn:a:b", {}, { meta: { ts: 1 } }), BabelQueueError);

  const env = EnvelopeCodec.make("urn:a:b", { n: 1 }, { meta: { ext: { acme: 1 } } });
  assert.equal(env.meta.lang, "node");
  const hop = EnvelopeCodec.decode(EnvelopeCodec.encode(env));
  assert.ok(EnvelopeCodec.accepts(hop));
  hop.attempts += 1;
  const out = JSON.parse(EnvelopeCodec.encode(hop)) as { meta: { ext: unknown } };
  assert.deepEqual(out.meta.ext, { acme: 1 });
});

test("encode: throws on a non-canonical key and produces no output", () => {
  const env = EnvelopeCodec.make("urn:a:b", {});
  (env as unknown as Record<string, unknown>).urn = "urn:a:b";
  assert.throws(() => EnvelopeCodec.encode(env), (e: unknown) => e instanceof InvalidEnvelopeKeyError && e.pointer === "/urn");
});

test("decode: blank ASCII job falls back to urn; Unicode space does not", () => {
  for (const blank of ["", " ", "  ", "\t", "\n\v\f\r"]) {
    const env = EnvelopeCodec.decode(JSON.stringify({ ...JSON.parse(canonicalBody), job: blank, urn: "urn:acme:b" }));
    assert.equal(env.job, "urn:acme:b", JSON.stringify(blank));
    assert.ok(!("urn" in env));
    assert.ok(EnvelopeCodec.accepts(env));
  }
  const warnings: string[] = [];
  const nonAscii = EnvelopeCodec.decode(
    JSON.stringify({ ...JSON.parse(canonicalBody), job: "\u2003", urn: "urn:acme:b" }),
    { onWarning: (m) => warnings.push(m) },
  );
  assert.equal(nonAscii.job, "\u2003");
  assert.ok(!("urn" in nonAscii));
  assert.ok(warnings.some((w) => w.includes("/urn")));
  assert.ok(EnvelopeCodec.accepts(nonAscii));
  assert.ok(!EnvelopeCodec.accepts(EnvelopeCodec.decode(JSON.stringify({ ...JSON.parse(canonicalBody), job: " " }))));
});

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

test("forbidden keys: encode throws for each non-canonical key instead of warning", () => {
  const cases: Array<[string, (e: Envelope) => void]> = [
    ["/timestamp", (e) => void ((e as unknown as Record<string, unknown>).timestamp = 1)],
    ["/urn", (e) => void ((e as unknown as Record<string, unknown>).urn = "urn:x:y")],
    ["/meta/max_retries", (e) => void (e.meta.max_retries = 3)],
    ["/meta/attempts", (e) => void (e.meta.attempts = 2)],
    ["/meta/source", (e) => void (e.meta.source = "php")],
    ["/meta/ts", (e) => void (e.meta.ts = 1)],
  ];
  for (const [pointer, dirty] of cases) {
    const env = EnvelopeCodec.make("urn:babel:orders:created", { id: 1 });
    dirty(env);
    assert.throws(
      () => EnvelopeCodec.encode(env),
      (e: unknown) => e instanceof InvalidEnvelopeKeyError && e.pointer === pointer,
      pointer,
    );
  }
  // Clean envelopes still encode, extras (unknown keys) are kept.
  const env = EnvelopeCodec.make("urn:x:y", {});
  (env as unknown as Record<string, unknown>).extra_top = "kept";
  assert.equal((JSON.parse(EnvelopeCodec.encode(env)) as Record<string, unknown>).extra_top, "kept");
});

test("decode: a urn that cannot stand in for a missing job is dropped with a warning", () => {
  const base = JSON.parse(canonicalBody) as Record<string, unknown>;
  delete base.job;
  for (const urn of [123, null, {}, [], true]) {
    const warnings: string[] = [];
    const env = EnvelopeCodec.decode(JSON.stringify({ ...base, urn }), { onWarning: (m) => warnings.push(m) });
    assert.ok(!("urn" in env), JSON.stringify(urn));
    assert.ok(warnings.some((w) => w.includes("/urn")), JSON.stringify(urn));
    assert.ok(!EnvelopeCodec.accepts(env), JSON.stringify(urn));
    // The decoded object is re-encodable (no forbidden key is left on it).
    assert.doesNotThrow(() => EnvelopeCodec.encode(env as Envelope));
  }
});

test("decode: a non-string job keeps winning over urn, and urn is dropped with a warning", () => {
  const base = JSON.parse(canonicalBody) as Record<string, unknown>;
  const warnings: string[] = [];
  const env = EnvelopeCodec.decode(JSON.stringify({ ...base, job: 5, urn: "urn:acme:b" }), {
    onWarning: (m) => warnings.push(m),
  });
  assert.equal((env as unknown as Record<string, unknown>).job, 5);
  assert.ok(!("urn" in env));
  assert.ok(warnings.some((w) => w.includes("/urn")));
  assert.doesNotThrow(() => EnvelopeCodec.encode(env as Envelope));
});

test("encode: the deprecated options argument is still accepted and its onWarning is never called", () => {
  let called = false;
  const env = EnvelopeCodec.make("urn:a:b", {});
  EnvelopeCodec.encode(env, { onWarning: () => void (called = true) });
  assert.equal(called, false);
});

test("make: meta is copied shallowly - top-level keys are detached, nested values are shared", () => {
  const meta: Record<string, unknown> = { flag: true, ext: { acme: { tier: "gold" } } };
  const env = EnvelopeCodec.make("urn:a:b", {}, { meta });
  meta.flag = false;
  meta.late = 1;
  assert.equal(env.meta.flag, true);
  assert.ok(!("late" in env.meta));
  // Documented in MakeOptions.meta: nested values are shared, like `data`.
  (meta.ext as { acme: { tier: string } }).acme.tier = "silver";
  assert.deepEqual(env.meta.ext, { acme: { tier: "silver" } });
});

test("CHANGELOG: documents the exact encode error text and does not outrun package.json", () => {
  const changelog = readFileSync(fileURLToPath(new URL("../CHANGELOG.md", import.meta.url)), "utf8");
  const pkg = JSON.parse(readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8")) as {
    version: string;
  };
  // The text a user greps for after the warn -> throw change must be the real one.
  const sample = new InvalidEnvelopeKeyError("<pointer>", "<reason>").message;
  assert.equal(sample, "BabelQueue: cannot encode envelope key <pointer>: <reason>.");
  assert.ok(changelog.includes("BabelQueue: cannot encode envelope key <pointer>: <reason>."));
  // The newest dated release heading is the version package.json will publish.
  const released = /^## \[(\d+\.\d+\.\d+)\]/m.exec(changelog)?.[1];
  assert.equal(released, pkg.version);
});
