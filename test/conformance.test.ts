import assert from "node:assert/strict";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";
import { after, test } from "node:test";

import {
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
  type Counter,
  type Histogram,
  type Meter,
  type MetricOptions,
  type Tracer,
} from "@opentelemetry/api";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import {
  BabelQueueError,
  EnvelopeCodec,
  FORBIDDEN_KEY_WARNING_CODE,
  NON_STRING_JOB_WARNING_CODE,
  InMemoryStore,
  InvalidEnvelopeKeyError,
  Wrap,
  annotate,
  buildPoisonRecord,
  classifyBody,
  retryBackoffDelay,
} from "../src/index.js";
import type { Envelope } from "../src/index.js";
import { createDeadLetterObserver, publish, remoteParentFromHeaders, traceIdOf, wrapHandler } from "../src/otel.js";
import type { PublishOptions } from "../src/otel.js";
import { MapProvider, validateSchema } from "../src/schema.js";
import type { SchemaNode } from "../src/schema.js";

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

// --- results.json collection (ADR-0032 section 2) ---------------------------------
// Every section runner reports through `runCase`, so the per-case outcome lands in
// `results` whether the case passes or fails. `BQ_CONFORMANCE_RESULTS` names the file the
// collected results are written to; with the variable unset nothing is written.

interface CaseResult {
  status: "pass" | "fail" | "skip";
  reason?: string;
}

const results: Record<string, Record<string, CaseResult>> = {};

function recordResult(section: string, id: string, result: CaseResult): void {
  (results[section] ??= {})[id] = result;
}

async function runCase(section: string, id: string, fn: () => void | Promise<void>): Promise<void> {
  try {
    await fn();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    recordResult(section, id, { status: "fail", reason: message.slice(0, 500) || "failed" });
    throw error;
  }
  recordResult(section, id, { status: "pass" });
}

const packageRoot = new URL("../", import.meta.url);
const packageJson = JSON.parse(readFileSync(fileURLToPath(new URL("package.json", packageRoot)), "utf8")) as {
  version: string;
  exports?: Record<string, unknown>;
};

/**
 * Transport / helper names that denote a conformance binding (`redis`, `rabbitmq` -> `amqp`, ...).
 * The one table is `scripts/conformance-bindings.json`, shared with the CI completeness gate.
 */
const BINDING_BY_EXPORT = JSON.parse(
  readFileSync(fileURLToPath(new URL("scripts/conformance-bindings.json", packageRoot)), "utf8"),
) as Record<string, string>;

/** Binding names found in `src/`: any file or directory (at any depth) named after a known transport. */
function sourceBindingNames(dir: string): string[] {
  const names: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const base = entry.name.replace(/\.[cm]?[jt]s$/, "").toLowerCase();
    if (Object.hasOwn(BINDING_BY_EXPORT, base)) {
      names.push(base);
    }
    if (entry.isDirectory()) {
      names.push(...sourceBindingNames(`${dir}/${entry.name}`));
    }
  }
  return names;
}

/**
 * Every binding this package carries, derived (never hard-coded) from two independent sources:
 * the public export map (any path segment of an export key, so `./otel` and `./transports/redis`
 * both count) and the `src/` tree (a transport shipped without an export is still carried).
 */
function deriveBindings(): string[] {
  const bindings = new Set<string>();
  const names = new Set<string>(sourceBindingNames(fileURLToPath(new URL("src", packageRoot))));
  for (const key of Object.keys(packageJson.exports ?? {})) {
    for (const segment of key.split("/")) {
      names.add(segment.toLowerCase());
    }
  }
  for (const name of names) {
    if (Object.hasOwn(BINDING_BY_EXPORT, name)) {
      bindings.add(BINDING_BY_EXPORT[name] as string);
    }
  }
  return [...bindings].sort();
}

const bindings = deriveBindings();

const fullManifest = JSON.parse(readJson("manifest.json")) as Record<string, unknown> & {
  sections: Record<string, { scope: string }>;
};

/** Case ids of one section, per the rules of `schema/results.schema.json`. */
function expectedCaseIds(name: string): string[] {
  const body = fullManifest[name] as Record<string, unknown> | unknown[];
  const caseId = (section: string, c: unknown, index: number): string => {
    const named = (c as { name?: unknown } | null)?.name;
    return typeof named === "string" ? named : `${section}#${index}`;
  };
  if (Array.isArray(body)) {
    return body.map((c, i) => caseId(name, c, i));
  }
  const ids: string[] = [];
  if (Array.isArray(body.cases)) {
    ids.push(...body.cases.map((c, i) => caseId(name, c, i)));
  }
  for (const [key, value] of Object.entries(body)) {
    if (key === "cases" || key === "description" || key === "schema") continue;
    if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
    const sub = value as { cases?: unknown };
    if (Array.isArray(sub.cases)) {
      ids.push(...sub.cases.map((c, i) => `${key}/${typeof (c as { name?: unknown }).name === "string" ? (c as { name: string }).name : i}`));
    } else {
      ids.push(key);
    }
  }
  return ids;
}

/** Sections in scope for this package: `core` plus every derived binding (from the manifest `sections` map). */
function inScopeSections(): string[] {
  return Object.entries(fullManifest.sections)
    .filter(([, def]) => def.scope === "core" || bindings.includes(def.scope))
    .map(([name]) => name);
}

after(() => {
  const target = process.env.BQ_CONFORMANCE_RESULTS;
  if (target === undefined || target === "") {
    return;
  }
  const sections: Record<string, Record<string, CaseResult>> = {};
  for (const name of Object.keys(fullManifest.sections)) {
    if (results[name] !== undefined) {
      sections[name] = results[name];
    }
  }
  const document = {
    sdk: "babelqueue-node/core",
    version: packageJson.version,
    conformance_version: readJson("CONFORMANCE_VERSION").trim(),
    generated_at: new Date().toISOString(),
    bindings,
    sections,
  };
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, `${JSON.stringify(document, null, 2)}\n`);
});

test("conformance manifest matches the core schema version", () => {
  assert.equal(manifest.schema_version, EnvelopeCodec.SCHEMA_VERSION);
  assert.ok(manifest.cases.length > 0, "manifest has no cases");
});

// The shared cross-SDK suite — the same fixtures every BabelQueue SDK must
// satisfy. Per-message fields (meta.id, trace_id, meta.created_at) are
// intrinsically unique and are checked for presence, not value.
for (const testCase of manifest.cases) {
  test(`conformance: ${testCase.name}`, () => runCase("cases", testCase.name, () => {
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
  }));
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
  test(`conformance roundtrip: ${c.name}`, () => runCase("roundtrip", c.name, () => {
    assert.deepEqual(runRoundtrip(c, readJson(c.file)), []);
  }));
}

for (const c of behaviour.data_shape?.cases ?? []) {
  test(`conformance data_shape: ${c.name}`, () => runCase("data_shape", c.name, () => {
    if (c.mode === "encode") {
      const env = EnvelopeCodec.make(c.urn, c.data, { queue: c.queue });
      const expected = JSON.stringify(JSON.parse(c.expect_encoded_data_json));
      assert.equal(rawDataJson(EnvelopeCodec.encode(env)), expected);
      return;
    }
    const verdict = EnvelopeCodec.accepts(EnvelopeCodec.decode(readJson(c.file)));
    assert.equal(verdict, c.valid, c.reason ?? "");
  }));
}

for (const c of behaviour.forbidden_keys?.cases ?? []) {
  test(`conformance forbidden_keys: ${c.name}`, () => runCase("forbidden_keys", c.name, () => {
    assert.equal(c.expect, "warn");
    assert.deepEqual(runForbiddenKeys(c, readJson(c.file)), []);
  }));
}

for (const c of behaviour.extension_write?.cases ?? []) {
  test(`conformance extension_write: ${c.name}`, () => runCase("extension_write", c.name, () => {
    assert.deepEqual(runExtensionWrite(c), []);
  }));
}

for (const c of behaviour.encode_reject?.cases ?? []) {
  // `meta_entries` is a repeated key, which one `MakeOptions.meta` map cannot express.
  if (c.meta_entries !== undefined) {
    recordResult("encode_reject", c.name, {
      status: "skip",
      reason:
        "ADR-0031 (b): a repeated extension key is not expressible through the single-map MakeOptions.meta write API; the manifest allows every SDK except Go to skip it",
    });
  }
  test(`conformance encode_reject: ${c.name}`, { skip: c.meta_entries !== undefined }, () => runCase("encode_reject", c.name, () => {
    assert.equal(c.expect, "reject");
    assert.deepEqual(runEncodeReject(c), []);
  }));
}

// --- trim, job_type, traceparent (conformance v1.5.0, ADR-0032 / ADR-0028) -----------

interface PoisonDlqCase {
  name: string;
  body?: string;
  body_base64?: string;
  expect: {
    shape: "splice" | "wrapper" | "splice_or_wrapper";
    record: Record<string, unknown>;
    splice_literals?: string[];
    splice_keys?: string[];
  };
}

const nodeSections = fullManifest as unknown as {
  trim: { cases: TrimCase[] };
  job_type: { cases: JobTypeCase[] };
  traceparent: { cases: TraceparentCase[] };
  retry_backoff: { cases: RetryBackoffCase[] };
  poison_dlq: { original_queue: string; cases: PoisonDlqCase[] };
  idempotency: {
    dedup_key: { envelope_file: string; expected_key: string };
    sequences: { cases: IdempotencyCase[] };
  };
  payload_schema: { schema: SchemaNode; cases: PayloadSchemaCase[] };
  payload_schema_unicode: { schema: SchemaNode; cases: PayloadSchemaCase[] };
  payload_schema_keywords: { cases: PayloadKeywordsCase[] };
};

interface PayloadKeywordsCase {
  name: string;
  schema: SchemaNode;
  data: unknown;
  valid: boolean;
  warnings?: Array<{ pointer: string; keyword: string }>;
}

interface RetryBackoffCase {
  name: string;
  retry_backoff: number;
  retry_backoff_multiplier: number;
  retry_backoff_max: number;
  retry_jitter: "none" | "full";
  n: number;
  expect_delay_seconds?: number;
  expect_delay_range_seconds?: { min: number; max: number };
}

interface TrimCase {
  name: string;
  mode: "make" | "decode" | "accepts" | "reemit";
  urn?: string;
  trace_id?: string;
  file?: string;
  valid?: boolean;
  expect?: "reject" | { job?: string; trace_id?: string; urn?: string };
  expect_warning?: boolean;
  expect_accepts?: boolean;
  expect_decode_ok?: boolean;
  expect_attempts?: number;
  expect_preserved?: Record<string, unknown>;
}

interface JobTypeCase {
  name: string;
  file: string;
  valid: boolean;
  expect?: { urn: string };
  expect_warning?: boolean;
}

interface TraceparentCase {
  name: string;
  mode: "extract";
  traceparent: string | null;
  trace_id: string;
  expect: { source: string; trace_id_hex: string; parent_span_id?: string; flags?: string };
}

interface IdempotencyDelivery {
  meta_id: string;
  expect_effect: "run" | "skip";
  outcome?: "ok" | "throw";
  forget_before?: boolean;
}

interface IdempotencyCase {
  name: string;
  deliveries: IdempotencyDelivery[];
  expected_effects: number;
}

interface PayloadSchemaCase {
  name: string;
  valid: boolean;
  data: Record<string, unknown>;
}

/**
 * Decode `raw`, collecting warnings. The Node decoder never throws: "decode raises" in the
 * manifest maps to a result without a usable identity (`urn()` is `""`, so `accepts` is false).
 */
function decodeCollectingWarnings(raw: string): { env: ReturnType<typeof EnvelopeCodec.decode>; warnings: string[] } {
  const warnings: string[] = [];
  const env = EnvelopeCodec.decode(raw, { onWarning: (m) => warnings.push(m) });
  return { env, warnings };
}

/** The decode / job_type verdict for one case: `[]` when the case holds, else the violations. */
function checkDecodeIdentity(
  raw: string,
  valid: boolean,
  expectUrn: string | undefined,
  expectWarning: boolean | undefined,
): string[] {
  const { env, warnings } = decodeCollectingWarnings(raw);
  const identity = EnvelopeCodec.urn(env);
  if (!valid) {
    return identity === "" ? [] : [`decode must yield no valid identity, got ${JSON.stringify(identity)}`];
  }
  const violations: string[] = [];
  if (identity !== expectUrn) {
    violations.push(`identity must be ${JSON.stringify(expectUrn)}, got ${JSON.stringify(identity)}`);
  }
  if (expectWarning !== undefined) {
    // A warning is expected specifically about the non-string `job` (ADR-0032 section 6), not any warning.
    const jobWarnings = warnings.filter((w) => w.includes("/job"));
    if (jobWarnings.length > 0 !== expectWarning || (!expectWarning && warnings.length > 0)) {
      violations.push(`warning about /job expected=${String(expectWarning)}, got ${JSON.stringify(warnings)}`);
    }
  }
  return violations;
}

/** The `trim` runner for one case. */
function runTrim(c: TrimCase): string[] {
  switch (c.mode) {
    case "make": {
      let env: Envelope | undefined;
      let failure: unknown;
      try {
        env = EnvelopeCodec.make(c.urn ?? "", {}, c.trace_id === undefined ? {} : { traceId: c.trace_id });
      } catch (error) {
        failure = error;
      }
      if (c.expect === "reject") {
        return failure instanceof BabelQueueError ? [] : ["make must reject a URN that is blank after trimming"];
      }
      if (env === undefined || c.expect === undefined) {
        return [`make must succeed, got ${String(failure)}`];
      }
      const violations: string[] = [];
      if (c.expect.job !== undefined && env.job !== c.expect.job) {
        violations.push(`job must be ${JSON.stringify(c.expect.job)}, got ${JSON.stringify(env.job)}`);
      }
      if (c.expect.trace_id !== undefined && env.trace_id !== c.expect.trace_id) {
        violations.push(`trace_id must be ${JSON.stringify(c.expect.trace_id)}, got ${JSON.stringify(env.trace_id)}`);
      }
      return violations;
    }
    case "decode": {
      const expectUrn = typeof c.expect === "object" ? c.expect.urn : undefined;
      return checkDecodeIdentity(readJson(c.file ?? ""), c.valid === true, expectUrn, c.expect_warning);
    }
    case "accepts": {
      const { env } = decodeCollectingWarnings(readJson(c.file ?? ""));
      const violations: string[] = [];
      if (EnvelopeCodec.accepts(env) !== c.expect_accepts) {
        violations.push(`accepts must be ${String(c.expect_accepts)}`);
      }
      if (c.expect_decode_ok === true && (Object.keys(env).length === 0 || typeof env.trace_id !== "string")) {
        violations.push("decode itself must succeed");
      }
      return violations;
    }
    case "reemit": {
      const { env } = decodeCollectingWarnings(readJson(c.file ?? ""));
      const hop = env as Envelope;
      hop.attempts += 1;
      const second = JSON.parse(EnvelopeCodec.encode(hop)) as unknown;
      const violations = checkPreserved(second, c.expect_preserved ?? {}).map((v) => `after re-emit: ${v}`);
      if (c.expect_attempts !== undefined && resolvePointer(second, "/attempts") !== c.expect_attempts) {
        violations.push(`after re-emit: /attempts must be ${String(c.expect_attempts)}`);
      }
      return violations;
    }
    default:
      return [`unknown trim mode ${String((c as { mode: unknown }).mode)}`];
  }
}

/** The consumer-side traceparent extraction (ADR-0028): header first, else the envelope `trace_id`. */
function extractTrace(c: TraceparentCase): Record<string, string> {
  const headers: Record<string, string> = {};
  if (typeof c.traceparent === "string") {
    headers.traceparent = c.traceparent;
  }
  const remote = remoteParentFromHeaders(headers);
  const spanContext = remote === undefined ? undefined : trace.getSpanContext(remote);
  if (spanContext !== undefined) {
    return {
      source: "traceparent",
      trace_id_hex: spanContext.traceId,
      parent_span_id: spanContext.spanId,
      flags: spanContext.traceFlags.toString(16).padStart(2, "0"),
    };
  }
  return { source: "trace_id", trace_id_hex: traceIdOf(c.trace_id) };
}

/** Replays one idempotency sequence through `Wrap` + `InMemoryStore`. */
async function runIdempotencySequence(c: IdempotencyCase): Promise<string[]> {
  const store = new InMemoryStore();
  const violations: string[] = [];
  let invoked = 0;
  let effects = 0;
  for (const [index, delivery] of c.deliveries.entries()) {
    if (delivery.forget_before === true) {
      store.forget(delivery.meta_id);
    }
    const before = invoked;
    const env = EnvelopeCodec.make("urn:acme:a", {});
    (env.meta as unknown as Record<string, unknown>).id = delivery.meta_id;
    const throwing = delivery.outcome === "throw";
    const guarded = Wrap(store, (): void => {
      invoked += 1;
      if (throwing) {
        throw new Error("handler failed");
      }
    });
    try {
      await guarded(env);
    } catch {
      // a failed run leaves the id unmarked (a later redelivery runs again)
    }
    if (invoked > before) {
      effects += 1; // every handler invocation counts, a throwing one included (expected_effects)
    }
    const effect = invoked > before ? "run" : "skip";
    if (effect !== delivery.expect_effect) {
      violations.push(`delivery ${String(index)} (${delivery.meta_id}): expected ${delivery.expect_effect}, got ${effect}`);
    }
  }
  if (effects !== c.expected_effects) {
    violations.push(`expected ${String(c.expected_effects)} effects, got ${String(effects)}`);
  }
  return violations;
}

for (const c of nodeSections.trim.cases) {
  test(`conformance trim: ${c.name}`, () => runCase("trim", c.name, () => {
    assert.deepEqual(runTrim(c), []);
  }));
}

for (const c of nodeSections.job_type.cases) {
  test(`conformance job_type: ${c.name}`, () => runCase("job_type", c.name, () => {
    assert.deepEqual(checkDecodeIdentity(readJson(c.file), c.valid, c.expect?.urn, c.expect_warning), []);
  }));
}

for (const c of nodeSections.traceparent.cases) {
  test(`conformance traceparent: ${c.name}`, () => runCase("traceparent", c.name, () => {
    assert.equal(c.mode, "extract");
    const expected: Record<string, string> = { ...c.expect };
    assert.deepEqual(extractTrace(c), expected);
  }));
}

// --- retry_backoff (conformance v1.6.0, ADR-0034 section 9) ---------------------------

const BACKOFF_JITTER_DRAWS = 50;

for (const c of nodeSections.retry_backoff.cases) {
  test(`conformance retry_backoff: ${c.name}`, () => runCase("retry_backoff", c.name, () => {
    const options = {
      retryBackoff: c.retry_backoff,
      retryBackoffMultiplier: c.retry_backoff_multiplier,
      retryBackoffMax: c.retry_backoff_max,
      retryJitter: c.retry_jitter,
    };
    if (c.retry_jitter === "none") {
      assert.equal(c.expect_delay_seconds !== undefined, true, `${c.name}: none needs expect_delay_seconds`);
      assert.equal(retryBackoffDelay(options, c.n), c.expect_delay_seconds);
      return;
    }
    const range = c.expect_delay_range_seconds;
    assert.ok(range !== undefined, `${c.name}: full jitter needs expect_delay_range_seconds`);
    for (let draw = 0; draw < BACKOFF_JITTER_DRAWS; draw += 1) {
      const delay = retryBackoffDelay(options, c.n);
      assert.ok(delay >= range.min && delay <= range.max, `${c.name}: ${delay} not in [${range.min}, ${range.max}]`);
    }
  }));
}

// --- poison_dlq: the DLQ record shape of a poison body (conformance v1.6.0, ADR-0035) -----

/** Distinct top-level member names of a JSON object text (a repeated name counts once). */
function topLevelNames(text: string): string[] {
  const names = new Set<string>();
  let depth = 0;
  let expectKey = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (c === "{" || c === "[") {
      depth++;
      expectKey = c === "{" && depth === 1;
    } else if (c === "}" || c === "]") {
      depth--;
    } else if (c === ",") {
      expectKey = depth === 1;
    } else if (c === '"') {
      let j = i + 1;
      while (text[j] !== '"') j += text[j] === "\\" ? 2 : 1;
      if (depth === 1 && expectKey) {
        names.add(JSON.parse(text.slice(i, j + 1)) as string);
        expectKey = false;
      }
      i = j;
    }
  }
  return [...names];
}

for (const c of nodeSections.poison_dlq.cases) {
  test(`conformance poison_dlq: ${c.name}`, () => runCase("poison_dlq", c.name, () => {
    const body = c.body_base64 !== undefined ? Buffer.from(c.body_base64, "base64") : Buffer.from(c.body ?? "", "utf8");
    const deadLetter = {
      reason: "poison",
      error: "e",
      exception: "BabelQueueError",
      failed_at: 1,
      original_queue: nodeSections.poison_dlq.original_queue,
      attempts: 0,
      lang: "node",
    };
    const record = buildPoisonRecord(body, JSON.stringify(deadLetter));

    const parsed = JSON.parse(record) as Record<string, unknown>;
    assert.deepEqual(parsed.dead_letter, deadLetter, c.name);
    const rest = { ...parsed };
    delete rest.dead_letter;
    const { shape, record: expected } = c.expect;
    if (shape === "wrapper" || (shape === "splice_or_wrapper" && "raw" in rest)) {
      assert.deepEqual(rest, expected, c.name);
      return;
    }
    // splice: the record is the original bytes plus the dead_letter member
    const text = new TextDecoder("utf-8", { fatal: true }).decode(body);
    assert.equal("raw" in parsed || "raw_encoding" in parsed, false, c.name);
    if (shape === "splice") assert.deepEqual(rest, expected, c.name);
    for (const literal of c.expect.splice_literals ?? []) {
      assert.ok(record.includes(literal), `${c.name}: missing literal ${literal}`);
    }
    if (c.expect.splice_keys !== undefined) {
      assert.deepEqual(
        topLevelNames(record).filter((k) => k !== "dead_letter").sort(),
        [...c.expect.splice_keys].sort(),
        c.name,
      );
    }
    assert.ok(record.startsWith(text.trim().slice(0, -1).trimEnd()), c.name);
  }));
}

test("conformance idempotency: dedup_key", () => runCase("idempotency", "dedup_key", () => {
  const key = nodeSections.idempotency.dedup_key;
  const env = EnvelopeCodec.decode(readJson(key.envelope_file));
  assert.equal((env as Envelope).meta.id, key.expected_key);
}));

for (const c of nodeSections.idempotency.sequences.cases) {
  test(`conformance idempotency: ${c.name}`, () => runCase("idempotency", `sequences/${c.name}`, async () => {
    assert.deepEqual(await runIdempotencySequence(c), []);
  }));
}

for (const section of ["payload_schema", "payload_schema_unicode"] as const) {
  for (const c of nodeSections[section].cases) {
    test(`conformance ${section}: ${c.name}`, () => runCase(section, c.name, () => {
      const verdict = validateSchema(nodeSections[section].schema, c.data) === null;
      assert.equal(verdict, c.valid, c.name);
    }));
  }
}

// payload_schema_keywords (ADR-0024, K-17): each case carries its own schema. A case passes when
// the verdict AND the registration warnings (a set of {pointer, keyword}, never the message text)
// match; the warnings come from registering the schema in a MapProvider, the SDK's registration point.
const pairKey = (w: { pointer: string; keyword: string }): string => JSON.stringify([w.pointer, w.keyword]);
for (const c of nodeSections.payload_schema_keywords.cases) {
  test(`conformance payload_schema_keywords: ${c.name}`, () => runCase("payload_schema_keywords", c.name, () => {
    const reported: string[] = [];
    const registered = new MapProvider({ "urn:babel:conformance:keywords": c.schema }, {
      onWarning: (warning) => reported.push(pairKey(warning)),
    });
    assert.equal(validateSchema(c.schema, c.data) === null, c.valid, `${c.name}: verdict`);
    const expected = (c.warnings ?? []).map(pairKey);
    assert.deepEqual([...reported].sort(), [...expected].sort(), `${c.name}: warnings`);
    assert.deepEqual(registered.warnings.map(pairKey).sort(), [...expected].sort(), `${c.name}: provider.warnings`);
  }));
}

// --- otel: span-shape, trace-id, metrics (conformance v1.9.0, ADR-0038 / ADR-0039) -------------

interface SpanShapeCase {
  name: string;
  mode: "span";
  operation: "send" | "process";
  system?: string;
  queue?: string;
  urn: string;
  consumer_group?: string;
  trace_id?: string;
  message_id?: string;
  attempts?: number;
  schema_version?: number;
  outcome: "ok" | "error";
  envelope_built?: boolean;
  expect: {
    span_name?: string;
    span_name_one_of?: string[];
    span_kind: "PRODUCER" | "CONSUMER";
    status: "unset" | "error";
    error_type?: "present";
    attributes: Record<string, string | number>;
    optional_attributes?: string[];
  };
}

interface TraceIdCase {
  name: string;
  mode: "map" | "decode" | "reemit" | "downstream" | "make";
  trace_id: string;
  urn?: string;
  expect: { trace_id_hex?: string; mapping?: "decode" | "hash"; trace_id?: string; decoded?: boolean };
}

interface MetricRecord {
  metric: string;
  kind: "add" | "record";
  value: number | "non_negative_seconds";
  attributes: Record<string, string>;
  error_type?: "present";
}

interface MetricsCase {
  name: string;
  mode: "instrument" | "consume" | "publish" | "dead_letter";
  system?: string;
  queue?: string;
  attempts?: number;
  outcome?: "ok" | "error";
  meter?: boolean;
  observer_called?: boolean;
  event?: { queue: string; dead_letter_queue: string; reason: string };
  expect: {
    records?: MetricRecord[];
    metric?: string;
    instrument?: "histogram" | "counter";
    unit?: string;
    operation_name?: string;
    attributes?: { always: string[]; when_destination_known: string[]; on_failure?: string[] };
    forbidden_attributes?: string[];
    advisory_bucket_boundaries?: number[];
  };
}

interface IdentityVerdictCase {
  name: string;
  body: string;
  expect: "ok" | "poison";
}

const otelSections = fullManifest as unknown as {
  "span-shape": { cases: SpanShapeCase[] };
  "trace-id": { cases: TraceIdCase[] };
  metrics: { cases: MetricsCase[] };
  identity_verdict: { cases: IdentityVerdictCase[] };
};

/** The ADR-0038 section 4 attribute keys: a key the case does not list must be absent from the span. */
const SPAN_TABLE_KEYS = [
  "messaging.system",
  "messaging.operation.type",
  "messaging.operation.name",
  "messaging.destination.name",
  "messaging.message.id",
  "messaging.message.conversation_id",
  "messaging.consumer.group.name",
  "babelqueue.urn",
  "babelqueue.schema_version",
  "babelqueue.attempts",
  "error.type",
  "messaging.operation",
  "messaging.babelqueue.attempts",
];

const isBlankAscii = (value: string): boolean => /^[ \t\n\v\f\r]*$/.test(value);

function spanRecorder(): { tracer: Tracer; exporter: InMemorySpanExporter } {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { tracer: provider.getTracer("conformance"), exporter };
}

/** Drives the producer or consumer wrapper for one span-shape case and compares the finished span. */
async function runSpanShape(c: SpanShapeCase): Promise<string[]> {
  const { tracer, exporter } = spanRecorder();
  let produced: Envelope | undefined;

  if (c.operation === "send") {
    const options: PublishOptions = {};
    if (c.system !== undefined) options.system = c.system;
    if (c.queue !== undefined) options.queue = c.queue;
    if (c.envelope_built === false) {
      // make() rejects a caller meta key that collides with a core one, so no envelope is ever built.
      options.meta = { id: "collides" };
    }
    try {
      await publish(
        tracer,
        c.urn,
        {},
        (envelope) => {
          produced = envelope;
          if (c.outcome === "error") {
            throw new Error("transport down");
          }
        },
        options,
      );
    } catch {
      // the failure is the case input; the span carries it
    }
  } else {
    const envelope = EnvelopeCodec.make(c.urn, {}, { queue: "placeholder" });
    envelope.trace_id = c.trace_id ?? envelope.trace_id;
    envelope.attempts = c.attempts ?? 0;
    envelope.meta.schema_version = c.schema_version ?? 1;
    const meta = envelope.meta as unknown as Record<string, unknown>;
    if (c.queue === undefined) delete meta.queue;
    else meta.queue = c.queue;
    if (c.message_id === undefined) delete meta.id;
    else meta.id = c.message_id;
    const options: { system?: string; consumerGroup?: string } = {};
    if (c.system !== undefined) options.system = c.system;
    if (c.consumer_group !== undefined) options.consumerGroup = c.consumer_group;
    const handle = wrapHandler(
      tracer,
      () => {
        if (c.outcome === "error") {
          throw new Error("handler failed");
        }
      },
      undefined,
      options,
    );
    try {
      await handle(envelope);
    } catch {
      // as above
    }
  }

  const spans = exporter.getFinishedSpans();
  if (spans.length !== 1) {
    return [`expected exactly one span, got ${String(spans.length)}`];
  }
  const span = spans[0]!;
  const violations: string[] = [];

  // Placeholders of a `send` case come from the envelope handed to the transport (ADR-0038 / v1.9.0 runner rule).
  const producedQueue = produced?.meta.queue ?? "";
  const queueKnown = !isBlankAscii(producedQueue);
  const substitute = (value: string | number): string | number | undefined => {
    if (typeof value !== "string") return value;
    switch (value) {
      case "$produced.meta.id":
        return produced?.meta.id;
      case "$produced.trace_id":
        return produced?.trace_id;
      case "$produced.meta.queue":
        return queueKnown ? producedQueue : undefined;
      default:
        return value;
    }
  };

  const expectedAttributes: Record<string, string | number> = {};
  for (const [key, value] of Object.entries(c.expect.attributes)) {
    const resolved = substitute(value);
    if (resolved !== undefined) expectedAttributes[key] = resolved;
    else if (typeof value === "string" && value.startsWith("$produced.meta.queue") === false) {
      violations.push(`placeholder ${value} of ${key} could not be resolved (no envelope reached the transport)`);
    }
  }
  const optional = new Set(c.expect.optional_attributes ?? []);

  if (c.expect.span_name !== undefined) {
    const expectedName = c.expect.span_name.replace(" $produced.meta.queue", queueKnown ? ` ${producedQueue}` : "");
    if (span.name !== expectedName) violations.push(`span name ${JSON.stringify(span.name)}, expected ${JSON.stringify(expectedName)}`);
  } else if (!(c.expect.span_name_one_of ?? []).includes(span.name)) {
    violations.push(`span name ${JSON.stringify(span.name)} is not one of ${JSON.stringify(c.expect.span_name_one_of)}`);
  }
  if (span.kind !== SpanKind[c.expect.span_kind]) violations.push(`span kind ${String(span.kind)}`);
  const expectedStatus = c.expect.status === "error" ? SpanStatusCode.ERROR : SpanStatusCode.UNSET;
  if (span.status.code !== expectedStatus) violations.push(`status ${String(span.status.code)}`);

  for (const [key, value] of Object.entries(expectedAttributes)) {
    if (span.attributes[key] !== value) {
      violations.push(`${key} is ${JSON.stringify(span.attributes[key])}, expected ${JSON.stringify(value)}`);
    }
  }
  for (const key of SPAN_TABLE_KEYS) {
    if (key in expectedAttributes || optional.has(key)) continue;
    if (key === "error.type") continue; // handled below
    if (key in span.attributes) violations.push(`${key} must not be on the span`);
  }
  for (const key of Object.keys(span.attributes)) {
    if (key.startsWith("babelqueue.") && !SPAN_TABLE_KEYS.includes(key)) violations.push(`${key} is not a table key`);
  }
  const errorType = span.attributes["error.type"];
  if (c.expect.error_type === "present") {
    if (typeof errorType !== "string" || errorType === "") violations.push("error.type must be a non-empty string");
  } else if (errorType !== undefined) {
    violations.push("error.type must not be on a successful span");
  }
  for (const key of optional) {
    if (key in span.attributes) {
      const given = c.queue ?? "";
      if (span.attributes[key] !== given) violations.push(`optional ${key} must equal the queue given (${JSON.stringify(given)})`);
      if (span.name !== `${c.operation} ${given}`) violations.push("span name must agree with the optional destination");
    } else if (span.name !== c.operation) {
      violations.push("span name must be the bare operation name when the destination is absent");
    }
  }
  if (c.expect.error_type === "present" && !span.events.some((e) => e.name === "exception")) {
    violations.push("the error must be recorded as an exception event");
  }
  return violations;
}

for (const c of otelSections["span-shape"].cases) {
  test(`conformance span-shape: ${c.name}`, () => runCase("span-shape", c.name, async () => {
    assert.deepEqual(await runSpanShape(c), []);
  }));
}

/** The lower-case hex bytes of the hash mapping: SHA-256 of the UTF-8 bytes, first 16 bytes. */
const hashHex = (value: string): string => createHash("sha256").update(value, "utf8").digest("hex").slice(0, 32);

function runTraceId(c: TraceIdCase): string[] {
  const violations: string[] = [];
  const expectTraceId = (actual: unknown): void => {
    if (actual !== c.expect.trace_id) {
      violations.push(`trace_id is ${JSON.stringify(actual)}, expected ${JSON.stringify(c.expect.trace_id)}`);
    }
  };
  switch (c.mode) {
    case "map": {
      const hex = traceIdOf(c.trace_id);
      if (hex !== c.expect.trace_id_hex) violations.push(`mapping gave ${hex}, expected ${String(c.expect.trace_id_hex)}`);
      const viaDecode = c.trace_id.replace(/-/g, "").toLowerCase();
      const reference = c.expect.mapping === "decode" ? viaDecode : hashHex(c.trace_id);
      if (hex !== reference) violations.push(`mapping is not the ${String(c.expect.mapping)} of the value`);
      break;
    }
    case "decode": {
      const body = JSON.stringify({ ...EnvelopeCodec.make("urn:acme:a", {}), trace_id: c.trace_id });
      const { env } = decodeCollectingWarnings(body);
      if (c.expect.decoded !== true || Object.keys(env).length === 0) violations.push("decode must succeed");
      expectTraceId(env.trace_id);
      break;
    }
    case "reemit": {
      const body = JSON.stringify({ ...EnvelopeCodec.make("urn:acme:a", {}), trace_id: c.trace_id });
      const { env } = decodeCollectingWarnings(body);
      const hop = env as Envelope;
      hop.attempts += 1;
      expectTraceId((JSON.parse(EnvelopeCodec.encode(hop)) as { trace_id: unknown }).trace_id);
      break;
    }
    case "downstream": {
      expectTraceId(EnvelopeCodec.make("urn:acme:next", {}, { traceId: c.trace_id }).trace_id);
      break;
    }
    case "make": {
      const made = EnvelopeCodec.make(c.urn ?? "urn:acme:a", {}, { traceId: c.trace_id });
      expectTraceId(made.trace_id);
      if (traceIdOf(made.trace_id) !== c.expect.trace_id_hex) violations.push("OTel mapping of the stored value differs");
      break;
    }
  }
  return violations;
}

for (const c of otelSections["trace-id"].cases) {
  test(`conformance trace-id: ${c.name}`, () => runCase("trace-id", c.name, () => {
    assert.deepEqual(runTraceId(c), []);
  }));
}

interface RecordedPoint {
  metric: string;
  kind: "add" | "record";
  value: number;
  attributes: Attributes;
}

interface CreatedInstrument {
  name: string;
  kind: "histogram" | "counter";
  options?: MetricOptions;
}

/** An in-memory metric reader: every instrument call lands in `points`, every creation in `created`. */
function inMemoryMeter(): { meter: Meter; points: RecordedPoint[]; created: CreatedInstrument[] } {
  const points: RecordedPoint[] = [];
  const created: CreatedInstrument[] = [];
  const meter = {
    createCounter(name: string, options?: MetricOptions): Counter {
      created.push({ name, kind: "counter", options });
      return { add: (value: number, attributes: Attributes = {}) => void points.push({ metric: name, kind: "add", value, attributes }) };
    },
    createHistogram(name: string, options?: MetricOptions): Histogram {
      created.push({ name, kind: "histogram", options });
      return { record: (value: number, attributes: Attributes = {}) => void points.push({ metric: name, kind: "record", value, attributes }) };
    },
  } as unknown as Meter;
  return { meter, points, created };
}

/** A recorded point or an expected record in one comparable form (ADR-0039: multiset of data points). */
function canonicalRecord(
  metric: string,
  kind: string,
  value: number | "non_negative_seconds",
  attributes: Record<string, unknown>,
  expectErrorType: boolean,
  actual: boolean,
): string {
  const attrs: Record<string, unknown> = { ...attributes };
  if (actual) {
    const errorType = attrs["error.type"];
    if (errorType !== undefined) {
      attrs["error.type"] = typeof errorType === "string" && errorType !== "" ? "<present>" : "<invalid>";
    }
  } else if (expectErrorType) {
    attrs["error.type"] = "<present>";
  }
  const shownValue =
    kind === "record"
      ? value === "non_negative_seconds" || (typeof value === "number" && Number.isFinite(value) && value >= 0)
        ? "<seconds>"
        : "<invalid>"
      : value === "non_negative_seconds"
        ? "<invalid>"
        : value;
  const sorted = Object.fromEntries(Object.entries(attrs).sort(([a], [b]) => (a < b ? -1 : 1)));
  return JSON.stringify({ metric, kind, value: shownValue, attributes: sorted });
}

function compareRecords(points: RecordedPoint[], expected: MetricRecord[], producedQueue?: string): string[] {
  const queueKnown = producedQueue !== undefined && !isBlankAscii(producedQueue);
  const wanted = expected.map((r) => {
    const attributes: Record<string, string> = {};
    for (const [key, value] of Object.entries(r.attributes)) {
      if (value === "$produced.meta.queue") {
        if (queueKnown) attributes[key] = producedQueue;
      } else {
        attributes[key] = value;
      }
    }
    return canonicalRecord(r.metric, r.kind, r.value, attributes, r.error_type === "present", false);
  });
  const got = points.map((p) => canonicalRecord(p.metric, p.kind, p.value, p.attributes, false, true));
  const missing = wanted.filter((w) => !got.includes(w));
  const extra = got.filter((g) => !wanted.includes(g));
  const violations = [...missing.map((m) => `missing ${m}`), ...extra.map((g) => `unexpected ${g}`)];
  if (violations.length === 0 && got.length !== wanted.length) violations.push(`expected ${String(wanted.length)} points, got ${String(got.length)}`);
  return violations;
}

async function runMetricsScenario(c: MetricsCase): Promise<string[]> {
  const { tracer } = spanRecorder();
  const { meter, points } = inMemoryMeter();
  const useMeter = c.meter !== false;
  const expected = c.expect.records ?? [];

  switch (c.mode) {
    case "consume": {
      const envelope = EnvelopeCodec.make("urn:acme:a", {}, { queue: "placeholder" });
      envelope.attempts = c.attempts ?? 0;
      const meta = envelope.meta as unknown as Record<string, unknown>;
      if (c.queue === undefined) delete meta.queue;
      else meta.queue = c.queue;
      const handle = wrapHandler(
        tracer,
        () => {
          if (c.outcome === "error") throw new Error("handler failed");
        },
        undefined,
        { system: c.system, meter: useMeter ? meter : undefined },
      );
      try {
        await handle(envelope);
      } catch {
        // the failure is the case input
      }
      return compareRecords(points, expected);
    }
    case "publish": {
      let producedQueue = "";
      const options: PublishOptions = { system: c.system, meter: useMeter ? meter : undefined };
      if (c.queue !== undefined) options.queue = c.queue;
      try {
        await publish(
          tracer,
          "urn:acme:a",
          {},
          (envelope) => {
            producedQueue = envelope.meta.queue;
            if (c.outcome === "error") throw new Error("transport down");
          },
          options,
        );
      } catch {
        // the failure is the case input
      }
      return compareRecords(points, expected, producedQueue);
    }
    case "dead_letter": {
      if (c.observer_called === true && c.event !== undefined) {
        const observe = createDeadLetterObserver({ system: c.system, meter: useMeter ? meter : undefined });
        await observe({ queue: c.event.queue, deadLetterQueue: c.event.dead_letter_queue, reason: c.event.reason });
      }
      // observer_called:false cases (sdk_test_required) are enforced by the transports' own tests; here nothing is delivered.
      return compareRecords(points, expected);
    }
    default:
      return [`unknown metrics mode ${String(c.mode)}`];
  }
}

/** Runs every scenario the `instrument` cases need and checks name, kind, unit, attribute key set and forbidden keys. */
async function runMetricsInstrument(c: MetricsCase): Promise<string[]> {
  const violations: string[] = [];
  const { tracer } = spanRecorder();
  const { meter, points, created } = inMemoryMeter();
  const queued = EnvelopeCodec.make("urn:acme:a", {}, { queue: "orders" });
  queued.attempts = 1;
  await wrapHandler(tracer, () => undefined, undefined, { system: "kafka", meter })(queued);
  await wrapHandler(tracer, () => { throw new Error("handler failed"); }, undefined, { system: "kafka", meter })(queued).catch(() => undefined);
  await publish(tracer, "urn:acme:a", {}, () => undefined, { queue: "orders", system: "kafka", meter });
  await publish(tracer, "urn:acme:a", {}, () => { throw new Error("transport down"); }, { queue: "orders", system: "kafka", meter }).catch(() => undefined);
  createDeadLetterObserver({ system: "kafka", meter })({ queue: "orders", deadLetterQueue: "orders.dlq", reason: "failed" });

  const expect = c.expect;
  const instrument = created.find((i) => i.name === expect.metric);
  if (instrument === undefined) return [`${String(expect.metric)} was never created`];
  if (instrument.kind !== expect.instrument) violations.push(`instrument is a ${instrument.kind}`);
  if (instrument.options?.unit !== expect.unit) violations.push(`unit is ${String(instrument.options?.unit)}`);
  if (expect.advisory_bucket_boundaries !== undefined) {
    const advice = instrument.options?.advice?.explicitBucketBoundaries;
    if (!isDeepStrictEqual(advice, expect.advisory_bucket_boundaries)) violations.push("advisory bucket boundaries differ");
  }
  const own = points.filter((p) => p.metric === expect.metric);
  if (own.length === 0) violations.push("no data point was recorded");
  const spec = expect.attributes ?? { always: [], when_destination_known: [] };
  const allowed = new Set([...spec.always, ...spec.when_destination_known, ...(spec.on_failure ?? [])]);
  const seen = new Set<string>();
  for (const point of own) {
    const keys = Object.keys(point.attributes);
    for (const key of spec.always) if (!keys.includes(key)) violations.push(`${key} is missing from a point`);
    for (const key of keys) {
      seen.add(key);
      if (!allowed.has(key)) violations.push(`${key} is not an attribute of ${String(expect.metric)}`);
    }
    if (point.attributes["messaging.operation.name"] !== expect.operation_name) violations.push("messaging.operation.name differs");
  }
  for (const key of allowed) if (!seen.has(key)) violations.push(`${key} was never written`);
  for (const point of points) {
    for (const forbidden of expect.forbidden_attributes ?? []) {
      if (forbidden in point.attributes) violations.push(`forbidden attribute ${forbidden} on ${point.metric}`);
    }
  }
  return violations;
}

for (const c of otelSections.metrics.cases) {
  test(`conformance metrics: ${c.name}`, () => runCase("metrics", c.name, async () => {
    assert.deepEqual(c.mode === "instrument" ? await runMetricsInstrument(c) : await runMetricsScenario(c), []);
  }));
}

// --- Sections for bindings this package does not carry (ADR-0032 section 2) -----------
// `delay` (sqs), `backoff` and `kafka_poison` (kafka), `redis_layout`, `redis_delay` and `redis` (redis) are absent from results.json:
// the schema forbids a section of a binding missing from `bindings`. The transports live in
// the node-adapters repository, which publishes its own results. Unknown sections are never run.

// --- identity_verdict (core scope, conformance v1.7.0; ADR-0036 decision 4) ---------------
// Each case's raw `body` goes to the strict body classifier `classifyBody`, which must give the verdict `expect`.
// `EnvelopeCodec.decode` is not the oracle: it is lenient about the type of a known member (it keeps
// `attempts: "3"` and `data: []` as parsed), so it would say `ok` where the suite expects `poison`.

for (const c of otelSections.identity_verdict.cases) {
  test(`conformance identity_verdict: ${c.name}`, () => runCase("identity_verdict", c.name, () => {
    const verdict = classifyBody(c.body);
    assert.equal(verdict.verdict, c.expect, `body ${JSON.stringify(c.body)}: ${verdict.verdict === "poison" ? verdict.cause : "ok"}`);
  }));
}

test("identity_verdict: the section holds the 97 cases of conformance v1.7.0 or later", () => {
  assert.equal(otelSections.identity_verdict.cases.length, 97);
});

test("classifyBody: a poison verdict carries a cause, an ok verdict carries none", () => {
  assert.deepEqual(classifyBody('{"job":"urn:x"}'), { verdict: "ok" });
  for (const body of ["", "not-json", "[]", "null", "{}", '{"job":5}', '{"job":"urn:x","attempts":"3"}', '{"job":"urn:x","data":[]}']) {
    const verdict = classifyBody(body);
    assert.equal(verdict.verdict, "poison", body);
    assert.ok(verdict.verdict === "poison" && verdict.cause.length > 0, body);
  }
});

test("classifyBody leaves EnvelopeCodec.decode lenient: the wrongly typed body still decodes, the verdict is poison", () => {
  for (const body of ['{"job":"urn:x","attempts":"3"}', '{"job":"urn:x","data":[]}']) {
    const envelope = EnvelopeCodec.decode(body, { onWarning: () => undefined });
    assert.equal(EnvelopeCodec.urn(envelope), "urn:x");
    assert.equal(classifyBody(body).verdict, "poison");
  }
  assert.equal(EnvelopeCodec.decode('{"job":"urn:x","attempts":"3"}').attempts, "3");
});

// The conformance cases of the typed members are identity-less, so they are poison for that reason alone. These bodies
// carry a valid identity, so only the member-type rule can make them poison.
test("classifyBody: a wrong typed known member of an identified body is poison and names the member path", () => {
  const cases: Array<[string, string]> = [
    ['{"job":"urn:x","trace_id":7}', "trace_id"],
    ['{"job":"urn:x","attempts":1.5}', "attempts"],
    ['{"job":"urn:x","attempts":9223372036854775808}', "attempts"],
    ['{"job":"urn:x","meta":"x"}', "meta"],
    ['{"job":"urn:x","meta":{"id":5}}', "meta.id"],
    ['{"job":"urn:x","meta":{"queue":5}}', "meta.queue"],
    ['{"job":"urn:x","meta":{"lang":true}}', "meta.lang"],
    ['{"job":"urn:x","meta":{"schema_version":"1"}}', "meta.schema_version"],
    ['{"job":"urn:x","meta":{"created_at":1.5}}', "meta.created_at"],
    ['{"job":"urn:x","meta":{"created_at":9223372036854775808}}', "meta.created_at"],
    ['{"job":"urn:x","dead_letter":[]}', "dead_letter"],
    ['{"job":"urn:x","dead_letter":{"reason":5}}', "dead_letter.reason"],
    ['{"job":"urn:x","dead_letter":{"lang":{}}}', "dead_letter.lang"],
    ['{"job":"urn:x","dead_letter":{"failed_at":1.5}}', "dead_letter.failed_at"],
    ['{"job":"urn:x","dead_letter":{"attempts":"3"}}', "dead_letter.attempts"],
    ['{"job":"urn:x","data":{"n":1e400}}', "data"],
  ];
  for (const [body, member] of cases) {
    const verdict = classifyBody(body);
    assert.equal(verdict.verdict, "poison", body);
    assert.ok(verdict.verdict === "poison" && verdict.cause.includes(`'${member}'`), `${body}: ${JSON.stringify(verdict)}`);
  }
});

test("classifyBody: well typed members of an identified body are ok (schema_version is not checked)", () => {
  for (const body of [
    '{"job":"urn:x","meta":{"schema_version":2}}',
    '{"job":"urn:x","meta":{"created_at":9223372036854775807,"id":null}}',
    '{"job":"urn:x","dead_letter":{"failed_at":1,"error":null}}',
    '{"job":"urn:x","data":{"n":1e300}}',
  ]) {
    assert.deepEqual(classifyBody(body), { verdict: "ok" }, body);
  }
});

test("classifyBody: a duplicated identity key keeps its last value", () => {
  assert.equal(classifyBody('{"job":5,"job":"urn:x"}').verdict, "ok");
  assert.equal(classifyBody('{"job":"urn:x","job":5}').verdict, "poison");
});

test("classifyBody never throws, including on a deeply nested or lone-surrogate body", () => {
  const nested = `{"job":"urn:x","x":${"[".repeat(5000)}${"]".repeat(5000)}}`;
  assert.equal(classifyBody(nested).verdict, "ok");
  assert.equal(classifyBody(`{"job":"urn:x","x":${"[".repeat(20000)}${"]".repeat(20000)}}`).verdict, "poison");
  assert.equal(classifyBody('{"job":"urn:x","\\ud800":1}').verdict, "ok");
  assert.equal(classifyBody(undefined as unknown as string).verdict, "poison");
});

test("conformance results: every in-scope section is run, every case reports once", () => {
  assert.deepEqual(bindings, ["otel"], "this package carries the otel binding only");
  for (const name of inScopeSections()) {
    const expected = expectedCaseIds(name).sort();
    const recorded = Object.keys(results[name] ?? {}).sort();
    assert.deepEqual(recorded, expected, `section ${name}`);
  }
  for (const name of ["delay", "backoff", "kafka_poison", "redis_layout", "redis_delay", "redis", "rabbitmq"]) {
    assert.equal(results[name], undefined, `section ${name} belongs to a binding this package does not carry`);
  }
});

test("trim: ASCII whitespace is cut, U+00A0 / U+2003 / U+3000 are kept (ADR-0032 section 5)", () => {
  for (const space of [" ", " ", "　"]) {
    const env = EnvelopeCodec.make(`${space}urn:acme:a${space}`, {}, { traceId: `${space}trace-1${space}` });
    assert.equal(env.job, `${space}urn:acme:a${space}`);
    assert.equal(env.trace_id, `${space}trace-1${space}`);
    assert.equal(EnvelopeCodec.urn({ job: `${space}urn:acme:a${space}` }), `${space}urn:acme:a${space}`);
    assert.equal(EnvelopeCodec.accepts({ ...env, trace_id: space }), true, "a Unicode space is not a blank trace_id");
  }
  const env = EnvelopeCodec.make(" \t urn:acme:a \n ", {});
  assert.equal(env.job, " urn:acme:a ");
  assert.equal(EnvelopeCodec.urn({ job: "  x  " }), " x ");
});

test("trim: a very long whitespace run is trimmed in linear time (no regex backtracking)", () => {
  // `"a" + n spaces + "a"` made the former edge-anchored regex quadratic (160k spaces took ~16 s).
  const run = " ".repeat(200_000);
  const started = process.hrtime.bigint();
  const env = EnvelopeCodec.make(`urn:acme:a${run}b`, {}, { traceId: `a${run}a` });
  assert.equal(env.trace_id, `a${run}a`);
  assert.equal(EnvelopeCodec.urn({ job: `${run}x${run}` }), "x");
  assert.equal(EnvelopeCodec.accepts({ ...env, trace_id: `a${run}a` }), true);
  assert.equal(EnvelopeCodec.accepts({ ...env, trace_id: run }), false);
  assert.equal(EnvelopeCodec.make(`${run}urn:acme:a${run}`, {}).job, "urn:acme:a");
  const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
  assert.ok(elapsedMs < 1000, `whitespace handling took ${elapsedMs.toFixed(0)} ms for a 200k run`);
});

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

test("decode: the default warning for a non-string job carries its own code", async () => {
  const base = JSON.parse(canonicalBody) as Record<string, unknown>;
  const seen = await captureProcessWarnings(() => {
    EnvelopeCodec.decode(JSON.stringify({ ...base, job: 5, urn: "urn:acme:b" }));
  });
  const hits = seen.filter((w) => w.message.includes("not a string"));
  assert.equal(hits.length, 1, "a process warning for the non-string job must be emitted once");
  assert.equal((hits[0] as Error & { code?: string }).code, NON_STRING_JOB_WARNING_CODE);
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

test("decode: a non-string job is ignored with a warning and urn takes over (ADR-0032 section 6)", () => {
  const base = JSON.parse(canonicalBody) as Record<string, unknown>;
  for (const job of [5, null, true, {}, []]) {
    const warnings: string[] = [];
    const env = EnvelopeCodec.decode(JSON.stringify({ ...base, job, urn: "urn:acme:b" }), {
      onWarning: (m) => warnings.push(m),
    });
    assert.equal(env.job, "urn:acme:b", JSON.stringify(job));
    assert.ok(!("urn" in env));
    assert.ok(warnings.some((w) => w.startsWith("BabelQueue:") && w.includes("not a string")), JSON.stringify(job));
    assert.ok(EnvelopeCodec.accepts(env));
    assert.doesNotThrow(() => EnvelopeCodec.encode(env as Envelope));
  }
  // Neither a valid job nor a valid urn: no identity, so the message is not accepted.
  const none = EnvelopeCodec.decode(JSON.stringify({ ...base, job: 5, urn: 7 }), { onWarning: () => undefined });
  assert.equal(EnvelopeCodec.urn(none), "");
  assert.ok(!EnvelopeCodec.accepts(none));
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
