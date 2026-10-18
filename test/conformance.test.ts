import assert from "node:assert/strict";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";
import { after, test } from "node:test";

import { trace } from "@opentelemetry/api";

import {
  BabelQueueError,
  EnvelopeCodec,
  FORBIDDEN_KEY_WARNING_CODE,
  NON_STRING_JOB_WARNING_CODE,
  InMemoryStore,
  InvalidEnvelopeKeyError,
  Wrap,
  annotate,
} from "../src/index.js";
import type { Envelope } from "../src/index.js";
import { remoteParentFromHeaders, traceIdOf } from "../src/otel.js";
import { validateSchema } from "../src/schema.js";
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

const nodeSections = fullManifest as unknown as {
  trim: { cases: TrimCase[] };
  job_type: { cases: JobTypeCase[] };
  traceparent: { cases: TraceparentCase[] };
  idempotency: {
    dedup_key: { envelope_file: string; expected_key: string };
    sequences: { cases: IdempotencyCase[] };
  };
  payload_schema: { schema: SchemaNode; cases: PayloadSchemaCase[] };
  payload_schema_unicode: { schema: SchemaNode; cases: PayloadSchemaCase[] };
};

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

// --- Sections for bindings this package does not carry (ADR-0032 section 2) -----------
// `delay` (sqs), `backoff` (kafka) and `redis_layout` (redis) are absent from results.json:
// the schema forbids a section of a binding missing from `bindings`. The transports live in
// the node-adapters repository, which publishes its own results. Unknown sections are never run.

test("conformance results: every in-scope section is run, every case reports once", () => {
  assert.deepEqual(bindings, ["otel"], "this package carries the otel binding only");
  for (const name of inScopeSections()) {
    const expected = expectedCaseIds(name).sort();
    const recorded = Object.keys(results[name] ?? {}).sort();
    assert.deepEqual(recorded, expected, `section ${name}`);
  }
  for (const name of ["delay", "backoff", "redis_layout", "redis", "rabbitmq"]) {
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
