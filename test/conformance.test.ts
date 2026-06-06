import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { EnvelopeCodec } from "../src/index.js";

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
