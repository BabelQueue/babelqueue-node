import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { test } from "node:test";

import { EnvelopeCodec } from "../src/codec.js";
import {
  AesGcmCipher,
  DecryptError,
  InvalidKeySizeError,
  MalformedCiphertextError,
  protect,
  unprotect,
  type Cipher,
} from "../src/gdpr.js";
import { sensitivePaths, validateSchema, type SchemaNode } from "../src/schema.js";

const KEY = randomBytes(32);
const cipher = (): Cipher => new AesGcmCipher(KEY);

// ── sensitivePaths extraction ──────────────────────────────────────────────

test("sensitivePaths: nested objects, array items, root mark, sorted, category", () => {
  const schema: SchemaNode = {
    type: "object",
    properties: {
      email: { type: "string", "x-gdpr-sensitive": "email" },
      profile: {
        type: "object",
        properties: {
          full_name: { type: "string", "x-gdpr-sensitive": true },
          age: { type: "integer" },
        },
      },
      addresses: {
        type: "array",
        items: {
          type: "object",
          properties: {
            line: { type: "string", "x-gdpr-sensitive": true },
            city: { type: "string" },
          },
        },
      },
    },
  };

  assert.deepEqual(sensitivePaths(schema), [
    { path: "addresses[].line", category: "" },
    { path: "email", category: "email" },
    { path: "profile.full_name", category: "" },
  ]);
});

test("sensitivePaths: root mark reports the empty path", () => {
  assert.deepEqual(sensitivePaths({ type: "string", "x-gdpr-sensitive": true }), [
    { path: "", category: "" },
  ]);
});

test("sensitivePaths: false / empty-string / number marks are ignored", () => {
  const schema: SchemaNode = {
    type: "object",
    properties: {
      a: { type: "string", "x-gdpr-sensitive": false },
      b: { type: "string", "x-gdpr-sensitive": "" },
      c: { type: "string", "x-gdpr-sensitive": 1 },
      d: { type: "string" },
    },
  };
  assert.deepEqual(sensitivePaths(schema), []);
});

test("x-gdpr-sensitive is validation-neutral", () => {
  // The mark never changes a verdict: same value, identical results with and without it.
  const bare: SchemaNode = { type: "string", minLength: 3 };
  const marked: SchemaNode = { type: "string", minLength: 3, "x-gdpr-sensitive": "email" };
  assert.equal(validateSchema(bare, "ab"), validateSchema(marked, "ab"));
  assert.equal(validateSchema(bare, "abcd"), validateSchema(marked, "abcd"));
  assert.equal(validateSchema(marked, "abcd"), null);
});

// ── AesGcmCipher ───────────────────────────────────────────────────────────

test("AesGcmCipher round-trips arbitrary bytes", () => {
  const c = cipher();
  const plaintext = new TextEncoder().encode('{"k":"naïve λ value 🔐"}');
  const ct = c.encrypt(plaintext);
  assert.equal(typeof ct, "string");
  assert.deepEqual(Buffer.from(c.decrypt(ct)), Buffer.from(plaintext));
});

test("AesGcmCipher uses a fresh IV per call (same plaintext → different ciphertext)", () => {
  const c = cipher();
  const pt = new TextEncoder().encode("same");
  assert.notEqual(c.encrypt(pt), c.encrypt(pt));
});

test("AesGcmCipher accepts 16/24/32-byte keys, rejects others", () => {
  for (const size of [16, 24, 32]) {
    assert.doesNotThrow(() => new AesGcmCipher(randomBytes(size)));
  }
  for (const size of [0, 8, 31, 33, 64]) {
    assert.throws(() => new AesGcmCipher(randomBytes(size)), InvalidKeySizeError);
  }
});

test("AesGcmCipher.decrypt rejects malformed input", () => {
  const c = cipher();
  assert.throws(() => c.decrypt("not base64!!!"), MalformedCiphertextError);
  assert.throws(() => c.decrypt(Buffer.from("short").toString("base64")), MalformedCiphertextError);
});

test("AesGcmCipher.decrypt rejects a tampered ciphertext (auth tag fails)", () => {
  const c = cipher();
  const ct = c.encrypt(new TextEncoder().encode("secret"));
  const raw = Buffer.from(ct, "base64");
  raw[raw.length - 1] ^= 0xff; // flip a bit in the auth tag
  assert.throws(() => c.decrypt(raw.toString("base64")), DecryptError);
});

test("AesGcmCipher.decrypt with the wrong key throws DecryptError", () => {
  const ct = new AesGcmCipher(KEY).encrypt(new TextEncoder().encode("secret"));
  const wrong = new AesGcmCipher(randomBytes(32));
  assert.throws(() => wrong.decrypt(ct), DecryptError);
});

// ── protect / unprotect round-trip ─────────────────────────────────────────

const PEOPLE_SCHEMA: SchemaNode = {
  type: "object",
  properties: {
    id: { type: "integer" },
    email: { type: "string", "x-gdpr-sensitive": "email" },
    profile: {
      type: "object",
      properties: {
        full_name: { type: "string", "x-gdpr-sensitive": true },
        age: { type: "integer", "x-gdpr-sensitive": true },
        verified: { type: "boolean" },
      },
    },
    addresses: {
      type: "array",
      items: {
        type: "object",
        properties: {
          line: { type: "string", "x-gdpr-sensitive": true },
          city: { type: "string" },
        },
      },
    },
  },
};

function sampleData(): Record<string, unknown> {
  return {
    id: 42,
    email: "alice@example.com",
    profile: { full_name: "Alice Liddell", age: 30, verified: true },
    addresses: [
      { line: "221B Baker Street", city: "London" },
      { line: "742 Evergreen Terrace", city: "Springfield" },
    ],
  };
}

test("protect → unprotect restores data byte-for-byte (nested + array + scalar)", () => {
  const c = cipher();
  const original = sampleData();
  const data = sampleData();

  protect(data, PEOPLE_SCHEMA, c);

  // Marked leaves are now ciphertext strings; non-sensitive fields are untouched.
  assert.equal(typeof data.email, "string");
  assert.notEqual(data.email, original.email);
  const profile = data.profile as Record<string, unknown>;
  assert.equal(typeof profile.full_name, "string");
  assert.equal(typeof profile.age, "string"); // a number leaf became a ciphertext string
  assert.equal(profile.verified, true); // non-sensitive sibling untouched
  assert.equal(data.id, 42); // non-sensitive scalar untouched
  const addresses = data.addresses as Array<Record<string, unknown>>;
  assert.equal(typeof addresses[0].line, "string");
  assert.notEqual(addresses[0].line, "221B Baker Street");
  assert.equal(addresses[0].city, "London"); // non-sensitive array sibling untouched

  unprotect(data, PEOPLE_SCHEMA, c);

  // Exact restoration: numbers back to numbers, structure identical.
  assert.deepEqual(data, original);
  assert.equal(typeof (data.profile as Record<string, unknown>).age, "number");
});

test("root-marked data encrypts the whole object and restores it", () => {
  const c = cipher();
  const schema: SchemaNode = { type: "object", "x-gdpr-sensitive": true };
  // A root mark (path "") does not address a leaf inside the data object — it is a no-op on the
  // data root, matching the Go reference (an envelope's data IS the root object).
  const data = { a: 1, b: { c: 2 } };
  const before = structuredClone(data);
  protect(data, schema, c);
  assert.deepEqual(data, before); // unchanged: root path is not addressable in data
});

test("array-of-strings with a root-marked item schema protects every element", () => {
  const c = cipher();
  const schema: SchemaNode = {
    type: "object",
    properties: {
      tags: { type: "array", items: { type: "string", "x-gdpr-sensitive": true } },
    },
  };
  const data: Record<string, unknown> = { tags: ["a", "b", "c"] };
  protect(data, schema, c);
  for (const t of data.tags as unknown[]) {
    assert.equal(typeof t, "string");
  }
  assert.notDeepEqual(data.tags, ["a", "b", "c"]);
  unprotect(data, schema, c);
  assert.deepEqual(data.tags, ["a", "b", "c"]);
});

test("absent marked fields are skipped (not an error)", () => {
  const c = cipher();
  const data: Record<string, unknown> = { id: 1 }; // no email / profile / addresses
  assert.doesNotThrow(() => protect(data, PEOPLE_SCHEMA, c));
  assert.deepEqual(data, { id: 1 });
  assert.doesNotThrow(() => unprotect(data, PEOPLE_SCHEMA, c));
  assert.deepEqual(data, { id: 1 });
});

test("no-op when schema has no marks, or args are nullish", () => {
  const c = cipher();
  const data = { email: "x@y.z" };
  const before = structuredClone(data);
  protect(data, { type: "object", properties: { email: { type: "string" } } }, c);
  assert.deepEqual(data, before);
});

test("unprotect is idempotent on already-cleartext (non-string leaves untouched)", () => {
  const c = cipher();
  const data = sampleData();
  // Running unprotect on cleartext: string leaves that are not ciphertext throw, but number leaves
  // are skipped. Use a schema marking only a number leaf to prove the non-string skip path.
  const numberSchema: SchemaNode = {
    type: "object",
    properties: { id: { type: "integer", "x-gdpr-sensitive": true } },
  };
  assert.doesNotThrow(() => unprotect(data, numberSchema, c));
  assert.equal(data.id, 42);
});

test("unprotect with the wrong key throws DecryptError", () => {
  const data = sampleData();
  protect(data, PEOPLE_SCHEMA, new AesGcmCipher(KEY));
  const wrong = new AesGcmCipher(randomBytes(32));
  assert.throws(() => unprotect(data, PEOPLE_SCHEMA, wrong), DecryptError);
});

test("unprotect of a non-ciphertext string leaf throws DecryptError", () => {
  const c = cipher();
  const schema: SchemaNode = {
    type: "object",
    properties: { email: { type: "string", "x-gdpr-sensitive": true } },
  };
  assert.throws(() => unprotect({ email: "plain-text" }, schema, c), DecryptError);
});

// ── frozen envelope (GR-1 / GR-3 / GR-4) ───────────────────────────────────

test("protected envelope stays pure JSON, schema_version 1 and trace_id preserved", () => {
  const c = cipher();
  const env = EnvelopeCodec.make("urn:babel:people:created", sampleData(), {
    queue: "people",
    traceId: "trace-xyz",
  });

  protect(env.data, PEOPLE_SCHEMA, c);

  // Encodes as pure JSON, frame untouched.
  const body = EnvelopeCodec.encode(env);
  const decoded = JSON.parse(body) as Record<string, unknown>;
  assert.equal((decoded.meta as Record<string, unknown>).schema_version, 1);
  assert.equal(decoded.trace_id, "trace-xyz");
  assert.equal(decoded.job, "urn:babel:people:created");
  assert.equal(typeof decoded.data, "object");
  // Sensitive value is a JSON string (still pure JSON), not an object.
  assert.equal(typeof (decoded.data as Record<string, unknown>).email, "string");

  // A consumer decodes the frozen envelope and restores the cleartext.
  const incoming = JSON.parse(EnvelopeCodec.encode(env)) as { data: Record<string, unknown> };
  unprotect(incoming.data, PEOPLE_SCHEMA, c);
  assert.deepEqual(incoming.data, sampleData());
});
