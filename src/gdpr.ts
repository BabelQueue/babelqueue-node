/**
 * Runtime GDPR field encryption (ADR-0030) — the SDK-enforcement half.
 *
 * The registry only **declares** and **audits** which `data` fields are personal/sensitive
 * (`x-gdpr-sensitive`) and offers a one-way mask for safe logging. This module **enforces** that
 * sensitivity on the wire: a producer encrypts each marked leaf before publish, a consumer
 * decrypts it after decode. It is the Node mirror of the Go reference (`babelqueue-go/gdpr`), so
 * the contract is deliberately tight and identical across SDKs.
 *
 * Two invariants hold it together:
 *
 * - **Frozen envelope (GR-1).** {@link protect} rewrites only **values** inside `data`: a
 *   sensitive leaf's value becomes a ciphertext **string**. It never adds, renames, removes or
 *   retypes an envelope field; `meta.schema_version` stays **1**; `trace_id` is untouched (GR-4).
 *   `data` stays **pure JSON** (GR-3) — a JSON string is still pure JSON — so any SDK can carry
 *   the envelope even without the key (it just can't read the protected fields). Which fields are
 *   sensitive lives in the schema, not the message, so nothing about the frame changes.
 * - **Zero-dep core (GR-7).** The crypto is a caller-provided {@link Cipher} interface
 *   (KMS / Vault / HSM / tokenisation). The bundled {@link AesGcmCipher} is built **only** on the
 *   `node:crypto` standard library — no third-party dependency.
 *
 * The sensitive paths come from the **same** per-URN schema the produce/consume validation path
 * already loads (`schema.sensitivePaths`) — the `x-gdpr-sensitive` marks ride on it.
 * {@link protect}/{@link unprotect} are standalone helpers the caller invokes, so it is strictly
 * **opt-in**: a producer/consumer that never calls them behaves exactly as before.
 *
 * Typical wiring (producer):
 *
 * ```ts
 * import { EnvelopeCodec, gdpr, schema } from "@babelqueue/core";
 *
 * const env = EnvelopeCodec.make("urn:babel:orders:created", data, { queue: "orders" });
 * const s = provider.schemaFor(env.job);
 * if (s) {
 *   await schema.validate(provider, env.job, env.data); // optional: validate CLEARTEXT first
 *   gdpr.protect(env.data, s, cipher);                  // encrypt marked leaves IN PLACE
 * }
 * const body = EnvelopeCodec.encode(env);               // ciphertext rides inside data
 * ```
 *
 * and the inverse on the consumer, after decode and before the handler reads `env.data`:
 *
 * ```ts
 * const s = provider.schemaFor(env.job);
 * if (s) {
 *   gdpr.unprotect(env.data, s, cipher);                // decrypt marked leaves IN PLACE
 *   await schema.validate(provider, env.job, env.data); // validate CLEARTEXT after
 * }
 * ```
 *
 * Validate **cleartext** — *before* {@link protect} / *after* {@link unprotect} — because a schema
 * that constrains a sensitive field (`minLength`, `enum`, …) would reject the ciphertext string.
 *
 * @module
 */

import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

import { BabelQueueError } from "./errors.js";
import { type SchemaNode, sensitivePaths } from "./schema.js";

/**
 * The field-level protection primitive the **caller provides** — a seam onto a KMS, Vault transit,
 * an HSM, a tokenisation service, or the reference {@link AesGcmCipher} below. {@link protect}
 * runs {@link Cipher.encrypt} over every `x-gdpr-sensitive` leaf's value (after it is canonically
 * JSON-encoded); {@link unprotect} runs {@link Cipher.decrypt} to restore it. Keeping this an
 * interface is what holds GR-7: the core never pulls a crypto/KMS dependency — only a caller who
 * imports a concrete backend does.
 *
 * Both methods are **synchronous** (mirroring the Go reference), so {@link protect}/
 * {@link unprotect} are synchronous and exact. A KMS-backed implementation that can only work
 * asynchronously is outside this seam; bind a synchronous wrapper (a local data key, an envelope
 * cipher) instead.
 *
 * Contract for an implementation:
 *
 * - `encrypt` takes the canonical JSON bytes of one field value and returns the ciphertext as a
 *   **string** that is valid for placement inside a JSON document (the {@link AesGcmCipher}
 *   reference returns base64, which is). The same plaintext MAY encrypt to a different string each
 *   call (a random IV/nonce is expected and good).
 * - `decrypt` is the exact inverse: given a string `encrypt` produced, it returns the original
 *   JSON bytes byte-for-byte. A string it did not produce, or one produced under a different key,
 *   MUST throw rather than return silent garbage, so a wrong-key consume fails loudly.
 */
export interface Cipher {
  /** Protect one field value (its canonical JSON bytes) and return a JSON-safe ciphertext string. */
  encrypt(plaintext: Uint8Array): string;
  /** Reverse {@link Cipher.encrypt}, returning the original field-value JSON bytes. */
  decrypt(ciphertext: string): Uint8Array;
}

/**
 * Thrown by {@link unprotect} when a protected field cannot be restored — a wrong key, a
 * tampered/garbled ciphertext, or a value that is not what {@link protect} produced.
 * {@link unprotect} stops at the first such failure, so it is distinguishable from a missing field
 * (which is skipped, not an error). The consumer should fail the message (retry / dead-letter)
 * rather than process unreadable PII.
 */
export class DecryptError extends BabelQueueError {
  constructor(message: string, readonly cause?: unknown) {
    super(`Cannot decrypt a protected field: ${message}.`);
    this.name = "DecryptError";
  }
}

/**
 * Thrown by {@link AesGcmCipher} when its key is not 16, 24 or 32 bytes (AES-128/192/256).
 */
export class InvalidKeySizeError extends BabelQueueError {
  constructor(readonly length: number) {
    super(`AES key must be 16, 24, or 32 bytes (got ${length}).`);
    this.name = "InvalidKeySizeError";
  }
}

/**
 * Thrown by {@link AesGcmCipher.decrypt} when the input is not valid base64 or is too short to
 * contain an IV — i.e. not something this cipher produced.
 */
export class MalformedCiphertextError extends BabelQueueError {
  constructor(reason: string) {
    super(`Malformed ciphertext: ${reason}.`);
    this.name = "MalformedCiphertextError";
  }
}

const GCM_IV_BYTES = 12; // the standard, recommended GCM IV/nonce length
const GCM_TAG_BYTES = 16; // AES-GCM authentication tag length

/**
 * A reference {@link Cipher} built **only** on `node:crypto`: AES-GCM authenticated encryption
 * with a fresh random 12-byte IV per call, the IV **prepended** to the ciphertext, the whole thing
 * base64-encoded so it drops straight into a JSON string. The key is the **caller's** — this class
 * performs no key management, rotation or derivation; bind a KMS-backed {@link Cipher} for that.
 *
 * The wire layout per value is `base64(iv || ciphertext || tag)`. AES-256-GCM is used when the key
 * is 32 bytes (the recommended size); 16- and 24-byte keys select AES-128/192-GCM. GCM
 * authenticates the ciphertext, so {@link AesGcmCipher.decrypt} rejects any tampered or wrong-key
 * input by throwing (it never returns corrupt plaintext). Zero third-party dependency.
 */
export class AesGcmCipher implements Cipher {
  private readonly key: Buffer;
  private readonly algorithm: "aes-128-gcm" | "aes-192-gcm" | "aes-256-gcm";

  /**
   * Build an AES-GCM cipher from a raw symmetric key. The key length selects the AES variant:
   * 32 bytes → AES-256-GCM (recommended), 24 → AES-192-GCM, 16 → AES-128-GCM. Any other length
   * throws {@link InvalidKeySizeError}.
   */
  constructor(key: Uint8Array) {
    switch (key.length) {
      case 16:
        this.algorithm = "aes-128-gcm";
        break;
      case 24:
        this.algorithm = "aes-192-gcm";
        break;
      case 32:
        this.algorithm = "aes-256-gcm";
        break;
      default:
        throw new InvalidKeySizeError(key.length);
    }
    this.key = Buffer.from(key);
  }

  /**
   * Seal `plaintext` with a fresh random IV, prepend the IV, append the GCM auth tag, and
   * base64-encode the result. Implements {@link Cipher.encrypt}.
   */
  encrypt(plaintext: Uint8Array): string {
    const iv = randomBytes(GCM_IV_BYTES);
    const cipher = createCipheriv(this.algorithm, this.key, iv);
    const sealed = Buffer.concat([cipher.update(Buffer.from(plaintext)), cipher.final()]);
    const tag = cipher.getAuthTag();
    return Buffer.concat([iv, sealed, tag]).toString("base64");
  }

  /**
   * Reverse {@link AesGcmCipher.encrypt}: base64-decode, split off the prepended IV and trailing
   * auth tag, and open the GCM ciphertext. A wrong key or tampered input fails GCM authentication
   * and throws {@link DecryptError} (never corrupt plaintext); input that is not valid base64 or is
   * too short throws {@link MalformedCiphertextError}. Implements {@link Cipher.decrypt}.
   */
  decrypt(ciphertext: string): Uint8Array {
    const raw = Buffer.from(ciphertext, "base64");
    // Buffer.from(..., "base64") is lenient; round-trip to detect input that is not real base64.
    if (raw.toString("base64").replace(/=+$/, "") !== ciphertext.replace(/=+$/, "")) {
      throw new MalformedCiphertextError("not base64");
    }
    if (raw.length < GCM_IV_BYTES + GCM_TAG_BYTES) {
      throw new MalformedCiphertextError("shorter than IV plus auth tag");
    }
    const iv = raw.subarray(0, GCM_IV_BYTES);
    const tag = raw.subarray(raw.length - GCM_TAG_BYTES);
    const sealed = raw.subarray(GCM_IV_BYTES, raw.length - GCM_TAG_BYTES);
    const decipher = createDecipheriv(this.algorithm, this.key, iv);
    decipher.setAuthTag(tag);
    try {
      return Buffer.concat([decipher.update(sealed), decipher.final()]);
    } catch (err) {
      // GCM authentication failed: wrong key, tampered ciphertext, or not our output.
      throw new DecryptError("authentication failed", err);
    }
  }
}

/**
 * Encrypt, in place, every value in `data` located at a path the schema marked
 * `x-gdpr-sensitive` — the producer-side step, run after building `data` and before encode /
 * publish. Each marked leaf's value is canonically JSON-encoded and replaced by
 * {@link Cipher.encrypt}'s ciphertext **string**; the envelope frame, non-sensitive fields and
 * key order are untouched (GR-1).
 *
 * A marked path absent from `data` is skipped (not an error) — schemas evolve and a message need
 * not carry every optional field. A `null`/missing schema, or one with no marks, is a no-op
 * (nothing is sensitive). Container marks (a whole object/array marked sensitive) are supported:
 * the entire sub-value is encoded and encrypted as one ciphertext string.
 *
 * On any cipher error this throws and leaves `data` in a partially-protected state; treat a
 * `protect` error as fatal for that message (do not publish it).
 */
export function protect(
  data: Record<string, unknown>,
  schema: SchemaNode,
  cipher: Cipher,
): void {
  walk(data, schema, cipher, encryptLeaf);
}

/**
 * The consumer-side inverse of {@link protect}: decrypt, in place, every value in `data` at an
 * `x-gdpr-sensitive` path, restoring the original JSON value **byte-for-byte**. Run it after
 * decode and before the handler reads `data`.
 *
 * An absent path is skipped. A leaf that is **not** a string — it was never protected, or this is
 * a re-run after a successful unprotect — is left as-is, so re-invoking {@link unprotect} on
 * already-cleartext data is safe (idempotent for non-string leaves). A string the cipher cannot
 * open (wrong key, tampered, or not a ciphertext) throws {@link DecryptError} — the consumer
 * should fail the message (retry / dead-letter) rather than process unreadable PII.
 */
export function unprotect(
  data: Record<string, unknown>,
  schema: SchemaNode,
  cipher: Cipher,
): void {
  walk(data, schema, cipher, decryptLeaf);
}

/**
 * Transforms a single sensitive leaf value (encrypt or decrypt). Returns the new value and a
 * `replace` flag: `false` means the value was absent/skippable and the caller leaves the slot
 * alone. Mirrors the Go `leafOp`.
 */
type LeafOp = (value: unknown, cipher: Cipher) => { value: unknown; replace: boolean };

/**
 * Drives a {@link LeafOp} over every `x-gdpr-sensitive` path the schema declares. It resolves each
 * path against `data` itself (not by re-walking the schema over the value), so the operation
 * touches exactly the declared leaves and nothing else — non-sensitive siblings are never read or
 * copied. Mirrors the Go `walk`.
 */
function walk(
  data: Record<string, unknown> | null | undefined,
  schema: SchemaNode | null | undefined,
  cipher: Cipher | null | undefined,
  op: LeafOp,
): void {
  if (!data || !schema || !cipher) {
    return;
  }
  for (const sp of sensitivePaths(schema)) {
    applyAtPath(data, parsePath(sp.path), cipher, op);
  }
}

/**
 * One step of a sensitive path: a named object key, optionally with array-descent.
 * `"addresses[].line"` parses to `[{key:"addresses", array:true}, {key:"line", array:false}]`.
 */
interface Segment {
  key: string;
  /** This key holds an array; descend into every element before the next segment. */
  array: boolean;
}

/**
 * Split a sensitive path (`"email"`, `"profile.full_name"`, `"addresses[].line"`) into segments.
 * The `[]` marker binds to the segment it trails, signalling array descent. A root mark (path `""`)
 * yields no segments and addresses nothing in `data` (an envelope's `data` is the object root).
 */
function parsePath(path: string): Segment[] {
  if (path === "") {
    return [];
  }
  return path.split(".").map((part) => {
    if (part.length >= 2 && part.endsWith("[]")) {
      return { key: part.slice(0, -2), array: true };
    }
    return { key: part, array: false };
  });
}

/**
 * Resolve `segs` against the current node and run `op` on the leaf(s). It descends objects by key
 * and, when a segment is an array, fans out over every element. An absent key or a type mismatch
 * (a path that does not exist in this particular message) is skipped silently — schemas describe
 * the union of possible shapes; a given message need not contain every field. Mirrors the Go
 * `applyAtPath`.
 */
function applyAtPath(node: unknown, segs: Segment[], cipher: Cipher, op: LeafOp): void {
  if (segs.length === 0) {
    return; // root mark or exhausted path with no leaf key — nothing addressable in data
  }
  if (typeof node !== "object" || node === null || Array.isArray(node)) {
    return; // expected an object here but the message has something else — skip
  }
  const obj = node as Record<string, unknown>;
  const seg = segs[0];
  if (!(seg.key in obj)) {
    return; // absent field — skip (not an error)
  }
  const child = obj[seg.key];
  const last = segs.length === 1;

  if (seg.array) {
    if (!Array.isArray(child)) {
      return; // declared array but message has a non-array — skip
    }
    for (let i = 0; i < child.length; i++) {
      if (last) {
        const result = op(child[i], cipher);
        if (result.replace) {
          child[i] = result.value;
        }
      } else {
        applyAtPath(child[i], segs.slice(1), cipher, op);
      }
    }
    return;
  }

  if (last) {
    const result = op(child, cipher);
    if (result.replace) {
      obj[seg.key] = result.value;
    }
    return;
  }
  applyAtPath(child, segs.slice(1), cipher, op);
}

/**
 * Canonically JSON-encode one field value and replace it with the cipher's ciphertext string. The
 * JSON encoding is what makes the round-trip exact: {@link decryptLeaf}'s `JSON.parse` restores
 * the same decoded-JSON value (number, object, …) the codec would have produced, so
 * protect → unprotect is byte-for-byte. Mirrors the Go `encryptLeaf`.
 */
function encryptLeaf(value: unknown, cipher: Cipher): { value: unknown; replace: boolean } {
  const plaintext = new TextEncoder().encode(JSON.stringify(value));
  return { value: cipher.encrypt(plaintext), replace: true };
}

/**
 * Reverse {@link encryptLeaf}. A non-string leaf is left untouched (`replace: false`) so
 * {@link unprotect} is safe to re-run on already-cleartext data; a string that fails to open or to
 * JSON-decode throws {@link DecryptError} so the consumer fails the message rather than handling
 * unreadable PII. Mirrors the Go `decryptLeaf`.
 */
function decryptLeaf(value: unknown, cipher: Cipher): { value: unknown; replace: boolean } {
  if (typeof value !== "string") {
    // Not a ciphertext string (already cleartext, or never protected) — leave as-is.
    return { value, replace: false };
  }
  let plaintext: Uint8Array;
  try {
    plaintext = cipher.decrypt(value);
  } catch (err) {
    if (err instanceof DecryptError) {
      throw err;
    }
    throw new DecryptError("cipher rejected the value", err);
  }
  let restored: unknown;
  try {
    restored = JSON.parse(new TextDecoder().decode(plaintext));
  } catch (err) {
    throw new DecryptError("decoded plaintext is not JSON", err);
  }
  return { value: restored, replace: true };
}
