/**
 * Optional per-URN payload schema validation (ADR-0024).
 *
 * The Node mirror of the Go `schema` package and PHP `BabelQueue\Schema`. A
 * {@link SchemaProvider} supplies a JSON Schema for a message URN — typically built from a
 * babelqueue-registry `registry.json` — and the message's `data` is validated against it.
 * It is opt-in: a URN with no registered schema is never validated.
 *
 * ```ts
 * import { schema } from "@babelqueue/core";
 *
 * const provider = schema.MapProvider.fromJson({ "urn:babel:orders:created": ORDERS_JSON });
 * schema.validate(provider, "urn:babel:orders:created", { order_id: 7 }); // throws on mismatch
 * const handler = schema.wrap(provider, async (env) => {  ...  });        // consumer safety net
 * ```
 *
 * The core stays dependency-free and I/O-free, so it carries no file-based provider: a Node
 * app or adapter reads its `registry.json` (with `node:fs`, etc.) and passes the schemas to
 * {@link MapProvider.fromJson}. The validator is a small subset of JSON Schema (draft-07)
 * whose verdicts match the Go, PHP, Python, Java and .NET validators and babelqueue-registry's
 * `compat` linter: `type` (a string or an array of names), `required`, `properties`,
 * `additionalProperties`, `items`, `enum`, `const`, `minLength`, `maxLength`, `minimum`.
 * A keyword outside the subset never changes a verdict (it is evaluated as if absent), but a
 * {@link MapProvider} reports one {@link SchemaWarning} per unsupported keyword occurrence when
 * the schema is registered (see {@link unsupportedKeywords}); annotation keywords (`$schema`,
 * `$id`, `title`, `description`, `examples`, `default`, `$comment`, `x-*`) never warn.
 */
import type { Envelope } from "./codec.js";
import { InvalidPayloadError } from "./errors.js";

/** A parsed JSON Schema node. */
export type SchemaNode = Record<string, unknown>;

/**
 * One keyword outside the supported subset, found when a schema is registered. The schema is
 * evaluated as if the keyword were absent, so a warning never changes a verdict. `pointer` is the
 * RFC 6901 JSON Pointer, within the schema document, of the schema object carrying the keyword
 * (`""` for the root, then `/properties/<name>`, `/items`, `/additionalProperties`; property
 * names are escaped `~` -> `~0`, `/` -> `~1`).
 */
export interface SchemaWarning {
  pointer: string;
  keyword: string;
}

/** A {@link SchemaWarning} tagged with the URN whose schema produced it. */
export interface RegisteredSchemaWarning extends SchemaWarning {
  urn: string;
}

/** Options for {@link MapProvider}. */
export interface MapProviderOptions {
  /**
   * Receives one {@link RegisteredSchemaWarning} (and a human-readable message) per unsupported
   * keyword occurrence, at registration time. Defaults to `process.emitWarning` with the code
   * {@link UNSUPPORTED_KEYWORD_WARNING_CODE}, once per message per process.
   */
  onWarning?: (warning: RegisteredSchemaWarning, message: string) => void;
}

/** The `process.emitWarning` code used for an unsupported schema keyword. */
export const UNSUPPORTED_KEYWORD_WARNING_CODE = "BABELQUEUE_UNSUPPORTED_SCHEMA_KEYWORD";

const SUPPORTED_KEYWORDS = new Set([
  "type", "required", "properties", "additionalProperties", "items",
  "enum", "const", "minLength", "maxLength", "minimum",
]);
const ANNOTATION_KEYWORDS = new Set([
  "$schema", "$id", "title", "description", "examples", "default", "$comment",
]);

const emittedWarnings = new Set<string>();

function defaultWarning(_warning: RegisteredSchemaWarning, message: string): void {
  if (emittedWarnings.has(message)) return;
  emittedWarnings.add(message);
  if (typeof process !== "undefined" && typeof process.emitWarning === "function") {
    process.emitWarning(message, { code: UNSUPPORTED_KEYWORD_WARNING_CODE });
  }
}

function isSchemaObject(value: unknown): value is SchemaNode {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function escapePointer(name: string): string {
  return name.replaceAll("~", "~0").replaceAll("/", "~1");
}

/**
 * Every keyword of `schema` outside the supported subset, one entry per occurrence, in a
 * deterministic order. The walk descends only into the values of `properties`, `items` and
 * `additionalProperties` (each when a JSON object) and never into the value of an unsupported
 * keyword. Annotation keywords and every `x-*` keyword are not unsupported. Pure: it never
 * emits anything; {@link MapProvider} reports its result at registration.
 */
export function unsupportedKeywords(schema: SchemaNode, pointer = ""): SchemaWarning[] {
  const out: SchemaWarning[] = [];
  for (const keyword of Object.keys(schema).sort()) {
    if (SUPPORTED_KEYWORDS.has(keyword) || ANNOTATION_KEYWORDS.has(keyword) || keyword.startsWith("x-")) {
      continue;
    }
    out.push({ pointer, keyword });
  }
  const properties = schema.properties;
  if (isSchemaObject(properties)) {
    for (const name of Object.keys(properties).sort()) {
      const sub = properties[name];
      if (isSchemaObject(sub)) {
        out.push(...unsupportedKeywords(sub, `${pointer}/properties/${escapePointer(name)}`));
      }
    }
  }
  if (isSchemaObject(schema.items)) {
    out.push(...unsupportedKeywords(schema.items, `${pointer}/items`));
  }
  if (isSchemaObject(schema.additionalProperties)) {
    out.push(...unsupportedKeywords(schema.additionalProperties, `${pointer}/additionalProperties`));
  }
  return out;
}

/** A consume handler: receives a decoded envelope, may be sync or async. */
export type SchemaHandler = (env: Envelope) => void | Promise<void>;

/**
 * A source of per-URN `data` schemas, keyed on the message URN. `schemaFor` may be sync or
 * async so a production provider can be service- or cache-backed; the reference
 * {@link MapProvider} is synchronous.
 */
export interface SchemaProvider {
  schemaFor(urn: string): SchemaNode | undefined | Promise<SchemaNode | undefined>;
}

/** In-memory {@link SchemaProvider}, for tests and for embedding schemas in code. */
export class MapProvider implements SchemaProvider {
  private readonly schemas: Map<string, SchemaNode>;

  /**
   * The unsupported-keyword warnings produced when the schemas were registered, in URN then
   * schema order. The same entries were already passed to `options.onWarning`.
   */
  readonly warnings: readonly RegisteredSchemaWarning[];

  constructor(schemas: Record<string, SchemaNode>, options: MapProviderOptions = {}) {
    this.schemas = new Map(Object.entries(schemas));
    const onWarning = options.onWarning ?? defaultWarning;
    const warnings: RegisteredSchemaWarning[] = [];
    for (const [urn, node] of this.schemas) {
      for (const found of unsupportedKeywords(node)) {
        const warning = { urn, ...found };
        warnings.push(warning);
        onWarning(
          warning,
          `babelqueue schema: unsupported keyword "${found.keyword}" at "${found.pointer}" for ${urn} is ignored`,
        );
      }
    }
    this.warnings = warnings;
  }

  /** Build a provider from URN -> raw JSON Schema strings, parsing each. */
  static fromJson(raw: Record<string, string>, options: MapProviderOptions = {}): MapProvider {
    const schemas: Record<string, SchemaNode> = {};
    for (const [urn, body] of Object.entries(raw)) {
      const decoded: unknown = JSON.parse(body);
      if (typeof decoded !== "object" || decoded === null || Array.isArray(decoded)) {
        throw new Error(`schema: invalid JSON schema for "${urn}"`);
      }
      schemas[urn] = decoded as SchemaNode;
    }
    return new MapProvider(schemas, options);
  }

  schemaFor(urn: string): SchemaNode | undefined {
    return this.schemas.get(urn);
  }
}

/**
 * The first `data` violation for `(urn, data)`, or null when it is valid or when no schema is
 * registered for the URN (opt-in). For producer-side branching.
 */
export async function check(
  provider: SchemaProvider,
  urn: string,
  data: Record<string, unknown>,
): Promise<string | null> {
  const schemaNode = await provider.schemaFor(urn);
  if (!schemaNode) {
    return null;
  }
  return validateSchema(schemaNode, data);
}

/**
 * Validate `(urn, data)` against its registered schema, throwing {@link InvalidPayloadError}
 * otherwise. The producer-side guard; call it before publishing.
 */
export async function validate(
  provider: SchemaProvider,
  urn: string,
  data: Record<string, unknown>,
): Promise<void> {
  const violation = await check(provider, urn, data);
  if (violation !== null) {
    throw new InvalidPayloadError(urn, violation);
  }
}

/**
 * Wrap a consume handler so each message's `data` is validated against its URN's schema
 * before the handler runs (consumer-side safety net). Invalid data throws
 * {@link InvalidPayloadError}, so the adapter redelivers (and eventually dead-letters) the
 * poison message; a URN with no schema runs the handler unchanged. Prefer {@link check}
 * producer-side to keep invalid data out of the queue entirely.
 */
export function wrap(provider: SchemaProvider, handler: SchemaHandler): SchemaHandler {
  return async (env: Envelope): Promise<void> => {
    await validate(provider, env.job, env.data);
    await handler(env);
  };
}

/**
 * One property marked `x-gdpr-sensitive`, located by its dotted path from the schema root.
 * Array element schemas contribute a `field[]` segment (matching the Go `SensitivePath` and the
 * registry's path convention). `category` is the optional `"x-gdpr-sensitive": "<category>"`
 * string (e.g. `"email"`), or `""` when the mark was the boolean `true`.
 */
export interface SensitivePath {
  /** Dotted path from the schema root, e.g. `"email"`, `"profile.full_name"`, `"addresses[].line"`. A mark on the root schema itself is the empty string `""`. */
  path: string;
  /** Free-form category, or `""` when the keyword was the boolean `true`. */
  category: string;
}

/**
 * Whether a schema node carries the `x-gdpr-sensitive` mark (ADR-0030), with its optional
 * category. Accepts either the boolean `true` or a non-empty string category (the
 * documentation form, e.g. `"email"`); any other shape — `false`, `""`, a number — is unmarked.
 * The keyword is **validation-neutral**: {@link validateSchema} never reads it, so annotating a
 * schema can never change a verdict (adding the mark is non-breaking, GR-1). Mirrors the Go
 * `fromMap` parse and the registry's parser.
 */
function gdprMark(schema: SchemaNode): { marked: boolean; category: string } {
  const raw = schema["x-gdpr-sensitive"];
  if (raw === true) {
    return { marked: true, category: "" };
  }
  if (typeof raw === "string" && raw !== "") {
    return { marked: true, category: raw };
  }
  return { marked: false, category: "" };
}

/**
 * Every property a schema marks `x-gdpr-sensitive`, in sorted path order (ADR-0030). It descends
 * nested objects (dotted paths) and array item schemas (a `field[]` segment), and reports a mark
 * on the root schema itself as the path `""`. This is the value-level counterpart to the
 * registry's inventory: {@link protect}/{@link unprotect} use these paths to locate the leaves to
 * encrypt on produce and decrypt on consume. It is the Node mirror of the Go
 * `Schema.SensitivePaths()`.
 */
export function sensitivePaths(schema: SchemaNode): SensitivePath[] {
  const out: SensitivePath[] = [];
  collectSensitive(schema, "", out);
  out.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return out;
}

function collectSensitive(schema: SchemaNode, path: string, out: SensitivePath[]): void {
  const mark = gdprMark(schema);
  if (mark.marked) {
    out.push({ path, category: mark.category });
  }

  const properties = schema.properties;
  if (typeof properties === "object" && properties !== null && !Array.isArray(properties)) {
    for (const [name, sub] of Object.entries(properties as Record<string, unknown>)) {
      if (typeof sub === "object" && sub !== null && !Array.isArray(sub)) {
        collectSensitive(sub as SchemaNode, join(path, name), out);
      }
    }
  }

  const items = schema.items;
  if (typeof items === "object" && items !== null && !Array.isArray(items)) {
    collectSensitive(items as SchemaNode, `${path}[]`, out);
  }
}

/** The first violation of `value` against a (subset) JSON Schema node, or null. */
export function validateSchema(schema: SchemaNode, value: unknown, path = ""): string | null {
  if ("const" in schema && !equal(value, schema.const)) {
    return violation(path, "wrong_const");
  }
  const enumValues = schema.enum;
  if (Array.isArray(enumValues) && !enumValues.some((item) => equal(value, item))) {
    return violation(path, "not_in_enum");
  }

  const types = knownTypes(schema);
  if (types.length === 0) {
    return null;
  }
  const matched = types.find((t) => matchesType(t, value));
  if (matched === undefined) {
    return violation(path, types.length === 1 ? TYPE_REASONS[types[0]] : "not_one_of_types");
  }

  switch (matched) {
    case "object":
      return checkObject(schema, value, path);
    case "array":
      return checkArray(schema, value, path);
    case "string": {
      const str = value as string;
      // Length is counted in Unicode code points (not UTF-16 code units), so an
      // astral character such as an emoji counts once — the cross-SDK rule.
      const length = codePointLength(str);
      const minLength = schema.minLength;
      if (typeof minLength === "number" && length < minLength) {
        return violation(path, "below_min_length");
      }
      const maxLength = schema.maxLength;
      if (typeof maxLength === "number" && length > maxLength) {
        return violation(path, "above_max_length");
      }
      return null;
    }
    case "integer":
    case "number":
      return checkMinimum(schema, value as number | bigint, path);
    default:
      return null;
  }
}

const TYPE_REASONS: Record<string, string> = {
  object: "not_an_object",
  array: "not_an_array",
  string: "not_a_string",
  integer: "not_an_integer",
  number: "not_a_number",
  boolean: "not_a_boolean",
  null: "not_null",
};

/** The schema's `type` names (a string, or every string of an array) restricted to the seven JSON Schema types; an unknown name constrains nothing. */
function knownTypes(schema: SchemaNode): string[] {
  const raw = schema.type;
  const names = typeof raw === "string" ? [raw] : Array.isArray(raw) ? raw : [];
  return names.filter((n): n is string => typeof n === "string" && Object.hasOwn(TYPE_REASONS, n));
}

function matchesType(type: string, value: unknown): boolean {
  switch (type) {
    case "object":
      return isSchemaObject(value);
    case "array":
      return Array.isArray(value);
    case "string":
      return typeof value === "string";
    case "integer":
      return isInteger(value);
    case "number":
      return typeof value === "number" || typeof value === "bigint";
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    default:
      return false;
  }
}

function checkObject(schema: SchemaNode, value: unknown, path: string): string | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return violation(path, "not_an_object");
  }
  const obj = value as Record<string, unknown>;

  const required = schema.required;
  if (Array.isArray(required)) {
    for (const key of required) {
      if (typeof key === "string" && !(key in obj)) {
        return violation(join(path, key), "missing_required");
      }
    }
  }

  const properties =
    typeof schema.properties === "object" && schema.properties !== null
      ? (schema.properties as Record<string, unknown>)
      : {};
  const additionalAllowed = schema.additionalProperties !== false;

  for (const [name, item] of Object.entries(obj)) {
    const propSchema = properties[name];
    if (typeof propSchema === "object" && propSchema !== null) {
      const found = validateSchema(propSchema as SchemaNode, item, join(path, name));
      if (found !== null) {
        return found;
      }
      continue;
    }
    if (!additionalAllowed) {
      return violation(join(path, name), "additional_not_allowed");
    }
  }

  return null;
}

/** The number of Unicode code points in `value` (a surrogate pair counts once). */
function codePointLength(value: string): number {
  return [...value].length;
}

function checkArray(schema: SchemaNode, value: unknown, path: string): string | null {
  if (!Array.isArray(value)) {
    return violation(path, "not_an_array");
  }
  const items = schema.items;
  if (typeof items !== "object" || items === null) {
    return null;
  }
  for (let i = 0; i < value.length; i++) {
    const found = validateSchema(items as SchemaNode, value[i], `${path}[${i}]`);
    if (found !== null) {
      return found;
    }
  }
  return null;
}

function checkMinimum(schema: SchemaNode, value: number | bigint, path: string): string | null {
  const minimum = schema.minimum;
  if (typeof minimum === "number" && value < minimum) {
    return violation(path, "below_minimum");
  }
  return null;
}

// An integer is a number whose mathematical value is integral (`1`, `1.0` and `1e2` — JS cannot
// tell them apart — but not `1.5`), or a bigint; never a boolean or a string.
function isInteger(value: unknown): value is number | bigint {
  return typeof value === "bigint" || (typeof value === "number" && Number.isInteger(value));
}

// Structural equality for enum/const checks: numbers compare by mathematical value (a number
// and a bigint included), arrays element by element in order, objects by key set and values
// (key order is irrelevant). A boolean never equals a number, a string never equals a number,
// and null equals only null.
function equal(a: unknown, b: unknown): boolean {
  if ((typeof a === "number" || typeof a === "bigint") && (typeof b === "number" || typeof b === "bigint")) {
    // Loose equality compares a number and a bigint by mathematical value.
    return a == b;
  }
  if (Array.isArray(a)) {
    return Array.isArray(b) && a.length === b.length && a.every((item, i) => equal(item, b[i]));
  }
  if (isSchemaObject(a)) {
    if (!isSchemaObject(b)) return false;
    const keys = Object.keys(a);
    return (
      keys.length === Object.keys(b).length &&
      keys.every((k) => Object.hasOwn(b, k) && equal(a[k], b[k]))
    );
  }
  return a === b;
}

function violation(path: string, reason: string): string {
  return `${path === "" ? "<root>" : path}: ${reason}`;
}

function join(path: string, key: string): string {
  return path === "" ? key : `${path}.${key}`;
}
