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
 * whose verdicts match the Go, PHP and Python validators and babelqueue-registry's `compat`
 * linter: `type`, `required`, `properties`, `additionalProperties`, `items`, `enum`,
 * `const`, `minLength`, `minimum`. Unknown keywords are ignored.
 */
import type { Envelope } from "./codec.js";
import { InvalidPayloadError } from "./errors.js";

/** A parsed JSON Schema node. */
export type SchemaNode = Record<string, unknown>;

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

  constructor(schemas: Record<string, SchemaNode>) {
    this.schemas = new Map(Object.entries(schemas));
  }

  /** Build a provider from URN -> raw JSON Schema strings, parsing each. */
  static fromJson(raw: Record<string, string>): MapProvider {
    const schemas: Record<string, SchemaNode> = {};
    for (const [urn, body] of Object.entries(raw)) {
      const decoded: unknown = JSON.parse(body);
      if (typeof decoded !== "object" || decoded === null || Array.isArray(decoded)) {
        throw new Error(`schema: invalid JSON schema for "${urn}"`);
      }
      schemas[urn] = decoded as SchemaNode;
    }
    return new MapProvider(schemas);
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

/** The first violation of `value` against a (subset) JSON Schema node, or null. */
export function validateSchema(schema: SchemaNode, value: unknown, path = ""): string | null {
  if ("const" in schema && !equal(value, schema.const)) {
    return violation(path, "wrong_const");
  }
  const enumValues = schema.enum;
  if (Array.isArray(enumValues) && !enumValues.some((item) => equal(value, item))) {
    return violation(path, "not_in_enum");
  }

  const type = typeof schema.type === "string" ? schema.type : "";
  switch (type) {
    case "object":
      return checkObject(schema, value, path);
    case "array":
      return checkArray(schema, value, path);
    case "string": {
      if (typeof value !== "string") {
        return violation(path, "not_a_string");
      }
      const minLength = schema.minLength;
      if (typeof minLength === "number" && value.length < minLength) {
        return violation(path, "below_min_length");
      }
      return null;
    }
    case "integer":
      if (!isInteger(value)) {
        return violation(path, "not_an_integer");
      }
      return checkMinimum(schema, value, path);
    case "number":
      if (typeof value !== "number") {
        return violation(path, "not_a_number");
      }
      return checkMinimum(schema, value, path);
    case "boolean":
      return typeof value === "boolean" ? null : violation(path, "not_a_boolean");
    case "null":
      return value === null ? null : violation(path, "not_null");
    default:
      return null;
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

function checkMinimum(schema: SchemaNode, value: number, path: string): string | null {
  const minimum = schema.minimum;
  if (typeof minimum === "number" && value < minimum) {
    return violation(path, "below_minimum");
  }
  return null;
}

// JSON numbers are all `number` in JS; an integer is a whole number (and never a boolean).
function isInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value);
}

// Structural equality for enum/const checks: JSON.stringify distinguishes a string "1" from
// a number 1, matching the strict comparisons in the other SDK validators.
function equal(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function violation(path: string, reason: string): string {
  return `${path === "" ? "<root>" : path}: ${reason}`;
}

function join(path: string, key: string): string {
  return path === "" ? key : `${path}.${key}`;
}
