import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { InvalidEnvelopeKeyError } from "../src/index.js";

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
