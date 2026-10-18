#!/usr/bin/env node
"use strict";
// Completeness gate for a conformance results.json (ADR-0032 section 3 / Compliance).
// Usage: node scripts/check-conformance-results.cjs <results.json> <manifest.json> <package.json> [src-dir]
// Shared by ci.yml and release.yml so a tag cannot publish an asset CI would have refused.
const fs = require("node:fs");
const path = require("node:path");

const [resultsPath, manifestPath, pkgPath, srcDir = "src"] = process.argv.slice(2);
if (!resultsPath || !manifestPath || !pkgPath) {
  console.error("usage: check-conformance-results.cjs <results.json> <manifest.json> <package.json> [src-dir]");
  process.exit(2);
}
const results = JSON.parse(fs.readFileSync(resultsPath, "utf8"));
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
const errors = [];

// Transport / helper names that denote a conformance binding; the single table shared with
// test/conformance.test.ts.
const BINDING_BY_NAME = JSON.parse(fs.readFileSync(path.join(__dirname, "conformance-bindings.json"), "utf8"));

const carried = new Set(["core"]);
const note = (name) => {
  if (Object.hasOwn(BINDING_BY_NAME, name.toLowerCase())) carried.add(BINDING_BY_NAME[name.toLowerCase()]);
};
// 1. every path segment of every public export key (so ./otel and ./transports/redis both count)
for (const key of Object.keys(pkg.exports || {})) key.split("/").forEach(note);
// 2. every file/directory name under src/ (a transport shipped without an export is still carried)
const walk = (dir) => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    note(entry.name.replace(/\.[cm]?[jt]s$/, ""));
    if (entry.isDirectory()) walk(path.join(dir, entry.name));
  }
};
if (fs.existsSync(srcDir)) walk(srcDir);

for (const b of carried) {
  if (b !== "core" && !results.bindings.includes(b)) {
    errors.push(`binding "${b}" is carried by the package but missing from results.bindings`);
  }
}
for (const b of results.bindings) {
  if (!carried.has(b)) errors.push(`results.bindings lists "${b}", which the package does not carry`);
}

const inScope = (scope) => scope === "core" || results.bindings.includes(scope);
for (const [name, def] of Object.entries(manifest.sections)) {
  const present = Object.prototype.hasOwnProperty.call(results.sections, name);
  if (inScope(def.scope) && !present) errors.push(`in-scope section "${name}" (scope ${def.scope}) is missing`);
  if (!inScope(def.scope) && present) errors.push(`section "${name}" (scope ${def.scope}) must be absent`);
}

let fail = 0;
for (const [section, cases] of Object.entries(results.sections)) {
  for (const [id, r] of Object.entries(cases)) {
    if (r.status === "fail") {
      fail++;
      errors.push(`${section}/${id}: fail`);
    }
  }
}
if (errors.length) {
  console.error(errors.join("\n"));
  process.exit(1);
}
console.log(
  `results.json complete: bindings=[${results.bindings}], sections=${Object.keys(results.sections).length}, fail=${fail}`,
);
