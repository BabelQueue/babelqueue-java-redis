#!/usr/bin/env python3
"""Validate the results.json of this package (ADR-0032).

usage: check-conformance-results.py <results.json> <vendor-dir> <expected-conformance-version>

This package carries the `redis` binding only, so the in-scope manifest sections are those whose
scope is `redis`; the `core` sections are run and reported by babelqueue-core. Fails (exit 1) when
results.json
  * does not conform to schema/results.schema.json,
  * reports a conformance_version other than the pinned one, or bindings other than ["redis"],
  * has a `fail` case, or any `skip` (CI runs against a live Redis with BQ_REQUIRE_REDIS=1, so a
    skipped case means the behavioural cases did not really run),
  * omits a redis-scope section or a case id that the manifest defines for it, or lists a section
    that is not in scope.
"""
import json
import os
import sys

from jsonschema import Draft202012Validator

BINDINGS = ["redis"]


def case_ids(section, body):
    """The case-id rule of the conformance README ("Sections, scopes and exceptions")."""
    def named(prefix, fallback, items):
        return [prefix + (c["name"] if isinstance(c, dict) and isinstance(c.get("name"), str) else fallback(i))
                for i, c in enumerate(items)]

    if isinstance(body, list):
        return named("", lambda i: f"{section}#{i}", body)
    if not isinstance(body, dict):
        return []
    ids = []
    if isinstance(body.get("cases"), list):
        ids += named("", lambda i: f"{section}#{i}", body["cases"])
    for key, value in body.items():
        if key in ("description", "schema", "cases") or not isinstance(value, dict):
            continue
        if isinstance(value.get("cases"), list):
            ids += named(key + "/", str, value["cases"])
        else:
            ids.append(key)
    return ids


def main():
    results_path, vendor, pinned = sys.argv[1:4]
    results = json.load(open(results_path))
    manifest = json.load(open(os.path.join(vendor, "manifest.json")))
    schema = json.load(open(os.path.join(vendor, "schema", "results.schema.json")))
    errors = [e.message for e in Draft202012Validator(schema).iter_errors(results)]

    if results.get("conformance_version") != pinned:
        errors.append(f"conformance_version {results.get('conformance_version')!r} != pinned {pinned!r}")
    if sorted(results.get("bindings", [])) != BINDINGS:
        errors.append(f"bindings {results.get('bindings')} != {BINDINGS}")

    sections = results.get("sections", {})
    for name, meta in manifest["sections"].items():
        if meta["scope"] not in BINDINGS:
            if name in sections:
                errors.append(f"section {name!r} (scope {meta['scope']}) is out of scope and must be absent")
            continue
        if name not in sections:
            errors.append(f"in-scope section {name!r} is missing from results.json")
            continue
        missing = [i for i in case_ids(name, manifest.get(name)) if i not in sections[name]]
        if missing:
            errors.append(f"section {name!r} is missing cases: {', '.join(missing)}")
    for name in sections:
        if name not in manifest["sections"]:
            errors.append(f"section {name!r} is not defined by the manifest")

    for status in ("fail", "skip"):
        bad = [f"{s}/{i}" for s, cs in sections.items() for i, r in cs.items() if r.get("status") == status]
        if bad:
            errors.append(f"{status} cases: " + ", ".join(bad))

    if errors:
        print("results.json check FAILED:", file=sys.stderr)
        for e in errors:
            print(" - " + e, file=sys.stderr)
        return 1
    total = sum(len(cs) for cs in sections.values())
    print(f"results.json OK: {len(sections)} sections, {total} cases, bindings={results['bindings']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
