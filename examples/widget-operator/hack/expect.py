#!/usr/bin/env python3
"""expected-vs-observed ownership check for the M0-06 trace.

Implements the partition spec (docs/partition-spec.md) independently in
Python -- the same implementation that produced the golden vectors -- and
diffs it against live widget statuses.

Inputs (files): --plan (shardplan json), --namespaces (namespace list
json), --widgets (widget list json). Prints a per-namespace table and
exits nonzero on any mismatch. Namespaces without widgets are checked
against the window (reported, never a failure: no writes to observe).
"""

import argparse
import json
import sys

MASK = 0xFFFFFFFFFFFFFFFF
BASIS = 14695981039346656037
PRIME = 1099511628211


def fnv1a64(data: bytes) -> int:
    h = BASIS
    for b in data:
        h ^= b
        h = (h * PRIME) & MASK
    return h


def bucket(name: str) -> int:
    return fnv1a64(name.encode()) % 1000


def expected_owner(spec, namespaces):
    mode = spec["canary"]["mode"]
    weight = spec["canary"]["weightPerMille"]
    include = set(spec["canary"].get("include", {}).get("namespaces", []) or [])
    excludes = (spec["canary"].get("exclude", {}).get("selector", {}) or {}).get(
        "matchLabels", {}
    ) or {}
    offset = fnv1a64(spec["seed"].encode()) % 1000
    out = {}
    for ns in namespaces:
        name = ns["metadata"]["name"]
        labels = ns["metadata"].get("labels", {}) or {}
        if mode in ("Off", "Shadow"):
            out[name] = ("stable", "mode-" + mode)
        elif all(labels.get(k) == v for k, v in excludes.items()) and excludes:
            out[name] = ("stable", "excluded")
        elif name in include:
            out[name] = ("canary", "included")
        elif (bucket(name) - offset) % 1000 < weight:
            out[name] = ("canary", f"bucket-{bucket(name)}")
        else:
            out[name] = ("stable", f"bucket-{bucket(name)}")
    return out, offset


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--namespaces", required=True)
    ap.add_argument("--widgets", required=True)
    ap.add_argument("--only", default="", help="comma-separated namespaces to check")
    args = ap.parse_args()

    plan = json.load(open(args.plan))["spec"]
    namespaces = json.load(open(args.namespaces))["items"]
    widgets = json.load(open(args.widgets))["items"]
    only = set(filter(None, args.only.split(",")))

    want, offset = expected_owner(plan, namespaces)
    observed = {}
    for w in widgets:
        ns = w["metadata"]["namespace"]
        observed.setdefault(ns, []).append(
            (
                w["metadata"]["name"],
                (w.get("status", {}) or {}).get("ownerTrack", ""),
                (w.get("status", {}) or {}).get("ownerRevision", ""),
                (w.get("status", {}) or {}).get("writes", 0),
            )
        )

    print(
        f"mode={plan['canary']['mode']} weight={plan['canary']['weightPerMille']} "
        f"seed={plan['seed']} offset={offset} epoch={plan['epoch']}"
    )
    failures = 0
    for ns in sorted(want):
        if only and ns not in only:
            continue
        exp, reason = want[ns]
        got = observed.get(ns, [])
        if not got:
            print(f"{ns:24} want={exp:6} ({reason:14}) observed=<no widgets>")
            continue
        tracks = {t for _, t, _, _ in got}
        ok = tracks == {exp}
        if not ok:
            failures += 1
        mark = "OK " if ok else "FAIL"
        detail = ",".join(f"{n}:{t}/{r}#{c}" for n, t, r, c in sorted(got))
        print(f"{mark} {ns:20} want={exp:6} ({reason:14}) observed=[{detail}]")
    print(f"mismatches: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
