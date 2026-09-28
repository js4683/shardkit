#!/bin/bash
# Scripted manual handoff trace for M0-06, re-run against the M1
# library integration. Drives the ShardPlan through Off ->
# Active(10+cohort) -> Active(500) -> abort(Off), then runs the
# delayed-write experiment twice (guard on/off), then asserts the
# system is idle at rest. Every ownership phase diffs live widget
# status against hack/expect.py; the delayed-write phases scrape
# operator logs for late writes with exact-count verdicts (5 cohort
# widgets, 5 workers, so every cohort widget demonstrably straddles
# the flip). Raw artifacts land in /tmp/shardkit-trace-<ts>/; the
# script exits nonzero on any verdict failure. Uses ONLY the
# kind-shardkit-dev context.
#
# M1 note: the M0 prototype wrote through a raw client, so phase 6
# (guard off) reproduced the straddle hazard: 5 stable writes landed
# after canary's. The library integration writes through the guarded
# client, which re-checks ownership at the API boundary, so phase 6
# now asserts the fix: zero late stable writes with the re-check
# disabled. The canary positive control is now two counts: the 5
# cohort widgets plus the full handoff wave (all 21 widgets, each
# stamped exactly once — the M1 observer enqueues every gained
# object, where M0's touch-only triggering reached just the touched
# subset). Each delayed phase also settles its Off prestate before
# touching, so a re-stamp backlog aborts loudly instead of voiding
# the flip verdicts.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OP="$ROOT/examples/widget-operator"
CTX=kind-shardkit-dev
NS_SYS=widget-system
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT=/tmp/shardkit-trace-$TS
mkdir -p "$OUT"
echo "artifacts: $OUT"

k() { kubectl --context "$CTX" "$@"; }

ONLY=""
for i in $(seq 0 19); do ONLY="$ONLY,$(printf 'demo-%02d' "$i")"; done
ONLY="$ONLY,sandbox-a"
ONLY="${ONLY#,}"
# Five cohort widgets for five workers: every touched reconcile starts
# pre-flip and wakes post-flip, so verdict counts are exact, not bounds.
SUBSET="demo-00,demo-01,demo-02,demo-03,demo-04"
ONLY_N=$(echo "$ONLY" | tr ',' ' ' | wc -w | tr -d ' ')

fail() { echo "VERDICT FAIL: $1"; echo "$1" >>"$OUT/FAILURES"; }
pass() { echo "VERDICT PASS: $1"; }

plan_epoch() {
  k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{.spec.epoch}'
}

# set_plan <mode> <weight> <include-csv-or-empty> <rollout>: fresh
# rollout IDs on frozen-tuple change per spec V9; epoch auto-bumps.
set_plan() {
  local mode=$1 weight=$2 include_csv=$3 rollout=$4 epoch
  epoch=$(($(plan_epoch) + 1))
  local includes="[]"
  if [ -n "$include_csv" ]; then
    includes=$(printf '%s' "$include_csv" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().split(",")))')
  fi
  k -n "$NS_SYS" patch shardplan widget-operator --type=merge -p \
    "{\"spec\":{\"epoch\":$epoch,\"rollout\":\"$rollout\",\"canary\":{\"mode\":\"$mode\",\"weightPerMille\":$weight,\"include\":{\"namespaces\":$includes}}}}" >/dev/null
  echo "plan -> mode=$mode weight=$weight include=[$include_csv] rollout=$rollout epoch=$epoch"
}

touch_ns() { # csv namespaces -> annotate their widgets to force reconciles
  local csv=$1
  for ns in ${csv//,/ }; do
    k -n "$ns" annotate widgets --all "trace.shardkit.dev/$TS=$RANDOM" --overwrite >/dev/null
  done
}

snapshot() { # <label> -> fetch json artifacts
  local label=$1
  k -n "$NS_SYS" get shardplan widget-operator -o json >"$OUT/$label-plan.json"
  k get namespaces -o json >"$OUT/$label-namespaces.json"
  k get widgets -A -o json >"$OUT/$label-widgets.json"
}

# settle <label> <timeout-s>: poll until expect.py passes or timeout.
settle() {
  local label=$1 timeout=$2 waited=0
  while [ "$waited" -lt "$timeout" ]; do
    snapshot "$label"
    if python3 "$OP/hack/expect.py" --plan "$OUT/$label-plan.json" \
      --namespaces "$OUT/$label-namespaces.json" \
      --widgets "$OUT/$label-widgets.json" --only "$ONLY" >"$OUT/$label-expect.txt" 2>&1; then
      cat "$OUT/$label-expect.txt"
      return 0
    fi
    sleep 10
    waited=$((waited + 10))
  done
  cat "$OUT/$label-expect.txt"
  return 1
}

echo "=== phase 1: Off baseline (expect all stable) ==="
touch_ns "$ONLY"
if settle phase1-off 150; then pass "phase1 all stable at rest"; else fail "phase1 baseline"; fi

echo "=== phase 2: Active w=10 + cohort sandbox-a (fresh rollout r-002) ==="
set_plan Active 10 "sandbox-a" r-002
touch_ns "$ONLY"
if settle phase2-w10 150; then pass "phase2 w=10 distribution"; else fail "phase2 w=10"; fi

echo "=== phase 3: Active w=500 ==="
set_plan Active 500 "sandbox-a" r-002
touch_ns "$ONLY"
if settle phase3-w500 150; then pass "phase3 w=500 distribution"; else fail "phase3 w=500"; fi

echo "=== phase 4: abort to Off (include kept, must be ignored) ==="
set_plan Off 0 "sandbox-a" r-002
touch_ns "$ONLY"
if settle phase4-abort 150; then pass "phase4 abort returns all to stable"; else fail "phase4 abort"; fi

# late_writes <log> <flip-ts> <track> : print <track> WRITE lines at or
# after flip (second precision). The zap dev-mode message is a bare
# tab-delimited word (INFO\tWRITE\t{...}), NOT a quoted string.
late_writes() {
  python3 - "$1" "$2" "$3" <<'EOF'
import re, sys
logf, flip, track = sys.argv[1], sys.argv[2][:19], sys.argv[3]
n = 0
for line in open(logf, errors="replace"):
    if "\tWRITE\t" not in line:
        continue
    if f'"track": "{track}"' not in line and f'"track":"{track}"' not in line:
        continue
    ts = line.split()[0][:19]
    m = re.search(r'"widget":\s*"([^"]+)"', line)
    w = m.group(1) if m else "?"
    if ts >= flip:
        n += 1
        print(f"LATE {track} WRITE {ts} {w}")
print(f"late-{track}-writes: {n}")
EOF
}

# joined_counterexamples <stable-log> <canary-log> <flip-ts>: late stable
# writes for widgets the canary also wrote (true handoff overlap).
joined_counterexamples() {
  python3 - "$1" "$2" "$3" <<'EOF'
import re, sys
stablef, canaryf, flip = sys.argv[1], sys.argv[2], sys.argv[3][:19]
def writes(path, track):
    # Last WRITE per widget: canary logs span all phases, so only the
    # latest timestamp per widget is relevant to this flip. Bare
    # tab-delimited zap message (see late_writes).
    out = {}
    for line in open(path, errors="replace"):
        if "\tWRITE\t" not in line:
            continue
        if f'"track": "{track}"' not in line and f'"track":"{track}"' not in line:
            continue
        m = re.search(r'"widget":\s*"([^"]+)"', line)
        if m:
            out[m.group(1)] = line.split()[0][:19]
    return out
stable, canary = writes(stablef, "stable"), writes(canaryf, "canary")
n = 0
for w, sts in sorted(stable.items()):
    cts = canary.get(w)
    if sts >= flip and cts is not None and cts >= flip:
        n += 1
        print(f"OVERLAP {w} canary={cts} stable-late={sts}")
print(f"joined-overlaps: {n}")
EOF
}

suppressed_count() {
  grep -c 'SKIP guarded-after-delay' "$1" 2>/dev/null || true
}

# canary_widgets <log> <flip-ts> <subset-csv-or-empty>: print distinct
# widgets with a post-flip canary WRITE ("W <ns>/<name>" lines),
# optionally restricted to the subset namespaces. The M1 handoff wave
# re-stamps every gained widget, so the total is the full set while
# M0's touch-only triggering stamped just the touched subset.
canary_widgets() {
  python3 - "$1" "$2" "$3" <<'EOF'
import re, sys
logf, flip, subset = sys.argv[1], sys.argv[2][:19], sys.argv[3]
want = set(subset.split(",")) if subset else None
seen = set()
for line in open(logf, errors="replace"):
    if "\tWRITE\t" not in line:
        continue
    if '"track": "canary"' not in line and '"track":"canary"' not in line:
        continue
    if line.split()[0][:19] < flip:
        continue
    m = re.search(r'"widget":\s*"([^"]+)"', line)
    if not m:
        continue
    w = m.group(1)
    if want is not None and w.split("/")[0] not in want:
        continue
    if w not in seen:
        seen.add(w)
        print(f"W {w}")
print(f"widgets: {len(seen)}")
EOF
}

delayed_phase() { # <label> <guard-bool> <rollout>
  local label=$1 guard=$2 rollout=$3
  echo "=== $label: delayed-write, GUARD_WRITES=$guard ==="
  set_plan Off 0 "" "$rollout" # stable owns all; first wave will sleep
  sleep 10
  k -n "$NS_SYS" set env deploy/widget-stable WRITE_DELAY=15s GUARD_WRITES="$guard" >/dev/null
  k -n "$NS_SYS" rollout status deploy/widget-stable --timeout=180s >/dev/null
  # Drain the post-restart startup wave before touching: 21 first-wave
  # reconciles plus up to 5 status-triggered follow-ups, each sleeping
  # 15s over 5 workers (about 78s minimum), plus pod startup (cache
  # sync, lease acquisition). The touched wave needs idle workers.
  sleep 120
  # Verify the Off prestate (all stable) instead of assuming the
  # re-stamp wave finished: phase 6 enters all-canary, so its
  # re-stamps race the flip without this gate. Abort on failure:
  # flipping from a mixed prestate would void every later verdict.
  if ! settle "$label-prestate" 150; then
    fail "$label prestate never settled all-stable (re-stamp backlog?)"
    return 1
  fi
  pass "$label prestate all stable"
  touch_ns "$SUBSET"
  sleep 3
  FLIP_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  set_plan Active 1000 "" "$rollout"
  sleep 5
  touch_ns "$SUBSET" # post-flip wave so canary reconciles under new plan
  echo "waiting for stable first-wave wakeups..."
  sleep 60
  k -n "$NS_SYS" logs deploy/widget-stable --timestamps >"$OUT/$label-stable.log" 2>/dev/null || true
  k -n "$NS_SYS" logs deploy/widget-canary --timestamps >"$OUT/$label-canary.log" 2>/dev/null || true
  snapshot "$label"
  echo "flip-ts: $FLIP_TS"
}

delayed_phase phase5-guard-on true r-003
LATE5=$(late_writes "$OUT/phase5-guard-on-stable.log" "$FLIP_TS" stable | tee "$OUT/phase5-late.txt" | grep -c '^LATE' || true)
SUPP5=$(suppressed_count "$OUT/phase5-guard-on-stable.log")
CANARY5=$(late_writes "$OUT/phase5-guard-on-canary.log" "$FLIP_TS" canary | tee "$OUT/phase5-canary-postflip.txt" | grep -c '^LATE' || true)
SUBSET5=$(canary_widgets "$OUT/phase5-guard-on-canary.log" "$FLIP_TS" "$SUBSET" | tee "$OUT/phase5-canary-subset.txt" | grep -c '^W ' || true)
TOTAL5=$(canary_widgets "$OUT/phase5-guard-on-canary.log" "$FLIP_TS" "" | tee "$OUT/phase5-canary-total.txt" | grep -c '^W ' || true)
echo "phase5: late=$LATE5 suppressed=$SUPP5 subset=$SUBSET5 total=$TOTAL5 lines=$CANARY5 (expect 0/5/5/$ONLY_N/$ONLY_N)"
if [ "$LATE5" = "0" ] && [ "$SUPP5" = "5" ] && [ "$SUBSET5" = "5" ] && [ "$TOTAL5" = "$ONLY_N" ] && [ "$CANARY5" = "$TOTAL5" ] 2>/dev/null; then
  pass "phase5 guard suppressed all 5 straddlers; canary stamped the cohort once each plus the full handoff wave"
else
  fail "phase5 guard (late=$LATE5 suppressed=$SUPP5 subset=$SUBSET5 total=$TOTAL5 lines=$CANARY5)"
fi

delayed_phase phase6-guard-off false r-004
LATE6=$(late_writes "$OUT/phase6-guard-off-stable.log" "$FLIP_TS" stable | tee "$OUT/phase6-late.txt" | grep -c '^LATE' || true)
JOIN6=$(joined_counterexamples "$OUT/phase6-guard-off-stable.log" \
  "$OUT/phase6-guard-off-canary.log" "$FLIP_TS" | tee "$OUT/phase6-join.txt" | grep -c '^OVERLAP' || true)
CANARY6=$(late_writes "$OUT/phase6-guard-off-canary.log" "$FLIP_TS" canary | tee "$OUT/phase6-canary-postflip.txt" | grep -c '^LATE' || true)
SUBSET6=$(canary_widgets "$OUT/phase6-guard-off-canary.log" "$FLIP_TS" "$SUBSET" | tee "$OUT/phase6-canary-subset.txt" | grep -c '^W ' || true)
TOTAL6=$(canary_widgets "$OUT/phase6-guard-off-canary.log" "$FLIP_TS" "" | tee "$OUT/phase6-canary-total.txt" | grep -c '^W ' || true)
echo "phase6: late=$LATE6 joined=$JOIN6 subset=$SUBSET6 total=$TOTAL6 lines=$CANARY6 (expect 0/0/5/$ONLY_N/$ONLY_N)"
if [ "$LATE6" = "0" ] && [ "$JOIN6" = "0" ] && [ "$SUBSET6" = "5" ] && [ "$TOTAL6" = "$ONLY_N" ] && [ "$CANARY6" = "$TOTAL6" ] 2>/dev/null; then
  pass "phase6 guarded client fenced all 5 straddlers with the re-check off; canary stamped every gained widget once"
else
  fail "phase6 fence broken (late=$LATE6 joined=$JOIN6 subset=$SUBSET6 total=$TOTAL6 lines=$CANARY6)"
fi

echo "=== restore: guard on, no delay, Off, all stable ==="
k -n "$NS_SYS" set env deploy/widget-stable WRITE_DELAY=0s GUARD_WRITES=true >/dev/null
k -n "$NS_SYS" rollout status deploy/widget-stable --timeout=120s >/dev/null
set_plan Off 0 "" r-004
touch_ns "$ONLY"
if settle restore-off 150; then pass "restore all stable"; else fail "restore"; fi

echo "=== idle: no writes at rest (30s) ==="
k get widgets -A -o json >"$OUT/idle-before.json"
sleep 30
k get widgets -A -o json >"$OUT/idle-after.json"
if python3 - "$OUT/idle-before.json" "$OUT/idle-after.json" <<'EOF'
import json, sys
def sig(path):
    items = json.load(open(path))["items"]
    return {
        w["metadata"]["namespace"] + "/" + w["metadata"]["name"]: (
            (w.get("status") or {}).get("ownerTrack"),
            (w.get("status") or {}).get("ownerRevision"),
            (w.get("status") or {}).get("writes"),
            w["metadata"]["resourceVersion"],
        )
        for w in items
    }
before, after = sig(sys.argv[1]), sig(sys.argv[2])
if before == after:
    print(f"idle: {len(before)} widgets unchanged for 30s")
else:
    for k in sorted(set(before) | set(after)):
        if before.get(k) != after.get(k):
            print(f"BUSY {k} {before.get(k)} -> {after.get(k)}")
    sys.exit(1)
EOF
then
  pass "idle no writes at rest"
else
  fail "idle widgets changed at rest"
fi

k -n "$NS_SYS" logs deploy/widget-stable --timestamps >"$OUT/final-stable.log" 2>/dev/null || true
k -n "$NS_SYS" logs deploy/widget-canary --timestamps >"$OUT/final-canary.log" 2>/dev/null || true

echo "=== verdicts ==="
if [ -f "$OUT/FAILURES" ]; then
  cat "$OUT/FAILURES"
  echo "TRACE FAILED (artifacts: $OUT)"
  exit 1
fi
echo "TRACE PASSED (artifacts: $OUT)"
