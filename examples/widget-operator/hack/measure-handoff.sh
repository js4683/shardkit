#!/bin/bash
# M0-07 measurement: handoff convergence latency, operator memory with
# one vs two tracks, and API writes per handoff. Prototype scope (21
# widgets): convergence is measured from flip plus an explicit
# annotation touch, because the M0 gate has no HandoffSource to enqueue
# gained namespaces itself; the touch models the enqueue trigger M1
# provides. Memory is read from the container cgroup (no
# metrics-server install). Raw artifacts land in
# /tmp/shardkit-measure-<ts>/; the script exits nonzero on any
# convergence failure. Uses ONLY the kind-shardkit-dev context.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OP="$ROOT/examples/widget-operator"
CTX=kind-shardkit-dev
NS_SYS=widget-system
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT=/tmp/shardkit-measure-$TS
mkdir -p "$OUT"
echo "artifacts: $OUT"
FLIPS=10
ROLLOUT=m-007

k() { kubectl --context "$CTX" "$@"; }

ONLY=""
for i in $(seq 0 19); do ONLY="$ONLY,$(printf 'demo-%02d' "$i")"; done
ONLY="$ONLY,sandbox-a"
ONLY="${ONLY#,}"
LAT_A="$OUT/lat-active.txt"
LAT_O="$OUT/lat-off.txt"
: >"$LAT_A"
: >"$LAT_O"

fail() { echo "MEASURE FAIL: $1"; echo "$1" >>"$OUT/FAILURES"; }
pass() { echo "MEASURE PASS: $1"; }

plan_epoch() {
  k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{.spec.epoch}'
}

set_plan() { # <mode> <weight>: prints the new epoch on stdout
  local mode=$1 weight=$2 epoch
  epoch=$(($(plan_epoch) + 1))
  k -n "$NS_SYS" patch shardplan widget-operator --type=merge -p \
    "{\"spec\":{\"epoch\":$epoch,\"rollout\":\"$ROLLOUT\",\"canary\":{\"mode\":\"$mode\",\"weightPerMille\":$weight,\"include\":{\"namespaces\":[]}}}}" >/dev/null
  echo "plan -> mode=$mode weight=$weight epoch=$epoch" >&2
  echo "$epoch"
}

# One annotate call across all namespaces, so the enqueue trigger
# lands near-simultaneously (a per-namespace loop would dominate the
# measured latency).
touch_all() {
  k annotate widgets -A --all "measure.shardkit.dev/$TS=$RANDOM" --overwrite >/dev/null
}

# converge <label> <timeout-s> <out-file> <winner> <epoch>: flip is
# already applied; touch, then poll expect.py until the live statuses
# match. Records second-precision touch-to-converge plus a
# millisecond WRITE spread from the winner's log. WRITE lines are
# attributed by their logged epoch (immune to clock skew); the
# kubectl fractional timestamps carry 9 digits, which strptime %f
# cannot parse, so they are truncated to microseconds. touch_plus
# values are corrected by the measured mac-to-node skew.
converge() {
  local label=$1 timeout=$2 outfile=$3 winner=$4 epoch=$5 t0 t1
  touch_all
  local touch_epoch touch_since
  touch_epoch=$(python3 -c 'import time; print(time.time())')
  touch_since=$(python3 -c 'import time; print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 120)))')
  t0=$(date +%s)
  local waited=0
  while [ "$waited" -lt "$timeout" ]; do
    k -n "$NS_SYS" get shardplan widget-operator -o json >"$OUT/$label-plan.json"
    k get namespaces -o json >"$OUT/$label-namespaces.json"
    k get widgets -A -o json >"$OUT/$label-widgets.json"
    if python3 "$OP/hack/expect.py" --plan "$OUT/$label-plan.json" \
      --namespaces "$OUT/$label-namespaces.json" \
      --widgets "$OUT/$label-widgets.json" --only "$ONLY" >"$OUT/$label-expect.txt" 2>&1; then
      t1=$(date +%s)
      echo "$((t1 - t0))" >>"$outfile"
      echo "$label converged in $((t1 - t0))s"
      spread_out=$(
        k -n "$NS_SYS" logs deploy/widget-$winner --since-time="$touch_since" --timestamps 2>/dev/null | \
          python3 -c 'import re, sys, datetime
epoch, touch, skew_ms = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
touch += skew_ms / 1000.0
first = last = None
n = 0
for line in sys.stdin:
    if "\tWRITE\t" not in line:
        continue
    m = re.search(r"\"epoch\":\s*(\d+)", line)
    if not m or m.group(1) != epoch:
        continue
    ts = re.sub(r"(\.\d{6})\d*Z$", r"\1Z", line.split()[0])
    try:
        t = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        continue
    n += 1
    if first is None:
        first = t
    last = t
if first is None:
    print("writes=0")
else:
    print(f"writes={n} first_plus={(first-touch)*1000:.0f}ms last_plus={(last-touch)*1000:.0f}ms spread={(last-first)*1000:.0f}ms")
' "$epoch" "$touch_epoch" "$SKEW_MS" || echo "spread-unavailable"
      )
      echo "$spread_out" | tee "$OUT/$label-spread.txt"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  cat "$OUT/$label-expect.txt"
  return 1
}

# mem_sample <label>: cgroup current + anon bytes per track pod.
mem_sample() {
  local label=$1
  for track in stable canary; do
    if ! k -n "$NS_SYS" get deploy/widget-$track -o jsonpath='{.spec.replicas}' 2>/dev/null | grep -qx 1; then
      echo "$label $track scaled-to-zero" >>"$OUT/mem.txt"
      continue
    fi
    local cur anon
    cur=$(k -n "$NS_SYS" exec deploy/widget-$track -- cat /sys/fs/cgroup/memory.current)
    anon=$(k -n "$NS_SYS" exec deploy/widget-$track -- cat /sys/fs/cgroup/memory.stat | awk '$1=="anon"{print $2}')
    echo "$label $track current=$cur anon=$anon" >>"$OUT/mem.txt"
  done
  tail -n 2 "$OUT/mem.txt"
}

stats() { # <file>: min median p95 max over integer seconds
  python3 - "$1" <<'EOF'
import sys
vals = sorted(int(l) for l in open(sys.argv[1]) if l.strip())
n = len(vals)
if n == 0:
    print("n=0 (no converged flips)")
    sys.exit(0)
def pct(p):
    return vals[min(n - 1, int(n * p / 100))]
print(f"n={n} min={vals[0]} median={vals[n//2]} p95={pct(95)} max={vals[-1]} vals={vals}")
EOF
}

echo "=== versions ==="
(go version; kind version; kubectl version --client=true) | tee "$OUT/versions.txt"
k -n "$NS_SYS" get pods -o wide | tee "$OUT/pods-before.txt"
echo "=== clock skew (kind node minus mac, ms) ==="
SKEW_MS=0
SKEW_NOTE="unmeasured"
if docker exec shardkit-dev-control-plane true 2>/dev/null; then
  NODE_TS=$(docker exec shardkit-dev-control-plane date +%s.%N)
  MAC_TS=$(python3 -c 'import time; print(time.time())')
  SKEW_MS=$(python3 -c "print(int(($NODE_TS - $MAC_TS) * 1000))")
  SKEW_NOTE="measured"
fi
echo "skew_ms=$SKEW_MS $SKEW_NOTE" | tee "$OUT/skew.txt"

echo "=== A: memory, two tracks ==="
: >"$OUT/mem.txt"
for i in 1 2 3; do mem_sample "dual-$i"; sleep 10; done

echo "=== B: handoff latency ($FLIPS flips each way) ==="
for i in $(seq 1 "$FLIPS"); do
  EPOCH=$(set_plan Active 1000)
  if converge "flip-$i-active" 120 "$LAT_A" canary "$EPOCH"; then pass "flip $i to Active"; else fail "flip $i to Active"; fi
  EPOCH=$(set_plan Off 0)
  if converge "flip-$i-off" 120 "$LAT_O" stable "$EPOCH"; then pass "flip $i to Off"; else fail "flip $i to Off"; fi
done
echo "to-Active:   $(stats "$LAT_A")"
echo "to-Off:      $(stats "$LAT_O")"

echo "=== C: memory, single track (canary scaled to 0) ==="
k -n "$NS_SYS" scale deploy/widget-canary --replicas=0 >/dev/null
k -n "$NS_SYS" rollout status deploy/widget-canary --timeout=120s >/dev/null
sleep 15
for i in 1 2 3; do mem_sample "single-$i"; sleep 10; done
k -n "$NS_SYS" scale deploy/widget-canary --replicas=1 >/dev/null
k -n "$NS_SYS" rollout status deploy/widget-canary --timeout=180s >/dev/null
echo "waiting for restarted canary to report the current epoch..."
for _ in $(seq 1 12); do
  ready=$(k -n "$NS_SYS" get shardplan widget-operator -o json | python3 -c 'import json, sys
d = json.load(sys.stdin)
c = [t for t in d["status"].get("tracks", []) if t["name"] == "canary"]
print("ready" if c and c[0].get("observedEpoch") == d["spec"]["epoch"] else "wait")')
  [ "$ready" = "ready" ] && break
  sleep 5
done
echo "canary: $ready"

echo "=== D: writes per full handoff (log lines by logged epoch) ==="
# Each successful widget status write logs exactly one WRITE line
# carrying its observed epoch, so per-epoch line counts are exact API
# write counts. (Wall-time windows bled across flips: a wave landing
# early in a wall-clock second falls inside the next flip's
# --since-time floor. Widget resourceVersions cannot serve either:
# the annotation touch itself bumps them.)
winner_check() { # <track> <epoch>: exactly 21 WRITE lines, 21 unique widgets
  local track=$1 epoch=$2
  k -n "$NS_SYS" logs deploy/widget-$track --timestamps 2>/dev/null | python3 -c '
import re, sys
epoch = sys.argv[1]
n = 0
widgets = set()
for line in sys.stdin:
    if "\tWRITE\t" not in line:
        continue
    m = re.search(r"\"epoch\":\s*(\d+)", line)
    if not m or m.group(1) != epoch:
        continue
    n += 1
    w = re.search(r"\"widget\":\s*\"([^\"]+)\"", line)
    if w:
        widgets.add(w.group(1))
print(f"epoch {epoch}: {n} writes {len(widgets)} unique")
sys.exit(0 if (n, len(widgets)) == (21, 21) else 1)
' "$epoch"
}
loser_check() { # <track> <epoch> <prev>: zero current-epoch WRITEs and
  # exactly 21 prior-epoch WRITEs (any stale-cache write would add lines)
  local track=$1 epoch=$2 prev=$3
  k -n "$NS_SYS" logs deploy/widget-$track --timestamps 2>/dev/null | python3 -c '
import re, sys
epoch, prev = sys.argv[1], sys.argv[2]
cur = old = 0
for line in sys.stdin:
    if "\tWRITE\t" not in line:
        continue
    m = re.search(r"\"epoch\":\s*(\d+)", line)
    if not m:
        continue
    if m.group(1) == epoch:
        cur += 1
    elif m.group(1) == prev:
        old += 1
print(f"epoch {epoch}: {cur} writes; epoch {prev}: {old} writes")
sys.exit(0 if (cur, old) == (0, 21) else 1)
' "$epoch" "$prev"
}
EPOCH=$(set_plan Active 1000)
if converge "w-active" 120 /dev/null canary "$EPOCH"; then
  if winner_check canary "$EPOCH" | tee "$OUT/w-to-active.txt" && loser_check stable "$EPOCH" "$((EPOCH - 1))" | tee "$OUT/s-to-active.txt"; then
    pass "to-Active: canary wrote each widget exactly once, stable silent"
  else
    fail "to-Active write count"
  fi
else
  fail "write-count flip to Active"
fi
EPOCH=$(set_plan Off 0)
if converge "w-off" 120 /dev/null stable "$EPOCH"; then
  if winner_check stable "$EPOCH" | tee "$OUT/w-to-off.txt" && loser_check canary "$EPOCH" "$((EPOCH - 1))" | tee "$OUT/s-to-off.txt"; then
    pass "to-Off: stable wrote each widget exactly once, canary silent"
  else
    fail "to-Off write count"
  fi
else
  fail "write-count flip to Off"
fi

echo "=== restarts (must be zero: a restart would void memory samples) ==="
k -n "$NS_SYS" get pods -o jsonpath='{range .items[*]}{.metadata.name} restarts={.status.containerStatuses[0].restartCount}{"\n"}{end}' | tee "$OUT/restarts.txt"
if grep -qv 'restarts=0' "$OUT/restarts.txt"; then fail "pod restarts observed"; else pass "no pod restarts"; fi

echo "=== verdicts ==="
if [ -f "$OUT/FAILURES" ]; then
  cat "$OUT/FAILURES"
  echo "MEASURE FAILED (artifacts: $OUT)"
  exit 1
fi
echo "latency to-Active: $(stats "$LAT_A")"
echo "latency to-Off:    $(stats "$LAT_O")"
echo "MEASURE PASSED (artifacts: $OUT)"
