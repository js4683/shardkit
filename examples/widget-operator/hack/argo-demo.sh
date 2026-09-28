#!/usr/bin/env bash
# M2 Argo demo driver: good path (1->5->25->50->100% via the
# js4683/shardkit traffic-router plugin) and chaos path (template
# change -> hash rotation -> CHAOS=mass-delete on canary ->
# error-rate analysis fails -> abort -> Off/0 -> CLI recovery).
#
# Usage: ./hack/argo-demo.sh good|chaos [--context NAME]
# The good path ends with canary owning everything (Active/1000);
# the chaos path ends back at the Off rest state (stable owns all).
set -euo pipefail

cd "$(dirname "$0")/../../.."

MODE="${1:-}"; shift || true
CONTEXT="kind-shardkit-dev"
while (($# > 0)); do
	case "$1" in
		--context) CONTEXT="$2"; shift 2 ;;
		*) echo "usage: $0 good|chaos [--context NAME]" >&2; exit 2 ;;
	esac
done
case "$MODE" in good|chaos) ;; *) echo "usage: $0 good|chaos" >&2; exit 2 ;; esac

NS="widget-system"
PLAN="widget-operator"
ROLLOUT="widget-demo"
OP="examples/widget-operator"
TRACE="${TRACE_DIR:-/tmp/shardkit-argo-$MODE}"
mkdir -p "$TRACE"

k() { kubectl --context "$CONTEXT" "$@"; }
log() { printf '\n### %s\n' "$*" | tee -a "$TRACE/driver.log"; }

plan_field() { k -n "$NS" get shardplan "$PLAN" -o "jsonpath={$1}"; }

# wait_plan_epoch waits until the live spec epoch exceeds $1.
wait_epoch() {
	local base="$1" label="$2" deadline=$((SECONDS + 600)) cur
	while ((SECONDS < deadline)); do
		cur="$(plan_field '.spec.epoch')"
		if ((cur > base)); then echo "$cur"; return 0; fi
		sleep 5
	done
	echo "TIMEOUT $label (epoch still $cur, want > $base)" >&2; return 1
}

# wait_canary_ack waits until the canary status entry acks the
# live (epoch, generation, revision).
wait_canary_ack() {
	local deadline=$((SECONDS + 600)) out
	while ((SECONDS < deadline)); do
		out="$(k -n "$NS" get shardplan "$PLAN" -o json)"
		if python3 - "$out" <<'EOF'
import json,sys
plan=json.loads(sys.argv[1])
e=[t for t in plan.get("status",{}).get("tracks",[]) if t["name"]=="canary"]
ok = bool(e) and e[0].get("planUID")==plan["metadata"]["uid"] \
  and e[0].get("observedEpoch")==plan["spec"]["epoch"] \
  and e[0].get("observedGeneration")==plan["metadata"]["generation"] \
  and e[0].get("revision")==((plan["spec"].get("tracks") or {}).get("canary") or {}).get("revision")
sys.exit(0 if ok else 1)
EOF
		then return 0; fi
		sleep 5
	done
	echo "TIMEOUT waiting for canary ack" >&2; return 1
}

rollout_phase() { k -n "$NS" get rollout "$ROLLOUT" -o jsonpath='{.status.phase}'; }

# sync_revision binds the canary operator's REVISION to the live
# spec revision (S7). UpdateHash rotations change the spec under
# us; until the operator follows, the canary is fenced and
# VerifyWeight stays NotVerified by design. No-op when in sync.
sync_revision() {
	local want have
	want="$(plan_field '.spec.tracks.canary.revision')"
	have="$(k -n "$NS" get deploy/widget-canary -o "jsonpath={.spec.template.spec.containers[0].env[?(@.name=='REVISION')].value}")"
	if [[ "$want" == "$have" ]]; then return 0; fi
	log "revision drift spec=$want deploy=${have:-unset}; syncing operator"
	k -n "$NS" set env deploy/widget-canary "REVISION=$want" | tee -a "$TRACE/driver.log"
	k -n "$NS" rollout status deploy/widget-canary --timeout=180s | tee -a "$TRACE/driver.log"
	log "waiting for canary ack after revision sync"
	wait_canary_ack
}

# assert_quiet proves steady-state quiescence: at most one spec
# epoch over the window. The M2 oscillation bug wrote ~1 epoch per
# 5s on a completed rollout; any recurrence fails loudly instead
# of faking a clean trace.
assert_quiet() {
	local label="$1" e0 e1
	e0="$(plan_field '.spec.epoch')"
	sleep 45
	e1="$(plan_field '.spec.epoch')"
	log "quiet check ($label): epoch $e0 -> $e1"
	if ((e1 - e0 > 1)); then
		echo "CHURN: $((e1 - e0)) epochs in 45s ($label)" >&2
		return 1
	fi
}

if [[ "$MODE" == "good" ]]; then
	log "apply templates + services + rollout"
	k -n "$NS" apply -f "$OP/config/analysis/error-rate.yaml" | tee -a "$TRACE/driver.log"
	k -n "$NS" apply -f "$OP/config/analysis/handshake-degraded.yaml" | tee -a "$TRACE/driver.log"
	k apply -f "$OP/config/argo/rollout.yaml" | tee -a "$TRACE/driver.log"

	ROLLOUT_UID="$(k -n "$NS" get rollout "$ROLLOUT" -o jsonpath='{.metadata.uid}')"
	bound="$(plan_field '.spec.rollout')"
	if [[ "$bound" != "$ROLLOUT_UID" ]]; then
		# Pause before binding so no step fires pre-bind (a paused
		# rollout never syncs plugin hashes; resume after).
		k -n "$NS" patch rollout "$ROLLOUT" --type=merge -p '{"spec":{"paused":true}}' >/dev/null
		log "rollout UID=$ROLLOUT_UID; binding plan"
		k -n "$NS" patch shardplan "$PLAN" --type=merge \
			-p "{\"spec\":{\"rollout\":\"$ROLLOUT_UID\"}}" | tee -a "$TRACE/driver.log"
		base="$(plan_field '.spec.epoch')"
		k -n "$NS" patch rollout "$ROLLOUT" --type=merge -p '{"spec":{"paused":false}}' >/dev/null
		wait_epoch "$base" 'first plugin write' >/dev/null
	fi
	k -n "$NS" get shardplan "$PLAN" -o yaml >"$TRACE/plan-bound.yaml"

	# A rollout whose template matches stable is fully promoted
	# from the first sync: steps never execute. A timestamped
	# label guarantees a new canary hash and a real progression.
	assert_quiet "pre-rollout steady state"
	tag="run-$(date -u +%Y%m%dT%H%M%SZ)"
	log "template change (stub-gen=$tag) -> real rollout"
	k -n "$NS" patch rollout "$ROLLOUT" --type=merge -p \
		"{\"spec\":{\"template\":{\"metadata\":{\"labels\":{\"app\":\"widget-demo\",\"stub-gen\":\"$tag\"}}}}}" | tee -a "$TRACE/driver.log"

	base="$(plan_field '.spec.epoch')"
	log "waiting for UpdateHash rotation (epoch > $base)"
	new_epoch="$(wait_epoch "$base" 'UpdateHash rotation')"
	if [[ "$(plan_field '.spec.tracks.stable.revision')" != "rev-a" ]]; then
		echo "STABLE REVISION MOVED: $(plan_field '.spec.tracks.stable.revision') (want rev-a)" >&2
		return 1
	fi
	log "rotation clean: stable still rev-a"
	sync_revision

	log "driving 1->5->25->50->100 with revision sync"
	deadline=$((SECONDS + 1500))
	while ((SECONDS < deadline)); do
		sync_revision
		phase="$(rollout_phase)"
		step="$(k -n "$NS" get rollout "$ROLLOUT" -o jsonpath='{.status.currentStepIndex}')"
		pe="$(plan_field '.spec.epoch')"; pw="$(plan_field '.spec.canary.weightPerMille')"
		co="$(k -n "$NS" get shardplan "$PLAN" -o jsonpath='{range .status.tracks[?(@.name=="canary")]}{.ownedNamespaces}{end}')"
		nr="$(k -n "$NS" get analysisrun --no-headers 2>/dev/null | wc -l)"
		printf '%s phase=%s step=%s planEpoch=%s weight=%s canaryOwned=%s analysisruns=%s\n' \
			"$(date -u +%H:%M:%S)" "$phase" "$step" "$pe" "$pw" "${co:-0}" "$nr" | tee -a "$TRACE/progress.log"
		if [[ "$phase" == "Healthy" ]]; then break; fi
		if [[ "$phase" == "Degraded" ]]; then
			echo "rollout degraded (unexpected in good path)" >&2; break
		fi
		sleep 20
	done

	assert_quiet "post-promotion steady state"
	log "capturing final state"
	k -n "$NS" describe rollout "$ROLLOUT" >"$TRACE/rollout-describe.txt"
	k -n "$NS" get analysisrun -o yaml >"$TRACE/analysisruns.yaml"
	k -n "$NS" get analysisrun --no-headers | tee -a "$TRACE/driver.log"
	k -n "$NS" get shardplan "$PLAN" -o yaml >"$TRACE/plan-final.yaml"
	k -n "$NS" get rollout "$ROLLOUT" -o yaml >"$TRACE/rollout-final.yaml"
	echo "good-path done: phase=$(rollout_phase)"
fi

if [[ "$MODE" == "chaos" ]]; then
	phase="$(rollout_phase)"
	[[ "$phase" == "Healthy" ]] || { echo "need Healthy rollout first (got $phase)" >&2; exit 1; }
	assert_quiet "pre-chaos steady state"
	tag="chaos-$(date -u +%Y%m%dT%H%M%SZ)"
	log "template change (stub-gen=$tag) -> new canary hash"
	# NOTE: Rollouts are CRs (JSON merge patch): the labels map is
	# replaced whole, so the selector label must be repeated.
	k -n "$NS" patch rollout "$ROLLOUT" --type=merge -p \
		"{\"spec\":{\"template\":{\"metadata\":{\"labels\":{\"app\":\"widget-demo\",\"stub-gen\":\"$tag\"}}}}}" | tee -a "$TRACE/driver.log"

	base="$(plan_field '.spec.epoch')"
	log "waiting for UpdateHash rotation (epoch > $base)"
	new_epoch="$(wait_epoch "$base" 'UpdateHash rotation')"
	if [[ "$(plan_field '.spec.tracks.stable.revision')" != "rev-a" ]]; then
		echo "STABLE REVISION MOVED: $(plan_field '.spec.tracks.stable.revision') (want rev-a)" >&2
		return 1
	fi
	hash="$(plan_field '.spec.tracks.canary.revision')"
	log "rotated: epoch=$new_epoch canaryRevision=$hash"
	sync_revision

	log "waiting for weight 250 (25% step), then inject CHAOS"
	deadline=$((SECONDS + 900))
	while ((SECONDS < deadline)); do
		pw="$(plan_field '.spec.canary.weightPerMille')"
		printf '%s weight=%s\n' "$(date -u +%H:%M:%S)" "$pw" | tee -a "$TRACE/progress.log"
		if ((pw >= 250)); then break; fi
		sleep 10
	done
	log "inject CHAOS=mass-delete on canary"
	k -n "$NS" set env deploy/widget-canary CHAOS=mass-delete | tee -a "$TRACE/driver.log"
	k -n "$NS" rollout status deploy/widget-canary --timeout=120s >/dev/null

	log "waiting for abort (plan Off/0)"
	deadline=$((SECONDS + 900))
	while ((SECONDS < deadline)); do
		phase="$(rollout_phase)"
		pm="$(plan_field '.spec.canary.mode')"; pw="$(plan_field '.spec.canary.weightPerMille')"
		printf '%s phase=%s mode=%s weight=%s\n' "$(date -u +%H:%M:%S)" "$phase" "$pm" "$pw" | tee -a "$TRACE/progress.log"
		if [[ "$pm" == "Off" && "$pw" == "0" ]]; then break; fi
		sleep 10
	done

	log "capturing abort state"
	k -n "$NS" describe rollout "$ROLLOUT" >"$TRACE/rollout-abort.txt"
	k -n "$NS" get analysisrun -o yaml >"$TRACE/analysisruns-abort.yaml"
	k -n "$NS" get shardplan "$PLAN" -o yaml >"$TRACE/plan-abort.yaml"

	log "recover: clear CHAOS, reseed deleted widgets, CLI abort, wait sync"
	k -n "$NS" set env deploy/widget-canary CHAOS- | tee -a "$TRACE/driver.log"
	k -n "$NS" rollout status deploy/widget-canary --timeout=180s >/dev/null
	missing=0
	for ns in $(k get ns -o "jsonpath={range .items[*]}{.metadata.name}{'\n'}{end}" | grep -E '^demo-[0-9]+$'; echo sandbox-a); do
		if ! k -n "$ns" get widget w0 >/dev/null 2>&1; then
			k apply -f - >/dev/null <<EOF
apiVersion: shardkit.dev/v1alpha1
kind: Widget
metadata: {name: w0, namespace: $ns}
spec: {value: "$ns"}
EOF
			missing=$((missing + 1))
		fi
	done
	echo "reseeded $missing widgets" | tee -a "$TRACE/driver.log"
	go build -o "$TRACE/kubectl-shardplan" ./cmd/kubectl-shardplan
	"$TRACE/kubectl-shardplan" abort "$PLAN" --context "$CONTEXT" -n "$NS" | tee -a "$TRACE/driver.log"
	deadline=$((SECONDS + 300))
	while ((SECONDS < deadline)); do
		if "$TRACE/kubectl-shardplan" status "$PLAN" --context "$CONTEXT" -n "$NS" 2>/dev/null | grep -q '^verdict: in sync'; then
			echo "re-converged at rest" | tee -a "$TRACE/driver.log"; break
		fi
		sleep 5
	done
	"$TRACE/kubectl-shardplan" status "$PLAN" --context "$CONTEXT" -n "$NS" | tee -a "$TRACE/driver.log"
	k -n "$NS" get shardplan "$PLAN" -o yaml >"$TRACE/plan-rest.yaml"
	echo "chaos-path done"
fi
