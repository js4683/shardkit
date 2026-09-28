#!/usr/bin/env bash
# CLI recovery demo (M1 item 6): drive the widget-operator ShardPlan on
# kind with only the kubectl-shardplan CLI — preview a rollout,
# start it, watch the handshake converge, explain one namespace,
# then abort (recover) and watch both tracks ack the abort epoch.
# Ends at the rest state (Off), so re-running is safe; aborting an
# already-aborted plan is a no-op.
#
# Usage: ./hack/demo.sh [--context NAME] [-n NAMESPACE] [PLAN]
# Env: SHARDPLAN_CLI overrides the CLI binary (default: build it).
set -euo pipefail

cd "$(dirname "$0")/../../.."

CONTEXT="kind-shardkit-dev"
NS="widget-system"
PLAN="widget-operator"
while (($# > 0)); do
	case "$1" in
		--context) CONTEXT="$2"; shift 2 ;;
		-n|--namespace) NS="$2"; shift 2 ;;
		-*) echo "usage: $0 [--context NAME] [-n NAMESPACE] [PLAN]" >&2; exit 2 ;;
		*) PLAN="$1"; shift ;;
	esac
done

if [[ -z "${SHARDPLAN_CLI:-}" ]]; then
	SHARDPLAN_CLI="$(mktemp -d)/kubectl-shardplan"
	go build -o "$SHARDPLAN_CLI" ./cmd/kubectl-shardplan
fi
CTX=(--context "$CONTEXT" -n "$NS")

say() { printf '\n### %s\n' "$*"; }

# wait_sync polls status until the handshake verdict is in sync.
wait_sync() {
	local label="$1" out verdict
	local deadline=$((SECONDS + 180))
	while ((SECONDS < deadline)); do
		if out="$("$SHARDPLAN_CLI" status "$PLAN" "${CTX[@]}" 2>&1)"; then
			verdict="$(echo "$out" | grep '^verdict:' || true)"
			echo "$verdict"
			if echo "$verdict" | grep -q '^verdict: in sync'; then
				echo "--- $label converged"
				return 0
			fi
		else
			echo "(status exit 1, retrying)"
		fi
		sleep 3
	done
	echo "TIMEOUT waiting for in-sync ($label); last status:" >&2
	echo "$out" >&2
	return 1
}

say "1/6 baseline status"
"$SHARDPLAN_CLI" status "$PLAN" "${CTX[@]}"

say "2/6 simulate the rollout (read-only, no writes made)"
"$SHARDPLAN_CLI" simulate "$PLAN" --weight 500 --mode Active -q "${CTX[@]}"

say "3/6 start the rollout: set-weight 500"
"$SHARDPLAN_CLI" set-weight "$PLAN" 500 --mode Active "${CTX[@]}"
wait_sync "rollout at weight 500"

say "4/6 explain one namespace"
"$SHARDPLAN_CLI" explain "$PLAN" demo-12 "${CTX[@]}"

say "5/6 recover: abort the rollout"
"$SHARDPLAN_CLI" abort "$PLAN" "${CTX[@]}"
wait_sync "abort (Off)"

say "6/6 final status (rest state)"
"$SHARDPLAN_CLI" status "$PLAN" "${CTX[@]}"
echo
echo "demo ok: rollout started, converged, explained, aborted, re-converged"
