#!/bin/bash
# Install Argo Rollouts plus the baked shardkit plugin on kind
# (M2 install, M4: image v0.2.3, probe ClusterRole). Captures the
# previously-imperative install so reinstalls are reproducible:
# upstream manifests, plugin ConfigMap, ShardPlan RBAC (namespaced
# writes + cluster read-only probe grant), custom controller
# image, smoke test. Safe to re-run; uses ONLY kind-shardkit-dev.
# Env: PLUGIN_VERSION (default v0.2.3), UPSTREAM_MANIFEST (default
# Argo Rollouts v1.10.0 install.yaml).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OP="$ROOT/examples/widget-operator"
CLUSTER=shardkit-dev
CTX="kind-$CLUSTER"
NS_ARGO=argo-rollouts
PLUGIN_VERSION="${PLUGIN_VERSION:-v0.2.3}"
UPSTREAM_MANIFEST="${UPSTREAM_MANIFEST:-https://github.com/argoproj/argo-rollouts/releases/download/v1.10.0/install.yaml}"

k() { kubectl --context "$CTX" "$@"; }

# Server-side: the upstream CRDs exceed the last-applied-configuration
# annotation limit, so client-side apply fails on re-install.
echo "==> upstream Argo Rollouts v1.10.0"
k apply --server-side -f "$UPSTREAM_MANIFEST"
k -n "$NS_ARGO" wait --for=condition=Available deploy/argo-rollouts --timeout=300s

echo "==> build plugin $PLUGIN_VERSION"
cd "$ROOT"
export GOOS=linux GOARCH=arm64 CGO_ENABLED=0
go build -ldflags "-X main.version=$PLUGIN_VERSION" \
  -o plugins/argo-rollouts/shardkit-plugin ./plugins/argo-rollouts
unset GOOS GOARCH CGO_ENABLED
cd "$OP"
docker build -q -t "shardkit-argo-rollouts:$PLUGIN_VERSION" \
  -f "$ROOT/plugins/argo-rollouts/Dockerfile" "$ROOT"
kind load docker-image "shardkit-argo-rollouts:$PLUGIN_VERSION" --name "$CLUSTER"

echo "==> plugin ConfigMap + ShardPlan RBAC"
k apply -f "$OP/config/argo/argo-rollouts-config.yaml"
k apply -f "$OP/config/argo/shardplan-rbac.yaml"

echo "==> controller image"
k -n "$NS_ARGO" set image deploy/argo-rollouts \
  "argo-rollouts=shardkit-argo-rollouts:$PLUGIN_VERSION"
k -n "$NS_ARGO" rollout status deploy/argo-rollouts --timeout=180s

echo "==> smoke: probe grant + clean init"
k auth can-i list shardplans.shardkit.dev \
  --as=system:serviceaccount:argo-rollouts:argo-rollouts --all-namespaces
sleep 45
if k -n "$NS_ARGO" logs deploy/argo-rollouts --since=60s 2>/dev/null | grep -q "level=error"; then
  echo "controller errors since install:" >&2
  k -n "$NS_ARGO" logs deploy/argo-rollouts --since=60s 2>/dev/null | grep "level=error" | head -3 >&2
  exit 1
fi
echo "install ok: $PLUGIN_VERSION serving, init clean"
