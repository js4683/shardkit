#!/bin/bash
# Bring up the M0-06 prototype: build images, create the dedicated
# kind cluster, load, deploy, seed demo namespaces/widgets, smoke test.
# Safe to re-run: cluster creation and seeding are idempotent.
# Uses ONLY the kind-shardkit-dev context; never touches the current one.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OP="$ROOT/examples/widget-operator"
CLUSTER=shardkit-dev
CTX="kind-$CLUSTER"
KUBECTL="kubectl --context $CTX"
NS_SYS=widget-system
DEMO_N=20

k() { kubectl --context "$CTX" "$@"; }

echo "==> versions"
go version
kind version
kubectl version --client=true

echo "==> build linux/arm64 binary"
cd "$ROOT"
export GOOS=linux GOARCH=arm64 CGO_ENABLED=0
go build -o "$OP/widget-operator" ./examples/widget-operator
unset GOOS GOARCH CGO_ENABLED

echo "==> build images rev-a rev-b"
cd "$OP"
docker build -q --build-arg REVISION=rev-a -t shardkit-widget:rev-a -f Dockerfile .
docker build -q --build-arg REVISION=rev-b -t shardkit-widget:rev-b -f Dockerfile .

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "==> cluster $CLUSTER exists, reusing"
else
  echo "==> create cluster $CLUSTER (kindest/node:v1.36.4)"
  kind create cluster --name "$CLUSTER" --image kindest/node:v1.36.4 --wait 5m
fi

echo "==> load images"
kind load docker-image shardkit-widget:rev-a shardkit-widget:rev-b --name "$CLUSTER"

echo "==> namespaces"
k create namespace "$NS_SYS" --dry-run=client -o yaml | k apply -f -
for i in $(seq 0 $((DEMO_N - 1))); do
  k create namespace "$(printf 'demo-%02d' "$i")" --dry-run=client -o yaml | k apply -f -
done
k create namespace sandbox-a --dry-run=client -o yaml | k apply -f -

echo "==> CRDs"
k apply -f "$ROOT/config/crd/shardkit.dev_shardplans.yaml"
k apply -f "$ROOT/config/crd/shardkit.dev_widgets.yaml"
k wait --for=condition=Established crd/shardplans.shardkit.dev --timeout=60s
k wait --for=condition=Established crd/widgets.shardkit.dev --timeout=60s

echo "==> RBAC + plan + operators"
k apply -f "$OP/config/rbac/role.yaml"
if k -n "$NS_SYS" get shardplan widget-operator >/dev/null 2>&1; then
  # The server forbids backward epochs (CEL V9), so an existing plan
  # resets forward to an exact Off rest state, never via the epoch-1
  # bootstrap sample. Fresh rollout ID permits the tuple change.
  epoch=$(($(k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{.spec.epoch}') + 1))
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  k -n "$NS_SYS" patch shardplan widget-operator --type=json -p \
    "[{\"op\":\"replace\",\"path\":\"/spec\",\"value\":{\"key\":\"namespace\",\"rollout\":\"bringup-$ts\",\"epoch\":$epoch,\"seed\":\"9a1f2e\",\"tracks\":{\"stable\":{\"revision\":\"rev-a\"},\"canary\":{\"revision\":\"rev-b\"}},\"canary\":{\"mode\":\"Off\",\"weightPerMille\":0,\"include\":{\"namespaces\":[]},\"exclude\":{}},\"singletonOwner\":\"stable\"}}]" >/dev/null
  echo "plan reset forward to Off epoch=$epoch"
else
  k apply -f "$OP/config/samples/shardplan-off.yaml"
fi
k apply -f "$OP/config/manager/deployments.yaml"
# Rebuilt images reuse the rev-a/rev-b tags, so a re-run must force a
# rollout; otherwise old pods keep serving under IfNotPresent.
k -n "$NS_SYS" rollout restart deploy/widget-stable deploy/widget-canary
k -n "$NS_SYS" rollout status deploy/widget-stable --timeout=180s
k -n "$NS_SYS" rollout status deploy/widget-canary --timeout=180s

echo "==> seed widgets (one per demo namespace + sandbox-a)"
for i in $(seq 0 $((DEMO_N - 1))); do
  ns="$(printf 'demo-%02d' "$i")"
  k apply -f - >/dev/null <<EOF
apiVersion: shardkit.dev/v1alpha1
kind: Widget
metadata: {name: w0, namespace: $ns}
spec: {value: "$ns"}
EOF
done
k apply -f - >/dev/null <<EOF
apiVersion: shardkit.dev/v1alpha1
kind: Widget
metadata: {name: w0, namespace: sandbox-a}
spec: {value: sandbox-a}
EOF

echo "==> smoke: both leases held, both tracks reporting"
for _ in $(seq 1 12); do
  a=$(k -n "$NS_SYS" get lease/widget-operator-stable -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
  b=$(k -n "$NS_SYS" get lease/widget-operator-canary -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
  [ -n "$a" ] && [ -n "$b" ] && break
  sleep 5
done
echo "stable holder: ${a:-MISSING} canary holder: ${b:-MISSING}"
[ -n "${a:-}" ] && [ -n "${b:-}" ] || { echo "LEASE SMOKE FAILED"; exit 1; }
echo "waiting for status from both tracks..."
for _ in $(seq 1 18); do
  n=$(k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{.status.tracks[*].name}' 2>/dev/null | wc -w)
  [ "$n" -eq 2 ] && break # numeric: wc pads with spaces
  sleep 10
done
k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{range .status.tracks[]}{.name} owned={.ownedNamespaces} epoch={.observedEpoch}{"\n"}{end}'
n=$(k -n "$NS_SYS" get shardplan widget-operator -o jsonpath='{.status.tracks[*].name}' 2>/dev/null | wc -w)
[ "$n" -eq 2 ] || { echo "STATUS SMOKE FAILED"; exit 1; }
echo "BRING-UP OK"
