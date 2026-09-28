# Admission example (M4)

Serves `pkg/shardkit.ShardPlanValidator` as a validating webhook.
It adds exactly one rule over CEL: **V9** (frozen-tuple changes
need a fresh rollout ID; spec changes need an epoch bump). Read
`docs/webhook.md` first — the webhook stays optional for
availability reasons, and this example is the wiring the doc
sketches.

## Install on kind

```sh
# 1. Self-signed serving cert (example-grade; use cert-manager in prod).
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout tls.key -out tls.crt -subj "/CN=shardkit-admission.widget-system.svc" \
  -addext "subjectAltName=DNS:shardkit-admission.widget-system.svc"
kubectl --context kind-shardkit-dev -n widget-system create secret tls \
  shardkit-admission-certs --cert=tls.crt --key=tls.key

# 2. Substitute the CA bundle (the placeholder is not valid
# base64, so the API server rejects the file unsubstituted) and
# deploy the server plus the API-server wiring.
CABUNDLE=$(kubectl --context kind-shardkit-dev get secret -n widget-system \
  shardkit-admission-certs -o jsonpath='{.data.tls\.crt}')
sed 's|caBundle: PLACEHOLDER_SEE_README|caBundle: '"$CABUNDLE"'|' \
  config/admission.yaml > /tmp/admission-live.yaml
kubectl --context kind-shardkit-dev apply -f /tmp/admission-live.yaml

# 3. Build and load the server image (linux target: kind nodes
# are linux; a darwin binary fails with "exec format error").
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o admission .
docker build -f Dockerfile -t shardkit-admission:dev .
kind load docker-image shardkit-admission:dev --name shardkit-dev
kubectl --context kind-shardkit-dev -n widget-system \
  rollout restart deploy/shardkit-admission
```

`failurePolicy: Fail` is deliberate (fail closed, like the gate):
a webhook outage blocks ShardPlan writes rather than passing
malformed specs. During an outage, delete the
`ValidatingWebhookConfiguration` to recover write access — CEL
still guards V1–V8 and V11.

## Try it

A frozen-tuple change without a fresh rollout ID is rejected:

```sh
kubectl --context kind-shardkit-dev -n widget-system patch shardplan widget-operator \
  --type=merge -p '{"spec":{"seed":"tampered"}}'
# Error from server (Forbidden): admission webhook
# "validate.shardplan.shardkit.dev" denied the request:
# V9: frozen tuple (key, seed, include, exclude) changed without
# a fresh rollout ID
```

Verified live on kind 2026-09-27: the tamper was denied with the
V9 message, `spec.seed` stayed `9a1f2e`, and a no-op patch was
admitted (`patched (no change)`). Demo resources were removed
afterwards — do not leave a `failurePolicy: Fail` hook installed
beyond the experiment.

CEL already rejects most malformed specs, so expect the webhook
to fire only on V9-class writes.
