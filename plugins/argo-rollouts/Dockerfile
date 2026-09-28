# Argo Rollouts controller image with the shardkit traffic-router
# plugin baked in (the "build the plugin into the controller
# container" method from the upstream plugin docs: deterministic,
# no download-at-startup dependency). Build from the repo root
# after compiling the static binary:
#
#   GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build \
#     -ldflags "-X main.version=vX.Y.Z" \
#     -o plugins/argo-rollouts/shardkit-plugin ./plugins/argo-rollouts
#   docker build -t shardkit-argo-rollouts:vX.Y.Z -f plugins/argo-rollouts/Dockerfile .
#   kind load docker-image shardkit-argo-rollouts:vX.Y.Z --name shardkit-dev
#
# The ConfigMap entry in ../argo/argo-rollouts-config.yaml points
# at the COPY destination below.
FROM quay.io/argoproj/argo-rollouts:v1.10.0
COPY plugins/argo-rollouts/shardkit-plugin /home/argo-rollouts/plugins/shardkit-plugin
