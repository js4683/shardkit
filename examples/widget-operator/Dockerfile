# Prototype image: prebuilt static binary on alpine (shell kept for
# debugging; M1 hardens the base). Build from the repo root with
# --build-arg REVISION=<rev>; TRACK comes from the Deployment.
FROM alpine:3.21
ARG REVISION=dev
ENV REVISION=$REVISION
COPY widget-operator /usr/local/bin/widget-operator
ENTRYPOINT ["/usr/local/bin/widget-operator"]
