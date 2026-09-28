# Admission example image: prebuilt static binary on alpine.
# Build from the repo root: go build -o admission ./examples/admission
FROM alpine:3.21
COPY admission /usr/local/bin/admission
ENTRYPOINT ["/usr/local/bin/admission"]
