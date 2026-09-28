.PHONY: check test test-envtest manifests generate release
VERSION ?= v0.2.0
# Release binaries with checksums (M2 exit). CLI ships for the two
# desktop/server OSes; the plugin only runs inside the Linux
# Rollouts controller image, so it builds for Linux only. dist/ is
# gitignored; verify with `shasum -c dist/SHA256SUMS`.
release:
	rm -rf dist && mkdir -p dist
	for os in linux darwin; do for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath \
			-ldflags "-X main.version=$(VERSION)" \
			-o dist/kubectl-shardplan-$$os-$$arch ./cmd/kubectl-shardplan; \
	done; done
	for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -trimpath \
			-o dist/shardkit-plugin-linux-$$arch ./plugins/argo-rollouts; \
	done
	cd dist && shasum -a 256 kubectl-shardplan-* shardkit-plugin-* > SHA256SUMS && shasum -c SHA256SUMS
check:
	python3 scripts/check_docs.py
test:
	go test ./...
# Pinned generator (M1): CRDs and deepcopy regenerate bit-identically
# with controller-gen v0.22.0. Override CONTROLLER_GEN to use a local
# binary instead of the pinned go run.
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0
manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=config/crd
generate:
	$(CONTROLLER_GEN) object paths=./api/...
# Hermetic API-server tests (envtest 1.36.x). Unit-only `make test`
# skips this package when KUBEBUILDER_ASSETS is unset.
test-envtest:
	KUBEBUILDER_ASSETS="$${KUBEBUILDER_ASSETS:?set via eval $$(setup-envtest use -p env 1.36.x)}" go test ./test/envtest/...
