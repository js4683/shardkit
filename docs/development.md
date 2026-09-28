# Development setup

Clone `github.com/js4683/shardkit`; `origin` `main` is protected
(green CI required, no force pushes, no deletions), so work on
focused branches and open a pull request. Pins live in
[decisions](decisions.md): Go 1.27.1, controller-runtime v0.24.1,
k8s.io v0.36.4, kindest/node:v1.36.4, controller-gen v0.22.0,
envtest 1.36.x, Argo Rollouts v1.10.0 (plugin demo).

## Gates (run before every PR)

```sh
gofmt -l . && go vet ./... && go test ./... -count=1
make check        # docs links + whitespace
make test-envtest # real API server; provision binaries first:
eval $(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.24.1 use -p env 1.36.x)
make manifests generate  # must leave the tree clean (CI enforces it)
```

## kind demo fleet

```sh
./examples/widget-operator/hack/bring-up.sh   # kind-shardkit-dev cluster, CRD, operators, widgets
./examples/widget-operator/hack/demo.sh       # CLI rollout, explain, abort, re-converge
```

All commands pin the `kind-shardkit-dev` context; the scripts never
touch your current context.

## Release

`make release` builds the CLI/plugin matrix with checksums into
`dist/` (gitignored). Pushing a `v*` tag runs the Release workflow,
which rebuilds and attaches the binaries to a GitHub Release. The
`VERSION` default in the Makefile names the current version.
