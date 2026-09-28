# Development setup

## Available now

This workspace contains plans and a runnable documentation check. Run:

```sh
cd /Users/jashanpreetsingh/Desktop/code/Projects/oss-contributions/shardkit
make check
git status --short
```

The check validates local Markdown file destinations and whitespace; it does not
validate remote links, Markdown anchors, technical guarantees, or Kubernetes behavior.

Host inventory observed on 2026-09-26: Go 1.27.1, kind 0.33.0, kubectl 1.37.0,
Git, Python 3, Make, Docker CLI, and no-mistakes are installed. These are observed
tools, not a supported compatibility matrix. A read-only check outside the sandbox
confirmed Docker Engine 29.5.2 is reachable. No kind cluster or envtest environment
has been created or verified; those belong to M0-06 after the prototype exists.

## Pinned toolchain (M0-04, 2026-09-26; extended M0-06, 2026-09-27)

- Module `github.com/js4683/shardkit`, `go 1.27.1`.
- controller-runtime v0.24.1 with k8s.io/api, apimachinery, client-go
  v0.36.4 (canonical `go mod tidy` with sumdb verification; `go.sum`
  committed). Transitives resolve from the module graph.
- kindest/node:v1.36.4 for the `shardkit-dev` cluster
  (`kind create --image`; note v1.36.0 was never published as a kind
  image). kubectl v1.37.0 client against the 1.36 server.
- D7 stays open for the Argo plugin protocol pin (M2), but the
  controller-runtime/Kubernetes side is proven: the M0-06 operator
  builds, elects per-track leases, and reconciles on this stack.
- Sandbox note: this sandbox has no docker socket, localhost TCP, or
  module-proxy/registry egress. Offline Go work used a throwaway
  modfile (`-modfile=/tmp/shardkit.offline.mod`, never committed)
  with a file-proxy over the warm module cache plus `GOSUMDB=off`;
  docker/kind/kubectl steps ran as single approved commands. If the
  warm cache ever falls short, the fallback is one networked
  `go mod tidy`, not more pins.

## Code generation and envtest (M1, 2026-09-27)

- `controller-gen v0.22.0` generates `config/crd/` and
  `api/v1alpha1/zz_generated.deepcopy.go` from Go markers:
  `make manifests` and `make generate`. The Makefile pins the
  version via `go run ...@v0.22.0`, so regeneration is
  reproducible; both outputs round-trip bit-identically.
  Hand-written `deepcopy.go` and hand-written CRDs are gone; the
  Widget CRD moved from the example tree into `config/crd/`.
- `setup-envtest` (controller-runtime v0.24.1 tool) provisions
  envtest 1.36.2 binaries (darwin/arm64 here; closest available to
  the 1.36.4 kind pin):
  `eval $(setup-envtest use -p env 1.36.x)`, then
  `make test-envtest`. The suite (`test/envtest/`) covers
  server-side CEL validation and concurrent per-track status
  publishing. Plain `make test` / `go test ./...` skips that
  package when `KUBEBUILDER_ASSETS` is unset.
- Server-side CEL (spec V1-V8 plus epoch never-decrease) is live
  on the kind cluster too. Consequence: `bring-up.sh` never
  re-applies the epoch-1 bootstrap sample over a live plan; it
  resets existing plans forward (Off, epoch+1, fresh rollout ID).
  The sample is marked bootstrap-only.

## Before the first implementation

1. Complete decisions D1–D4 in [decisions](decisions.md).
2. Initialize the Go module with the agreed public repository path. Pin a compatible
   controller-runtime/client-go/Kubernetes combination based on upstream manifests;
   record exact Go and envtest versions. Commit `go.mod` and `go.sum` together.
3. Add the pure partition model and property tests before manager integration.
4. Add reproducible envtest installation and test targets; pin downloaded artifacts.
5. For kind tests, verify `docker info`, choose a pinned node image, and use a
   dedicated `shardkit-dev` cluster with explicit context `kind-shardkit-dev`.
   Never apply demo manifests to the user's current context implicitly.

Do not create empty Go packages, placeholder controllers, or generated CRDs until
the corresponding contract is reviewed. The intended source layout remains in the
[original brief](project-brief.md).

## Delivery tooling

The installed `no-mistakes init --help` requires a repository with an `origin` remote.
This local project has none because its public owner and publication are undecided.
An attempted initialization confirmed the exact blocker: `no 'origin' remote`.
The local docs check is usable now; no-mistakes validation remains unperformed.
After the owner and real remote are supplied, initialize the gate, commit on a
feature branch, and run it with the complete task intent. Publication is a separate
authorized step. Never invent a remote to make initialization succeed.
