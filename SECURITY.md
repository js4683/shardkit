# Security Policy

## Reporting a vulnerability

Do not open a public issue for suspected vulnerabilities. Use
GitHub's private vulnerability reporting (Security tab →
"Report a vulnerability") on this repository so the report stays
private until a fix is available. Include the affected version or
commit, steps to reproduce, and the impact you see.

## Scope

In scope: the `pkg/shardkit` and `pkg/partition` libraries, the
Argo Rollouts plugin, the CLI, the admission example, and the
RBAC manifests under `examples/`. The widget operator is a demo
workload: report library/plugin flaws found through it, not
cosmetic demo issues.

## Response

The maintainer (`js4683`) acknowledges reports within a few days,
develops a fix privately, then discloses with the release. No
bounty program.
