# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's **Report a vulnerability** button (Security tab → Advisories). Do not open a public issue. Include the affected component, steps to reproduce, and the impact you observed.

## Scope

In scope: sandbox escapes or privilege gains from a worker or exec environment, API keys or other credentials becoming visible inside a sandbox, bypassing the Gateway's budgets, tool quota or egress checks, cross-user access to sessions, tasks or artifacts, and authentication or session flaws.

## Known limits (not vulnerabilities by themselves)

- Isolation is container-level on a shared kernel. The privilege-boundary tests are regression tests, not a proof that no escape exists. Do not use this project to contain hostile multi-tenant code.
- External calls are not exactly-once, and budgets are conservative estimates that can overshoot slightly.

See the "Limits" section of the [README](README.md) and §1.3 and §20 of the [design spec](docs/design/2026-10-03-v0.2-first-release-design.md).
