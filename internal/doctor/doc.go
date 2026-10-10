// Package doctor implements `agentbox doctor-traces`: it reads failed and slow runs (an eval run directory or a
// time window of a running server, optionally enriched from Tempo, Prometheus and Loki), classifies their root
// causes with deterministic rules, and proposes safe, bounded configuration changes as a reviewable patch.
// An optional model step (off by default, hard budget) summarises the findings.
//
// The package is a client of public surfaces only: the eval run files (internal/eval's schema), the operator
// REST API (through internal/eval.Client) and the HTTP query APIs of the observability stack. It never imports
// server internals (archtest TestDoctorUsesPublicSurfacesOnly) and never changes a running server: proposals are
// written as a patch and, with --apply-to, as a new flags file for an experiment run.
//
// Design: docs/design/2026-10-10-trace-doctor-design.md.
package doctor
