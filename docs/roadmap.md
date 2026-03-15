# Roadmap

## 0.1 Baseline (Current)

- Parse ACLineSegment and EnergyConsumer fragments and EQ files.
- Build branch rows by joining ACLineSegment and Terminal endpoint references.
- Write Arrow/Parquet with Raptrix metadata.
- Provide ignored integration tests for external CGMES datasets.

## 0.2 Near Term

- Add richer branch parameters from TP and SSH where available.
- Add bus-row extraction from ConnectivityNode and related equipment.
- Replace placeholder branch defaults (tap, phase, rate_a, status) when profile data exists.
- Add deterministic benchmark command for parse and map phases.

## 0.3 Mid Term

- Multi-profile merge support (EQ + TP + SV + SSH).
- Validation layer for required links and data quality checks.
- CLI entrypoint for profile selection and output path controls.

## 1.0 Production Goals

- Stable schema and versioning policy.
- CI performance guardrails on representative datasets.
- Full documentation for solver integration patterns.
- Contributor workflow for safe schema evolution.

## How to Propose Work

Use GitHub issue templates for bug reports and feature requests.
For larger design changes, open a discussion first and link it to the issue.
