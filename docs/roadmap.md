# Roadmap

## 0.1 Baseline (Delivered)

- Parse ACLineSegment and EnergyConsumer fragments and EQ files.
- Build branch rows by joining ACLineSegment and Terminal endpoint references.
- Write Arrow/Parquet with Raptrix metadata.
- Provide ignored integration tests for external CGMES datasets.

## 0.5 Schema Lock (Delivered)

- Lock canonical Arrow contract for Raptrix PowerFlow Interchange.
- Define all required tables and deterministic table ordering.
- Add expanded transformer detail in `transformers_2w` and `transformers_3w`.
- Add `dynamics_models` table for .dyn model payloads.
- Tighten contingency element payload and allowed event types.
- Add solved-results `contingency_id` scoping requirement.

## 0.6 Near Term Implementation

- Add richer branch parameters from TP and SSH where available.
- Add bus-row extraction from ConnectivityNode and related equipment.
- Materialize additional v0.5 tables beyond current demo bus/branch pipeline.
- Replace placeholder branch defaults (tap, phase, rate_a, status) when profile data exists.
- Add deterministic benchmark command for parse and map phases.

## 0.7 Schema Tightening

- Promote nominal base-voltage fields into core bus, branch, and transformer tables.
- Add generic contingency equipment identity for switch and split-bus workflows.
- Keep the 0.7 change additive so downstream readers can migrate as a minor contract update.

## 0.8 Dynamics Roadmap

- Improve dynamics coverage beyond first-pass generator-derived payloads.
- Wait for integration feedback from `raptrix-core` and Smart Wires device workflows before locking new dynamics fields.
- Prefer correctness of the interchange contract over early backend expansion.

## Later Work

- Multi-profile merge support (EQ + TP + SV + SSH).
- Validation layer for required links and data quality checks.
- CLI entrypoint for profile selection and output path controls.
- `.rpf` Arrow IPC writer path for primary interchange container.

## 1.0 Production Goals

- Stable schema and versioning policy.
- CI performance guardrails on representative datasets.
- Full documentation for solver integration patterns.
- Contributor workflow for safe schema evolution.

## How to Propose Work

Use GitHub issue templates for bug reports and feature requests.
For larger design changes, open a discussion first and link it to the issue.
