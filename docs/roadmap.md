# Roadmap

## Market Interoperability Position

- CIM-first messaging: IEC 61970 CIM 17+ baseline for North American and European integrations.
- EU public validation: ENTSO-E CGMES v3.0.3 conformity suite remains the canonical public regression source.
- Public dataset reality: equivalent open test-configuration packages are limited outside ENTSO-E CAS.

## 0.1 Baseline (Delivered)

- Parse ACLineSegment and EnergyConsumer fragments and EQ files.
- Build branch rows by joining ACLineSegment and Terminal endpoint references.
- Write Arrow IPC `.rpf` with Raptrix metadata.

## 0.5 Schema Lock (Delivered)

- Lock canonical Arrow contract for Raptrix PowerFlow Interchange.
- Define all required tables and deterministic table ordering.
- Add expanded transformer detail in `transformers_2w` and `transformers_3w`.
- Add `dynamics_models` table.
- Tighten contingency element payload and allowed event types.

## 0.6 Multi-Profile Ingest (Delivered)

- Full EQ topology extraction: lines, transformers, machines, loads, shunts, tap changers.
- TP TopologicalNode bus collapse (default) and ConnectivityNode granularity (opt-in).
- SV and SSH profile joins for solved state and steady-state hypothesis values.
- Production CLI with auto-detect and explicit profile modes.

## 0.7 Schema Tightening (Delivered)

- Nominal base-voltage fields in core bus, branch, and transformer tables.
- Generic contingency equipment identity fields.
- Connectivity-detail and node-breaker optional output tables.

## 0.8 Dynamics and Diagram (Delivered)

- DY profile ingest for dynamics model parameters (GENROU, GENCLS, SYNC_MACHINE_EQ).
- EQ-fallback derivation for generators without DY coverage.
- DL profile ingest for IEC 61970-453 diagram layout objects and points.
- Full ENTSO-E CGMES v3.0 conformity suite: 11/11 test cases, 44 variants, 100% pass.

## 0.9.1 Load-Model Fidelity (Delivered, Additive)

- Added additive `loads` ZIP-fidelity columns: `p_i_pu`, `q_i_pu`, `p_y_pu`, `q_y_pu` (nullable).
- Preserved existing `p_pu` / `q_pu` semantics as constant-power components with no behavior change.
- Kept required table set/order unchanged and retained backward-compatible read behavior.
- Documented PSS/E-to-RPF mapping formulas and sign conventions in `docs/schema-contract.md`.

Public positioning (sanitized):

- RPF schema v0.9.1 introduces additive, backward-compatible fidelity extensions for richer source-model preservation while keeping existing workflows stable.
- These enhancements improve interchange completeness for advanced planning/operations datasets without changing required table structure or breaking current readers.
- The mapping is rooted in standard per-unit normalization and physical decomposition of ZIP load terms (constant-power, constant-current, constant-admittance), preserving source signs and avoiding fabricated values.

## Ongoing Focus Areas

- Broaden multi-file CGMES ingest coverage for assembled network cases.
- Expand CIM class coverage for additional converter and shunt equipment families.
- Keep automated regression checks robust for contract and ingest behavior.
- Maintain `docs/schema-contract.md` as the normative contract and keep `docs/rpf-field-guide.md` aligned.
- Strengthen reproducible performance benchmarking guardrails.
- Improve downstream integration options for non-Rust toolchains.

## Public Release Readiness Gate (Required Before Public Pushes)

- **Safety checks must pass**: run `./scripts/public-safety-check.sh --mode tracked` locally and keep `.github/workflows/public-safety.yml` green.
- **Release matrix must pass for all target platforms**: Windows x86_64, Linux x86_64, macOS arm64.
- **No external confidential dataset leakage**: keep utility/partner datasets only in ignored locations such as `tests/data/external/`.
- **No internal strategy leakage**: keep internal-only roadmap, partner strategy, and GTM artifacts out of tracked files.
- **Contract/version consistency**: keep README claims, `docs/schema-contract.md`, and `raptrix-cim-arrow/src/schema.rs` aligned on the locked contract version.

## Internal Roadmap Hygiene (Required)

- Public roadmap (`docs/roadmap.md`) must remain sanitized and implementation-focused.
- Internal roadmap must live only in ignored files (for example `docs/roadmap.internal.md`) or private systems.
- Do not commit partner-specific commercial plans, confidential timelines, customer names, or non-public dataset references.
- Before release, run a manual sweep for sensitive terms in docs and commit messages.

## Handoff Alignment (Near-Term)

- Keep schema ownership in this repository: contract semantics, versioning, compatibility policy, and validation behavior.
- Coordinate exporter-specific population work (for example PSS/E ZIP-field population) in source-format repos after contract release lands.
- Prioritize seamless planning-to-operations-to-long-term handoff via strict schema governance and explicit provenance semantics.

## CIM Coverage Risk Checklist (Triple-Check Before Public Announcements)

- **Contingencies completeness**: current contract supports contingency rows, but splitter/breaker paths still include stub semantics in some flows and must be called out transparently.
- **Dynamics completeness**: DY ingest is implemented with EQ fallback; placeholder dynamics rows may still appear when source coverage is missing.
- **FACTS and HVDC breadth**: class-level ingest coverage should be validated case-by-case before claiming full operational parity.
- **Public corpus gap**: no public test-configuration suite currently matches ENTSO-E CAS coverage; publish notes should keep this limitation explicit.

## How to Propose Work

Use GitHub issue templates for bug reports and feature requests.
For larger design changes, open a discussion first and link it to the issue.
