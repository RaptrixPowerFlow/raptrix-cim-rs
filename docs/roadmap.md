# Roadmap

## Market Interoperability Position

- CIM-first messaging: IEC 61970 CIM 17+ baseline for North American and European integrations.
- EU public validation: ENTSO-E CGMES v3.0.3 conformity suite remains the canonical public regression source.
- US deployment reality: no public NAESB test-configuration package equivalent is currently available; partner utility data is used for private validation when available.

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

## Next

- **Multi-TSO EQ merge**: Support multiple EQ files (e.g., BE + NL authority sets) as a merged ingest unit, enabling the MicroGrid assembled cases.
- **Expanded CIM class coverage**: HVDC converters, FACTS devices, and additional shunt compensation types.
- **CI regression gate**: Automated CGMES conformity runs on pull requests with pass/fail enforcement.
- **Performance benchmarking suite**: Deterministic throughput tests on representative datasets with guardrails against regressions.
- **Python bindings or C FFI**: Expose `write_complete_rpf_with_options` for downstream tooling without requiring a Rust build.

## Public Release Readiness Gate (Required Before Public Pushes)

- **Safety checks must pass**: run `./scripts/public-safety-check.sh --mode tracked` locally and keep `.github/workflows/public-safety.yml` green.
- **Release matrix must pass for all target platforms**: Windows x86_64, Linux x86_64, macOS x86_64, macOS arm64.
- **No external confidential dataset leakage**: keep utility/partner datasets only in ignored locations such as `tests/data/external/`.
- **Contract/version consistency**: keep README claims, `docs/schema-contract.md`, and `raptrix-cim-arrow/src/schema.rs` aligned on the locked contract version.

## CIM Coverage Risk Checklist (Triple-Check Before Public Announcements)

- **Contingencies completeness**: current contract supports contingency rows, but splitter/breaker paths still include stub semantics in some flows and must be called out transparently.
- **Dynamics completeness**: DY ingest is implemented with EQ fallback; placeholder dynamics rows may still appear when source coverage is missing.
- **FACTS and HVDC breadth**: schema support is expanding, but class-level ingest coverage should be validated case-by-case before claiming full operational parity.
- **Public NAESB-style corpus gap**: there is still no public NAESB test-configuration suite equivalent to ENTSO-E CAS, so publish notes should keep that limitation explicit.

## How to Propose Work

Use GitHub issue templates for bug reports and feature requests.
For larger design changes, open a discussion first and link it to the issue.
