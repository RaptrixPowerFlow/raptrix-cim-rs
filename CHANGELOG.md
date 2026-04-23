# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning for schema and converter release communication.

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix PowerFlow

Copyright (c) 2026 Raptrix PowerFlow

## [Schema Contract 0.8.4] - 2026-04-07

## [Schema Contract 0.8.5] - 2026-04-09

## [Schema Contract 0.8.6] - 2026-04-13

## [Schema Contract 0.8.7] - 2026-04-17

## [Schema Contract 0.8.8] - 2026-04-19

## [Schema Contract 0.8.9] - 2026-04-19

## [Schema Contract 0.9.0] - 2026-04-23

### Converter release: Crate version 0.3.0 (raptrix-cim-arrow) / 0.3.0 (raptrix-cim-rs) | Arrow schema v0.9.0

### Removed

- `ibr_devices` table permanently removed. IBRs are now modeled exclusively in the unified `generators` table using `is_ibr = true` and `ibr_subtype`. Writers must not emit an `ibr_devices` root column in v0.9.0+ files.

### Added

- `contingencies` table: 6 new nullable Sentinel operational-outcome columns appended after `elements`:
  - `risk_score` (Float64, nullable)
  - `cleared_by_reserves` (Boolean, nullable)
  - `voltage_collapse_flag` (Boolean, nullable)
  - `recovery_possible` (Boolean, nullable)
  - `recovery_time_min` (Float64, nullable)
  - `greedy_reserve_summary` (Utf8, nullable)
- `metadata` table: 5 new nullable Sentinel-readiness fields:
  - `hour_ahead_uncertainty_band` (Float64, nullable)
  - `commitment_source` (Utf8, nullable)
  - `solver_q_limit_infeasible_count` (Int32, nullable)
  - `pv_to_pq_switch_count` (Int32, nullable)
  - `real_time_discovery` (Boolean, nullable)
- New optional `scenario_context` table (15 fields) for Sentinel export context: real-time, hour-ahead advisory, and planning feedback records.
- `case_mode` metadata field now accepts `"hour_ahead_advisory"` in addition to existing values.

### Changed

- Branding/schema constants bumped to v0.9.0.
- `SUPPORTED_RPF_VERSIONS` now only accepts v0.9.0 (`"v0.9.0"`, `"0.9.0"`).
- Canonical table count reduced from 19 to 18.

### Breaking change note

- v0.9.0 is a hard schema break with no backward compatibility to v0.8.9 and earlier.
- v0.8.9 files are rejected at the version gate in `io.rs` even if their `ibr_devices` table was empty.



### Converter release: Crate version 0.2.9 (raptrix-cim-arrow) / 0.2.9 (raptrix-cim-rs) | Arrow schema v0.8.9

### Added

- Breaking `generators` table redesign for hierarchical unit modeling:
  - New identity and hierarchy fields: `generator_id`, `unit_type`, `hierarchy_level`, `parent_generator_id`, `aggregation_count`
  - MW/MVAR-native dispatch fields: `p_sched_mw`, `p_min_mw`, `p_max_mw`, `q_min_mvar`, `q_max_mvar`
  - Ownership/market/extension fields: `owner_id`, `market_resource_id`, `params`
  - Unit classification fields: `is_ibr`, `ibr_subtype`
  - Optional UOL/LOL and ramp-rate fields: `uol_mw`, `lol_mw`, `ramp_rate_up_mw_min`, `ramp_rate_down_mw_min`
- Ownership enrichment:
  - `buses.owner_id` (nullable)
  - `branches.owner_id` (nullable)
  - Extended `owners` table columns: `short_name`, `type`, `params`

### Changed

- Branding/schema constants bumped to v0.8.9.
- `SUPPORTED_RPF_VERSIONS` now only accepts v0.8.9 (`"v0.8.9"`, `"0.8.9"`).
- CIM export now emits rich generator rows with per-unit metadata mapped from source machine payload where available.

### Breaking change note

- v0.8.9 is a hard schema break with no backward compatibility to v0.8.8 and earlier.

### Converter release: Crate version 0.2.8 (raptrix-cim-arrow) / 0.2.8 (raptrix-cim-rs) | Arrow schema v0.8.8

### Added

- New required tables for modern-grid modeling:
  - `multi_section_lines`
  - `dc_lines_2w`
  - `switched_shunt_banks`
  - `ibr_devices`
- New required metadata-row fields:
  - `modern_grid_profile`
  - `has_ibr`
  - `has_smart_valve`
  - `has_multi_terminal_dc`
- New nullable metadata-row fields:
  - `ibr_penetration_pct`
  - `study_purpose`
  - `scenario_tags`
- `branches` additive columns:
  - `parent_line_id`
  - `section_index`

### Release automation

- Added GitHub Actions workflow `.github/workflows/release-binaries.yml` to build
  platform binaries (Windows, Linux, macOS) and publish them to a GitHub Release
  when CI completes and a release/tag is created. To publish release artifacts,
  create an annotated tag (for example `v0.2.8`) and push it to GitHub; the workflow
  will produce and attach platform artifacts to the Release.

### Changed

- Branding/schema constants bumped to v0.8.8.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.8 as current.
- `switched_shunts.b_steps` is now emitted as capacitive-only values; inductive steps are emitted in `switched_shunt_banks`.

### Converter release: Crate version 0.2.7 (raptrix-cim-arrow) / 0.2.7 (raptrix-cim-rs) | Arrow schema v0.8.7

### Added

- **Transformer Representation Contract** — producers and consumers can now negotiate
  how 3-winding transformers are materialized in an RPF file.
  - New required file metadata key `rpf.transformer_representation_mode`:
    - `native_3w` — 3-winding transformers appear in the `transformers_3w` table (default, fully backward-compatible).
    - `expanded` — each 3-winding transformer is star-expanded into three 2-winding legs placed in `transformers_2w`; impedances are converted delta→wye; synthetic star buses receive IDs > 10 000 000.
  - New `METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE` constant promoted to the shared `raptrix-cim-arrow` crate (was previously a `raptrix-psse-rs`-local constant).
  - New `validate_transformer_representation_mode_value(value: &str) -> Result<(), String>` reader helper in `raptrix-cim-arrow::schema`.
  - New `TransformerRepresentationMode` enum in `raptrix-cim-rs` (`Native3W` default, `Expanded`).
  - `WriteOptions.transformer_representation_mode` field (default: `Native3W`).

### Changed

- Branding/schema constants bumped to v0.8.7.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.7 as current.

### Migration note

- Default mode is `native_3w` — no change for existing callers; they automatically receive the new key stamped as `native_3w`.
- Readers consuming files from pre-v0.8.7 producers (key absent) should treat missing key as `native_3w`.
- See MIGRATION.md — *Transformer Representation Contract (v0.8.7)*.


### Added

- Additive FACTS extension on `branches` with new nullable fields:
  - `device_type`, `control_mode`, `control_target_flow_mw`
  - `x_min_pu`, `x_max_pu`
  - `injected_voltage_mag_pu`, `injected_voltage_angle_deg`
  - `facts_params` (Map<String, Float64>)
- New optional `facts_devices` table for device-level FACTS metadata and dynamics linkage.
- New optional `facts_solved` table for solved snapshot replay.
- New optional feature metadata keys:
  - `raptrix.features.facts`
  - `raptrix.features.facts_solved`
  - `rpf.facts_solved_state_presence = actual_solved | not_available`
- Canonical SmartValve token and alias handling in schema helpers:
  - canonical: `smartvalve`
  - accepted read alias: `SV` (case-insensitive)

### Changed

- Branding/schema constants bumped to v0.8.6.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.6 as current.
- Reader reconstruction now tolerates older table shapes missing additive trailing nullable columns (fills nulls), preserving v0.8.3-v0.8.5 compatibility for additive schema growth.

### Migration note

- v0.8.6 adds FACTS/SV fields and optional tables additively; old files still load.
- Writers should emit v0.8.6 when FACTS data exists.

### Converter release: Crate version 0.2.4 (raptrix-cim-arrow) / 0.2.3 (raptrix-cim-rs) | Arrow schema v0.8.5

### Added

**Switched-shunt stable identity and per-bank solved state** — highest-value schema fix from round-trip testing; bus_id-only addressing is ambiguous when multiple banks exist at the same bus.

- `switched_shunts.shunt_id` (dict, nullable, v0.8.5+) — stable per-bank identifier. CIM path: `ShuntCompensator` mRID. PSS/E path: synthesized as `"{bus_id}_shunt_{n}"` (1-indexed). Nullable for backward compatibility; writers must populate when available. Readers must use this field — not `bus_id` alone — to cross-reference into `switched_shunts_solved`.
- New optional solved-state table **`switched_shunts_solved`** (v0.8.5+, emitted only when `case_mode = solved_snapshot` and `solved_shunt_state_presence = actual_solved`):
  - `bus_id` (Int32, non-null) — FK into `switched_shunts`.
  - `shunt_id` (dict, nullable) — FK into `switched_shunts.shunt_id`.
  - `current_step_solved` (Int32, nullable) — energized step index after convergence (1-indexed).
  - `b_pu_solved` (Float64, nullable) — post-solve total susceptance in per-unit.
  - `provenance` (dict, nullable).

**Extended `generators_solved` for first-class round-trip** — promotes to a fully required solved-snapshot table when `case_mode = solved_snapshot`.

- `generators_solved.p_mw` (Float64, nullable, v0.8.5+) — actual real power in MW (`= p_actual_pu × base_mva`); solver-native unit convenience. Always consistent with `p_actual_pu`.
- `generators_solved.q_mvar` (Float64, nullable, v0.8.5+) — actual reactive power in MVAR.
- `generators_solved.status` (Boolean, nullable, v0.8.5+) — in-service status at solve time; distinguishes planning-in-service units excluded by unit commitment from units that genuinely ran.

**Solved angle-reference metadata** — prevents silent reference-frame mismatch when snapshots are re-used.

- `metadata.slack_bus_id_solved` (Int32, nullable, v0.8.5+) — `bus_id` of the angle reference (slack) bus used in the solve.
- `metadata.angle_reference_deg` (Float64, nullable, v0.8.5+) — angle reference in degrees applied at the slack bus; typically 0.0.
- New file-level metadata keys (written only when `solved_state_presence = actual_solved`):
  - `rpf.solver.slack_bus_id`
  - `rpf.solver.angle_reference_deg`

**Solved-shunt provenance metadata** — lets loaders fail fast or warn when a solved snapshot claims solved but lacks full shunt state.

- `metadata.solved_shunt_state_presence` (dict, nullable, v0.8.5+) — `actual_solved | not_available`.
- New file-level metadata key `rpf.solver.solved_shunt_state_presence` (written only when `solved_state_presence = actual_solved`).

**New public Rust types in `rpf_writer`:**

- `SolvedShuntStatePresence` enum (`ActualSolved` | `NotAvailable`).
- `SolverProvenance.slack_bus_id_solved`, `.angle_reference_deg`, `.solved_shunt_state_presence` fields.
- `SwitchedShuntRow.shunt_id` field (CIM mRID populated by CIM exporter).
- `MetadataRow.slack_bus_id_solved`, `.angle_reference_deg`, `.solved_shunt_state_presence` fields.

### Changed

- Branding and version constants bumped to v0.8.5.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.5 as current; v0.8.4 and earlier remain readable.
- Parser checklist updated: `switched_shunts_solved` is required when `solved_shunt_state_presence = actual_solved`; `not_available` triggers a warning not a failure.
- Optional solved-state table ordering now includes `switched_shunts_solved` after `generators_solved` in `solved_state_table_schemas()`.

### Non-negotiable invariants (unchanged from v0.8.4, restated for v0.8.5)

- Flat-start planning must always exist for a valid model.
- Solved-state fields are optional and nullable; exporters must **never** fabricate solved values.
- PV→PQ switching outcome lives only in `generators_solved.pv_to_pq`; must never be back-propagated.
- `generators_solved.status = false` means the solver excluded this unit; it does not imply the unit was out of service in the planning case.
- `switched_shunts_solved` absence with `solved_shunt_state_presence = not_available` is a valid solved snapshot; loaders should warn, not fail.

## [Schema Contract 0.8.4] - 2026-04-07

### Converter release: Crate version 0.2.3 (raptrix-cim-arrow) / 0.2.2 (raptrix-cim-rs) | Arrow schema v0.8.4

### Added

**Strict planning-vs-solved semantics** — root cause fix for cross-converter false / mixed-state data.

- `metadata.case_mode` (dict, **required**, non-null) — explicit case classification:
  - `flat_start_planning` — all bus voltages at 1.0 pu / 0°; no solved-state data.
  - `warm_start_planning` — planning setpoints from a prior solve (warm start); still a planning case.
  - `solved_snapshot` — post-solve snapshot; solved-state tables are expected.
- `metadata.solved_state_presence` (dict, nullable) — per-export provenance tag for solved fields:
  - `actual_solved` — solver ran and produced results.
  - `not_available` — solved data not obtainable on this export path.
  - `not_computed` — no solve has run; planning-only case (default for all CIM exports).
- Solver provenance columns on `metadata` row (all nullable; populated only when `solved_state_presence = actual_solved`):
  - `solver_version` (Utf8) — solver software version string, e.g. `"raptrix-core 1.4.2"`.
  - `solver_iterations` (Int32) — Newton-Raphson iteration count until convergence.
  - `solver_accuracy` (Float64) — final mismatch residual norm.
  - `solver_mode` (dict) — bus control mode after convergence, e.g. `"PV"`, `"PV_to_PQ"`.
- Optional **`buses_solved`** table (v0.8.4+, emitted only when `case_mode = solved_snapshot`):
  - `bus_id` (Int32, non-null) — foreign key into `buses`.
  - `v_mag_pu` (Float64, nullable) — post-solve voltage magnitude in per-unit.
  - `v_ang_deg` (Float64, nullable) — post-solve voltage angle in degrees.
  - `p_inj_pu`, `q_inj_pu` (Float64, nullable) — net injection.
  - `bus_type_solved` (Int8, nullable) — effective bus type after PV→PQ switching.
  - `provenance` (dict, nullable).
- Optional **`generators_solved`** table (v0.8.4+, emitted only when `case_mode = solved_snapshot`):
  - `bus_id`, `id` (non-null) — foreign keys into `generators`.
  - `p_actual_pu`, `q_actual_pu` (Float64, nullable) — post-solve real/reactive output.
  - `pv_to_pq` (Boolean, nullable) — true when this unit's bus switched PV→PQ during solve.
  - `provenance` (dict, nullable).
- New public Rust types in `rpf_writer`:
  - `CaseMode` enum (`FlatStartPlanning` | `WarmStartPlanning` | `SolvedSnapshot`).
  - `SolvedStatePresence` enum (`ActualSolved` | `NotAvailable` | `NotComputed`).
  - `SolverProvenance` struct (all fields optional).
- `WriteOptions.case_mode` and `WriteOptions.solver_provenance` fields.
- New file-level metadata keys: `rpf.case_mode`, `rpf.solved_state_presence`, `rpf.solver.version`, `rpf.solver.iterations`, `rpf.solver.accuracy`, `rpf.solver.mode`.
- `RootWriteOptions.include_solved_state` flag for solver-side RPF assembly.
- `root_rpf_schema_with_options()` helper in `raptrix-cim-arrow::io`.
- `solved_state_table_schemas()` helper in `raptrix-cim-arrow::schema`.

### Changed

- `validate_pre_write_contract` now also calls `validate_planning_fields_finite` — all buses must have finite, positive `v_mag_set` and finite `v_ang_set`, `v_min`, `v_max` before an RPF file is written.
- Exporter fails fast with clear errors on:
  - `case_mode = solved_snapshot` without `solver_provenance` set.
  - `solver_provenance` set on a non-solved (planning) case.
  - Planning fields containing NaN or Inf.
- `metadata.is_planning_case` is now derived from `case_mode` rather than hardcoded `true`.
- Branding and version constants bumped to v0.8.4.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.4 as current.

### Non-negotiable invariants enforced

- Flat-start planning must always exist for a valid model.
- Solved-state fields are optional and nullable; exporters must **never** fabricate solved values.
- PV→PQ switching outcome lives only in `generators_solved.pv_to_pq`; it must never be back-propagated into `generators.p_sched_mw` or any other planning field.
- Contradictory metadata combinations (`case_mode = solved_snapshot` + `solved_state_presence ≠ actual_solved`, or vice-versa) are hard errors.

## [Schema Contract 0.8.3] - 2026-04-06

### Converter release: Crate version 0.2.2 (raptrix-cim-arrow) / 0.2.1 (raptrix-cim-rs) | Arrow schema v0.8.3

### Added

- `switched_shunts.b_init_pu` (Float64, nullable) — authoritative initial susceptance in per-unit.
  - **PSS/E path**: written as `BINIT / base_mva` directly, so mixed-sign inductive/capacitive banks
    round-trip exactly regardless of step ordering (fixes the 1.0 pu deficit at mixed-sign
    switched-shunt buses in the Eastern Interconnect model).
  - **CIM path**: written as `b_steps[current_step - 1]` (the CIM `SvShuntCompensator.sections`
    energised susceptance from the cumulative b_steps array).
  - Nullable so v0.8.2 files remain readable; writers **must** populate this field going forward.
  - Readers should prefer `b_init_pu` over reconstructing from `b_steps + current_step` when present.

### Changed

- Branding and version constants bumped to v0.8.3.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.3 as current.

## [Schema Contract 0.8.2] - 2026-04-05
### Converter release: Crate version 0.2.1 | Arrow schema v0.8.2

### ⚠️ BREAKING CHANGE

- `buses.bus_uuid` is now required (non-null).
- `metadata` now includes required case identity and validation fields:
  - `source_case_id`
  - `snapshot_timestamp_utc`
  - `case_fingerprint`
  - `validation_mode`

### Added

- Required root metadata keys:
  - `rpf.case_fingerprint`
  - `rpf.validation_mode`
- Deterministic case fingerprint generation from source paths + file metadata + snapshot timestamp.

### Changed

- Branding and version constants bumped to v0.8.2.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.2 as current.
- Test validation now enforces required metadata columns and non-null `buses.bus_uuid`.

## [Schema Contract 0.8.0] - 2026-04-05

### Converter release: Crate version 0.2.1 | Arrow schema v0.8.0

### ⚠️ BREAKING CHANGE

- **Dropped CGMES 2.4.x support** — raptrix-cim-rs now targets **CGMES v3.0+ only** (v17+ CIM standard).
  - Rationale: CGMES 2.4.x remains in legacy use; CGMES 3.0+ (v3.0, v3.0.1, v3.0.2, v3.0.3) is the current, production-grade ENTSO-E standard as of 2025–2026.
  - Benefit: Eliminates dual-track parsing logic, complex difference-file assembly, and fallback heuristics. Parser is now 100% aligned with modern ENTSO-E XSD/UML for cleaner, faster ingestion.
  - Migration: Upstream any legacy CGMES 2.4 datasets to v3.0+ before using raptrix-cim-rs. Most vendor tools (PowerFactory, PSS/ODMS, CIMdesk, CIMbion, etc.) provide automated converters.

### Changed

- Updated schema contract branding to v0.8.0 with CGMES 3.0+ positioning.
- Clarified compatibility policy in `docs/schema-contract.md`: single-track CGMES 3.0+ ingest target (removed dual-track language).
- Updated README, roadmap, and release-sync workflow to reflect v3.0+ compatibility only.
- `SUPPORTED_RPF_VERSIONS` now includes v0.8.0 as the current version; v0.7.1, v0.7.0 remain readable for backward compatibility.

### Added

- Diagram layout support (v0.8.0 continues additive v0.7.1 diagram_objects and diagram_points tables when DL profile present).
- Additional test artifacts in v3.0 format with new diagram layout features.

### Removed

- Conditional CGMES 2.4.x profile auto-detection logic from parser and writer.
- Fallback multi-pattern heuristics for v2.4 vs v3.0 naming conventions.

## [Schema Contract 0.7.1] - 2026-04-04

### Converter release: Crate version 0.2.1

### Added

- Diagram layout optional tables (`diagram_objects`, `diagram_points`) for CGMES DL profile support.
- Metadata flag `raptrix.features.diagram_layout` to indicate presence of diagram tables.
- CLI flags `--dl` and `--no-diagram` for diagram profile control.

## [Unreleased]

### Converter release: Crate version 0.2.0

### Changed

- Bumped the locked schema contract from `v0.6.0` to `v0.7.0` for additive network-voltage and contingency-identity improvements.
- Migrated workspace crates to Rust 2024 edition.
- Auto-detect profile discovery now scans recursively and accepts XML/RDF filename token patterns used across CGMES 2.4.x and 3.x datasets.
- Writer metadata defaults are now configurable through CLI flags (`--base-mva`, `--frequency-hz`, `--study-name`, `--timestamp-utc`).
- Metadata timestamp now defaults to current UTC instead of a fixed epoch placeholder.
- Fallback voltage-name inference now supports broader grid voltage ranges via numeric token extraction instead of a fixed regional list.
- Added BaseVoltage extraction and equipment/BaseVoltage joins so fallback naming can use profile-derived nominal kV when available.
- Added nullable nominal-kV columns to core bus, branch, and transformer tables so downstream tools can consume source voltage provenance directly.
- Added explicit file-level metadata flags for provisional table payloads: `raptrix.features.contingencies_stub` and `raptrix.features.dynamics_stub`.
- Contingencies now use a hybrid path: derive from switch/open-state payloads when available, with stub fallback only when derived rows are unavailable.
- Contingency elements now carry generic `equipment_kind` and `equipment_id` fields for switch and split-bus payloads that do not fit branch/gen/load IDs cleanly.
- `raptrix.features.contingencies_stub` is now emitted conditionally only when placeholder contingency rows are present.
- Dynamics now use a first-pass real extraction path from generator rows (`SynchronousMachine` parameters), with stub fallback only when generator-derived rows are unavailable.
- `raptrix.features.dynamics_stub` now reflects whether dynamics payload is placeholder-derived.
- First-pass `dynamics_models.model_type` is now conservatively inferred from available generator parameters (`GENROU`, `GENCLS`, or `SYNC_MACHINE_EQ`) without requiring a full DY parser.
- Aligned `raptrix.branding` constant text with the documented schema contract value.
- Consolidated metadata key usage through shared schema constants to reduce key drift across crates.
- GitHub workflow now uses `actions/checkout@v6` so the validation pipeline runs on the native Node 24 action runtime instead of a forced compatibility path.

### Documentation

- Added explicit compatibility/versioning rules for forward compatibility and MAJOR/MINOR/PATCH bump criteria in `docs/schema-contract.md`.
- Added cross-repo release synchronization workflow (`docs/release-sync-workflow.md`) and linked it from README.
- Added a 0.8 roadmap note that richer dynamics coverage is waiting on feedback from `raptrix-core` and Smart Wires device workflows.

## [Schema Contract 0.6.0] - 2026-03-22

### Converter release: Crate version 0.1.3

### Added

- Optional node-breaker detail tables: `node_breaker_detail`, `switch_detail`, and `connectivity_nodes`.
- Explicit `--node-breaker` CLI flag for opt-in operational topology emission.
- `.rpf` file-level Arrow IPC metadata key `raptrix.features.node_breaker=true` for toolchain-driven activation.
- Zero-copy guarantee for the default planning-model path: strict core bus-branch tables only, memory-mapped `.rpf` Arrow IPC to Arrow, with no extra allocations or copies introduced by node-breaker support.
- Normative parser-author documentation for `.rpf` root layout, root column ordering, row-count metadata trimming, and optional table detection.

### Changed

- Locked schema contract bumped from `v0.5.2` to `v0.6.0` as a MINOR release.
- Documentation now formalizes split versioning: schema contract `v0.6.0` and converter crate `0.1.3`.
- Schema contract now documents enough wire-format detail for third parties to implement a compatible reader without inspecting the Rust source.

### Compatibility

- Backwards compatibility is preserved: existing `v0.5.2` Parquet files remain valid for the core ingest path.
- This is a MINOR bump because the new node-breaker functionality is additive and optional, aligned with Semantic Versioning and interoperability goals.
- PATCH releases remain reserved for fixes only; this release unlocks full operational CGMES fidelity while leaving the lean planning-model core untouched for speed.

