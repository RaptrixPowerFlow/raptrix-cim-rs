# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning for schema and converter release communication.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

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
