# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning for schema and converter release communication.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

## [Unreleased]

### Changed

- Migrated workspace crates to Rust 2024 edition.
- Auto-detect profile discovery now scans recursively and accepts XML/RDF filename token patterns used across CGMES 2.4.x and 3.x datasets.
- Writer metadata defaults are now configurable through CLI flags (`--base-mva`, `--frequency-hz`, `--study-name`, `--timestamp-utc`).
- Metadata timestamp now defaults to current UTC instead of a fixed epoch placeholder.
- Fallback voltage-name inference now supports broader grid voltage ranges via numeric token extraction instead of a fixed regional list.
- Added BaseVoltage extraction and equipment/BaseVoltage joins so fallback naming can use profile-derived nominal kV when available.
- Added explicit file-level metadata flags for provisional table payloads: `raptrix.features.contingencies_stub` and `raptrix.features.dynamics_stub`.
- Contingencies now use a hybrid path: derive from switch/open-state payloads when available, with stub fallback only when derived rows are unavailable.
- `raptrix.features.contingencies_stub` is now emitted conditionally only when placeholder contingency rows are present.
- Aligned `raptrix.branding` constant text with the documented schema contract value.
- Consolidated metadata key usage through shared schema constants to reduce key drift across crates.

### Documentation

- Added explicit compatibility/versioning rules for forward compatibility and MAJOR/MINOR/PATCH bump criteria in `docs/schema-contract.md`.
- Added cross-repo release synchronization workflow (`docs/release-sync-workflow.md`) and linked it from README.

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
