# Changelog

All notable changes to this project are documented in this file.

The format is based on Keep a Changelog,
and this project follows Semantic Versioning for schema and converter release communication.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

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
