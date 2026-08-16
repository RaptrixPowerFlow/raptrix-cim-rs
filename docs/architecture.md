# Architecture

## Purpose

raptrix-cim-rs turns CIM RDF/XML (including CGMES profile sets) into Arrow-native outputs for power-flow and related solver pipelines, with a locked **v0.14.0** Raptrix Power Interchange schema contract (`raptrix-cim-arrow` **0.7.0**; dual-read of v0.13.x).

The architecture is IEC 61970 CIM 17+ based, with ENTSO-E CGMES v3.0.3 used as the public regression corpus.

**Normative contract:** [`docs/schema-contract.md`](schema-contract.md) and `raptrix-cim-arrow/src/schema.rs`. This architecture note is descriptive; when they disagree, the schema contract wins.

## Design Goals

- High throughput parsing with low allocation overhead where possible.
- Deterministic Arrow schema contracts.
- Explicit metadata branding and schema versioning.
- Incremental model coverage with testable milestones.
- CIM-first open path (no mandatory vendor detour) while remaining faithful to legacy formats via sibling converters.

## Current Pipeline

1. Read CGMES profile XML (EQ, and optionally TP / SV / SSH / DY / DL / GL / EQBD) from file or reader.
2. Extract CIM elements of interest (for example ACLineSegment, Terminal, EnergyConsumer, SynchronousMachine).
3. Deserialize typed model structs through quick-xml and serde.
4. Resolve references needed for topology, voltages, and numeric rows (including optional BaseVoltage / geo joins).
5. Collapse connectivity to solver-friendly topological buses when configured (default).
6. Build Arrow arrays and RecordBatch values for the locked RPF root layout.
7. Serialize with Raptrix schema metadata (`raptrix.version` / `rpf_version` = `v0.14.0`).

Current serialization status:

- Contract target container: `.rpf` Arrow IPC (streaming or memory-mapped).
- Writers emit the locked v0.14.0 root; readers accept `v0.14.0` / `0.14.0` and retain v0.13.1 / v0.13.0 (pre-0.13 remains rejected).

## Core Modules

- `raptrix-cim-arrow/src/schema.rs`: locked v0.14.0 table schemas, metadata constants, and table registry helpers.
- `raptrix-cim-arrow/src/io.rs`: generic `.rpf` root-file assembly, validation, and readback helpers.
- `src/models`: CIM types and trait hierarchy.
- `src/parser.rs`: parse helpers and profile-specific row mapping.
- `src/rpf_writer.rs`: CIM-specific row mapping and orchestration into canonical table batches.
- `src/main.rs`: CLI entrypoint for CGMES-to-RPF conversion and inspection.
- `tests/integration_parse.rs`: live-data ignored integration path.

## Data-Flow Boundaries

- Parsing boundary: XML to typed Rust model values.
- Mapping boundary: typed model values to solver-oriented row structures.
- Serialization boundary: row structures to Arrow RecordBatch and output container bytes.

Locked schema boundaries in **v0.14.0** (additive on the v0.13.0 clean cut):

- all **18 required tables** must materialize (empty allowed)
- hybrid identity: dense `Int32 bus_id` solver FKs + required `buses.bus_uuid`; optional equipment `mrid`
- dictionary-encoded bus types (`PQ` / `PV` / `Slack`) and other string identity fields
- explicit keys and FK references
- required non-null `nominal_kv` on buses/branches/transformers
- optional GIS (`buses.latitude` / `longitude`) and source provenance metadata (replaces required `psse_version`)
- strict planning-vs-solved semantics via `case_mode` and solved-state tables
- nested Arrow types for contingencies, dynamics (`classical_params`), and RAS
- unknown trailing columns ignored by readers (forward compatibility)

## Canonical RAS Model

- `remedial_action_schemes` is the single canonical RAS/SPS schema for new writes.
- Legacy `protection_contingencies` / `topology_changes` may appear in older files; pre-0.13 contracts are not dual-read — re-emit through a current writer.
- Execution semantics are node/branch-first: trigger and action targeting can be represented without breaker-level topology.
- Breaker-level refinement remains optional via existing node-breaker detail tables when available.
- Public repository requirement: all RAS examples and fixtures are synthetic demonstration data only (no CEII).

## Error Handling

- anyhow::Result is used at orchestration boundaries and tests.
- parser helpers surface meaningful failure context for file path and parse stage.

## Observability

- Integration tests print parsed counts and first-item spot checks.
- Benchmark-style parser test prints approximate parse rates for baseline tracking.
- Post-write validation enforces the locked table set and version gate.

## Ongoing Evolution

- Keep `.rpf` Arrow IPC writer and reader utilities centralized in `raptrix-cim-arrow`.
- Maintain ENTSO-E CGMES v3 conformity gates and golden regression paths.
- Schema bumps remain explicit versioned clean cuts or additive trailing-column extensions documented in `schema-contract.md`.
