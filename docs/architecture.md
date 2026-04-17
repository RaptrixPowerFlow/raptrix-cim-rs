# Architecture

## Purpose

raptrix-cim-rs turns CIM RDF/XML (including CGMES profile sets) into Arrow-native outputs for power-flow and related solver pipelines, with a locked v0.8.5 Raptrix PowerFlow Interchange schema contract.

The architecture is CIM-first for both US and EU use: IEC 61970 CIM 17+ is the model baseline, while ENTSO-E CGMES v3.0.3 is used as the public regression corpus.

## Design Goals

- High throughput parsing with low allocation overhead where possible.
- Deterministic Arrow schema contracts.
- Explicit metadata branding and schema versioning.
- Incremental model coverage with testable milestones.

## Current Pipeline

1. Read CGMES EQ XML from file or reader.
2. Extract CIM elements of interest (for example ACLineSegment, Terminal, EnergyConsumer).
3. Deserialize typed model structs through quick-xml and serde.
4. Resolve references needed for topology and numeric rows.
5. Build Arrow arrays and RecordBatch values.
6. Serialize with Raptrix schema metadata.

Current serialization status:

- Contract target container: `.rpf` Arrow IPC (streaming or memory-mapped).
- Demo path currently in tree: Parquet writer for validation and interoperability checks.

## Core Modules

- raptrix-cim-arrow/src/schema.rs: locked v0.8.5 table schemas, metadata constants, and table registry helpers.
- raptrix-cim-arrow/src/io.rs: generic `.rpf` root-file assembly, validation, and readback helpers.
- src/models: CIM types and trait hierarchy.
- src/parser.rs: parse helpers and profile-specific row mapping.
- src/rpf_writer.rs: CIM-specific row mapping and orchestration into canonical table batches.
- src/main.rs: CLI entrypoint for CGMES-to-RPF conversion and inspection.
- tests/integration_parse.rs: live-data ignored integration path.

## Data-Flow Boundaries

- Parsing boundary: XML to typed Rust model values.
- Mapping boundary: typed model values to solver-oriented row structures.
- Serialization boundary: row structures to Arrow RecordBatch and output container bytes.

Locked schema boundaries in v0.8.5:

- all 15 required tables must materialize (empty allowed)
- dictionary-encoded string identity fields
- explicit keys and FK references
- nullable nominal-kV provenance on core network tables when source BaseVoltage joins exist
- generic contingency equipment identity for switch-oriented workflows
- strict planning-vs-solved semantics via case-mode and solved-state metadata
- solved shunt-state and angle-reference provenance for solved snapshots
- nested Arrow types for contingencies and dynamics model params

## Error Handling

- anyhow::Result is used at orchestration boundaries and tests.
- parser helpers surface meaningful failure context for file path and parse stage.

## Observability

- Integration tests print parsed counts and first-item spot checks.
- Benchmark-style parser test prints approximate parse rates for baseline tracking.

## Near-Term Evolution

- Add TP/SV/SSH joins for richer branch and bus attributes.
- Keep `.rpf` Arrow IPC writer and reader utilities centralized in `raptrix-cim-arrow`.
- Add explicit performance harnesses for parse, map, and write phases.
- Add schema evolution policy validation in CI.
