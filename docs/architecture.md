# Architecture

## Purpose

raptrix-cim-rs turns CGMES RDF/XML into Arrow and Parquet outputs for power-flow and related solver pipelines.

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
6. Write Parquet with Raptrix metadata.

## Core Modules

- src/models: CIM types and trait hierarchy.
- src/parser.rs: parse helpers and profile-specific row mapping.
- src/arrow_schema.rs: Arrow schema definitions and metadata constants.
- src/main.rs: small end-to-end writer sample.
- tests/integration_parse.rs: live-data ignored integration path.

## Data-Flow Boundaries

- Parsing boundary: XML to typed Rust model values.
- Mapping boundary: typed model values to solver-oriented row structures.
- Serialization boundary: row structures to Arrow RecordBatch and Parquet bytes.

## Error Handling

- anyhow::Result is used at orchestration boundaries and tests.
- parser helpers surface meaningful failure context for file path and parse stage.

## Observability

- Integration tests print parsed counts and first-item spot checks.
- Benchmark-style parser test prints approximate parse rates for baseline tracking.

## Near-Term Evolution

- Add TP/SV/SSH joins for richer branch and bus attributes.
- Add explicit performance harnesses for parse, map, and write phases.
- Add schema evolution policy validation in CI.
