# Workspace Migration

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

## What Changed

This repository was refactored from a single crate into a Cargo workspace with two responsibilities:

- `raptrix-cim-rs`: CIM parsing, CGMES profile handling, row mapping, and CLI orchestration
- `raptrix-cim-arrow`: locked canonical schema definitions and generic `.rpf` Arrow IPC infrastructure

## What Moved Into `raptrix-cim-arrow`

- all schema definitions previously in `src/arrow_schema.rs`
- branding and version metadata constants
- canonical table ordering and lookup helpers
- root `.rpf` Arrow IPC assembly logic
- root `.rpf` validation helpers
- generic `.rpf` readback, summary, and metadata inspection helpers

## What Stayed In `raptrix-cim-rs`

- CIM model types in `src/models`
- RDF/XML parsing helpers in `src/parser.rs`
- CGMES-specific row construction in `src/rpf_writer.rs`
- CLI behavior in `src/main.rs`

This boundary is intentional: the shared crate should not know how CIM, PSS/E, MATLAB, or any future format is parsed. It should only know the canonical contract and how to emit and validate a compliant `.rpf` file.

## Why The Split Was Done

- keeps the locked RPF contract in one source of truth
- reduces duplication for future converter repositories
- lets format-specific bugs and parser changes stay isolated from Arrow contract changes
- makes contract fixes available to every converter that depends on the shared crate

## How Future Converter Crates Should Reuse It

For a future converter such as `raptrix-psse-rs`:

1. Depend on `raptrix-cim-arrow`
2. Parse the source format into canonical table rows or `RecordBatch` values
3. Use the schema helpers from `raptrix-cim-arrow` when constructing batches
4. Call `write_root_rpf` to emit the final `.rpf` file
5. Use `read_rpf_tables`, `summarize_rpf`, and `rpf_file_metadata` in tests to verify compatibility

That keeps all converters aligned on one exact Arrow schema contract and one exact root-file layout.