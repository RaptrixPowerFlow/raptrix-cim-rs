# raptrix-cim-rs
High-performance Rust implementation of the IEC 61970 Common Information Model (CIM), focused on a zero-copy-friendly pipeline from CGMES RDF/XML into the locked Raptrix PowerFlow Interchange (`.rpf`) Arrow IPC format for power-flow and SCED workflows.

This project is MPL-2.0 licensed and branded for Musto Technologies LLC.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

## Workspace Layout

This repository is now a Cargo workspace with two crates:

- `raptrix-cim-rs`: CIM-specific parsing, CGMES profile resolution, row mapping, and the production CLI
- `raptrix-cim-arrow`: shared canonical Arrow schema definitions, metadata constants, root `.rpf` Arrow IPC assembly, and generic `.rpf` inspection helpers

That split keeps the locked RPF contract in one reusable place so future converters such as `raptrix-psse-rs` can depend on the same crate and produce byte-compatible artifacts without copying schema or file IO code.

## Current Capabilities

### Input capabilities

- Parse single CIM XML fragments from string input:
	- cim:ACLineSegment
	- cim:EnergyConsumer
- Parse full RDF/XML EQ files from a reader and extract:
	- all ACLineSegment rows
	- all EnergyConsumer rows
	- all SynchronousMachine rows
- Parse TP profile topology and merge with EQ terminal connectivity:
	- TopologicalNode bus collapse mapping
	- ConnectivityNode group preservation for split-bus analysis
- Build branch-ready rows from live EQ topology by joining:
	- ACLineSegment electrical fields (r, x, bch)
	- Terminal.ConductingEquipment references
	- Terminal.ConnectivityNode references

### Output capabilities

- Build Arrow schema objects for the locked Raptrix PowerFlow Interchange v0.6.0 contract:
	- metadata
	- buses
	- branches
	- generators
	- loads
	- fixed_shunts
	- switched_shunts
	- transformers_2w
	- transformers_3w
	- areas
	- zones
	- owners
	- contingencies
	- interfaces
	- dynamics_models
- Build Arrow RecordBatch objects for demo bus and branch data.
- Write Parquet via ArrowWriter with custom metadata:
	- raptrix.branding
	- raptrix.version
- Generate example outputs:
	- example_powerflow.parquet (dummy data from main)
	- smallgrid_branches.parquet (live CGMES integration test)

## Data Contract (Locked)

- Current schema contract: v0.6.0
- Canonical source: raptrix-cim-arrow/src/schema.rs
- Contract policy and semantics: docs/schema-contract.md
- Cross-repo propagation workflow: docs/release-sync-workflow.md
- Target ingest compatibility: CGMES 2.4.x and CGMES 3.x profile sets (auto-detect and explicit mode supported)

### Versioning Policy

Raptrix uses split versioning by design: schema contract version and crate release version evolve independently. The file-format contract is now locked at schema `v0.6.0` for interoperability and deterministic ingest behavior, while the converter crate release tracks implementation maturity and is currently `0.1.3`.

This split preserves compatibility guarantees for downstream tools: existing `v0.5.2` Parquet artifacts remain valid to read on the core path, and new `v0.6.0` optional features are additive only.

For third-party implementers, [docs/schema-contract.md](docs/schema-contract.md) is the authoritative reader/writer contract. It now documents the `.rpf` Arrow IPC container layout, canonical root column ordering, row-count metadata trimming rules, optional table detection, and full column/type references needed to build a compatible parser.

Key lock points now documented and enforced:

- deterministic table list and ordering via all_table_schemas()
- strict table lookup via table_schema(name)
- expanded transformer detail (2w and 3w)
- explicit dynamics_models table
- tightened contingencies element payload
- solved-results contingency scoping field (contingency_id)
- shared root Arrow IPC assembly and validation via `raptrix-cim-arrow`

## How It Works

High-level pipeline:

1. Read CGMES EQ RDF/XML text.
2. Extract relevant CIM elements (ACLineSegment, Terminal, EnergyConsumer).
3. Deserialize typed CIM structs with quick-xml + serde.
4. Join line elements with terminal endpoint references.
5. If TP is available, collapse buses from ConnectivityNode to TopologicalNode by default.
6. Optionally emit connectivity_groups detail table for split-bus preservation.
7. Build Arrow arrays and RecordBatch.
8. Write Arrow IPC `.rpf` with Raptrix metadata.

## Design Decisions

- Topological by default: solver-facing bus IDs are collapsed to TP TopologicalNode for interoperability with common power-flow toolchains.
- Connectivity preserved optionally: `--connectivity-detail` keeps granular bus mapping and emits `connectivity_groups` so ML and detailed contingency workflows can reconstruct split-bus structure.
- Optional node-breaker support: `--node-breaker` emits additive node-breaker detail tables for operational/viewer fidelity while default ingest stays strict core tables only for maximum zero-copy speed.
- Split-bus contingency stub: a placeholder `split_bus` contingency element is emitted when TP groups indicate multiple connectivity nodes under one topological bus (full breaker-state parsing is intentionally deferred).
- Benchmark note: on SmallGrid-scale datasets this merge substantially reduces bus count versus raw ConnectivityNode granularity and improves solve-stage matrix dimensions.

Contribution guidance:

- Follow the `SynchronousMachine` model/parser pattern when adding new CIM classes.
- Keep zero-copy semantics in hot parsing/mapping paths (`Cow`, borrowed refs, deterministic dense IDs).

Note: `.rpf` Arrow IPC container support is the locked target profile; current demo writer still emits Parquet while ingestion and mapping layers evolve.

Current implementation priority is a clean and testable path to Arrow/Parquet output, while keeping APIs simple for incremental model coverage.

## Performance Snapshot

Latest local benchmark-style parser test results (debug profile, machine-dependent):

- ACLineSegment: 50,000 parses in ~1.280s (~39,056 parses/s)
- EnergyConsumer: 50,000 parses in ~1.054s (~47,425 parses/s)

Use these as baseline indicators, not final production benchmarks.

## Project Layout

- raptrix-cim-arrow/src/schema.rs: v0.6.0 table schemas, metadata constants, and schema registry helpers
- raptrix-cim-arrow/src/io.rs: generic root `.rpf` assembly, validation, readback, and summary helpers
- src/models: CIM data structures and traits
- src/parser.rs: parse helpers and EQ-to-branch mapping
- src/rpf_writer.rs: CIM-specific mapping from parsed CGMES content into canonical table batches

### Locked contract: v0.6.0 naming additions

- Added optional dictionary-encoded `name` columns to:
	- branches
	- generators
	- loads
	- transformers_2w
	- transformers_3w
- Existing `buses.name` remains required and now prioritizes CIM human-readable names with deterministic fallback.
- src/main.rs: production CLI for CGMES-to-RPF conversion
- src/test_utils.rs: test-only path helper for external CGMES data
- tests/integration_parse.rs: ignored live-data integration test

## CLI Usage

Build the production CLI in release mode:

- `cargo build --release`

Explicit profile mode:

- `cargo run --release -- convert --eq path/to/case_EQ.xml --tp path/to/case_TP.xml --sv path/to/case_SV.xml --ssh path/to/case_SSH.xml --dy path/to/case_DY.xml --output case.rpf`

Auto-detect mode:

- `cargo run --release -- convert --input-dir cgmes_case/ --output case.rpf`

Optional metadata defaults for mixed-profile or partial datasets:

- `--base-mva <FLOAT>` (default `100.0`)
- `--frequency-hz <FLOAT>` (default `60.0`)
- `--study-name <TEXT>`
- `--timestamp-utc <RFC3339>`

Inspect an existing `.rpf` file:

- `cargo run --release -- view --input case.rpf`

Inspect an existing `.rpf` file with root metadata and feature flags:

- `cargo run --release -- view --input case.rpf --verbose`

The CLI requires `--output` to end with `.rpf`. In auto-detect mode it recursively scans the provided directory for XML/RDF files and matches filenames to `EQ`, `TP`, `SV`, `SSH`, and `DY` profile tokens case-insensitively; `EQ` must be present.

## First Working `.rpf` (Generate + View)

Create a first `.rpf` artifact from the SmallGrid EQ profile (contains required terminal connectivity):

- `cargo run --release -- convert --eq C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0\SmallGrid\SmallGrid-Merged\SmallGrid_EQ.xml --output tests/data/external/smallgrid_eq.rpf`

Then inspect table counts and coverage:

- `cargo run --release -- view --input tests/data/external/smallgrid_eq.rpf`

The `view` command prints table-by-table row counts for quick import checks in `raptrix-core` and `raptrix-cim-viewer`.

Use `--verbose` when validating interoperability because it also prints the root Arrow IPC metadata entries, including `raptrix.version`, `raptrix.features.node_breaker`, and `rpf.rows.*` logical row counts used by compliant external parsers.

## Library Usage

Use the CIM converter directly from Rust:

```rust
use raptrix_cim_rs::write_complete_rpf;

fn convert(eq_path: &str, output_path: &str) -> anyhow::Result<()> {
	write_complete_rpf(&[eq_path], output_path)
}
```

Use the shared contract crate when building another converter:

```rust
use raptrix_cim_arrow::{all_table_schemas, write_root_rpf, RootWriteOptions};
```

See MIGRATION.md for the rationale and exact ownership boundary.

## Running in VS Code (Beginner-Friendly)

Open the repository in VS Code, then use Terminal -> New Terminal.

Run all normal tests:

- cargo test

Run parser throughput test with printed rates:

- cargo test benchmark_fragment_parse_speed -- --nocapture

Run live SmallGrid integration test (PowerShell):

1. $env:RAPTRIX_TEST_DATA_ROOT = "C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0"
2. cargo test parse_smallgrid_eq_aclinesegment -- --ignored --nocapture

Run CLI in auto-detect mode:

- cargo run --release -- convert --input-dir cgmes_case/ --output case.rpf

Enable `rdf:about` fallback diagnostics only when debugging parser edge cases:

1. `$env:RAPTRIX_LOG_RDF_ABOUT_FALLBACK = "1"`
2. `cargo test --test integration_parse -- --ignored --nocapture`

## Running Automated Validation

The repository includes a standalone pytest validator at `tests/inspect_rpf.py` that:

- runs the CLI to generate a `.rpf` file from SmallGrid EQ input
- validates one canonical root IPC batch with all 15 required struct columns
- verifies `raptrix.branding` and `raptrix.version` metadata
- checks bus and branch row counts against source EQ XML topology
- spot-checks first branch `r`/`x` values against EQ XML

Prerequisites:

- `RAPTRIX_TEST_DATA_ROOT` points to the CGMES v3.0 dataset root
- `pyarrow` and `pytest` are available in your Python environment

Run:

- `python -m pytest tests/inspect_rpf.py -q`

If external data is unavailable, the test is marked/treated as ignored and skipped.

## External CGMES Setup

1. Download ENTSO-E CGMES v3.0 test configurations from:
	 https://www.entsoe.eu/data/cim/cim-for-grid-models-exchange/
2. Unzip to a local path, for example:
	 C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0
3. Set RAPTRIX_TEST_DATA_ROOT to that v3.0 folder.

Expected SmallGrid EQ location pattern:

- <RAPTRIX_TEST_DATA_ROOT>\SmallGrid\SmallGrid-Merged\SmallGrid_EQ.xml

If RAPTRIX_TEST_DATA_ROOT is not set, ignored integration tests can be skipped safely.

## Test Data Policy

- tests/data/fixtures: tiny committed XML snippets only
- tests/data/external: placeholder path for local links or local files
- tests/data/large and data: ignored for large datasets

Large model archives should stay outside the repository.

## Known Limits (Current Scope)

- Parsing focus is currently EQ profile extraction for key equipment, not full multi-profile CGMES graph reconstruction.
- Branch endpoint mapping currently relies on Terminal and ConnectivityNode references present in EQ.
- Demo writer currently exercises buses/branches only; other locked contract: v0.6.0 tables are schema-defined and ready for row-mapping implementation.
- Some solver fields are default-filled in integration mapping until richer profile joins (TP/SV/SSH) are added.
- BaseVoltage joins are not fully modeled yet, so voltage labels for generated fallback names are heuristic and may be `unknown` for sparse naming inputs.
- If CGMES metadata is absent, `base_mva` and `frequency_hz` use CLI defaults; set these explicitly for non-60 Hz systems.

## How To Request New Solver Features

For each requested feature, open a GitHub issue with this checklist:

1. Solver use-case (for example, Newton-Raphson initialization, contingency analysis).
2. Exact required inputs (CGMES profile, fields, cardinality).
3. Exact required outputs (Arrow columns, types, nullability, units).
4. Validation rule examples (range checks, required relationships).
5. Performance target (throughput, max memory, dataset size).
6. Acceptance test dataset (SmallGrid, MiniGrid, or custom).

This format makes implementation deterministic and keeps schema evolution compatible.

## Recommended GitHub Documentation Structure

README should remain the quick start and capability overview.

For world-class maintainability, add these next:

- docs/architecture.md for pipeline and design decisions
- docs/schema-contract.md for `.rpf` Arrow IPC contracts and versioning
- docs/roadmap.md for planned CGMES profile coverage
- GitHub issue templates for feature requests and bug reports
- GitHub Discussions for design trade-offs before implementation

## Documentation Index

- docs/architecture.md
- docs/schema-contract.md
- docs/roadmap.md
- docs/requirements-template.md

## Issue Intake

- .github/ISSUE_TEMPLATE/feature_request.yml
- .github/ISSUE_TEMPLATE/bug_report.yml

When these templates are used consistently, feature requests can be translated
to implementation tasks with much less ambiguity.

## Branding

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC
