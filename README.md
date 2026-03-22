# raptrix-cim-rs
High-performance Rust implementation of the IEC 61970 Common Information Model (CIM), focused on a zero-copy-friendly pipeline from CGMES RDF/XML into Arrow and Parquet for power-flow and SCED workflows.

This project is MPL-2.0 licensed and branded for Musto Technologies LLC.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC

Copyright (c) 2026 Musto Technologies LLC

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

- Build Arrow schema objects for the locked Raptrix PowerFlow Interchange v0.5.1 contract:
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

- Current schema contract: v0.5.1
- Canonical source: src/arrow_schema.rs
- Contract policy and semantics: docs/schema-contract.md

Key lock points now documented and enforced:

- deterministic table list and ordering via all_table_schemas()
- strict table lookup via table_schema(name)
- expanded transformer detail (2w and 3w)
- explicit dynamics_models table
- tightened contingencies element payload
- solved-results contingency scoping field (contingency_id)

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

- src/models: CIM data structures and traits
- src/parser.rs: parse helpers and EQ-to-branch mapping
- src/arrow_schema.rs: v0.5.1 table schemas, metadata constants, and schema registry helpers
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

Inspect an existing `.rpf` file:

- `cargo run --release -- view --input case.rpf`

The CLI requires `--output` to end with `.rpf`. In auto-detect mode it scans the provided directory for filenames containing `EQ`, `TP`, `SV`, `SSH`, and `DY` case-insensitively, and `EQ` must be present.

## First Working `.rpf` (Generate + View)

Create a first `.rpf` artifact from the SmallGrid EQ profile (contains required terminal connectivity):

- `cargo run --release -- convert --eq C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0\SmallGrid\SmallGrid-Merged\SmallGrid_EQ.xml --output tests/data/external/smallgrid_eq.rpf`

Then inspect table counts and coverage:

- `cargo run --release -- view --input tests/data/external/smallgrid_eq.rpf`

The `view` command prints table-by-table row counts for quick import checks in `raptrix-core` and `raptrix-cim-viewer`.

## Library Usage

Use the public writer directly from Rust:

```rust
use raptrix_cim_rs::write_complete_rpf;

fn convert(eq_path: &str, output_path: &str) -> anyhow::Result<()> {
	write_complete_rpf(&[eq_path], output_path)
}
```

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
- Demo writer currently exercises buses/branches only; other locked v0.5 tables are schema-defined and ready for row-mapping implementation.
- Some solver fields are default-filled in integration mapping until richer profile joins (TP/SV/SSH) are added.

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
- docs/schema-contract.md for Arrow/Parquet contracts and versioning
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
