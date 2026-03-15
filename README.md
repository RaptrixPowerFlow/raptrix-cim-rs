# raptrix-cim-rs
High-performance Rust implementation of the IEC 61970 Common Information Model (CIM), focused on a zero-copy-friendly pipeline from CGMES RDF/XML into Arrow and Parquet for power-flow and SCED workflows.

This project is MPL-2.0 licensed and branded for Musto Technologies LLC.

## Current Capabilities

### Input capabilities

- Parse single CIM XML fragments from string input:
	- cim:ACLineSegment
	- cim:EnergyConsumer
- Parse full RDF/XML EQ files from a reader and extract:
	- all ACLineSegment rows
	- all EnergyConsumer rows
- Build branch-ready rows from live EQ topology by joining:
	- ACLineSegment electrical fields (r, x, bch)
	- Terminal.ConductingEquipment references
	- Terminal.ConnectivityNode references

### Output capabilities

- Build Arrow schema objects for:
	- bus table (powerflow_schema)
	- branch table (branch_schema)
- Build Arrow RecordBatch objects for bus and branch data.
- Write Parquet via ArrowWriter with custom metadata:
	- raptrix.branding
	- raptrix.version
- Generate example outputs:
	- example_powerflow.parquet (dummy data from main)
	- smallgrid_branches.parquet (live CGMES integration test)

## Data Contract (Current)

### Branch table columns

The branch output uses this fixed column contract:

- from: Int32
- to: Int32
- r: Float64
- x: Float64
- b_shunt: Float64
- tap: Float64
- phase: Float64
- rate_a: Float64
- status: Boolean

For live SmallGrid integration testing today:

- from and to are deterministic bus IDs derived from unique ConnectivityNode references.
- r, x, b_shunt come from ACLineSegment values (default 0.0 when missing).
- tap, phase, rate_a, status are currently filled with default constants.

## How It Works

High-level pipeline:

1. Read CGMES EQ RDF/XML text.
2. Extract relevant CIM elements (ACLineSegment, Terminal, EnergyConsumer).
3. Deserialize typed CIM structs with quick-xml + serde.
4. Join line elements with terminal endpoint references.
5. Build Arrow arrays and RecordBatch.
6. Write Parquet with Raptrix metadata.

Current implementation priority is a clean and testable path to Arrow/Parquet output, while keeping APIs simple for incremental model coverage.

## Performance Snapshot

Latest local benchmark-style parser test results (debug profile, machine-dependent):

- ACLineSegment: 50,000 parses in ~1.280s (~39,056 parses/s)
- EnergyConsumer: 50,000 parses in ~1.054s (~47,425 parses/s)

Use these as baseline indicators, not final production benchmarks.

## Project Layout

- src/models: CIM data structures and traits
- src/parser.rs: parse helpers and EQ-to-branch mapping
- src/arrow_schema.rs: Arrow schema definitions and branding constants
- src/main.rs: minimal end-to-end Parquet writer demo
- src/test_utils.rs: test-only path helper for external CGMES data
- tests/integration_parse.rs: ignored live-data integration test

## Running in VS Code (Beginner-Friendly)

Open the repository in VS Code, then use Terminal -> New Terminal.

Run all normal tests:

- cargo test

Run parser throughput test with printed rates:

- cargo test benchmark_fragment_parse_speed -- --nocapture

Run live SmallGrid integration test (PowerShell):

1. $env:RAPTRIX_TEST_DATA_ROOT = "C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0"
2. cargo test parse_smallgrid_eq_aclinesegment -- --ignored --nocapture

Run demo writer:

- cargo run

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

- Focus is currently EQ profile extraction for key equipment, not full multi-profile CGMES graph reconstruction.
- Branch endpoint mapping currently relies on Terminal and ConnectivityNode references present in EQ.
- Some solver fields are default-filled until richer profile joins (TP/SV/SSH) are added.

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
