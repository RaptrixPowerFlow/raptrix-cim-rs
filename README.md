# raptrix-cim-rs

raptrix-cim-rs — the world’s first high-performance zero-copy Rust implementation of IEC 61970 CIM optimized for real-time power flow and SCED.

We close the physics gap — planning to real time.

Part of the Raptrix PowerFlow ecosystem.

Related repositories:

- [raptrix-psse-rs](https://github.com/RaptrixPowerFlow/raptrix-psse-rs) - Unlimited-size PSS/E to RPF converter
- [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) - Free unlimited RPF viewer/editor
- [RaptrixPowerFlow organization](https://github.com/RaptrixPowerFlow/) - Full open converter suite

Quick start:

```bash
cargo run --release -- convert --input-dir cgmes_case/ --output case.rpf
```

## Precompiled Releases

Most users should start with precompiled binaries instead of building from source.

- Latest binaries: [GitHub Releases](https://github.com/RaptrixPowerFlow/raptrix-cim-rs/releases)
- Current release targets: Windows x86_64, Linux x86_64, macOS arm64 (Apple Silicon)
- Use source builds only when you need local development changes or custom tooling integration.

![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)
MPL 2.0 - free to use, modify, and distribute.

Production-grid usage is supported through the commercial Raptrix core platform: [raptrix-core](https://github.com/RaptrixPowerFlow/raptrix-core).

Enterprise and academic inquiries: contact us via [Raptrix website](https://www.raptrixpowerflow.com/) or [RaptrixPowerFlow on GitHub](https://github.com/RaptrixPowerFlow/).

## Why RPF? CIM Direct to Power Flow, No Vendor Detour

CIM/XML profile exchange is rich and interoperable, but it was not designed as a zero-copy runtime handoff format for modern security analysis pipelines. RPF exists to close that gap.

| CIM exchange reality | How RPF solves it |
| --- | --- |
| **Profile joins at runtime are expensive** — EQ/TP/SV/SSH/DY/DL data must be stitched repeatedly before solve-stage workflows | **Single canonical Arrow IPC payload** — profiles are normalized once into deterministic tables, then memory-mapped downstream |
| **Semantic ambiguity across toolchains** — planning vs. solved intent can be unclear and inconsistently encoded | **Explicit case semantics** — `case_mode` and solved-state metadata are first-class, validated contract fields |
| **Operational pipelines inherit vendor friction** — many teams round-trip through legacy vendor workflows before contingency or SCED analysis | **Direct CIM-to-RPF path** — `raptrix-cim-rs` maps IEC 61970 input straight to solver-ready interchange without requiring a PSS/E conversion step |
| **Mixed proprietary/open ecosystems are hard to automate safely** | **Open, typed, versioned contract** — strict schema + metadata validation catches mismatches early and supports deterministic CI |

### Why this is critical

- **Time-to-answer**: remove unnecessary translation hops between planning data and contingency-ready power-flow input.
- **Data integrity**: one canonical contract lowers drift between engineering sources and solver execution payloads.
- **Operational independence**: CIM-first ingestion keeps the path open and reproducible for utilities, ISOs, consultants, and datacenter studies.

In short: this converter takes IEC 61970 CIM exchange data directly into Raptrix PowerFlow Interchange (`.rpf`) so power-flow workflows can run without a PSS/E-in-the-middle dependency.

Copyright (c) 2026 Raptrix PowerFlow

## Workspace Layout

This repository is now a Cargo workspace with two crates:

- `raptrix-cim-rs`: CIM-specific parsing, CGMES profile resolution, row mapping, and the production CLI
- `raptrix-cim-arrow`: shared canonical Arrow schema definitions, metadata constants, root `.rpf` Arrow IPC assembly, and generic `.rpf` inspection helpers

That split keeps the locked RPF contract in one reusable place so future converters such as `raptrix-psse-rs` can depend on the same crate and produce byte-compatible artifacts without copying schema or file IO code.

## Current Capabilities

raptrix-cim-rs is production-ready against the full ENTSO-E CGMES v3.0 conformity suite. All 11 test cases pass at 100% across four output variants in the release binary.

### CIM-First Positioning (US + EU)

This project is CIM-first and IEC 61970-native for both North American and European workflows:

- **CIM 17+ baseline**: The parser and schema target IEC 61970 CIM classes and RDF/XML profile exchange directly.
- **Public validation corpus**: ENTSO-E CGMES v3.0.3 conformity datasets are used as the canonical public regression corpus.
- **Shared model foundation**: Core ingest architecture is built around IEC 61970 profile families (EQ/TP/SV/SSH/DY/DL).
- **Public-data reality**: ENTSO-E CAS remains the strongest open validation proxy currently available.

### Profile ingest (CGMES 3.0+ only)

| Profile | Coverage |
| --- | --- |
| EQ | Full topology: AC lines, power transformers (2W/3W), synchronous machines, energy consumers, fixed/switched shunts, static VAr compensators, phase tap changers, ratio tap changers, base voltages, substations, voltage levels, terminals, connectivity nodes |
| TP | TopologicalNode bus collapse (default) or ConnectivityNode granularity on demand |
| SV | Solved state: bus voltage angles and magnitudes, branch active/reactive flows |
| SSH | Steady-state hypothesis: generator dispatch, load targets, shunt switching state |
| DY | Dynamics model parameters: GENROU, GENCLS, and SYNC_MACHINE_EQ for synchronous machines |
| DL | IEC 61970-453 diagram layout objects and diagram points |

Profiles beyond EQ are optional — any subset can be provided and missing profiles are silently skipped.

### Bus resolution modes

| Mode | Flag | Description |
| --- | --- | --- |
| Topological | *(default)* | Bus IDs collapse to TP TopologicalNode for solver interoperability |
| Connectivity detail | `--connectivity-detail` | Granular ConnectivityNode bus mapping; emits optional `connectivity_groups` table |
| Node-breaker | `--connectivity-detail --node-breaker` | Adds switch-topology detail tables for operational and viewer workflows |

### Output tables (schema contract v0.9.0)

**18 canonical tables (always emitted):** `metadata`, `buses`, `branches`, `multi_section_lines`, `dc_lines_2w`, `generators`, `loads`, `fixed_shunts`, `switched_shunts`, `switched_shunt_banks`, `transformers_2w`, `transformers_3w`, `areas`, `zones`, `owners`, `contingencies`, `interfaces`, `dynamics_models`

**Optional tables (emitted on demand):**

- `connectivity_groups` — with `--connectivity-detail`
- `node_breaker_detail`, `switch_detail`, `connectivity_nodes` — with `--node-breaker`
- `diagram_objects`, `diagram_points` — when DL profile is present (suppress with `--no-diagram`)
- `buses_solved`, `generators_solved`, `switched_shunts_solved` — when `case_mode = solved_snapshot` (v0.8.5+)
- `facts_devices`, `facts_solved` — optional FACTS extension tables (v0.8.6+)

### Detached island policy

| Policy | Flag | Behavior |
| --- | --- | --- |
| Permissive | `--detached-island-policy permissive` *(default)* | Islands without a slack bus are kept with a warning |
| Strict | `--detached-island-policy strict` | Aborts if any detached island is found |
| Prune | `--detached-island-policy prune-detached` | Silently removes detached islands before writing |

### Tested CGMES v3.0 conformity cases (11/11 passing, 44 variants)

| Case | Profiles | Notes |
| --- | --- | --- |
| FullGrid-Merged | EQ + TP | Large multi-TSO assembled case |
| MiniGrid-Merged | EQ + TP | Minimal conformity case |
| SmallGrid-Merged | EQ + TP + DL | Standard small test grid with diagram layout |
| RealGrid-Merged | EQ + TP | Representative real-network scale |
| Svedala-Merged | EQ + TP + DL | Swedish TSO reference with diagram layout |
| PowerFlow | EQ + TP + SV + SSH | Explicit power-flow validation case |
| PST Type1 | EQ + TP + SV + SSH + DL | PhaseTapChangerLinear |
| PST Type2 | EQ + TP + SV + SSH + DL | PhaseTapChangerLinear variant |
| PST Type3 | EQ + TP + SV + SSH + DL | PhaseTapChangerTable |

## Data Contract (Locked)

- Current schema contract: v0.9.0 (CGMES 3.0+ only)
- Canonical source: raptrix-cim-arrow/src/schema.rs
- Contract policy and semantics: docs/schema-contract.md
- Plain-English field guide: [docs/rpf-field-guide.md](docs/rpf-field-guide.md)
- Cross-repo propagation workflow: docs/release-sync-workflow.md
- **CGMES Ingest Target**: v3.0+ and later only (complete merged profiles; auto-detect and explicit mode supported)

This repository is the source of truth for the RPF file standard. The canonical wire contract lives in `docs/schema-contract.md` and `raptrix-cim-arrow/src/schema.rs`, with `docs/rpf-field-guide.md` as the human-readable companion.

RPF standardization here is intentional: it enables direct CIM-to-powerflow interchange without requiring a PSS/E conversion hop in the critical data path.

### Versioning Policy

Raptrix uses split versioning by design: schema contract version and crate release version evolve independently. The file-format contract is now locked at schema `v0.9.0` for interoperability and deterministic CGMES 3.0+ ingest behavior, while the converter crate release tracks implementation maturity and is currently `0.3.0`.

This split preserves compatibility guarantees for downstream tools at a given contract version. **Breaking change in v0.9.0**: readers in this repository now accept only v0.9.0 files; contracts below v0.9.0 must be migrated before ingestion.

**v0.9.0**: Removes `ibr_devices` table (IBRs unified into `generators` via `is_ibr`), adds operational-outcome columns to `contingencies`, adds analysis-readiness fields to `metadata`, and introduces the optional `scenario_context` table.

To keep crate and documentation versions consistent, use the version sync helper:

- `./scripts/sync-versions.ps1 -Version 0.3.0`
- CI also enforces this via `.github/workflows/version-consistency.yml`.

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
- Contingency derivation: when switch/open-state data is available, contingency rows are derived from switch state payloads; split-bus `split_bus` placeholder elements are still emitted when TP groups indicate multi-node topological buses.
- Voltage provenance: bus and branch-side nominal kV columns are now emitted when CGMES `BaseVoltage` joins are available, so downstream tools can reason about base voltage without reverse-parsing names.
- Contingency identity: contingency elements now carry generic `equipment_kind` and `equipment_id` fields for switch and split-bus workflows that do not map cleanly to branch/gen/load IDs.
- Nullability policy: the new 0.7 voltage and contingency identity fields are nullable by design when the source CIM payload cannot support an honest value; the writer should emit null rather than fabricate semantics.
- Dynamics derivation: when DY profile models are present, `dynamics_models` is populated from DY-linked equipment references and numeric model parameters; when DY is partial, unmatched generators fall back to EQ `SynchronousMachine` parameters (`H`, `xd_prime`, `D`, `mbase_mva`) with inferred `model_type` (`GENROU`, `GENCLS`, or `SYNC_MACHINE_EQ`). If no generator-linked dynamics can be built, a placeholder row is emitted and marked via metadata.
- Dynamics extensibility for Studio: `dynamics_models.model_type` is intentionally open-string so Studio can add new non-CIM models. Use a namespaced identifier such as `raptrix.smart_valve.v1` and keep parameters in `dynamics_models.params` as stable numeric key/value pairs.
- Benchmark note: on SmallGrid-scale datasets this merge substantially reduces bus count versus raw ConnectivityNode granularity and improves solve-stage matrix dimensions.

Contribution guidance:

- Follow the `SynchronousMachine` model/parser pattern when adding new CIM classes.
- Keep zero-copy semantics in hot parsing/mapping paths (`Cow`, borrowed refs, deterministic dense IDs).

Note: `.rpf` Arrow IPC is the locked target profile and the default writer output for this repository.

Current implementation priority is a clean and testable path to Arrow/Parquet output, while keeping APIs simple for incremental model coverage.

## Performance Snapshot

Release-mode local benchmarks (WSL2 Ubuntu, optimized build, April 23 2026):

Parser fragment throughput (`cargo test --release benchmark_fragment_parse_speed -- --nocapture`):

- `ACLineSegment`: 50,000 parses in 66.283 ms (~754,344 parses/s)
- `EnergyConsumer`: 50,000 parses in 55.156 ms (~906,528 parses/s)

RPF write/read throughput (`cargo test --release benchmark_write_complete_rpf_round_trip -- --ignored --nocapture`):

- 1,000-branch case:
  - Write: 42 ms, 2.54 MiB output, ~23,268 branches/s
  - Read: 18 tables in ~0 ms, ~21,311 tables/s
- 10,000-branch case:
  - Write: 738 ms, 24.60 MiB output, ~13,546 branches/s
  - Read: 18 tables in 4 ms, ~4,443 tables/s

These are local engineering reference numbers, not vendor comparison claims. Use them for regression tracking between commits and release tags.

## Project Layout

- raptrix-cim-arrow/src/schema.rs: v0.9.0 table schemas, metadata constants, and schema registry helpers
- raptrix-cim-arrow/src/io.rs: generic root `.rpf` assembly, validation, readback, and summary helpers
- src/models: CIM data structures and traits
- src/parser.rs: parse helpers and EQ-to-branch mapping
- src/rpf_writer.rs: CIM-specific mapping from parsed CGMES content into canonical table batches

### Contract Design Rationale (short)

v0.9.0 formalizes the design decision to treat modern-grid constructs (IBRs, DERs,
advanced flow-control devices, and DC workflows) as first-class components of the RPF interchange.
This README points to `docs/schema-contract.md` for full normative rationale and
required writer obligations.

### Locked contract v0.9.x notable fields

- v0.9.0 additions:
  - Removed `ibr_devices` table; IBRs now use `generators.is_ibr = true` + `ibr_subtype`
  - `contingencies` table: 6 new nullable operational-outcome columns
  - `metadata` table: 5 new nullable analysis-readiness fields; `case_mode` now accepts `hour_ahead_advisory`
  - New optional `scenario_context` table for structured analysis context
- v0.8.6 additions:
  - Optional `facts_devices` table for explicit FACTS device identity and control metadata
  - Optional `facts_solved` table for solved FACTS replay semantics
  - FACTS branch control columns and metadata feature flags (`raptrix.features.facts`, `raptrix.features.facts_solved`)
- v0.8.5 additions:
  - Optional `switched_shunts_solved` table for solved shunt switching state round-trip
  - `metadata.slack_bus_id_solved` and `metadata.angle_reference_deg` for explicit solved angle-reference provenance
  - `metadata.solved_shunt_state_presence` for solved shunt-state availability semantics
- v0.8.4 additions (planning-vs-solved semantics):
  - `metadata.case_mode` required — `flat_start_planning` | `warm_start_planning` | `solved_snapshot`
  - `metadata.solved_state_presence` nullable — `actual_solved` | `not_available` | `not_computed`
  - `metadata.solver_version`, `solver_iterations`, `solver_accuracy`, `solver_mode` nullable — only populated for `solved_snapshot` cases
  - Optional `buses_solved` and `generators_solved` tables — only present for `solved_snapshot` cases
  - Hard validation: exporter rejects NaN planning fields; rejects `solved_snapshot` without solver provenance; rejects solver provenance on planning cases
  - See [docs/rpf-field-guide.md](docs/rpf-field-guide.md) for a plain-English explanation of these semantics
- v0.8.3 additions:
  - `switched_shunts.b_init_pu` nullable authoritative initial susceptance per-unit field
  - Readers should prefer `b_init_pu` when present instead of reconstructing from `b_steps + current_step`
- v0.8.2 required additions:
  - `buses.bus_uuid` is required (non-null)
  - `metadata.source_case_id` required
  - `metadata.snapshot_timestamp_utc` required
  - `metadata.case_fingerprint` required
  - `metadata.validation_mode` required (`topology_only` or `solved_ready`)
- Added optional dictionary-encoded `name` columns to:
  - branches
  - generators
  - loads
  - transformers_2w
  - transformers_3w
- Added nullable nominal-kV columns to:
  - buses
  - branches
  - transformers_2w
  - transformers_3w
- Added generic contingency element identity fields:
  - `equipment_kind`
  - `equipment_id`
- Existing `buses.name` remains required and now prioritizes CIM human-readable names with deterministic fallback.
- `src/main.rs`: production CLI for CGMES-to-RPF conversion
- `src/test_utils.rs`: test-only path helper for external CGMES data
- `tests/integration_parse.rs`: ignored live-data integration test

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

Create a first `.rpf` artifact from the SmallGrid case using auto-detect mode:

- `cargo run --release -- convert --input-dir "C:\raptrix-cim-tests\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0\SmallGrid\SmallGrid-Merged" --output tests/data/external/smallgrid.rpf`

Then inspect table counts and coverage:

- `cargo run --release -- view --input tests/data/external/smallgrid.rpf`

The `view` command prints table-by-table row counts for quick import checks in `raptrix-core` and `raptrix-studio`.

Use `--verbose` when validating interoperability because it also prints the root Arrow IPC metadata entries, including `raptrix.version`, `raptrix.features.node_breaker`, `raptrix.features.contingencies_stub`, `raptrix.features.dynamics_stub`, and `rpf.rows.*` logical row counts used by compliant external parsers.

## Library Usage

Use the CIM converter directly from Rust:

```rust
use raptrix_cim_rs::{
    write_complete_rpf_with_options,
    rpf_writer::{WriteOptions, CaseMode},
};

fn convert_planning(eq_path: &str, output_path: &str) -> anyhow::Result<()> {
    // Default: flat-start planning case — no solved state
    write_complete_rpf_with_options(
        &[eq_path],
        output_path,
        &WriteOptions::default(),
    )
}
```

Use `write_complete_rpf` for the simple one-call form:

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

If Windows Application Control (WAC) blocks native test binaries (`os error 4551`), run tests through WSL2:

- `./scripts/test-wsl.ps1 -CargoCommand "test --workspace"`

VS Code task shortcut:

- `Terminal -> Run Task -> Test Workspace (WSL)`

Run all normal tests:

- cargo test

Run parser throughput test with printed rates:

- cargo test benchmark_fragment_parse_speed -- --nocapture

Run live SmallGrid integration test (PowerShell):

1. $env:RAPTRIX_TEST_DATA_ROOT = "C:\raptrix-cim-tests\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0"
2. cargo test parse_smallgrid_eq_aclinesegment -- --ignored --nocapture

Run CLI in auto-detect mode:

- cargo run --release -- convert --input-dir cgmes_case/ --output case.rpf

Enable `rdf:about` fallback diagnostics only when debugging parser edge cases:

1. `$env:RAPTRIX_LOG_RDF_ABOUT_FALLBACK = "1"`
2. `cargo test --test integration_parse -- --ignored --nocapture`

## Running Automated Validation

The repository includes a standalone pytest validator at `tests/inspect_rpf.py` that:

- runs the CLI to generate a `.rpf` file from SmallGrid EQ input
- validates one canonical root IPC batch with all 18 required struct columns
- verifies `raptrix.branding` and `raptrix.version` metadata
- checks bus and branch row counts against source EQ XML topology
- spot-checks first branch `r`/`x` values against EQ XML

Prerequisites:

- `RAPTRIX_TEST_DATA_ROOT` points to the CGMES v3.0 dataset root
- `pyarrow` and `pytest` are available in your Python environment

Run:

- `python -m pytest tests/inspect_rpf.py -q`

If external data is unavailable, the test is marked/treated as ignored and skipped.

## Required Regression Gate

Every non-trivial parser, mapper, schema, and writer change must run the full
RPF matrix regression before merge.

One-line Cargo command:

- `cargo rpf-regression -- --data-root C:\raptrix-cim-tests\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0 --profiles both --clean`

If `RAPTRIX_TEST_DATA_ROOT` is already set, `--data-root` can be omitted:

- `cargo rpf-regression -- --profiles both --clean`

Outputs are written to:

- `tests/data/external/results/debug`
- `tests/data/external/results/release`
- `tests/data/external/results/report.md`
- `tests/data/external/results/report.json`

Strict multi-profile check (includes SSH and DY inputs):

- `cargo rpf-regression -- --data-root C:\raptrix-cim-tests\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0 --profiles both --clean --include-ssh-dy`

Interpretation of failures:

- If conversion fails, no `.rpf` is emitted for that run (fail-fast behavior).
- This does **not** indicate a corrupted `.rpf`; it indicates conversion aborted.
- In strict mode, SSH/DY failures indicate profile-ingest coverage gaps and can
  imply missing operational/dynamic context in outputs if those profiles are
  excluded.

## External CGMES Setup

1. Download ENTSO-E CGMES v3.0 test configurations from:
  [ENTSO-E CIM for Grid Models Exchange](https://www.entsoe.eu/data/cim/cim-for-grid-models-exchange/)
2. Unzip to a local path, for example:
   C:\raptrix-cim-tests\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0
3. Set RAPTRIX_TEST_DATA_ROOT to that v3.0 folder.

Expected SmallGrid EQ location pattern:

- <RAPTRIX_TEST_DATA_ROOT>\SmallGrid\SmallGrid-Merged\SmallGrid_EQ.xml

If RAPTRIX_TEST_DATA_ROOT is not set, ignored integration tests can be skipped safely.

Redistribution note:

- The ENTSO-E datasets referenced above are external source data and are not redistributed in this repository.
- Keep test datasets local and out of source control to respect source licensing and repository policy.

## Test Data Policy

- tests/data/fixtures: tiny committed XML snippets only
- tests/data/external: placeholder path for local links or local files
- tests/data/large and data: ignored for large datasets

Large model archives should stay outside the repository.

## Known Limits

- CGMES 2.4.x is not supported; all ingest is CGMES 3.0+ only.
- Multi-TSO cases where separate EQ files exist per authority set (e.g., MicroGrid BE + NL TSOs) require a pre-merged single EQ file or passing all files together with `--input-dir`. Auto-detect selects one EQ file per directory — use the pre-merged case directories when available.
- BaseVoltage joins cover core equipment types; CIM models that omit BaseVoltage links may produce `unknown` labels in nominal-kV columns.
- If CGMES metadata is absent, `base_mva` and `frequency_hz` use CLI defaults (100 MVA, 60 Hz); set explicitly for non-60 Hz systems.
- No official back-converter from RPF to PSS/E or other vendor formats; the MPL 2.0 license permits community implementations.

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

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix PowerFlow

Copyright (c) 2026 Raptrix PowerFlow
`n`n
