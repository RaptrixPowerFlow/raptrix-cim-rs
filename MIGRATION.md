# Workspace Migration

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power

Copyright (c) 2026 Raptrix Power

## v0.14.1 (additive compatibility extension — dual-read of v0.14.0 / v0.13.x; no re-export required)

Trailing nullable facility-membership flags. Writers emit `v0.14.1`. Readers accept `v0.14.1` / `0.14.1` and retain `v0.14.0` / `0.14.0` / `v0.13.1` / `0.13.1` / `v0.13.0` / `0.13.0`. Older files pad the new columns as null.

### What changed

| Area | Action |
| --- | --- |
| Version gate | Accept `v0.14.1` / `0.14.1` **and** 0.14.0 / 0.13.x |
| Writers | Emit `v0.14.1`; converters leave membership **null** (do not invent BES from kV) |
| `branches`, `transformers_2w`, `transformers_3w`, `multi_section_lines` | Trailing nullable `is_secured`, `is_bes`, `is_bps`, `is_bptf` |
| Identity | `mrid`, `branch_id`, `line_id`, or terminals+`ckt` — **never** a 0-based vector index |
| Inheritance | Multi-section: non-null section row wins; else parent `multi_section_lines`; else unknown |
| Authoring | `enhance` spec key `facility_membership` |

### Consumer checklist

1. Accept `0.14.1` and prior dual-read versions at the version gate.
2. Treat missing membership columns as null (unknown).
3. Join overlays on `branch_id` / `mrid` / from-to-ckt, never vector indices.
4. Do not treat kV ≥ 100 as `is_bes=true`.

## v0.14.0 (additive MINOR — dual-read of v0.13.1 / v0.13.0; no re-export required)

True MINOR after the v0.13.1 compatibility-extension exception. Writers emit `v0.14.0`. Readers accept `v0.14.0` / `0.14.0` and retain `v0.13.1` / `0.13.1` / `v0.13.0` / `0.13.0`.

### What changed

| Area | Action |
| --- | --- |
| Version gate | Accept `v0.14.0` / `0.14.0` **and** 0.13.x |
| Writers | Emit `v0.14.0`; CIM converters leave new contingency columns null and omit sequences |
| `contingencies` | Trailing nullable `tpl_category`, `reserved` |
| `contingency_sequences` | Optional table; may be absent |
| Element token | Canonical `gen_trip`; `generator_trip` is a reader alias |

### Consumer checklist

1. Accept `0.14.0` and 0.13.x at the version gate.
2. Treat missing `tpl_category` / `reserved` as null (untagged / infer).
3. Treat a missing `contingency_sequences` table as “no in-file sequential pairs.”
4. Infer application: 1 element → N-1; 2+ → simultaneous; protection row → reserved simultaneous; sequence row / study pair → sequential.
5. Join machines on `generators.generator_id` (Int32) plus the documented string-label rule. There is no `generators.id`.

Downstream bump order: core PR-D on protection + study pairs first (works on 0.13.1); `reserved` / sequences additive after 0.14; Studio / psse-rs to crate 0.7.0 when they write the new fields.

## v0.13.1 (additive — dual-read of v0.13.0; no re-export required)

**Compatibility-extension exception:** new trailing optional columns on `computational_load_profiles` are stamped as `v0.13.1` (not `v0.14.0`) because readers ignore unknown trailing columns and `0.13.0` files remain valid without rewrite. Documented in `docs/schema-contract.md`.

### What changed

| Area | Action |
| --- | --- |
| Version gate | Accept `v0.13.1` / `0.13.1` **and** `v0.13.0` / `0.13.0` |
| Writers | Emit `v0.13.1` when producing enriched files |
| `computational_load_profiles` | Trailing optional: `voltage_transfer_curve`, `disturbance_counter`, `reconnection_params`, `voltage_measurement`, `protection_settings_provenance` |
| Existing CLP / `perc1_params` | Unchanged |

### Consumer checklist

1. Accept both `0.13.0` and `0.13.1` at the version gate.
2. Ignore unknown trailing CLP columns if not yet upgraded.
3. When reading v0.13.1 curves: null/empty curve → legacy scalar Phase D behavior.
4. Do not invent site settings; stamp illustrative curves as `study_assumption`.

Research freeze: `docs/V0131_VOLTAGE_TRANSFER_CURVE_RESEARCH.md`.

## v0.13.0 (breaking clean cut — re-export required)

**Product constraint:** no pre-0.13 compatibility path. Regenerate all goldens and case libraries through a v0.13.0-capable writer.

### What changed

| Area | Action |
| --- | --- |
| Version gate | Only `v0.13.0` / `0.13.0` accepted |
| `metadata.psse_version` | **Removed** |
| Provenance | **Add** nullable `source_format`, `source_format_version`, `source_identity_scheme` |
| `original_sentinel_case_id` | **Rename** → `baseline_source_case_id` |
| Timestamps | Utf8 RFC3339 → Arrow `Timestamp(Microsecond, UTC)` |
| `buses.type` | Int8 codes → Dictionary `PQ` / `PV` / `Slack` |
| `generators.controlled_bus_id` | Required + `0` sentinel → **nullable**; null = local |
| Loads / shunts | Optional trailing `mrid` |
| Dynamics | `classical_params` struct (prefer over map keys H/D/xd_prime/mbase_mva) |
| Computational load profiles | Large-load candidate columns; `trip_study_percentiles` are **0–100**, not fractions |
| Identity | Hybrid: dense `bus_id` FKs + optional mRID; stamp `rpf.identity.model=hybrid_solver_flat_v1` |

### Consumer checklist

1. Reject any file with `raptrix.version` ≠ 0.13.0.
2. Stop reading `psse_version` / `original_sentinel_case_id` / Int8 bus types / `controlled_bus_id == 0`.
3. Parse native UTC timestamps (or convert via Arrow).
4. Prefer `classical_params` when present for first-swing machines.
5. When `computational_load_mode` and profiles are non-empty, treat profiles as the default large-load candidate source; null/empty percentiles do **not** invent defaults in the wire contract (downstream study tools may apply their own configurable defaults).

### Not changed

- Generator power remains **MW/MVAr** (human-readable).
- Network injections remain **PU** on `metadata.base_mva`.
- FACTS tables remain multi-table by design.
- No upgrade CLI is provided.

## v0.12.5 (additive — no migration required)

### What changed

- **`buses.latitude` / `buses.longitude`**: nullable trailing `Float64` columns (WGS84 degrees) for operator-oriented relative bus layout.
- **GL profile ingest**: converters accept `--gl` / auto-detect `_GL`. CAS GeographicalLocation data typically attaches to `ACLineSegment` routes; endpoints map to from/to buses.
- **`SUPPORTED_RPF_VERSIONS`** accepts **`v0.12.5`** / **`0.12.5`** and retains **`v0.12.4`**–**`v0.12.1`**.

### Compatibility

- **No re-export required** for readers. Older bus tables without geo columns are null-padded on read.
- Re-export with a GL profile is required only when you want populated coordinates in new files.

### Reader upgrade checklist (downstream consumers)

- Accept **`v0.12.5`** / **`0.12.5`** in the RPF version gate; keep the explicit allowlist.
- Treat `buses.latitude` / `buses.longitude` as optional nullable trailing columns.

### Solve → re-export (patch-based)

- Solvers that load, solve, and re-emit an `.rpf` should overlay their results with `apply_rpf_patch` (C ABI `apply_rpf_patch_c`) rather than rebuilding the file from an in-memory solver projection. This preserves converter-owned tables (GIS, contingencies, RAS/SPS, diagrams, unknown enrichment).
- Ownership rules are defined in `docs/schema-contract.md` § Table Ownership. No wire-format change; this is a writer/pipeline guideline.

## v0.12.4 (additive — no migration required)

### What changed

- **Contract version tag and Power Interchange branding** advanced to v0.12.4 for current PowerFlow solved-snapshot exports.
- **New optional tables**: `q_limits_solved` (solved-state, 2 columns) and `feasibility_certificate_buses` (post-solve feasibility audit rows, 7 columns).
- **Read-compatibility dialects** documented and accepted for `multi_section_lines`, `dc_lines_2w`, `switched_shunt_banks`, and `generators_solved` as emitted by current solved-snapshot exports. Writers must continue to target the canonical layouts.
- **`SUPPORTED_RPF_VERSIONS`** accepts **`v0.12.4`** / **`0.12.4`** and retains **`v0.12.3`**, **`v0.12.2`**, and **`v0.12.1`** aliases.

### Compatibility

- **No re-export required.** v0.12.3 and earlier supported files remain readable.
- v0.12.4 solved-snapshot files may omit the nullable trailing `metadata` provenance columns introduced in v0.12.3; readers null-pad absent fields and reconstruct the canonical 45-column `metadata` shape.
- Generic readers now match root tables **by name** (fixed ordering is no longer required), tolerate nested list-item naming/nullability differences between conforming writers, and trim writer pad rows via `rpf.rows.*` row counts.

### Reader upgrade checklist (downstream consumers)

- Accept **`v0.12.4`** / **`0.12.4`** in the RPF version gate; keep the explicit allowlist (do not accept unknown future versions).
- Resolve root tables by column name, not position.
- Treat `q_limits_solved` and `feasibility_certificate_buses` as optional; absent in standard planning files.
- When validating struct layouts, match against the canonical layout first and the documented snapshot dialect second (see `docs/schema-contract.md`).

## v0.12.3 (additive — no migration required)

### What changed

- **baseline provenance** on `metadata`: ten nullable trailing columns for source-case upgrade tracking and convergence stats.
- **Change tracking** on optional `topology_changes`: nullable `change_source` and `applied_phase` dictionary columns.
- **`SUPPORTED_RPF_VERSIONS`** accepts **`v0.12.3`** / **`0.12.3`** and retains **`v0.12.2`** / **`0.12.2`** and **`v0.12.1`** / **`0.12.1`**.

### Compatibility

- **No re-export required.** v0.12.2 files remain readable; readers null-pad missing trailing columns.
- **New exports** stamp `rpf_version = v0.12.3` and emit all metadata columns (SAL fields null in standard CIM exports).

### Reader upgrade (raptrix-core C++ and downstream converters)

- Accept **`v0.12.3`** / **`0.12.3`** in the RPF version gate.
- Read optional nullable baseline provenance metadata fields; null means legacy or standard export.
- Read optional nullable `topology_changes.change_source` / `applied_phase`; absent columns are null-padded on read.

## v0.12.2 (additive — no migration required)

### What changed

- **Nullable `mrid` column** added to `branches`, `generators`, `transformers_2w`, and `transformers_3w`.
- **Schema metadata key `rpf.mrid_support = v1`** indicates stable equipment identifier support.
- **`SUPPORTED_RPF_VERSIONS`** accepts **`v0.12.2`** / **`0.12.2`** and retains **`v0.12.1`** / **`0.12.1`**.

### Compatibility

- **No re-export required.** v0.12.1 files remain readable; `mrid` columns are absent (null) in legacy files.
- **New exports** populate `mrid` from CIM source mRIDs where available.
- **Downstream guidance**: New `mrid` columns provide stable CIM-compatible identifiers. Downstream tools should prefer `mrid` for equipment_id mapping.

### Reader upgrade (raptrix-core C++ and downstream converters)

- Accept **`v0.12.2`** / **`0.12.2`** in the RPF version gate.
- Read optional nullable `mrid` on equipment tables; null means legacy file without stable identifiers.
- Prefer `mrid` over dense integer IDs for cross-system equipment mapping.

---

## v0.12.1 (breaking — re-export required)

### What changed

- **Schema contract bump** to v0.12.1 with unified optional tables: `remedial_action_schemes` (canonical RAS/SPS) and `contingency_island_analysis` (contingency topology filter audit rows).
- New optional file metadata: `raptrix.features.contingency_island_analysis`.
- **`SUPPORTED_RPF_VERSIONS`** accepts only **`v0.12.1`** / **`0.12.1`**.

### Compatibility

- **Re-export required.** v0.12.0 and all prior contract files are rejected by the version gate. Re-emit cases through a v0.12.1-capable writer.

### Reader upgrade (raptrix-core C++ and downstream converters)

- Accept **`v0.12.1`** / **`0.12.1`** in the RPF version gate.
- Detect optional `contingency_island_analysis` via `raptrix.features.contingency_island_analysis`; absent in standard planning files.
- Enable emission with `RootWriteOptions.include_contingency_island_analysis`.

---

## v0.11.0 (additive — no migration required)

### What changed

- Two new **optional** root tables: `protection_contingencies` (protection-driven outage sets; layered logical-group baseline + optional breaker-level `breaker_ids`) and `topology_changes` (declared post-event topology deltas; `provenance = declared` in this release).
- New optional file metadata: `raptrix.features.protection_contingencies`, `raptrix.features.topology_changes`, `rpf.protection.fidelity`.
- New doc-level `contingencies.elements.element_type` token `protection_event` (no wire-shape change).

### Compatibility

- **No re-export required.** v0.10.0 files remain valid and readable. The new tables are absent unless a writer opts in via `RootWriteOptions.include_protection_contingencies` / `include_topology_changes`. `SUPPORTED_RPF_VERSIONS` accepts v0.11.0 and retains v0.10.0 for reads.

### Reader upgrade (raptrix-core C++)

- Accept **`v0.11.0`** / **`0.11.0`** in the RPF version gate alongside v0.10.0.
- Detect the optional tables via `raptrix.features.protection_contingencies` / `raptrix.features.topology_changes`; both are absent in standard planning files.
- Phase 1 logical path (per `docs/adr/0001-protection-informed-contingencies.md`): read `protection_contingencies.tripped_elements` and apply it as a compound outage; respect `data_confidence` for logging; use `topology_change_id` to locate the matching `topology_changes` row. `breaker_ids` and `sequence` are optional refinements consumed in Phase 2.

---

## v0.9.5 (additive — no migration required)

### What changed

- `generators` gains required trailing column **`controlled_bus_id`** (Int32): PSS/E **IREG** / CIM **RegulatingControl** denormalized to dense `bus_id`. `0` or `bus_id` = local regulation.
- `metadata` gains nullable **`default_shunt_control_mode`** (dictionary-encoded string); optional file-level **`rpf.default_shunt_control_mode`** may mirror it for solver handoff (`planning_full` \| `real_time_hot_start` \| `real_time_frozen`).

### Compatibility

- **No re-export required.** v0.9.4 RPF files remain readable. Readers synthesize missing `controlled_bus_id` as **`0`**. Missing `default_shunt_control_mode` reads as null.

### Reader upgrade (raptrix-core C++)

- Accept **`v0.9.5`** / **`0.9.5`** in the RPF version gate alongside v0.9.4.
- Import **`controlled_bus_id`** after `params`; use **`0`** when the column is absent on legacy 24-column batches.
- Optionally read **`default_shunt_control_mode`** from the `metadata` row or `rpf.default_shunt_control_mode` file metadata; default consumer behavior may remain `planning_full` when absent.

---

## v0.3.4 / Schema v0.9.4 (Breaking)

### What changed

The `buses` table gains two new **required** (non-nullable) columns at positions 20–21:

- `qd_load_pu` (`Float64`, non-null): sum of in-service load reactive demand (QL) divided by SBASE; signed (positive for inductive, negative when PSS/E load QL < 0 for capacitive reactive injection).
- `qg_sched_pu` (`Float64`, non-null): sum of in-service generator scheduled reactive (QG) divided by SBASE, any sign.

The existing `q_sched` column (position 4) retains its canonical meaning as the **net scheduled injection**: `q_sched == qg_sched_pu - qd_load_pu` for all bus types. This identity is machine-checkable.

### Why

The prior schema had an overloaded `q_sched` column: different converters (Rust psse-rs vs. C++ core) could write different physical quantities into that column for PV/slack buses after the Q-limit enforcement fix in the C++ solver path. The new columns make the decomposition explicit and self-documenting so solvers can derive their internal PV/slack Q seed as `-qd_load_pu` without ambiguity.

### Backward compatibility

`SUPPORTED_RPF_VERSIONS` now accepts both `v0.9.4` / `0.9.4` (new) and `v0.9.3` / `0.9.3` (old) so the Rust reader is backward-compatible with existing v0.9.3 files. Any **writer** (psse-rs, core exporter) producing RPF must now populate both new columns.

### Reader upgrade (raptrix-core C++)

The `rpf_reader.cpp` version gate (`if (*rpf_version != "v0.9.3")`) must be updated to accept `"v0.9.4"`. Read `buses.qd_load_pu` with a fallback of `0.0` when the column is absent to retain backward compatibility with v0.9.3 files.

---

## v0.3.3 / Schema v0.9.3

- Nominal-kV columns are now strict required fields and must be finite and `> 0.0`:
  - `buses.nominal_kv`
  - `branches.from_nominal_kv`, `branches.to_nominal_kv`
  - `transformers_2w.from_nominal_kv`, `transformers_2w.to_nominal_kv`
  - `transformers_3w.nominal_kv_h`, `transformers_3w.nominal_kv_m`, `transformers_3w.nominal_kv_l`
- Reader compatibility is strict at v0.9.3 (`SUPPORTED_RPF_VERSIONS = v0.9.3 / 0.9.3`).

One-line re-ingest helper:

```bash
raptrix-cim-rs migrate-0.9.2-to-0.9.3 --input-dir <cgmes_dir> --output <case_v093.rpf>
```

If any required nominal-kV cannot be resolved from BaseVoltage (or resolves to non-positive), the writer now errors instead of silently emitting null.

## What Changed

This repository was refactored from a single crate into a Cargo workspace with two responsibilities:

- `raptrix-cim-rs`: CIM parsing, CGMES profile handling, row mapping, and CLI orchestration
- `raptrix-cim-arrow`: locked canonical schema definitions and generic `.rpf` Arrow IPC infrastructure

## v0.3.2 / Schema v0.9.2

- `generators` adds required `q_sched_mvar` (Float64) in canonical schema order immediately after `p_sched_mw`.
- Writers now populate `q_sched_mvar` from `RotatingMachine.q` when available.
- Readers enforce presence/type/nullability for `q_sched_mvar` through standard schema validation.

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

This boundary is intentional: the shared crate should not know how CIM, PSS/E, MATLAB, or other source formats are parsed. It should only know the canonical contract and how to emit and validate a compliant `.rpf` file.

## Why The Split Was Done

- keeps the locked RPF contract in one source of truth
- reduces duplication for additional converter repositories
- lets format-specific bugs and parser changes stay isolated from Arrow contract changes
- makes contract fixes available to every converter that depends on the shared crate

## How Other Converter Crates Should Reuse It

For another converter such as `raptrix-psse-rs`:

1. Depend on `raptrix-cim-arrow`
2. Parse the source format into canonical table rows or `RecordBatch` values
3. Use the schema helpers from `raptrix-cim-arrow` when constructing batches
4. Call `write_root_rpf` to emit the final `.rpf` file
5. Use `read_rpf_tables`, `summarize_rpf`, and `rpf_file_metadata` in tests to verify compatibility

That keeps all converters aligned on one exact Arrow schema contract and one exact root-file layout.

---

## Transformer Representation Contract (v0.8.7)

**Schema version**: v0.8.7 | **Crate version**: 0.2.7

### What changed

Every RPF file produced by v0.2.7+ now contains the required file-level metadata key:

```
rpf.transformer_representation_mode = native_3w | expanded
```

### Producer obligations

All writers must insert this key in the root Arrow IPC metadata before calling `write_root_rpf`.
`WriteOptions::default()` already sets the mode to `native_3w`, so existing callers that do not
opt in to `Expanded` receive the key automatically with no code changes required.

### Consumer / reader fallback semantics

Files produced before v0.8.7 will not contain the key.  Readers should treat a missing key as
`native_3w` (3-winding rows appear in `transformers_3w`, no synthetic star buses in
`transformers_2w`).

```rust
use raptrix_cim_arrow::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;

let mode = metadata
		.get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
		.map(String::as_str)
		.unwrap_or("native_3w");   // pre-v0.8.7 files default to native_3w
```

The reader helper `validate_transformer_representation_mode_value(value)` is available in
`raptrix_cim_arrow::schema` to validate the string before branching.

### Expanded mode — star bus range

When a file carries `rpf.transformer_representation_mode = expanded`:

- `transformers_3w` contains **zero active rows** (inactive rows may remain as bookkeeping).
- `transformers_2w` contains three synthetic legs per original 3-winding transformer.
- Synthetic star bus IDs are > 10 000 000 and should not appear in the `buses` table.
- Impedance conversion follows the delta→wye formula:
	- `r_h = (r_hm + r_hl − r_ml) / 2`
	- `r_m = (r_hm + r_ml − r_hl) / 2`
	- `r_l = (r_hl + r_ml − r_hm) / 2`
	- (same for `x`)

### Companion change (raptrix-psse-rs)

`raptrix-psse-rs` currently carries a local copy of `METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE`
at `src/lib.rs`.  Once v0.2.7 of `raptrix-cim-arrow` is published, that local copy should be
replaced with:

```rust
use raptrix_cim_arrow::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;
```

---

## Schema Contract 0.9.0 (Breaking)

**Schema version**: v0.9.0 | **Crate version**: 0.3.0

v0.9.0 is a hard breaking release. v0.8.9 files are rejected by the version gate in `io.rs`
even if their `ibr_devices` table was empty — they cannot be ingested without migration.
`SUPPORTED_RPF_VERSIONS` now accepts only `v0.9.0` / `0.9.0`.

### Removed: `ibr_devices` table

The `ibr_devices` table is permanently removed from the canonical table list. Writers must not
emit an `ibr_devices` root column in v0.9.0+ files.

**Migration**: Any code previously writing to `ibr_devices` must instead write IBRs into the
`generators` table with `is_ibr = true` and the appropriate `ibr_subtype` (e.g. `"SolarPV"`,
`"Wind"`, `"BESS"`, `"GenericIBR"`). The `generators` table has supported `is_ibr` and
`ibr_subtype` since v0.8.9.

### Extended: `contingencies` table — 6 new nullable columns

Six nullable operational-outcome columns are appended after `elements`. These are null
in standard planning files and populated in real-time analysis exports:

- `risk_score` (Float64, nullable)
- `cleared_by_reserves` (Boolean, nullable)
- `voltage_collapse_flag` (Boolean, nullable)
- `recovery_possible` (Boolean, nullable)
- `recovery_time_min` (Float64, nullable)
- `greedy_reserve_summary` (Utf8, nullable)

Readers that previously expected `contingencies` to have exactly 2 columns must be updated to
accept 8.

### Extended: `metadata` table — 5 new nullable fields

Five nullable analysis-readiness fields are appended at the end of the `metadata` row:

- `hour_ahead_uncertainty_band` (Float64, nullable)
- `commitment_source` (Utf8, nullable)
- `solver_q_limit_infeasible_count` (Int32, nullable)
- `pv_to_pq_switch_count` (Int32, nullable)
- `real_time_discovery` (Boolean, nullable)

`case_mode` now accepts the additional value `"hour_ahead_advisory"` in addition to the
existing `flat_start_planning`, `warm_start_planning`, and `solved_snapshot` values.

### New optional table: `scenario_context`

The `scenario_context` table is an optional analysis export table. It is absent from standard
planning files. Writers producing analysis exports should populate this table with one
row per flagged case. See `docs/schema-contract.md` for the full column reference.

---

## Schema Contract 0.9.1 (Additive)

**Schema version**: v0.9.1 | **Crate version**: 0.3.1

### What changed

`loads` now includes 4 optional ZIP-fidelity columns appended after `q_pu`:

- `p_i_pu` (constant-current active component, per-unit on system base)
- `q_i_pu` (constant-current reactive component, per-unit on system base)
- `p_y_pu` (constant-admittance active component, per-unit on system base)
- `q_y_pu` (constant-admittance reactive component, per-unit on system base)

Existing fields remain unchanged:

- `p_pu` is still constant-power active load.
- `q_pu` is still constant-power reactive load.

### Mapping guidance (PSS/E LOAD -> RPF loads)

Given system base `S_base`:

- `p_pu = PL / S_base`
- `q_pu = QL / S_base`
- `p_i_pu = IP / S_base`
- `q_i_pu = IQ / S_base`
- `p_y_pu = YP / S_base`
- `q_y_pu = YQ / S_base`

Writers must preserve source sign and emit `null` when source ZIP terms are absent.

### Compatibility

- This is a non-breaking additive change.
- Readers that ignore unknown fields continue to function unchanged.
- Readers implementing strict schema reconstruction should treat missing trailing nullable
  load ZIP columns as null when reading older files.

---

## Schema Contract 0.8.9 (Breaking)

**Schema version**: v0.8.9 | **Crate version**: 0.2.9

### Design Rationale

v0.8.9 formalizes contract updates for IBR-heavy operation, distributed flexibility,
advanced control workflows, and modern DC modeling. This is reflected in required root tables
and required metadata rather than optional add-ons.

### Compatibility Position

This release prioritizes a consistent canonical contract while preserving compatibility with
legacy workflows where practical. Contract design remains aligned with
IEC 61970 CIM semantics and power-system modeling fundamentals.

### Breaking support policy

- Reader support for contracts below v0.8.9 is deprecated and removed in this repository.
- `SUPPORTED_RPF_VERSIONS` now accepts only `v0.8.9` / `0.8.9`.
- Any file produced at v0.8.8 or below must be re-exported/migrated to v0.8.9 before ingestion.

### Required table changes

New required tables in canonical root order:

- `multi_section_lines`
- `dc_lines_2w`
- `ibr_devices`
- `switched_shunt_banks`

Arrow typing constraints for these tables are part of the wire contract:

- `multi_section_lines.section_branch_ids`: `list<int32>`
- `ibr_devices.params`: `map<string, float64>`
- Nullable fields remain nullable per schema contract for additive compatibility where possible.

### Generators table redesign (breaking)

The `generators` table is now a unified hierarchical contract for individual units,
IBR units, and aggregate DER records.

v0.8.9 canonical columns are:

- `generator_id`, `bus_id`, `name`
- `unit_type`, `hierarchy_level`, `parent_generator_id`, `aggregation_count`
- `status`, `is_ibr`, `ibr_subtype`
- `p_sched_mw`, `p_min_mw`, `p_max_mw`, `q_min_mvar`, `q_max_mvar`, `mbase_mva`
- `uol_mw`, `lol_mw`, `ramp_rate_up_mw_min`, `ramp_rate_down_mw_min`
- `owner_id`, `market_resource_id`, `params`

Notes:

- Legacy per-unit and direct dynamics columns (`p_sched_pu`, `p_min_pu`, `p_max_pu`,
  `q_min_pu`, `q_max_pu`, `H`, `xd_prime`, `D`) are no longer part of the `generators`
  table wire shape.
- Dynamics scalars remain expressible through `generators.params` when provided by source data.
- `fuel_type` is not part of the v0.8.9 `generators` contract.

### Ownership linkage changes

- `buses.owner_id` is now explicitly part of the contract (nullable).
- `branches.owner_id` is now explicitly part of the contract (nullable).
- `generators.owner_id` remains nullable for source datasets that lack owner attribution.

### Required metadata changes

`metadata` row now requires:

- `modern_grid_profile` (bool)
- `has_ibr` (bool)
- `has_smart_valve` (bool)
- `has_multi_terminal_dc` (bool)

New nullable metadata fields:

- `ibr_penetration_pct` (float64)
- `study_purpose` (utf8)
- `scenario_tags` (list<utf8>)

### Branch table changes

`branches` now includes additive linkage fields:

- `parent_line_id` (int32, nullable)
- `section_index` (int32, nullable)

### Switched shunt semantics

- `switched_shunts.b_steps` is now capacitive-only (positive values).
- Inductive steps must be represented in `switched_shunt_banks`.

### Writer obligations for modern-grid metadata

Writers must populate required v0.8.9 metadata flags:

- `modern_grid_profile`
- `has_ibr`
- `has_smart_valve`
- `has_multi_terminal_dc`

Writers should populate nullable context fields when known:

- `ibr_penetration_pct`
- `study_purpose`
- `scenario_tags`

### Backward compatibility boundaries

- Reader compatibility is intentionally strict at v0.8.9 only.
- Backward compatibility remains for additive nullable columns and empty required-table materialization within the v0.8.9 contract shape.
