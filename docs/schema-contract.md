# Schema Contract (Locked contract: v0.8.0 — CGMES 3.0+ Only)

## Contract Policy

- Schema changes are explicit and versioned.
- Column order is stable and treated as part of the contract.
- Column type and nullability changes require a version bump and migration note.

## Compatibility Policy

- **CGMES Ingest Target**: v3.0 and later only (complete merged profiles with EQ, TP, SV, DL, GL, SSH, etc.).
- **Legacy Support Dropped**: CGMES 2.4.x support was removed in v0.8.0. All ingest is now CGMES 3.0+ only. This enables cleaner parsing logic, better performance, and full alignment with ENTSO-E Conformity Assessment Scheme (v3.0.3 current).
- The `.rpf` contract is forward compatible for additive changes only. Readers must ignore unknown trailing root columns and unknown file metadata keys.
- Breaking file-format changes (required column rename/removal/reorder, required table rename/removal/reorder, type change for required fields) require a MAJOR contract bump.
- Additive changes (new optional columns, new optional tables, new optional metadata keys) require at least a MINOR bump.
- PATCH bumps are reserved for non-structural fixes: bug fixes, metadata text fixes, and documentation clarifications without wire-shape changes.

## 0.8 Nullability Guidance

- The new 0.7 fields are important, but they are not universally recoverable from every accepted CIM dataset.
- Writers should emit null for `nominal_kv`, `from_nominal_kv`, `to_nominal_kv`, `nominal_kv_h`, `nominal_kv_m`, `nominal_kv_l`, `equipment_kind`, and `equipment_id` when the source payload cannot support an honest value.
- Writers should not fabricate these fields from lossy heuristics just to satisfy a non-null contract.
- Solver-ready or production ingestion pipelines may enforce stricter local validation, for example rejecting files with unresolved nominal-kV fields on core network rows.

## File Metadata Keys

Every `.rpf` file must include:

- `raptrix.branding`
- `raptrix.version`

Current locked values:

- `raptrix.version = 0.8.0`
- `raptrix.branding = Raptrix CIM-Arrow / PowerFlow Interchange v0.8.0 - High-performance open profile by Musto Technologies LLC. Copyright (c) 2026 Musto Technologies LLC.`

Optional file-level metadata keys:

- `raptrix.features.node_breaker = true` when optional node-breaker detail tables are emitted
- `raptrix.features.diagram_layout = true` when optional IEC 61970-453 diagram layout tables are emitted
- `raptrix.features.contingencies_stub = true` when contingencies table is populated by placeholder/stub rows
- `raptrix.features.dynamics_stub = true` when dynamics_models table is populated by placeholder/stub rows
- `rpf.rows.<table_name> = <row_count>` for each emitted table

## File Container Layout

`.rpf` is a standard Arrow IPC File container, not a custom binary framing. A compliant reader must:

1. Open the file as Arrow IPC File format.
2. Read the root schema metadata.
3. Read one root record batch.
4. Interpret each root column as one table encoded as a nullable `StructArray`.

Current writer behavior emits exactly one root record batch. A future writer may emit more than one root batch, so readers should iterate record batches and reconstruct tables by root column name rather than assuming a single batch forever.

## Root Column Ordering

Required root columns are in this exact order:

1. `metadata`
2. `buses`
3. `branches`
4. `generators`
5. `loads`
6. `fixed_shunts`
7. `switched_shunts`
8. `transformers_2w`
9. `transformers_3w`
10. `areas`
11. `zones`
12. `owners`
13. `contingencies`
14. `interfaces`
15. `dynamics_models`

Optional root columns, when present, are appended after the required columns in this order:

16. `node_breaker_detail`
17. `switch_detail`
18. `connectivity_nodes`
19. `diagram_objects`
20. `diagram_points`

`connectivity_groups` is an optional detail table emitted only in connectivity-detail mode and is appended after the required root columns when that mode is active.

## Table Reconstruction Rules

Each root struct column may be null-padded to the maximum row count of any emitted table in the root batch. A compliant parser must use `rpf.rows.<table_name>` metadata, when present, as the authoritative logical row count for each table and trim any padded null tail beyond that count.

Recommended read algorithm:

1. Open Arrow IPC file and collect root schema metadata.
2. For each root column name, look up the expected schema by table name.
3. Downcast the root column to `StructArray`.
4. Trim each child array to `rpf.rows.<table_name>` rows.
5. Reconstruct the logical table record batch from the trimmed child arrays.

Readers should ignore unknown trailing root columns for forward compatibility, but they must reject reordered or renamed required root columns.

## Canonical Schema Source

The executable contract is defined in `raptrix-cim-arrow/src/schema.rs` and exported through the shared `raptrix-cim-arrow` crate:

- `all_table_schemas()` for canonical ordering
- `table_schema(name)` for table lookup

Generic root Arrow IPC file assembly, validation, readback, and metadata inspection live beside the schema in `raptrix-cim-arrow/src/io.rs`.

## Locked Tables

Required tables (empty tables allowed):

- `metadata`
- `buses`
- `branches`
- `generators`
- `loads`
- `fixed_shunts`
- `switched_shunts`
- `transformers_2w`
- `transformers_3w`
- `areas`
- `zones`
- `owners`
- `contingencies`
- `interfaces`
- `dynamics_models`

Optional detail table (emitted only in connectivity-detail mode):

- `connectivity_groups`

Optional detail tables (emitted only when `raptrix.features.node_breaker = true`):

- `node_breaker_detail`
- `switch_detail`
- `connectivity_nodes`

Optional diagram layout tables (emitted only when `raptrix.features.diagram_layout = true`):

- `diagram_objects`
- `diagram_points`

## Column Reference

This section is normative for external parser authors.

### metadata

- `base_mva`: Float64, required
- `frequency_hz`: Float64, required
- `psse_version`: Int32, required
- `study_name`: Dictionary<Int32, Utf8>, required
- `timestamp_utc`: Utf8, required
- `raptrix_version`: Utf8, required
- `is_planning_case`: Boolean, required
- `custom_metadata`: Map<String, String>, nullable

### buses

- `bus_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `type`: Int8, required
- `p_sched`: Float64, required
- `q_sched`: Float64, required
- `v_mag_set`: Float64, required
- `v_ang_set`: Float64, required
- `q_min`: Float64, required
- `q_max`: Float64, required
- `g_shunt`: Float64, required
- `b_shunt`: Float64, required
- `area`: Int32, required
- `zone`: Int32, required
- `owner`: Int32, required
- `v_min`: Float64, required
- `v_max`: Float64, required
- `p_min_agg`: Float64, required
- `p_max_agg`: Float64, required
- `nominal_kv`: Float64, nullable

### branches

- `branch_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Dictionary<Int32, Utf8>, required
- `r`: Float64, required
- `x`: Float64, required
- `b_shunt`: Float64, required
- `tap`: Float64, required
- `phase`: Float64, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `from_nominal_kv`: Float64, nullable
- `to_nominal_kv`: Float64, nullable

### generators

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `p_sched_mw`: Float64, required
- `p_min_mw`: Float64, required
- `p_max_mw`: Float64, required
- `q_min_mvar`: Float64, required
- `q_max_mvar`: Float64, required
- `status`: Boolean, required
- `mbase_mva`: Float64, required
- `H`: Float64, required
- `xd_prime`: Float64, required
- `D`: Float64, required
- `name`: Dictionary<UInt32, Utf8>, nullable

### loads

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `p_mw`: Float64, required
- `q_mvar`: Float64, required
- `name`: Dictionary<UInt32, Utf8>, nullable

### fixed_shunts

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `g_mw`: Float64, required
- `b_mvar`: Float64, required

### switched_shunts

- `bus_id`: Int32, required
- `status`: Boolean, required
- `v_low`: Float64, required
- `v_high`: Float64, required
- `b_steps`: List<Float64>, required
- `current_step`: Int32, required

### transformers_2w

- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Dictionary<Int32, Utf8>, required
- `r`: Float64, required
- `x`: Float64, required
- `winding1_r`: Float64, required
- `winding1_x`: Float64, required
- `winding2_r`: Float64, required
- `winding2_x`: Float64, required
- `g`: Float64, required
- `b`: Float64, required
- `tap_ratio`: Float64, required
- `nominal_tap_ratio`: Float64, required
- `phase_shift`: Float64, required
- `vector_group`: Dictionary<Int32, Utf8>, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `from_nominal_kv`: Float64, nullable
- `to_nominal_kv`: Float64, nullable

### transformers_3w

- `bus_h_id`: Int32, required
- `bus_m_id`: Int32, required
- `bus_l_id`: Int32, required
- `star_bus_id`: Int32, nullable
- `ckt`: Dictionary<Int32, Utf8>, required
- `r_hm`: Float64, required
- `x_hm`: Float64, required
- `r_hl`: Float64, required
- `x_hl`: Float64, required
- `r_ml`: Float64, required
- `x_ml`: Float64, required
- `tap_h`: Float64, required
- `tap_m`: Float64, required
- `tap_l`: Float64, required
- `phase_shift`: Float64, required
- `vector_group`: Dictionary<Int32, Utf8>, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `nominal_kv_h`: Float64, nullable
- `nominal_kv_m`: Float64, nullable
- `nominal_kv_l`: Float64, nullable

### areas

- `area_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `interchange_mw`: Float64, nullable

### zones

- `zone_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required

### owners

- `owner_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required

### contingencies

- `contingency_id`: Dictionary<Int32, Utf8>, required
- `elements`: List<Struct>, required

`elements` fields:

- `element_type`: Dictionary<Int32, Utf8>, required
- `branch_id`: Int32, nullable
- `bus_id`: Int32, nullable
- `gen_id`: Dictionary<Int32, Utf8>, nullable
- `load_id`: Dictionary<Int32, Utf8>, nullable
- `amount_mw`: Float64, nullable
- `status_change`: Boolean, required
- `equipment_kind`: Dictionary<Int32, Utf8>, nullable
- `equipment_id`: Dictionary<Int32, Utf8>, nullable

### interfaces

- `interface_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `monitored_branches`: List<Int32>, required
- `transfer_limit_mw`: Float64, required

### dynamics_models

- `bus_id`: Int32, required
- `gen_id`: Dictionary<Int32, Utf8>, required
- `model_type`: Dictionary<Int32, Utf8>, required
- `params`: Map<String, Float64>, required

Dynamics population rules for downstream consumers:

- Writers SHOULD prefer DY-profile-linked model rows when CGMES DY input is present and references a known generator.
- Writers SHOULD include parsed numeric DY parameters in `params` using normalized lowercase keys derived from CIM field names.
- When DY coverage is partial, writers SHOULD fall back to EQ-derived rows for unmatched generators to preserve generator coverage.
- When no generator-linked dynamics can be derived from DY or EQ, writers MAY emit a placeholder row and set `raptrix.features.dynamics_stub = true`.
- `model_type` is an open string vocabulary. Writers MAY emit CIM class names (for example `SynchronousMachineDynamics`) or extension names (for example `raptrix.smart_valve.v1`).
- For non-CIM extensions, writers SHOULD use namespaced `model_type` values and namespaced `params` keys to avoid collisions.
- Provenance keys currently emitted in `params` are:
	- `source_dy = 1.0` for DY-linked rows
	- `source_eq_fallback = 1.0` for EQ fallback rows
	- `source_stub = 1.0` for placeholder rows

### connectivity_groups

- `topological_bus_id`: Int32, required
- `topological_node_mrid`: Dictionary<Int32, Utf8>, required
- `connectivity_node_mrids`: List<Utf8>, required
- `connectivity_count`: Int32, required

### node_breaker_detail

- `switch_id`: Dictionary<Int32, Utf8>, required
- `switch_type`: Dictionary<Int32, Utf8>, required
- `from_bus_id`: Int32, nullable
- `to_bus_id`: Int32, nullable
- `connectivity_node_a`: Dictionary<Int32, Utf8>, nullable
- `connectivity_node_b`: Dictionary<Int32, Utf8>, nullable
- `is_open`: Boolean, nullable
- `normal_open`: Boolean, nullable
- `status`: Boolean, nullable

### switch_detail

- `switch_id`: Dictionary<Int32, Utf8>, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `switch_type`: Dictionary<Int32, Utf8>, required
- `is_open`: Boolean, nullable
- `normal_open`: Boolean, nullable
- `retained`: Boolean, nullable

### connectivity_nodes

- `connectivity_node_mrid`: Dictionary<Int32, Utf8>, required
- `topological_node_mrid`: Dictionary<Int32, Utf8>, nullable
- `bus_id`: Int32, nullable

## Optional Tables: diagram_objects and diagram_points

RPF v0.8.0 includes two optional Arrow tables for persisted one-line layout, aligned with IEC 61970-453 `DiagramObject` and `DiagramObjectPoint`. These tables are intended for viewer/editor workflows and are additive only: when absent, downstream tools may synthesize layout at runtime; when present, tools should restore the saved layout exactly. The payload is carried inside the standard Apache Arrow IPC `.rpf` root container and may be derived from CGMES RDF/XML diagram layout content commonly exchanged under IEC 61970-501 CGMES profile sets.

The two tables must be present together or both absent. A file with `diagram_objects` but no `diagram_points`, or vice versa, is malformed.

### diagram_objects

- `element_id`: Utf8, required. RPF-resolved layout key in namespaced form such as `bus:1`, `branch:1`, `generator:G1`, `fixed_shunt:SH1`, `breaker:BR1`, or `connectivity_node:CN1`.
- `element_type`: Utf8, required. Allowed values currently emitted by this writer include `bus`, `branch`, `generator`, `load`, `fixed_shunt`, `breaker`, and `connectivity_node`.
- `diagram_id`: Utf8, required. Named diagram view aligned with `cim:Diagram.name`; writers should prefer `overview` for the full-system one-line and use area/substation names for detail views.
- `rotation`: Float32, nullable. Clockwise rotation in degrees; null should be interpreted as zero.
- `visible`: Boolean, required. Whether the element is visible in the named diagram.
- `draw_order`: Int32, nullable. Z-order / drawing order; null should be interpreted as zero.

### diagram_points

- `element_id`: Utf8, required. Foreign key to `diagram_objects.element_id`.
- `diagram_id`: Utf8, required. Foreign key to `diagram_objects.diagram_id`.
- `seq`: Int32, required. Point ordering key aligned with IEC 61970-453 `DiagramObjectPoint.sequenceNumber`.
- `x`: Float64, required. Viewer-space X coordinate.
- `y`: Float64, required. Viewer-space Y coordinate.

### seq conventions by element_type

- `bus`: `seq=0` left endpoint of the bus bar, `seq=1` right endpoint.
- `branch`: `seq=0` from-end terminal, `seq=N` to-end terminal, intermediate values are bend vertices.
- `generator`, `load`, `fixed_shunt`: `seq=0` symbol center or connection point.
- `breaker`: `seq=0` terminal-1 side, `seq=1` terminal-2 side.
- `connectivity_node`: `seq=0` connection point.

### Coordinate convention

IEC 61970-453 uses an inverted-Y convention where larger Y values are lower on screen. Writers store the raw CIM values unchanged in `diagram_points.y`; renderers using a standard screen-space coordinate system should invert Y during display.

### Standard alignment and version

- Standard alignment: IEC 61970-453 `Diagram`, `DiagramObject`, and `DiagramObjectPoint`
- Exchange context: IEC 61970-501 CGMES RDF/XML profile sets, including merged datasets that carry diagram layout payloads
- Container format: Apache Arrow columnar IPC file layout already used by `.rpf`
- Introduced in: RPF v0.8.0

## Blocker Fixes Incorporated in Locked contract: v0.7.1

### 1) Expanded transformer detail

`transformers_2w` includes winding-level and vector fields:

- `winding1_r`, `winding1_x`
- `winding2_r`, `winding2_x`
- `nominal_tap_ratio`
- `vector_group` (dictionary string)

`transformers_3w` includes:

- per-leg impedance fields (`r_hm/x_hm`, `r_hl/x_hl`, `r_ml/x_ml`)
- `star_bus_id` (nullable Int32, fictitious star bus when present)
- `vector_group` (dictionary string)

### 2) Dynamics model table formalized

`dynamics_models` is locked with:

- `bus_id` (Int32)
- `gen_id` (dictionary string)
- `model_type` (dictionary string)
- `params` (Map<String, Float64>)

Compatibility alias: `dynamics` is accepted by `table_schema(name)`.

### 3) Contingency element payload tightened

`contingencies.elements` is a list of struct with explicit fields:

- `element_type` (dictionary string)
- `branch_id` (nullable Int32)
- `bus_id` (nullable Int32)
- `gen_id` (nullable dictionary string)
- `load_id` (nullable dictionary string)
- `amount_mw` (nullable Float64)
- `status_change` (Boolean)

Allowed `element_type` values are locked to:

- `branch_outage`
- `gen_trip`
- `load_shed`
- `shunt_switch`

### 4) Solved-result contingency scoping

Export-only solved-result tables must include:

- `contingency_id` (nullable dictionary string)

Semantics:

- `null` means base-case result.
- non-null values key each row to a contingency case.

The reusable schema helper is `solved_results_contingency_id_field()`.

### 5) TP merge policy (EQ + TP)

Default solver-facing bus construction is at TP `TopologicalNode` level:

- EQ `Terminal.ConnectivityNode` references are mapped to TP topological groups.
- Dense `buses.bus_id` values are assigned by sorted TopologicalNode mRID.
- `branches.from_bus_id` / `branches.to_bus_id` and `generators.bus_id` follow the collapsed topology.

This policy improves interoperability and reduces matrix dimensions versus raw
ConnectivityNode granularity while preserving CIM semantics.

Identifier compatibility note:

- TP parsing accepts either `rdf:ID` or `rdf:about` for `TopologicalNode` and
	`ConnectivityNode` identity extraction.
- When `rdf:about` is used, a leading `#` is stripped before mRID mapping.

### 6) Split-bus preservation via `connectivity_groups`

When connectivity-detail mode is requested, writers may emit
`connectivity_groups` with:

- `topological_bus_id` (Int32)
- `topological_node_mrid` (dictionary string)
- `connectivity_node_mrids` (List<Utf8>)
- `connectivity_count` (Int32)

This table preserves switchyard-level split-bus structure for ML and detailed
contingency analysis without changing core Locked contract: v0.7.0 table schemas.

### 7) `split_bus` contingency stub element

`contingencies.elements.element_type` now also permits:

- `split_bus`

Current writer behavior is stub-only (no breaker-status parsing yet). Stub
payload encodes:

- `topological_node_id`
- `connectivity_node_a`
- `connectivity_node_b`
- `breaker_mrid` (`stub` placeholder)

These values are serialized in the additive `equipment_kind` and `equipment_id`
fields to preserve strict Locked contract: v0.7.0 field layout while giving
switch and split-bus workflows a stable generic equipment identifier.

Current writer behavior for contingencies is hybrid:

- prefers switch-derived contingency rows when switch/open-state payloads are present
- emits `split_bus` placeholder elements only when split-bus topology hints are present
- emits `raptrix.features.contingencies_stub=true` only when placeholder contingency rows are present in the file

### 8) Optional node-breaker detail tables (opt-in only)

Locked contract: v0.7.0 adds optional node-breaker detail tables (`node_breaker_detail`, `switch_detail`, and `connectivity_nodes`) for operational CGMES fidelity and viewer workflows while preserving the strict core solver path. These tables are emitted only when explicitly requested with `--node-breaker` and are advertised in `.rpf` file-level Arrow IPC metadata with `raptrix.features.node_breaker=true`, so default power-flow ingest remains core tables only and preserves zero-copy performance semantics end-to-end (memory-mapped Arrow IPC to Arrow arrays with no additional allocations or copies on the default path).

## Parser Author Checklist

An independent parser is considered compliant if it:

1. Opens `.rpf` as Arrow IPC File format.
2. Verifies `raptrix.version = 0.7.0`.
3. Verifies required root columns appear in canonical order.
4. Uses `rpf.rows.<table_name>` metadata to trim padded null tails.
5. Treats the 15 required root columns as mandatory even when their logical row counts are zero.
6. Detects optional tables by root column presence and feature metadata, not by guesswork.
7. Ignores unknown future trailing root columns for forward compatibility.

## Compatibility Rules

- Additive columns should be appended and documented.
- Renaming or reordering columns is breaking.
- Removing columns is breaking.
- Type widening or narrowing is breaking unless consumers are migrated in lockstep.

## Change Checklist

1. Update `raptrix-cim-arrow/src/schema.rs` and any affected helpers in `raptrix-cim-arrow/src/io.rs`.
2. Update this file with version and column docs.
3. Add or update test coverage for schema construction and writer outputs.
4. Update README capability and known-limits sections.

Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
Copyright (c) 2026 Musto Technologies LLC
