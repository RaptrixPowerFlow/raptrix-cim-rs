# Schema Contract (Locked contract: v0.9.1 — CGMES 3.0+ Only)

This repository is the authoritative source of truth for the Raptrix PowerFlow Interchange (`.rpf`) wire contract used by CIM-first conversion pipelines.

v0.9.1 is the current contract and is an additive (non-breaking) release in this repository.

## v0.9.1 Additive Changes

- **`loads` table extended** with 4 new nullable ZIP-fidelity columns appended after `q_pu`:
  - `p_i_pu` (constant-current active component, per-unit on system base)
  - `q_i_pu` (constant-current reactive component, per-unit on system base)
  - `p_y_pu` (constant-admittance active component, per-unit on system base)
  - `q_y_pu` (constant-admittance reactive component, per-unit on system base)
- Existing `loads.p_pu` / `loads.q_pu` remain constant-power components with unchanged semantics.
- `SUPPORTED_RPF_VERSIONS` now accepts only `v0.9.1` / `0.9.1`.
- v0.9.0 files remain structurally compatible for additive readers that tolerate missing trailing nullable `loads` fields.

## v0.9.0 Breaking Changes

- **`ibr_devices` table removed.** IBRs are now modeled exclusively in the `generators` table using `is_ibr = true` and `ibr_subtype`. Files claiming v0.9.0 must not include an `ibr_devices` root column.
- **`contingencies` table extended** with 6 new nullable operational-outcome columns: `risk_score`, `cleared_by_reserves`, `voltage_collapse_flag`, `recovery_possible`, `recovery_time_min`, `greedy_reserve_summary`. These are null in standard planning files.
- **`metadata` table extended** with 5 new nullable analysis-readiness fields. `case_mode` now accepts the additional value `"hour_ahead_advisory"`.
- **New optional table `scenario_context`** for structured analysis context (real-time, hour-ahead advisory, planning feedback).
- **Canonical table count**: 18 required tables (was 19).
- `SUPPORTED_RPF_VERSIONS` now accepts `v0.9.1` / `0.9.1` and keeps `v0.9.0` / `0.9.0` for backward-compatible reads.

## Contract Design Rationale

- The v0.9.1 contract is designed for current grid models, including inverter-based resources (IBR), DER-heavy operation, advanced flow-control devices, and modern DC workflows.
- Required modern-grid tables (`multi_section_lines`, `dc_lines_2w`, `switched_shunt_banks`) are first-class contract elements, not side extensions.
- IBR modeling is unified in the `generators` table (`is_ibr = true`, `ibr_subtype`); no separate `ibr_devices` table.
- Arrow-native list and map types are used deliberately so parsers and solvers can ingest table payloads without lossy flattening.

## Compatibility Rationale

- This contract is not designed as a parity-first schema for any single legacy format.
- Interoperability with legacy toolchains may be achieved where practical, but the primary design goal is a stable, physically consistent interchange contract.
- The normative source remains IEC 61970 CIM semantics mapped into a stable Arrow contract for deterministic downstream ingestion.

## Contract Policy

- Schema changes are explicit and versioned.
- Column order is stable and treated as part of the contract.
- Column type and nullability changes require a version bump and migration note.

## Compatibility Policy

- **CGMES Ingest Target**: v3.0 and later only (complete merged profiles with EQ, TP, SV, DL, GL, SSH, etc.).
- **Legacy Support Dropped**: CGMES 2.4.x support was removed in v0.8.0. All ingest is now CGMES 3.0+ only. This enables cleaner parsing logic, better performance, and full alignment with ENTSO-E Conformity Assessment Scheme (v3.0.3 current).
- **CIM baseline**: raptrix-cim-rs targets IEC 61970 CIM 17+ classes and RDF/XML profile exchange directly.
- **Public validation corpus**: ENTSO-E CGMES v3.0.3 CAS remains the canonical public regression dataset.
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

- `raptrix.version = 0.9.1`
- `raptrix.branding = Raptrix CIM-Arrow / PowerFlow Interchange v0.9.1 - High-performance open CIM profile (CGMES 3.0+) by Raptrix PowerFlow. Copyright (c) 2026 Raptrix PowerFlow.`
- `rpf.case_fingerprint = <required deterministic case identity fingerprint>`
- `rpf.validation_mode = topology_only | solved_ready`
- `rpf.case_mode = flat_start_planning | warm_start_planning | solved_snapshot | hour_ahead_advisory` (v0.8.4+, required; `hour_ahead_advisory` added in v0.9.0)
- `rpf.solved_state_presence = actual_solved | not_available | not_computed` (v0.8.4+, required)

Optional file-level metadata keys:

- `raptrix.features.node_breaker = true` when optional node-breaker detail tables are emitted
- `raptrix.features.diagram_layout = true` when optional IEC 61970-453 diagram layout tables are emitted
- `raptrix.features.contingencies_stub = true` when contingencies table is populated by placeholder/stub rows
- `raptrix.features.dynamics_stub = true` when dynamics_models table is populated by placeholder/stub rows
- `raptrix.features.facts = true` when optional FACTS metadata table(s) are emitted (v0.8.6+)
- `raptrix.features.facts_solved = true` when optional `facts_solved` table is emitted (v0.8.6+)
- `rpf.rows.<table_name> = <row_count>` for each emitted table
- `rpf.solver.version = <string>` solver software version (only when `solved_state_presence = actual_solved`)
- `rpf.solver.iterations = <int>` Newton-Raphson iteration count (only when solved)
- `rpf.solver.accuracy = <float>` final mismatch residual (only when solved)
- `rpf.solver.mode = <string>` bus control mode, e.g. `PV`, `PV_to_PQ` (only when solved)
- `rpf.solver.slack_bus_id = <int>` the bus_id used as the angle reference (slack bus) in the solve (v0.8.5+, only when solved)
- `rpf.solver.angle_reference_deg = <float>` angle reference value in degrees, typically 0.0 (v0.8.5+, only when solved)
- `rpf.solver.solved_shunt_state_presence = actual_solved | not_available` (v0.8.5+, only when solved)
- `rpf.facts_solved_state_presence = actual_solved | not_available` (v0.8.6+, optional; defaults to `not_available` when `facts_devices` is present and `facts_solved` is absent)
- `rpf.transformer_representation_mode = native_3w | expanded` (v0.8.7+, **required**; readers treating files from pre-v0.8.7 producers should default to `native_3w` when the key is absent)
- `rpf.loads.zip_fidelity_presence = not_available | partial | complete` (v0.9.1+, optional; indicates whether `loads` ZIP fidelity columns are populated by source export path)

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
4. `multi_section_lines`
5. `dc_lines_2w`
6. `generators`
7. `loads`
8. `fixed_shunts`
9. `switched_shunts`
10. `switched_shunt_banks`
11. `transformers_2w`
12. `transformers_3w`
13. `areas`
14. `zones`
15. `owners`
16. `contingencies`
17. `interfaces`
18. `dynamics_models`

Optional root columns, when present, are appended after the required columns in this order:

19. `node_breaker_detail`
20. `switch_detail`
21. `connectivity_nodes`
22. `diagram_objects`
23. `diagram_points`
24. `buses_solved`
25. `generators_solved`
26. `switched_shunts_solved`
27. `facts_devices`
28. `facts_solved`
29. `scenario_context` (v0.9.0+, optional analysis context)

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
- `multi_section_lines`
- `dc_lines_2w`
- `generators`
- `loads`
- `fixed_shunts`
- `switched_shunts`
- `switched_shunt_banks`
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

Optional solved-state tables (emitted only when `case_mode = solved_snapshot`, v0.8.4+):

- `buses_solved`
- `generators_solved`
- `switched_shunts_solved` (v0.8.5+)

Optional FACTS tables (v0.8.6+, emitted only when FACTS metadata is present):

- `facts_devices`
- `facts_solved` (optional solved snapshot replay companion)

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
- `source_case_id`: Dictionary<Int32, Utf8>, required
- `snapshot_timestamp_utc`: Utf8, required
- `case_fingerprint`: Utf8, required
- `validation_mode`: Dictionary<Int32, Utf8>, required
- `custom_metadata`: Map<String, String>, nullable
- `case_mode`: Dictionary<Int32, Utf8>, required — `flat_start_planning` | `warm_start_planning` | `solved_snapshot` (v0.8.4+)
- `solved_state_presence`: Dictionary<Int32, Utf8>, nullable — `actual_solved` | `not_available` | `not_computed` (v0.8.4+)
- `solver_version`: Utf8, nullable — populated only when `solved_state_presence = actual_solved` (v0.8.4+)
- `solver_iterations`: Int32, nullable — Newton-Raphson iteration count (v0.8.4+)
- `solver_accuracy`: Float64, nullable — final mismatch residual norm (v0.8.4+)
- `solver_mode`: Dictionary<Int32, Utf8>, nullable — e.g. `PV`, `PV_to_PQ` (v0.8.4+)
- `slack_bus_id_solved`: Int32, nullable — bus_id of the angle reference (slack) bus used in the solve; prevents silent reference-frame mismatch when snapshots are re-used (v0.8.5+)
- `angle_reference_deg`: Float64, nullable — angle reference value in degrees applied at the slack bus; typically 0.0 (v0.8.5+)
- `solved_shunt_state_presence`: Dictionary<Int32, Utf8>, nullable — `actual_solved` | `not_available`; lets loaders fail fast or warn if solved snapshot claims solved but lacks full shunt state (v0.8.5+)
- `modern_grid_profile`: Boolean, required (v0.8.9+)
- `ibr_penetration_pct`: Float64, nullable (v0.8.9+)
- `has_ibr`: Boolean, required (v0.8.9+)
- `has_smart_valve`: Boolean, required (v0.8.9+)
- `has_multi_terminal_dc`: Boolean, required (v0.8.9+)
- `study_purpose`: Utf8, nullable (v0.8.9+)
- `scenario_tags`: List<Utf8>, nullable (v0.8.9+)
- `hour_ahead_uncertainty_band`: Float64, nullable (v0.9.0+) — load forecast uncertainty band as a percentage, e.g. `2.0` = ±2%
- `commitment_source`: Utf8, nullable (v0.9.0+) — e.g. `"day_ahead_market"`, `"operator_plan"`
- `solver_q_limit_infeasible_count`: Int32, nullable (v0.9.0+) — number of buses where Q-limit infeasibility was detected
- `pv_to_pq_switch_count`: Int32, nullable (v0.9.0+) — number of PV→PQ bus-type switches during solve
- `real_time_discovery`: Boolean, nullable (v0.9.0+) — `true` if this case originated from live State Estimator analysis

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
- `owner_id`: Int32, nullable
- `nominal_kv`: Float64, nullable
- `bus_uuid`: Dictionary<Int32, Utf8>, required

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
- `owner_id`: Int32, nullable
- `from_nominal_kv`: Float64, nullable
- `to_nominal_kv`: Float64, nullable
- `device_type`: Dictionary<Int32, Utf8>, nullable (v0.8.6+) — canonical token for SmartValve is `smartvalve`; reader normalization must accept alias `SV` (case-insensitive) and canonicalize to `smartvalve`.
- `control_mode`: Dictionary<Int32, Utf8>, nullable (v0.8.6+) — open vocabulary; recommended values include `series_impedance`, `phase_shift`, `voltage_injection`, `bypass`.
- `control_target_flow_mw`: Float64, nullable (v0.8.6+) — flow target used by flow-controlling FACTS.
- `x_min_pu`: Float64, nullable (v0.8.6+) — lower bound for effective series reactance in per-unit.
- `x_max_pu`: Float64, nullable (v0.8.6+) — upper bound for effective series reactance in per-unit.
- `injected_voltage_mag_pu`: Float64, nullable (v0.8.6+) — injected series-voltage magnitude in per-unit.
- `injected_voltage_angle_deg`: Float64, nullable (v0.8.6+) — injected series-voltage angle in degrees.
- `facts_params`: Map<String, Float64>, nullable (v0.8.6+) — additive vendor or model-specific scalar parameters.
- `parent_line_id`: Int32, nullable (v0.8.9+) — links branch sections to `multi_section_lines.line_id`.
- `section_index`: Int32, nullable (v0.8.9+) — ordered section index within a multi-section logical line.

### multi_section_lines

- `line_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Utf8, required
- `section_branch_ids`: List<Int32>, required
- `total_r_pu`: Float64, required
- `total_x_pu`: Float64, required
- `total_b_pu`: Float64, required
- `rate_a_mva`: Float64, required
- `rate_b_mva`: Float64, nullable
- `status`: Boolean, required
- `name`: Utf8, nullable

### dc_lines_2w

- `dc_line_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Utf8, required
- `r_ohm`: Float64, required
- `l_henry`: Float64, nullable
- `control_mode`: Utf8, required
- `p_setpoint_mw`: Float64, nullable
- `i_setpoint_ka`: Float64, nullable
- `v_setpoint_kv`: Float64, nullable
- `q_from_mvar`: Float64, nullable
- `q_to_mvar`: Float64, nullable
- `status`: Boolean, required
- `name`: Utf8, nullable
- `converter_type`: Utf8, required

Recommended `control_mode` tokens for `dc_lines_2w` are `power`, `current`, `voltage`, and `droop`.

### generators

- `generator_id`: Int32, required
- `bus_id`: Int32, required
- `name`: Utf8, nullable
- `unit_type`: Utf8, required
- `hierarchy_level`: Utf8, required
- `parent_generator_id`: Int32, nullable
- `aggregation_count`: Int32, nullable
- `status`: Boolean, required
- `is_ibr`: Boolean, required
- `ibr_subtype`: Utf8, nullable
- `p_sched_mw`: Float64, required
- `p_min_mw`: Float64, required
- `p_max_mw`: Float64, required
- `q_min_mvar`: Float64, required
- `q_max_mvar`: Float64, required
- `mbase_mva`: Float64, required
- `uol_mw`: Float64, nullable
- `lol_mw`: Float64, nullable
- `ramp_rate_up_mw_min`: Float64, nullable
- `ramp_rate_down_mw_min`: Float64, nullable
- `owner_id`: Int32, nullable
- `market_resource_id`: Utf8, nullable
- `params`: Map<String, Float64>, nullable

### ibr_devices

> **Removed in v0.9.0.** IBRs are now modeled in the `generators` table using `is_ibr = true` and `ibr_subtype`. Writers must not emit an `ibr_devices` root column in v0.9.0+ files.

### loads

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `p_pu`: Float64, required — constant-power active component (P term), per-unit on system base
- `q_pu`: Float64, required — constant-power reactive component (Q term), per-unit on system base
- `p_i_pu`: Float64, nullable (v0.9.1+) — constant-current active component, per-unit on system base
- `q_i_pu`: Float64, nullable (v0.9.1+) — constant-current reactive component, per-unit on system base
- `p_y_pu`: Float64, nullable (v0.9.1+) — constant-admittance active component, per-unit on system base
- `q_y_pu`: Float64, nullable (v0.9.1+) — constant-admittance reactive component, per-unit on system base
- `name`: Dictionary<UInt32, Utf8>, nullable

ZIP mapping semantics for PSS/E LOAD records (system base `S_base`):

- `p_pu = PL / S_base`
- `q_pu = QL / S_base`
- `p_i_pu = IP / S_base`
- `q_i_pu = IQ / S_base`
- `p_y_pu = YP / S_base`
- `q_y_pu = YQ / S_base`

Sign convention:

- Positive values represent net demand (load consumption) for both active and reactive components.
- Negative values represent net injection.
- Writers must preserve source sign without normalization.

Null/default behavior:

- When source data lacks a ZIP component (or the source format does not provide it), writers must emit `null` for that component.
- Writers must not fabricate zero values to imply absent source data.
- Legacy files without these fields remain readable; readers should treat missing columns as all-null for backward compatibility.
- Writers should stamp `rpf.loads.zip_fidelity_presence` as:
  - `not_available` when source/export path does not provide ZIP decomposition terms
  - `partial` when ZIP terms are populated for only a subset of load rows
  - `complete` when ZIP terms are populated (or explicitly zero-valued from source) for all load rows

### fixed_shunts

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `g_pu`: Float64, required
- `b_pu`: Float64, required

### switched_shunts

- `bus_id`: Int32, required
- `status`: Boolean, required
- `v_low`: Float64, required
- `v_high`: Float64, required
- `b_steps`: List<Float64>, required
- `current_step`: Int32, required
- `b_init_pu`: Float64, nullable — authoritative initial susceptance in per-unit (v0.8.3+). PSS/E source: `BINIT / base_mva`. CIM source: `b_steps[current_step - 1]`. Readers should prefer this field over reconstructing from `b_steps + current_step`. Nullable for backward compatibility; writers must populate this field.
- `shunt_id`: Dictionary<Int32, Utf8>, nullable — stable per-bank identity to disambiguate multiple banks at the same bus (v0.8.5+). CIM path: `ShuntCompensator` mRID. PSS/E path: synthesized as `"{bus_id}_shunt_{n}"` (1-indexed). Nullable for backward compatibility; writers must populate when available.

For v0.8.9+, `switched_shunts.b_steps` must contain strictly capacitive (positive) values.
Inductive steps must be represented in `switched_shunt_banks`.

### switched_shunt_banks

- `shunt_id`: Int32, required
- `bank_id`: Int32, required
- `b_mvar`: Float64, required
- `status`: Boolean, required
- `step`: Int32, required

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
- `risk_score`: Float64, nullable (v0.9.0+) — composite risk score
- `cleared_by_reserves`: Boolean, nullable (v0.9.0+) — true if contingency was cleared by greedy reserve dispatch
- `voltage_collapse_flag`: Boolean, nullable (v0.9.0+) — true if voltage collapse was detected
- `recovery_possible`: Boolean, nullable (v0.9.0+) — true if system recovery is achievable within NERC criteria
- `recovery_time_min`: Float64, nullable (v0.9.0+) — estimated recovery time in minutes
- `greedy_reserve_summary`: Utf8, nullable (v0.9.0+) — short text description of greedy reserve dispatch actions

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

### facts_devices

- `device_id`: Dictionary<Int32, Utf8>, required
- `branch_id`: Int32, nullable (null when bus-coupled)
- `bus_id`: Int32, nullable (null when branch-coupled)
- `device_type`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `control_mode`: Dictionary<Int32, Utf8>, nullable
- `target_flow_mw`: Float64, nullable
- `x_min_pu`: Float64, nullable
- `x_max_pu`: Float64, nullable
- `voltage_injection_mag_pu`: Float64, nullable
- `voltage_injection_angle_deg`: Float64, nullable
- `response_time_ms`: Float64, nullable
- `rating_mva`: Float64, nullable
- `dynamics_model_ref`: Dictionary<Int32, Utf8>, nullable
- `params`: Map<String, Float64>, nullable
- `name`: Dictionary<Int32, Utf8>, nullable

Token rules for `facts_devices.device_type` and `branches.device_type`:

- Canonical SmartValve token: `smartvalve`.
- Accepted ingestion alias: `SV` (case-insensitive).
- Writers must emit canonical `smartvalve`.
- Readers must normalize `SV` to `smartvalve`.

### facts_solved

- `device_id`: Dictionary<Int32, Utf8>, required
- `effective_x_pu`: Float64, nullable
- `injected_voltage_mag_pu`: Float64, nullable
- `injected_voltage_angle_deg`: Float64, nullable
- `p_effect_mw`: Float64, nullable
- `q_effect_mvar`: Float64, nullable
- `status`: Boolean, nullable

Solved presence convention (v0.8.6+):

- `rpf.facts_solved_state_presence = actual_solved` when `facts_solved` is emitted.
- `rpf.facts_solved_state_presence = not_available` when `facts_devices` is emitted but solved replay values are not present.

### scenario_context (optional, v0.9.0+)

Stores structured context for flagged or exported analysis cases. This table is optional — present in analysis exports, absent in standard planning files.

- `scenario_context_id`: Int32, required — primary key
- `case_id`: Utf8, required — links to `metadata.case_fingerprint`
- `source_type`: Utf8, required — `"real_time"` | `"hour_ahead_advisory"` | `"planning_study"`
- `priority`: Utf8, required — `"critical"` | `"high"` | `"medium"` | `"low"`
- `violation_type`: Utf8, nullable — e.g. `"voltage_collapse"`, `"q_limit_infeasible"`, `"unrecoverable_n2"`, `"limit_violation"`
- `nerc_recovery_status`: Utf8, nullable — `"recoverable_15min_lte"` | `"not_recoverable"` | `"unknown"`
- `recovery_time_min`: Float64, nullable — estimated recovery time in minutes
- `cleared_by_reserves`: Boolean, nullable — true if cleared by greedy reserve dispatch
- `planning_feedback_flag`: Boolean, required — true if this case should trigger a planning study review
- `planning_assumption_violated`: Utf8, nullable — description of the violated planning assumption
- `recommended_action`: Utf8, nullable — operator-readable recommended corrective action
- `investigation_summary`: Utf8, nullable — analysis narrative
- `load_forecast_error_pct`: Float64, nullable — forecast error contribution for hour-ahead cases
- `created_timestamp_utc`: Utf8, required — ISO 8601 UTC timestamp when this context record was created
- `params`: Map<String, Float64>, nullable — extensible key/value parameters

Schema-level example: parallel PST + SmartValve on one corridor

- `transformers_2w` row carries the PST tap/phase state for the physical transformer branch.
- `branches` row for the same electrical corridor may carry additive FACTS metadata (`device_type=smartvalve`, control/limits fields).
- `facts_devices` carries the authoritative device identity, linkage (`branch_id` or `bus_id`), and richer control metadata.
- `facts_solved` (when present) carries solved replay outputs (`effective_x_pu`, injected voltage, effective P/Q impact).
- Loaders should treat PST and SmartValve effects as composable controls on the same path, not mutually exclusive equipment classes.

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

## Optional Tables: buses_solved, generators_solved, switched_shunts_solved

These tables are emitted only when `case_mode = solved_snapshot` (v0.8.4+/v0.8.5+).
When `case_mode` is a planning variant, all three tables must be absent.

### buses_solved

- `bus_id`: Int32, non-null — FK into `buses`.
- `v_mag_pu`: Float64, nullable — post-solve voltage magnitude in per-unit.
- `v_ang_deg`: Float64, nullable — post-solve voltage angle in degrees.
- `p_inj_pu`: Float64, nullable — net active power injection in per-unit.
- `q_inj_pu`: Float64, nullable — net reactive power injection in per-unit.
- `bus_type_solved`: Int8, nullable — effective bus type after convergence: 1=PQ, 2=PV, 3=slack.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

### generators_solved

- `bus_id`: Int32, non-null — FK into `generators`.
- `id`: Dictionary<Int32, Utf8>, non-null — FK into `generators`.
- `p_actual_pu`: Float64, nullable — post-solve real power output in per-unit.
- `q_actual_pu`: Float64, nullable — post-solve reactive power output in per-unit.
- `p_mw`: Float64, nullable — actual real power in MW (`= p_actual_pu × base_mva`); solver-native unit convenience (v0.8.5+).
- `q_mvar`: Float64, nullable — actual reactive power in MVAR (`= q_actual_pu × base_mva`) (v0.8.5+).
- `status`: Boolean, nullable — in-service status at solve time (v0.8.5+).
- `pv_to_pq`: Boolean, nullable — true when this unit's bus switched PV→PQ during solve.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

### switched_shunts_solved

Emitted only when `solved_shunt_state_presence = actual_solved` (v0.8.5+).

- `bus_id`: Int32, non-null — FK into `switched_shunts`.
- `shunt_id`: Dictionary<Int32, Utf8>, nullable — FK into `switched_shunts.shunt_id`.
- `current_step_solved`: Int32, nullable — energized step after convergence (1-indexed).
- `b_pu_solved`: Float64, nullable — post-solve total susceptance in per-unit.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

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
2. Verifies `raptrix.version` is in the set of supported contract versions (current: `0.9.1`).
3. Verifies required root columns appear in canonical order.
4. Uses `rpf.rows.<table_name>` metadata to trim padded null tails.
5. Treats the 15 required root columns as mandatory even when their logical row counts are zero.
6. Detects optional tables by root column presence and feature metadata, not by guesswork.
7. Ignores unknown future trailing root columns for forward compatibility.
8. Reads and validates `rpf.case_mode` (required since v0.8.4): must be `flat_start_planning`, `warm_start_planning`, or `solved_snapshot`.
9. When `case_mode = solved_snapshot`: expects `rpf.solved_state_presence = actual_solved` and treats `buses_solved` and `generators_solved` as required; treats `switched_shunts_solved` as required when `rpf.solver.solved_shunt_state_presence = actual_solved`.
10. When `case_mode` is a planning variant: treats `buses_solved`, `generators_solved`, and `switched_shunts_solved` as absent; if found, the file is malformed.
11. Reads solver provenance keys (`rpf.solver.*`) only when `solved_state_presence = actual_solved`; ignores them otherwise.
12. When `rpf.solver.solved_shunt_state_presence = not_available`: warns that switched-shunt solved state is absent; does not fail (v0.8.5+).
13. When `facts_devices.device_type` or `branches.device_type` contains `SV` (case-insensitive), canonicalizes to `smartvalve`.
14. Treats `facts_devices` and `facts_solved` as optional additive tables; if `rpf.facts_solved_state_presence = actual_solved`, expects `facts_solved` to be present.

For a plain-English explanation of all fields see [rpf-field-guide.md](rpf-field-guide.md).

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

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix PowerFlow
Copyright (c) 2026 Raptrix PowerFlow

