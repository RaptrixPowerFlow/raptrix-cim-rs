<!--
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
-->

# RPF Field Guide — Plain-English Reference

**Schema contract: v0.14.0 (dual-read v0.13.1 / v0.13.0) | Format: Apache Arrow IPC**

This guide explains every table and field in an `.rpf` file in plain English. It is written for engineers who need to read, validate, or build tools against RPF files without digging into Arrow source code. For the normative type-level contract see [schema-contract.md](schema-contract.md). Migration notes for the v0.14.0 additive MINOR are in [MIGRATION.md](../MIGRATION.md).

This repository targets IEC 61970 CIM 17+ exchange for North American and European integrations. Public regression coverage is anchored on ENTSO-E CGMES v3.0.3 datasets.

This repo is also the source of truth for the RPF contract. Use `docs/schema-contract.md` for normative reader/writer requirements and this guide for plain-English implementation guidance. A synthetic dummy with every v0.14 column populated (plus protection / topology / sequence examples) is generated locally by `cargo test -p raptrix-cim-arrow --test v014_funnel_demo` as `tests/data/fixtures/v014_funnel_demo.rpf` (gitignored; not on GitHub).

---

## What is an RPF file?

An `.rpf` file is a single [Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html) file. Think of it as a container that holds several named tables — buses, branches, generators, and so on — all packed into one binary file with metadata attached.

You can open any `.rpf` file with the free [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) viewer, with Python via `pyarrow`, or by running `cargo run --release -- view --input case.rpf` on this repo.

Each table lives as a named column in the root Arrow record batch. The root metadata (key-value strings attached to the file header) describe what kind of case it is, who created it, feature flags, and row counts.

---

## Case Modes — the most important concept in v0.8.4

Every `.rpf` file now declares one of three **case modes**. This is the single most important field in the file because it tells every downstream tool what the data means and whether solved voltages and flows are trustworthy.

| `rpf.case_mode` | What it means |
|---|---|
| `flat_start_planning` | A planning or steady-state case prepared for the solver. Bus voltages are set to 1.0 pu / 0° (the classic flat-start initial condition). No solved results exist yet. |
| `warm_start_planning` | Like the above but the bus voltage setpoints have been seeded from a previous solution or engineering judgment. Still a pre-solve case — no Newton-Raphson result yet. |
| `solved_snapshot` | The solver has converged on this case. The planning tables carry the final dispatch that was solved, and the optional `buses_solved` and `generators_solved` tables carry the actual post-solution voltages and flows. |

**Rule**: The CIM exporter (`raptrix-cim-rs`) only produces planning cases. Setting solved voltages in a CIM export is the solver's job, not the parser's. The exporter will hard-fail if you try to label a planning case as `solved_snapshot` without providing solver provenance metadata, and it will also hard-fail if you attach solver provenance to a planning case. This prevents mixed-state files from being created silently.

---

## File-level metadata keys

These are key-value strings in the Arrow file header. Every RPF reader should check them first.

### Always present

| Key | Example value | What it means |
|---|---|---|
| `raptrix.version` | `v0.14.0` | The schema contract version this file was written to. Writers emit `v0.14.0`; readers accept `v0.14.0` / `0.14.0` and `v0.13.1` / `0.13.1` / `v0.13.0` / `0.13.0`. Pre-0.13 files must be re-exported. |
| `rpf.identity.model` | `hybrid_solver_flat_v1` | **(v0.13.0+, optional)** Declares the hybrid identity model: dense `bus_id` foreign keys for solvers, plus optional equipment `mrid` where available. |
| `raptrix.branding` | *(long string)* | Human-readable provenance string identifying the writing tool and copyright. |
| `rpf.case_fingerprint` | `abc123...` | A deterministic hash of the case identity. Useful for de-duplication and reproducibility checks. |
| `rpf.validation_mode` | `topology_only` or `solved_ready` | `topology_only` means the file has enough topology to run but may be missing some steady-state parameters. `solved_ready` means all parameters needed for full Newton-Raphson are present. |
| `rpf.case_mode` | `flat_start_planning` | See the case modes table above. Required since v0.8.4. |
| `rpf.solved_state_presence` | `not_computed` | Describes what solved state is in the file. See table below. |

### Optional: `rpf.default_shunt_control_mode` (v0.9.5+)

| Key | Example | What it means |
|---|---|---|
| `rpf.default_shunt_control_mode` | `planning_full` | **Optional.** When present, downstream solvers will default to this shunt mode (`planning_full` \| `real_time_hot_start` \| `real_time_frozen`). Enables fully declarative planning ↔ real-time handoff. Mirrors the nullable `metadata` table column of the same logical value. Planning exports from `raptrix-cim-rs` stamp `planning_full` by default. |

### `rpf.solved_state_presence` values

| Value | What it means |
|---|---|
| `actual_solved` | The file contains real solver output. The `buses_solved` and `generators_solved` tables are present and populated by the solver. |
| `not_available` | Solved state would normally exist for this case type but was not included in this file (for example, stripped for privacy or size). |
| `not_computed` | This is a planning case and no solve has been run yet. This is the normal value for every file produced by the CIM exporter. |

### Solver provenance (only present when `solved_state_presence = actual_solved`)

| Key | What it means |
|---|---|
| `rpf.solver.version` | Version string of the solver that produced this file (for example `solver-name X.Y.Z`). |
| `rpf.solver.iterations` | Number of Newton-Raphson iterations to convergence. |
| `rpf.solver.accuracy` | Final mismatch residual norm. Smaller is more accurate. Typical convergence target is 1e-6 or better. |
| `rpf.solver.mode` | Bus control mode at convergence, e.g. `PV` (voltage-controlled generation) or `PV_to_PQ` (generator hit a reactive limit and switched to constant-Q control). |
| `rpf.solver.slack_bus_id` | Integer `bus_id` of the angle reference bus used in the solve. Prevents silent reference-frame mismatch when snapshots are re-used across different network topologies. (v0.8.5+) |
| `rpf.solver.angle_reference_deg` | Angle reference value in degrees assigned to the slack bus, almost always 0.0. (v0.8.5+) |
| `rpf.solver.solved_shunt_state_presence` | `actual_solved` if the `switched_shunts_solved` table is present and authoritative; `not_available` if the solver did not track discrete shunt steps. (v0.8.5+) |

### Feature flags

| Key | What it means |
|---|---|
| `raptrix.features.node_breaker` | `true` if the optional node-breaker detail tables are present. |
| `raptrix.features.diagram_layout` | `true` if the optional diagram layout tables are present. |
| `raptrix.features.contingencies_stub` | `true` if the contingencies table contains placeholder rows rather than real contingency data. |
| `raptrix.features.dynamics_stub` | `true` if the dynamics_models table contains placeholder rows rather than real model parameters. |
| `raptrix.features.facts` | `true` if optional FACTS metadata tables are present. (v0.8.6+) |
| `raptrix.features.facts_solved` | `true` if optional solved FACTS replay table is present. (v0.8.6+) |
| `raptrix.features.protection_contingencies` | `true` if the optional `protection_contingencies` table is present. (v0.11.0+) |
| `raptrix.features.topology_changes` | `true` if the optional `topology_changes` table is present. (v0.11.0+) |
| `raptrix.features.contingency_sequences` | `true` if the optional `contingency_sequences` table is present. (v0.14.0+) |
| `rpf.protection.fidelity` | `logical`, `breaker_level`, or `mixed`: how protection rows are resolved. Defaults to `logical`. (v0.11.0+) |

Additional v0.8.6 solved FACTS metadata:

- `rpf.facts_solved_state_presence = actual_solved | not_available`

### Row count metadata

Keys in the form `rpf.rows.<table_name>` (e.g. `rpf.rows.buses = 118`) give the logical row count for each table. Arrow stores tables as padded columns, so always use these metadata values rather than the raw Arrow array length when slicing rows.

---

## Table-by-table field guide

### `metadata` — one row per file

This table always has exactly one row and summarizes the case.

| Field | Type | What it means |
|---|---|---|
| `base_mva` | number | The system MVA base for per-unit conversion. Almost always 100.0. Divide MVA values by this number to get per-unit. |
| `frequency_hz` | number | System frequency. 60.0 for North America, 50.0 for most of Europe and Asia. |
| `source_format` | text | **(v0.13.0+)** Optional closed set: `psse_raw` \| `pslf_epc` \| `cgmes` \| `powerworld` \| `rpf` \| `other`. Replaces the old required `psse_version` field. Null when unspecified. |
| `source_format_version` | text | **(v0.13.0+)** Optional version string for the source format (e.g. `"3.0"` for CGMES). |
| `source_identity_scheme` | text | **(v0.13.0+)** Optional closed set: `dense_bus_id` \| `mrid` \| `mixed` \| `synthetic_mrid`. How equipment identity was assigned. |
| `study_name` | text | Human-readable name for this case, if provided at export time. |
| `timestamp_utc` | timestamp (UTC) | **(v0.13.0+)** Native Arrow UTC timestamp of when this file was created (microsecond precision). |
| `raptrix_version` | text | Same as the `raptrix.version` metadata key. |
| `is_planning_case` | true/false | Legacy boolean. True when `case_mode` is any planning variant. Prefer checking `case_mode` directly. |
| `source_case_id` | text | Identifier of the source CIM dataset (typically the CGMES case name). |
| `snapshot_timestamp_utc` | timestamp (UTC) | **(v0.13.0+)** Native Arrow UTC timestamp of the original dataset, distinct from the export timestamp. |
| `case_fingerprint` | text | Same as `rpf.case_fingerprint` metadata key. |
| `validation_mode` | text | Same as `rpf.validation_mode` metadata key. |
| `custom_metadata` | key-value pairs | Arbitrary additional metadata attached at export time. |
| `case_mode` | text | `flat_start_planning`, `warm_start_planning`, or `solved_snapshot`. See case modes section above. |
| `solved_state_presence` | text | `actual_solved`, `not_available`, or `not_computed`. See above. |
| `solver_version` | text | Solver version string. Null for planning cases. |
| `solver_iterations` | integer | Newton-Raphson iterations. Null for planning cases. |
| `solver_accuracy` | number | Final mismatch norm. Null for planning cases. |
| `solver_mode` | text | Bus control mode at convergence. Null for planning cases. |
| `slack_bus_id_solved` | integer | The `bus_id` used as the angle reference (slack bus) in the solve. Prevents silent reference-frame mismatch when solved snapshots are re-used. Null for planning cases. (v0.8.5+) |
| `angle_reference_deg` | number | The angle value in degrees assigned to the slack bus during the solve, almost always 0.0. Null for planning cases. (v0.8.5+) |
| `solved_shunt_state_presence` | text | `actual_solved` when the `switched_shunts_solved` table is present and authoritative; `not_available` when the solver did not track discrete shunt steps. Null for planning cases. (v0.8.5+) |
| `default_shunt_control_mode` | text | **(v0.9.5+)** Optional. When present, downstream solvers will default to this shunt mode (`planning_full` \| `real_time_hot_start` \| `real_time_frozen`). Enables fully declarative planning ↔ real-time handoff. Null when unspecified. |
| `computational_load_mode` | boolean | **(v0.10.0+)** Optional. When `true`, consumers enforce the computational-load validation contract (for example non-empty `computational_load_profiles`). Null or absent means standard interchange without that contract. |
| `baseline_source_case_id` | text | **(v0.13.0+; renamed from `original_sentinel_case_id`)** Optional. Original source case identifier when this file is a baseline upgrade. Null in standard CIM exports. |
| `original_model_version` | text | **(v0.12.3+)** Optional. Model version of the source case, e.g. `"2026-01"`. |
| `target_baseline_version` | text | **(v0.12.3+)** Optional. Target baseline model version, e.g. `"2026-06"`. |
| `is_sal_enhanced` | true/false | **(v0.12.3+)** Optional. `true` when SAL enhancement was applied to produce this file. |
| `sal_enhancement_timestamp` | timestamp (UTC) | **(v0.13.0+)** Optional. Native Arrow UTC timestamp of SAL enhancement. |
| `cim_model_version_used` | text | **(v0.12.3+)** Optional. CIM model version used during the upgrade. |
| `planning_ready` | true/false | **(v0.12.3+)** Optional. Indicates the case is ready for planning studies after upgrade. |
| `upgrade_summary` | text | **(v0.12.3+)** Optional. Human-readable summary of model upgrades applied. |
| `convergence_time_ms` | number | **(v0.12.3+)** Optional. Solver convergence wall time in milliseconds. |
| `convergence_iterations` | integer | **(v0.12.3+)** Optional. Solver iteration count during upgrade convergence. |

---

### `buses` — one row per bus

Buses are the nodes of the network. Every generator, load, and branch connects to a bus.

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | Dense sequential integer ID assigned by the exporter. Starts at 1. This is the key used by all other tables. |
| `name` | text | Human-readable bus name from the CIM dataset. |
| `type` | text | **(v0.13.0+)** Bus type token: `PQ` (load), `PV` (voltage-controlled), or `Slack` (angle reference). Replaces the old Int8 codes 1/2/3. |
| `p_sched` | number | Scheduled net active power injection in per-unit. Positive = generation, negative = load. |
| `q_sched` | number | Scheduled net reactive power injection in per-unit. |
| `v_mag_set` | number | Voltage magnitude setpoint in per-unit. For a flat-start planning case this is 1.0. |
| `v_ang_set` | number | Voltage angle setpoint in degrees. For a flat-start planning case this is 0.0. |
| `q_min` | number | Minimum reactive power capability in per-unit. |
| `q_max` | number | Maximum reactive power capability in per-unit. |
| `g_shunt` | number | Total shunt conductance at the bus in per-unit. |
| `b_shunt` | number | Total shunt susceptance at the bus in per-unit. Positive = capacitive (voltage support). |
| `area` | integer | Foreign key into the `areas` table. |
| `zone` | integer | Foreign key into the `zones` table. |
| `owner` | integer | Foreign key into the `owners` table. |
| `v_min` | number | Voltage lower operating limit in per-unit. Typically 0.95. |
| `v_max` | number | Voltage upper operating limit in per-unit. Typically 1.05. |
| `p_min_agg` | number | Aggregate minimum generation in per-unit across all generators at this bus. Write `0` when unknown or the bus is not aggregated. |
| `p_max_agg` | number | Aggregate maximum generation in per-unit across all generators at this bus. Write `0` when unknown or the bus is not aggregated. |
| `nominal_kv` | number | Required nominal voltage level in kilovolts from the CIM `BaseVoltage`. Must be finite and strictly greater than 0. |
| `bus_uuid` | text | The CIM mRID (UUID) of the `TopologicalNode` this bus was collapsed from. Unique and stable across exports of the same case. |
| `latitude` | number or null | **(v0.12.5+)** Optional WGS84 latitude in degrees. Sourced from CIM GL `Location` + `PositionPoint` (yPosition). When GL attaches to an `ACLineSegment`, the line’s first/last vertices are applied to the from/to buses. Used by viewers for relative north→south ordering — not a GIS map projection. Null when unavailable. |
| `longitude` | number or null | **(v0.12.5+)** Optional WGS84 longitude in degrees. Sourced from CIM GL `Location` + `PositionPoint` (xPosition). Same line-endpoint → bus resolution as `latitude`. Null when unavailable. |

---

### `branches` — one row per AC line or series-compensated line

Branches are the transmission lines between buses.

| Field | Type | What it means |
|---|---|---|
| `branch_id` | integer | Dense sequential ID. |
| `from_bus_id` | integer | The sending-end bus (foreign key into `buses`). |
| `to_bus_id` | integer | The receiving-end bus (foreign key into `buses`). |
| `ckt` | text | Circuit identifier. Used to distinguish parallel lines between the same pair of buses. |
| `r` | number | Series resistance in per-unit. |
| `x` | number | Series reactance in per-unit. Higher X = higher impedance = less power transfer. |
| `b_shunt` | number | Total line charging susceptance in per-unit. Represents the distributed capacitance of the line. |
| `tap` | number | Off-nominal tap ratio. 1.0 for normal transmission lines; varies for transformer end-modeled lines. |
| `phase` | number | Phase shift in degrees. 0.0 for normal lines. |
| `rate_a` | number | Normal continuous rating in per-unit MVA. The everyday thermal limit. |
| `rate_b` | number | Short-term (emergency) rating in per-unit MVA. |
| `rate_c` | number | Emergency override rating in per-unit MVA. |
| `status` | true/false | True = in service, False = out of service. |
| `name` | text | Human-readable line name. Null if not provided. |
| `from_nominal_kv` | number | Required nominal kV at the from-end bus. Must be finite and strictly greater than 0. |
| `to_nominal_kv` | number | Required nominal kV at the to-end bus. Must be finite and strictly greater than 0. |

---

### `generators` — one row per generating unit

| Field | Type | What it means |
|---|---|---|
| `generator_id` | integer | Dense Int32 primary key for the machine. There is no `generators.id` column. |
| `bus_id` | integer | The bus this generator connects to. |
| `hierarchy_level` | text | Aggregation level. Leaf units use the default token `unit`. |
| `p_sched_pu` | number | Active power dispatch setpoint in per-unit. This is the planned output — not a solved result. |
| `p_min_pu` | number | Minimum stable generation in per-unit. |
| `p_max_pu` | number | Maximum generation capacity in per-unit. |
| `q_min_pu` | number | Minimum reactive power output in per-unit. |
| `q_max_pu` | number | Maximum reactive power output in per-unit. |
| `status` | true/false | True = in service. |
| `mbase_mva` | number | Machine MVA base. Used to convert machine-specific per-unit quantities. |
| `H` | number | Inertia constant in seconds. Important for dynamic stability. Zero if dynamic data is unavailable. |
| `xd_prime` | number | Transient direct-axis reactance in per-unit. Key parameter for dynamic simulation. |
| `D` | number | Damping coefficient. |
| `name` | text | Human-readable generator name. |
| `controlled_bus_id` | integer or null | **(v0.13.0+)** Dense `bus_id` of the bus whose voltage is regulated when the setpoint applies to a **remote** bus (PSS/E **IREG**; CIM **RegulatingControl**). **`null` = local** regulation at the generator’s terminal bus; a non-null value is the remote target. **Example:** generator on `bus_id=12` regulating remote bus `904` → `controlled_bus_id=904`. |
| `mrid` | text or null | **(v0.12.2+)** Optional stable CIM mRID for the machine. Distinct from market resource IDs. |

---

### `loads` — one row per load

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | The bus this load connects to. |
| `id` | text | Load identifier, unique per bus. |
| `status` | true/false | True = in service. |
| `p_pu` | number | Active power demand in per-unit. |
| `q_pu` | number | Reactive power demand in per-unit. |
| `name` | text | Human-readable load name. |
| `mrid` | text or null | **(v0.13.0+)** Optional stable equipment identity. Null when unavailable. |

---

### `fixed_shunts` — one row per fixed shunt device

Fixed shunts are permanently connected capacitor or reactor banks. They cannot be switched.

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | The bus this shunt device connects to. |
| `id` | text | Device identifier. |
| `status` | true/false | True = in service. |
| `g_pu` | number | Shunt conductance in per-unit. Positive = consumes reactive power (reactor). |
| `b_pu` | number | Shunt susceptance in per-unit. Positive = produces reactive power (capacitor). |
| `mrid` | text or null | **(v0.13.0+)** Optional stable equipment identity. Null when unavailable. |

---

### `switched_shunts` — one row per switchable shunt bank

Switched shunts are reactor or capacitor banks that can be switched in discrete steps by an operator or automatic control.

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | The bus this shunt bank connects to. |
| `status` | true/false | True = in service. |
| `v_low` | number | Lower voltage limit that triggers switching (per-unit). |
| `v_high` | number | Upper voltage limit that triggers switching (per-unit). |
| `b_steps` | list of numbers | Susceptance per step in per-unit. Each entry is one switchable step. |
| `current_step` | integer | Which step is currently in service. 1-indexed. |
| `b_init_pu` | number | Authoritative initial susceptance in per-unit. Always use this field — it is more reliable than reconstructing from `b_steps[current_step - 1]`. Populated from CGMES `ShuntCompensator.sections` or equivalent. |
| `shunt_id` | text | Stable per-bank identity to disambiguate multiple switched-shunt banks at the same bus. CIM path: the `ShuntCompensator` mRID. PSS/E path: synthesized as `"{bus_id}_shunt_{n}"` (1-indexed bank within the bus). Null when source data lacks a stable bank mRID. Use this field — not `bus_id` alone — to cross-reference into `switched_shunts_solved`. (v0.8.5+) |
| `mrid` | text or null | **(v0.13.0+)** Optional stable equipment identity (may equal `shunt_id` when mRID-backed). Null when unavailable. |

---

### `transformers_2w` — one row per two-winding transformer

| Field | Type | What it means |
|---|---|---|
| `from_bus_id` | integer | High-voltage (primary) winding bus. |
| `to_bus_id` | integer | Low-voltage (secondary) winding bus. |
| `ckt` | text | Circuit identifier for parallel transformers between the same buses. |
| `r` | number | Total leakage resistance referred to the primary in per-unit. |
| `x` | number | Total leakage reactance referred to the primary in per-unit. |
| `winding1_r`, `winding1_x` | numbers | Primary winding resistance and reactance individually. |
| `winding2_r`, `winding2_x` | numbers | Secondary winding resistance and reactance individually. |
| `g` | number | Core (magnetizing) conductance in per-unit. Represents core losses. |
| `b` | number | Core (magnetizing) susceptance in per-unit. |
| `tap_ratio` | number | Off-nominal turns ratio in per-unit. 1.0 = nominal tap. |
| `nominal_tap_ratio` | number | The nameplate nominal turns ratio. |
| `phase_shift` | number | Phase shift introduced by windings in degrees. Non-zero for phase-shifting transformers. |
| `vector_group` | text | IEC winding connection code, e.g. `Dyn11`, `YNyn0`. |
| `rate_a`, `rate_b`, `rate_c` | numbers | Normal, short-term, and emergency MVA ratings in per-unit. |
| `status` | true/false | True = in service. |
| `name` | text | Human-readable name. |
| `from_nominal_kv`, `to_nominal_kv` | numbers | Required nominal kV on each winding; each value must be finite and strictly greater than 0. |

---

### `transformers_3w` — one row per three-winding transformer

Three-winding transformers connect three voltage levels. RPF models them with a fictitious star bus at the center.

| Field | Type | What it means |
|---|---|---|
| `bus_h_id` | integer | High-voltage winding bus. |
| `bus_m_id` | integer | Medium-voltage winding bus. |
| `bus_l_id` | integer | Low-voltage winding bus. |
| `star_bus_id` | integer | Internal fictitious bus at the transformer star point. Null if not present in the source case. |
| `ckt` | text | Circuit identifier. |
| `r_hm`, `x_hm` | numbers | Resistance and reactance of the H↔M winding pair in per-unit. |
| `r_hl`, `x_hl` | numbers | Resistance and reactance of the H↔L winding pair. |
| `r_ml`, `x_ml` | numbers | Resistance and reactance of the M↔L winding pair. |
| `tap_h`, `tap_m`, `tap_l` | numbers | Off-nominal tap ratios for each winding. |
| `phase_shift` | number | Phase shift in degrees. |
| `vector_group` | text | IEC winding connection code. |
| `rate_a`, `rate_b`, `rate_c` | numbers | Thermal ratings in per-unit MVA. |
| `status` | true/false | True = in service. |
| `name` | text | Human-readable name. |
| `nominal_kv_h`, `nominal_kv_m`, `nominal_kv_l` | numbers | Required nominal kV for each of the three windings; each value must be finite and strictly greater than 0. |

---

### `areas`, `zones`, `owners` — classification tables

These three small tables provide the names and IDs for the classification codes referenced by the `buses` table.

- **`areas`**: Control areas. Each area has an `area_id`, a `name`, and an optional `interchange_mw` scheduled interchange.
- **`zones`**: Geographic or administrative zones. Each has a `zone_id` and `name`.
- **`owners`**: Equipment owners. Each has an `owner_id` and `name`.

---

### `contingencies` — N-1 and N-2 outage definitions

Each row defines one contingency event. The `elements` column is a list of one or more outages that happen **simultaneously**. One element is an N-1 parent. Two or more elements are a simultaneous / common-mode set (tower, P7-shaped). Sequential N-1-1 is **not** encoded by stuffing two elements in one row — use `contingency_sequences` or study JSON.

| Field | Type | What it means |
|---|---|---|
| `contingency_id` | text | Unique name for this contingency, e.g. `BRANCH_L123`. |
| `elements` | list of outage records | The set of equipment taken out of service **at once**. |
| `risk_score` … `greedy_reserve_summary` | analysis-only | **Always null** in planning / interchange files. May be populated in analysis exports. `scenario_context` is the structured ops→planning path. |
| `tpl_category` | text or null | **(v0.14.0+)** Optional NERC-oriented annotation: `P1`…`P7` / `unspecified`. Null = untagged, not invalid. Structural meaning stays element count / protection / sequences. |
| `reserved` | true/false or null | **(v0.14.0+)** `true` = never-trim; `false` = not reserved; **null** = infer from `protection_contingencies` / study list. |

Each outage record inside `elements` has:

| Field | What it means |
|---|---|
| `element_type` | What kind of outage: `branch_outage`, `gen_trip` (canonical; `generator_trip` is a reader alias normalized on write), `load_shed`, `shunt_switch`, `split_bus`, or `protection_event` (v0.11.0+, protection detail lives in the matching `protection_contingencies` row). |
| `branch_id` | For branch outages: which branch. |
| `bus_id` | For bus outages or generation trips: which bus. |
| `gen_id` | String label for a generation trip. Prefer `generators.mrid` when present; else a deterministic synthetic such as `{bus_id}:{name}`. There is no `generators.id`. For an unambiguous apply path also set `equipment_kind=generator` and `equipment_id` to the decimal `generators.generator_id`. |
| `load_id` | For load shed: the specific load ID. |
| `amount_mw` | For load shed: how many MW are shed. |
| `status_change` | True = the equipment changes from in-service to out-of-service. |
| `equipment_kind`, `equipment_id` | Generic equipment identity for switch or split-bus type outages that do not map cleanly to a branch/gen/load ID. |

---

### `interfaces` — monitored flowgates

Interfaces (also called flowgates or interfaces) define groups of branches whose combined flow is monitored against a transfer limit.

| Field | What it means |
|---|---|
| `interface_id` | Integer identifier. |
| `name` | Human-readable name, e.g. `NEPTUNE` or `UPNY_CTNY`. |
| `monitored_branches` | List of `branch_id` values that make up this interface. |
| `transfer_limit_mw` | Maximum allowable MW flow across this interface. |

---

### `dynamics_models` — dynamic simulation parameters

One row per generator-linked dynamic model. Used by dynamic (time-domain) simulation.

| Field | What it means |
|---|---|
| `bus_id` | The bus the generator is connected to. |
| `gen_id` | String label for the machine. Prefer `generators.mrid` when present; else a deterministic synthetic such as `{bus_id}:{name}`. Does **not** join to a `generators.id` column (there is none). |
| `model_type` | String name of the dynamic model, e.g. `GENROU`, `GENCLS`, `SYNC_MACHINE_EQ`, or a custom namespaced type like `raptrix.smart_valve.v1`. |
| `params` | A map of parameter name → numeric value. Normalized lowercase keys derived from CIM field names (e.g. `h`, `xd_prime`, `d`, `ra`, `xl`). Also includes provenance keys: `source_dy = 1.0` if parameters came from the CGMES DY profile, `source_eq_fallback = 1.0` if derived from EQ data only, `source_stub = 1.0` if this is a placeholder row. |
| `classical_params` | **(v0.13.0+)** Optional struct `{H, D, xd_prime, mbase_mva}` for classical first-swing machines. Prefer these fields over the same keys in `params` when both are present. |

---

### Optional: `computational_load_profiles` — large-load interchange (v0.10.0+; extended in v0.13.0 / v0.13.1)

One row per computational / large-load bus or load. Present when `metadata.computational_load_mode` is used and the writer includes the table. Power fields are **physical MW** (not PU). Exactly one of `bus_id` or `load_id` should be set per row.

| Field | What it means |
|---|---|
| `bus_id` / `load_id` | Anchor to a bus or load row (exactly one non-null when the mode contract is on). |
| `seasonal_envelope`, `buildout_schedule` | Optional seasonal MW envelopes and year/MW buildout steps. |
| `priority` | **(v0.13.0+)** Ranking 1–5 for candidate selection (1 = highest). Null = lowest priority. |
| `max_step_drop_mw` | **(v0.13.0+)** Maximum single-step MW drop considered for studies. |
| `trip_study_percentiles` | **(v0.13.0+)** List of **0–100 percentage points** (e.g. 60, 100) — not 0–1 fractions. Null/empty means the case file did not auto-generate percentiles; consumers may apply their own study defaults. |
| `facility_class` | **(v0.13.0+)** Closed set: `cloud_storage` \| `ai_hpc` \| `crypto` \| `mixed` \| `other`. |
| `common_mode_group`, `poi_name`, `mrid` | Grouping / labeling / optional identity. Common-mode groups are the primary ranking dimension for correlated multi-facility transfer risk. |
| `voltage_sensitivity_hint`, transfer / reconnection / ride-through maps | Optional screening and ride-through hints for large-load studies. |
| `voltage_transfer_curve` | **(v0.13.1+)** Typed multi-stage `(V,t)` transfer envelope: list of `{v_pu, t_ms, polarity, action, mw_fraction?, load_class?}`. Null/empty → legacy scalar threshold. |
| `disturbance_counter` | **(v0.13.1+)** Optional 3-strike / rolling-window latch struct. |
| `reconnection_params` | **(v0.13.1+)** Typed reconnection (`v_recover_pu`, `delay_ms`, `ramp_mw_per_min`, `manual_reset_required`). Opaque `reconnection_criteria` map retained. |
| `voltage_measurement` | **(v0.13.1+)** Measurement basis, filter `Tv` (ms), location, hysteresis. Default research filter ≈ 20 ms. |
| `protection_settings_provenance` | **(v0.13.1+)** `site_verified` \| `oem_default` \| `study_assumption` plus optional `profile_id` / `effective_date`. |

See [`V0131_VOLTAGE_TRANSFER_CURVE_RESEARCH.md`](V0131_VOLTAGE_TRANSFER_CURVE_RESEARCH.md) for the field validation matrix and PERC1 cross-walk.

---

### Optional: `protection_contingencies` and `topology_changes` — protection-informed contingencies (v0.11.0+)

Real contingencies are often driven by protection schemes that trip a group of equipment at
once (breaker failure, bus-differential lockout, transfer trip) and can split a bus or isolate
part of a substation. These two optional tables capture that, using a **layered model**: a
logical protection-group baseline that works on ordinary bus-branch data, plus optional
breaker-level detail when it is available. They are present in EMS / operations exports and
absent in standard planning files. Full design rationale and the cross-repo consumption
contract are in [adr/0001-protection-informed-contingencies.md](adr/0001-protection-informed-contingencies.md).

**`protection_contingencies`** — one row per protection event, keyed to a `contingencies.contingency_id`:

| Field | What it means |
|---|---|
| `contingency_id` | Links to the matching `contingencies` row. |
| `protection_group_id` | Stable identifier of the protection scheme/group. |
| `name` | Human-readable label. |
| `scheme_type` | Kind of protection action: `breaker_failure`, `stuck_breaker`, `relay_misoperation`, `bus_differential`, `zone_protection`, `line_protection`, `transfer_trip`, `sympathetic_trip`, `auto_reclose`, or any other token. |
| `initiating_equipment_kind` / `initiating_equipment_id` | The fault/trigger element. |
| `tripped_elements` | The resulting outage set — same record shape as `contingencies.elements`, so the same multi-element logic applies. |
| `sequence` | Optional ordered/timed steps (`step`, `delay_ms`, `equipment_kind`, `equipment_id`) for automatic sequences. |
| `topology_change_id` | Links to the `topology_changes` row describing the resulting topology, if any. |
| `data_confidence` | How trustworthy the outage set is: `modeled`, `inferred`, or `assumed`. |
| `breaker_ids` | Optional breaker/switch IDs (joining `switch_detail` / `node_breaker_detail`) for breaker-level refinement. |
| `params` | Extensible numeric parameters. |

**`topology_changes`** — one row per resulting topology delta:

| Field | What it means |
|---|---|
| `topology_change_id` | Primary key. |
| `contingency_id` | The contingency that produced the change. |
| `change_type` | `bus_split`, `island_formation`, `substation_isolation`, `partial_isolation`, or `element_isolation`. |
| `affected_bus_ids` | Buses involved in the change. |
| `resulting_islands` | Islands formed (`island_index`, `bus_ids`, `energized`). |
| `isolated_element_count` | How many elements were de-energized. |
| `summary` | Operator-readable narrative. |
| `provenance` | `declared` (planning intent — what current writers emit) or `solved` (what the solver actually produced — a future capability). |
| `params` | Extensible numeric parameters. |
| `change_source` | **(v0.12.3+)** Optional. Why the topology change was made, e.g. `SAL_CIM_Upgrade`, `Model_Alignment`. Dictionary-encoded. |
| `applied_phase` | **(v0.12.3+)** Optional. Which upgrade phase applied the change, e.g. `Jan_to_June_Baseline`, `Planning_Study_Prep`. Dictionary-encoded. |

#### Worked example 0 — a tower (simultaneous two-circuit) outage

Two circuits on one structure are **one** multi-element row, applied simultaneously. This is not sequential N-1-1.

```text
contingencies:
  contingency_id="TOWER_L1_L2"
  elements=[
    {element_type="branch_outage", branch_id=1, status_change=true},
    {element_type="branch_outage", branch_id=2, status_change=true}
  ]
  tpl_category="P7"
  reserved=true
```

#### Worked example 1 — a plain single-branch outage (no protection context)

A single line trip needs only the existing `contingencies` table; neither new table is emitted:

```text
contingencies: contingency_id="L_1023_OUT", elements=[ {element_type="branch_outage", branch_id=1023} ]
```

#### Worked example 2 — breaker failure that trips multiple elements and splits a bus

A fault on line 1023 with breaker failure at bus 47 clears the whole bus section, dropping two
more elements and splitting the bus:

```text
contingencies:
  contingency_id="BF_BUS47", elements=[ {element_type="protection_event", bus_id=47} ]

protection_contingencies:
  contingency_id="BF_BUS47", protection_group_id="BUS47_BF_ZONE", scheme_type="breaker_failure",
  tripped_elements=[ branch_outage 1023, branch_outage 1101, branch_outage 5004 ],
  topology_change_id=7, data_confidence="inferred"   # breaker_ids null (logical-only)

topology_changes:
  topology_change_id=7, contingency_id="BF_BUS47", change_type="bus_split",
  affected_bus_ids=[47], resulting_islands=[ {0,[47,48,49],energized=true}, {1,[201],energized=false} ],
  provenance="declared"
```

When the same case is later exported with node-breaker detail, `breaker_ids` is populated and
`rpf.protection.fidelity` becomes `mixed` or `breaker_level`, letting a topology processor
recompute the split from switch states instead of trusting the declared islands.

`protection_contingencies.sequence` (`delay_ms`) is millisecond-scale protection clearing.
It is **not** a TPL P3/P6 intervening window.

#### Worked example 3 — sequential N-1-1 (P3-shaped) in `contingency_sequences`

```text
contingencies:
  GEN_1  = one gen_trip element
  LINE_2 = one branch_outage element

contingency_sequences:
  sequence_id="SEQ_P3_1"
  primary_contingency_id="GEN_1"
  secondary_contingency_id="LINE_2"
  intervening_window_min=30
  tpl_category="P3"
  provenance="planner_file"
```

Endpoints should be single-element rows. A multi-element endpoint is simultaneous physics and is rare; writers do not hard-fail it. v0.14 writers may omit this table entirely.

---

### Optional: `connectivity_groups` — split-bus detail

Present only when `--connectivity-detail` is used. Maps each topological bus back to the ConnectivityNodes it aggregates. Useful for switchyard-level work and ML workflows that need sub-bus resolution.

| Field | What it means |
|---|---|
| `topological_bus_id` | The `bus_id` in the main `buses` table. |
| `topological_node_mrid` | The CIM mRID of the TopologicalNode. |
| `connectivity_node_mrids` | List of all ConnectivityNode mRIDs grouped under this bus. |
| `connectivity_count` | How many ConnectivityNodes are in this bus. |

---

### Optional: `node_breaker_detail`, `switch_detail`, `connectivity_nodes`

Present only when `--node-breaker` is used (and `raptrix.features.node_breaker = true`). These three tables provide operational substation topology fidelity for protection, restoration, and viewer workflows.

- **`node_breaker_detail`**: Each switch and its terminal bus connections, open/closed state, and normal state.
- **`switch_detail`**: Switch names, types (Breaker, Disconnector), and state flags.
- **`connectivity_nodes`**: Full connectivity node to topological node and bus ID mapping.

---

### Optional: `diagram_objects` and `diagram_points`

Present only when a CGMES DL (Diagram Layout) profile was provided and `raptrix.features.diagram_layout = true`.

These two tables store a one-line diagram layout aligned with IEC 61970-453. They must always be present together or both absent.

- **`diagram_objects`**: One row per equipment element in each named diagram view. Carries the element identifier, type (`bus`, `branch`, `generator`, etc.), which diagram it belongs to, rotation, visibility, and draw order.
- **`diagram_points`**: One or more coordinate points per diagram object. Branches can have multiple points for bends. The coordinate convention matches IEC 61970-453: larger Y values are lower on screen (inverted Y). Renderers using standard screen coordinates should invert Y on display.

---

### Optional: `buses_solved` and `generators_solved` — post-solution results (v0.8.4+)

**These tables are only present when `case_mode = solved_snapshot`.** For all planning cases they are absent entirely. This is enforced by the schema contract — a file claiming to be a planning case cannot contain these tables.

#### `buses_solved`

Post-converged bus voltages and net injections from the solver.

| Field | What it means |
|---|---|
| `bus_id` | Foreign key into `buses`. |
| `v_mag_pu` | Solved voltage magnitude in per-unit. |
| `v_ang_deg` | Solved voltage angle in degrees. |
| `p_inj_pu` | Net active power injection at this bus in per-unit (generation minus load). |
| `q_inj_pu` | Net reactive power injection at this bus in per-unit. |
| `bus_type_solved` | **(v0.13.0+)** Bus type at convergence: `PQ`, `PV`, or `Slack` (dictionary tokens, same vocabulary as `buses.type`). May differ from the planning type if voltage limits were hit. |
| `provenance` | Short string identifying the solver or data source that produced this row. |

#### `generators_solved`

Post-converged generator dispatch from the solver. Reflects the actual operating point after Newton-Raphson convergence, which may differ from the scheduled dispatch in `generators.p_sched_pu` if the solver re-dispatched to enforce limits.

| Field | What it means |
|---|---|
| `bus_id` | Foreign key into `buses`. |
| `id` | String label for the machine. Prefer `generators.mrid` when present; else `{bus_id}:{name}`. Join with `generators` on `(bus_id, label)`, not a `generators.id` column (there is none). |
| `p_actual_pu` | Actual active power output at convergence in per-unit. |
| `q_actual_pu` | Actual reactive power output at convergence in per-unit. |
| `p_mw` | number | Actual active power output at convergence in MW (`= p_actual_pu × base_mva`). Provided for solver-native unit convenience. (v0.8.5+) |
| `q_mvar` | number | Actual reactive power output at convergence in MVAR. (v0.8.5+) |
| `status` | true/false | In-service status at solve time. A generator may be in service in the planning case but excluded by the solver's unit commitment logic; this field captures that distinction. Null means unknown. (v0.8.5+) |
| `pv_to_pq` | True if this generator hit a reactive limit during the solve and switched from PV to PQ bus control. |
| `provenance` | Short string identifying the solver or data source. |

---

## How to check a file is valid

| `provenance` | Short string identifying the solver or data source that produced this row. |

#### `switched_shunts_solved`

Post-converged switched-shunt bank state from the solver. Present only when `case_mode = solved_snapshot` **and** `solved_shunt_state_presence = actual_solved`. One row per bank. When multiple banks exist at the same bus, use `shunt_id` (not `bus_id` alone) for correct cross-table joins. (v0.8.5+)

| Field | What it means |
|---|---|
| `bus_id` | Foreign key into `switched_shunts`. |
| `shunt_id` | Stable bank identifier, links to `switched_shunts.shunt_id`. Null when source data lacks a stable mRID. |
| `current_step_solved` | Energized step index after Newton-Raphson convergence (1-indexed). Maps to `switched_shunts.b_steps[current_step_solved - 1]`. |
| `b_pu_solved` | Post-solve total switched susceptance in per-unit. Should match `b_steps[current_step_solved - 1]` for well-formed cases. |
| `provenance` | Short string identifying the solver or data source. |

---

## How to check a file is valid

The quickest sanity checks for any RPF reader:

1. `raptrix.version` must be in the list of supported versions.
2. `rpf.case_mode` must be one of `flat_start_planning`, `warm_start_planning`, `solved_snapshot`.
3. If `rpf.case_mode = solved_snapshot`, `rpf.solved_state_presence` must be `actual_solved` and the `buses_solved` and `generators_solved` tables must be present.
4. If `rpf.case_mode = solved_snapshot` and `rpf.solver.solved_shunt_state_presence = actual_solved`, `switched_shunts_solved` must be present. If `not_available`, warn but do not fail. (v0.8.5+)
5. If `rpf.case_mode` is a planning variant, `buses_solved`, `generators_solved`, and `switched_shunts_solved` must be absent.
6. `rpf.rows.<table>` metadata must match the trimmed row counts for each table.
7. The 15 required root columns must be present in order, even if their row counts are zero.

---

## Reading an RPF file with Python

```python
import pyarrow.ipc as ipc
import pyarrow as pa

with ipc.open_file("case.rpf") as reader:
    schema_meta = reader.schema_arrow.metadata
    case_mode = schema_meta[b"rpf.case_mode"].decode()
    print("Case mode:", case_mode)

    batch = reader.get_batch(0)

    # Read the buses table
    buses_struct = batch.column("buses")
    bus_count = int(schema_meta[b"rpf.rows.buses"])
    buses = pa.RecordBatch.from_struct_array(buses_struct).slice(0, bus_count)
    print(f"Buses: {buses.num_rows}")

    # Check for solved tables
    if case_mode == "solved_snapshot":
        buses_solved = pa.RecordBatch.from_struct_array(batch.column("buses_solved"))
        print(f"Solved bus results: {buses_solved.num_rows}")
```

---

*Part of the Raptrix Power ecosystem — [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) | [raptrix-psse-rs](https://github.com/RaptrixPowerFlow/raptrix-psse-rs) | [RaptrixPowerFlow](https://github.com/RaptrixPowerFlow/)*

*Copyright (c) 2026 Raptrix Power — MPL 2.0*


