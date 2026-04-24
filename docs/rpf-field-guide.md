# RPF Field Guide — Plain-English Reference

**Schema contract: v0.8.6 | Format: Apache Arrow IPC**

This guide explains every table and field in an `.rpf` file in plain English. It is written for engineers who need to read, validate, or build tools against RPF files without digging into Arrow source code. For the normative type-level contract see [schema-contract.md](schema-contract.md).

This repository targets IEC 61970 CIM 17+ exchange for North American and European integrations. Public regression coverage is anchored on ENTSO-E CGMES v3.0.3 datasets.

This repo is also the source of truth for the RPF contract. Use `docs/schema-contract.md` for normative reader/writer requirements and this guide for plain-English implementation guidance.

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
| `raptrix.version` | `0.8.6` | The schema contract version this file was written to. Readers should reject files with an unsupported version. |
| `raptrix.branding` | *(long string)* | Human-readable provenance string identifying the writing tool and copyright. |
| `rpf.case_fingerprint` | `abc123...` | A deterministic hash of the case identity. Useful for de-duplication and reproducibility checks. |
| `rpf.validation_mode` | `topology_only` or `solved_ready` | `topology_only` means the file has enough topology to run but may be missing some steady-state parameters. `solved_ready` means all parameters needed for full Newton-Raphson are present. |
| `rpf.case_mode` | `flat_start_planning` | See the case modes table above. Required since v0.8.4. |
| `rpf.solved_state_presence` | `not_computed` | Describes what solved state is in the file. See table below. |

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
| `psse_version` | integer | PSS/E version compatibility flag. Carry-over field for cross-format compatibility; typically 0 for CIM-sourced files. |
| `study_name` | text | Human-readable name for this case, if provided at export time. |
| `timestamp_utc` | text | RFC 3339 timestamp of when this file was created. |
| `raptrix_version` | text | Same as the `raptrix.version` metadata key. |
| `is_planning_case` | true/false | Legacy boolean. True when `case_mode` is any planning variant. Prefer checking `case_mode` directly. |
| `source_case_id` | text | Identifier of the source CIM dataset (typically the CGMES case name). |
| `snapshot_timestamp_utc` | text | Timestamp of the original CIM dataset, distinct from the export timestamp. |
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

---

### `buses` — one row per bus

Buses are the nodes of the network. Every generator, load, and branch connects to a bus.

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | Dense sequential integer ID assigned by the exporter. Starts at 1. This is the key used by all other tables. |
| `name` | text | Human-readable bus name from the CIM dataset. |
| `type` | integer | Bus type code. 1 = load bus (PQ), 2 = voltage-controlled bus (PV), 3 = slack/reference bus. |
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
| `p_min_agg` | number | Aggregate minimum generation in per-unit across all generators at this bus. |
| `p_max_agg` | number | Aggregate maximum generation in per-unit across all generators at this bus. |
| `nominal_kv` | number | Nominal voltage level in kilovolts from the CIM `BaseVoltage`. Null if the source CIM payload does not have a recoverable base voltage for this bus. |
| `bus_uuid` | text | The CIM mRID (UUID) of the `TopologicalNode` this bus was collapsed from. Unique and stable across exports of the same case. |

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
| `from_nominal_kv` | number | Nominal kV at the from-end bus. Null if not recoverable from the source CIM. |
| `to_nominal_kv` | number | Nominal kV at the to-end bus. |

---

### `generators` — one row per generating unit

| Field | Type | What it means |
|---|---|---|
| `bus_id` | integer | The bus this generator connects to. |
| `id` | text | Generator identifier, unique per bus. |
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
| `from_nominal_kv`, `to_nominal_kv` | numbers | Nominal kV on each winding. |

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
| `nominal_kv_h`, `nominal_kv_m`, `nominal_kv_l` | numbers | Nominal kV for each of the three windings. |

---

### `areas`, `zones`, `owners` — classification tables

These three small tables provide the names and IDs for the classification codes referenced by the `buses` table.

- **`areas`**: Control areas. Each area has an `area_id`, a `name`, and an optional `interchange_mw` scheduled interchange.
- **`zones`**: Geographic or administrative zones. Each has a `zone_id` and `name`.
- **`owners`**: Equipment owners. Each has an `owner_id` and `name`.

---

### `contingencies` — N-1 and N-2 outage definitions

Each row defines one contingency event. The `elements` column is a list of one or more outages that happen simultaneously.

| Field | Type | What it means |
|---|---|---|
| `contingency_id` | text | Unique name for this contingency, e.g. `BRANCH_L123`. |
| `elements` | list of outage records | The set of equipment taken out of service. |

Each outage record inside `elements` has:

| Field | What it means |
|---|---|
| `element_type` | What kind of outage: `branch_outage`, `gen_trip`, `load_shed`, `shunt_switch`, or `split_bus`. |
| `branch_id` | For branch outages: which branch. |
| `bus_id` | For bus outages or generation trips: which bus. |
| `gen_id` | For generation trips: the specific generator ID. |
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
| `gen_id` | The generator identifier (links back to `generators.id`). |
| `model_type` | String name of the dynamic model, e.g. `GENROU`, `GENCLS`, `SYNC_MACHINE_EQ`, or a custom namespaced type like `raptrix.smart_valve.v1`. |
| `params` | A map of parameter name → numeric value. Normalized lowercase keys derived from CIM field names (e.g. `h`, `xd_prime`, `d`, `ra`, `xl`). Also includes provenance keys: `source_dy = 1.0` if parameters came from the CGMES DY profile, `source_eq_fallback = 1.0` if derived from EQ data only, `source_stub = 1.0` if this is a placeholder row. |

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
| `bus_type_solved` | Bus type at convergence: `PQ`, `PV`, or `slack`. May differ from the planning type if voltage limits were hit. |
| `provenance` | Short string identifying the solver or data source that produced this row. |

#### `generators_solved`

Post-converged generator dispatch from the solver. Reflects the actual operating point after Newton-Raphson convergence, which may differ from the scheduled dispatch in `generators.p_sched_pu` if the solver re-dispatched to enforce limits.

| Field | What it means |
|---|---|
| `bus_id` | Foreign key into `buses`. |
| `id` | Generator identifier, links to `generators.id`. |
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

*Part of the Raptrix Powerflow ecosystem — [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) | [raptrix-psse-rs](https://github.com/RaptrixPowerFlow/raptrix-psse-rs) | [RaptrixPowerFlow](https://github.com/RaptrixPowerFlow/)*

*Copyright (c) 2026 Raptrix PowerFlow — MPL 2.0*


