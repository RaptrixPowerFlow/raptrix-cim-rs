// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow schema definitions for the Raptrix PowerFlow Interchange v0.8.6 profile.
//!
//! **CGMES 3.0+ Only**: This module targets CGMES v3.0 and later (v17+ CIM) merged profiles.
//! Support for legacy CGMES 2.4.x was dropped in this release for simplicity and performance.
//!
//! This module exposes one exact Arrow schema per required table in the locked
//! `.rpf` contract, plus deterministic schema registry helpers used by both
//! writers and readers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Human-readable branding string embedded as file-level metadata.
pub const BRANDING: &str = "Raptrix CIM-Arrow / PowerFlow Interchange v0.8.6 - High-performance open CIM profile (CGMES 3.0+) by Musto Technologies LLC. Copyright (c) 2026 Musto Technologies LLC.";

/// Canonical RPF format version tag embedded as file-level metadata.
pub const RPF_VERSION: &str = "0.8.6";

/// Supported RPF versions accepted by generic Arrow IPC readers.
/// v0.8.6 adds additive FACTS fields on branches and optional facts_devices / facts_solved
///   tables, plus FACTS feature metadata and smartvalve token normalization helpers.
/// v0.8.5 adds switched-shunt stable identity, switched_shunts_solved table, extended
///   generators_solved (p_mw, q_mvar, status), angle-reference metadata
///   (slack_bus_id_solved, angle_reference_deg), and solved_shunt_state_presence.
/// v0.8.4 adds strict planning-vs-solved semantics: case_mode enum, solved_state_presence
///   provenance tags, solver metadata (version/iterations/accuracy/mode), and optional
///   buses_solved / generators_solved tables for post-solve round-trip.
/// v0.8.3 adds switched_shunts.b_init_pu for exact initial-susceptance round-trip.
/// v0.8.2 requires buses.bus_uuid and adds mandatory case identity + validation metadata fields.
/// v0.8.1 normalizes all power/admittance fields to per-unit on base_mva.
/// v0.8.0 introduced diagram layout support and dropped CGMES 2.4.x compatibility.
pub const SUPPORTED_RPF_VERSIONS: &[&str] = &[
    "v0.8.6", "0.8.6", "v0.8.5", "0.8.5", "v0.8.4", "0.8.4", "v0.8.3", "0.8.3",
    "v0.8.2", "0.8.2", "v0.8.1", "0.8.1", "v0.8.0", "0.8.0", "0.7.1", "0.7.0",
];

/// Backward-compatible alias retained for older call sites.
pub const SCHEMA_VERSION: &str = RPF_VERSION;

/// File-level metadata key for branding string.
pub const METADATA_KEY_BRANDING: &str = "raptrix.branding";
/// File-level metadata key for schema version.
pub const METADATA_KEY_VERSION: &str = "raptrix.version";
/// File-level metadata key for RPF version alias.
pub const METADATA_KEY_RPF_VERSION: &str = "rpf_version";
/// Required metadata key containing deterministic case identity fingerprint.
pub const METADATA_KEY_CASE_FINGERPRINT: &str = "rpf.case_fingerprint";
/// Required metadata key describing validation readiness mode.
pub const METADATA_KEY_VALIDATION_MODE: &str = "rpf.validation_mode";
/// Optional metadata key indicating node-breaker optional tables are emitted.
pub const METADATA_KEY_FEATURE_NODE_BREAKER: &str = "raptrix.features.node_breaker";
/// Optional metadata key indicating diagram layout optional tables are emitted.
pub const METADATA_KEY_FEATURE_DIAGRAM_LAYOUT: &str = "raptrix.features.diagram_layout";
/// Optional metadata key indicating contingencies table uses placeholder rows.
pub const METADATA_KEY_FEATURE_CONTINGENCIES_STUB: &str = "raptrix.features.contingencies_stub";
/// Optional metadata key indicating dynamics_models table uses placeholder rows.
pub const METADATA_KEY_FEATURE_DYNAMICS_STUB: &str = "raptrix.features.dynamics_stub";
/// Optional metadata key indicating FACTS metadata tables are emitted.
pub const METADATA_KEY_FEATURE_FACTS: &str = "raptrix.features.facts";
/// Optional metadata key indicating facts_solved table is emitted.
pub const METADATA_KEY_FEATURE_FACTS_SOLVED: &str = "raptrix.features.facts_solved";
/// Optional metadata key indicating export is a topology-only snapshot.
pub const METADATA_KEY_FEATURE_TOPOLOGY_ONLY: &str = "rpf.features.topology_only";
/// Optional metadata key indicating all injections were zeroed by export.
pub const METADATA_KEY_FEATURE_ZERO_INJECTION_STUB: &str = "rpf.features.zero_injection_stub";
/// Required metadata key describing the case mode (flat_start_planning | warm_start_planning | solved_snapshot).
/// Added in v0.8.4.
pub const METADATA_KEY_CASE_MODE: &str = "rpf.case_mode";
/// Required metadata key indicating presence/provenance of solved-state fields.
/// Values: actual_solved | not_available | not_computed. Added in v0.8.4.
pub const METADATA_KEY_SOLVED_STATE_PRESENCE: &str = "rpf.solved_state_presence";
/// Optional metadata key for solver software version string (written when solved_state_presence=actual_solved).
pub const METADATA_KEY_SOLVER_VERSION: &str = "rpf.solver.version";
/// Optional metadata key for solver iteration count (written when solved_state_presence=actual_solved).
pub const METADATA_KEY_SOLVER_ITERATIONS: &str = "rpf.solver.iterations";
/// Optional metadata key for solver final mismatch accuracy (written when solved_state_presence=actual_solved).
pub const METADATA_KEY_SOLVER_ACCURACY: &str = "rpf.solver.accuracy";
/// Optional metadata key for solver bus-type mode, e.g. "PV", "PV_to_PQ" (written when solved_state_presence=actual_solved).
pub const METADATA_KEY_SOLVER_MODE: &str = "rpf.solver.mode";
/// Optional metadata key for the angle-reference (slack) bus_id used in the solve.
/// Written when solved_state_presence=actual_solved. Integer encoded as string.
pub const METADATA_KEY_SOLVER_SLACK_BUS_ID: &str = "rpf.solver.slack_bus_id";
/// Optional metadata key for the angle reference value in degrees used in the solve.
/// Written when solved_state_presence=actual_solved. Float encoded as string.
pub const METADATA_KEY_SOLVER_ANGLE_REFERENCE_DEG: &str = "rpf.solver.angle_reference_deg";
/// Optional metadata key indicating solved shunt switching state presence.
/// Values: actual_solved | not_available. Written when solved_state_presence=actual_solved.
pub const METADATA_KEY_SOLVED_SHUNT_STATE_PRESENCE: &str = "rpf.solver.solved_shunt_state_presence";
/// Optional metadata key indicating facts_solved table presence/provenance.
/// Values: actual_solved | not_available.
pub const METADATA_KEY_FACTS_SOLVED_STATE_PRESENCE: &str = "rpf.facts_solved_state_presence";
/// Optional metadata key indicating total electrical island count.
pub const METADATA_KEY_TOPOLOGY_ISLAND_COUNT: &str = "rpf.topology.island_count";
/// Optional metadata key indicating largest-island bus count.
pub const METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT: &str =
    "rpf.topology.main_island_bus_count";
/// Optional metadata key indicating if detached islands exist.
pub const METADATA_KEY_TOPOLOGY_DETACHED_ISLANDS_PRESENT: &str =
    "rpf.topology.detached_islands_present";
/// Optional metadata key counting detached islands with any in-service network element.
pub const METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_NETWORK_ISLAND_COUNT: &str =
    "rpf.topology.detached_active_network_island_count";
/// Optional metadata key counting detached islands with any in-service load.
pub const METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_LOAD_ISLAND_COUNT: &str =
    "rpf.topology.detached_active_load_island_count";
/// Optional metadata key counting detached islands with any in-service generation.
pub const METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_GENERATION_ISLAND_COUNT: &str =
    "rpf.topology.detached_active_generation_island_count";

/// Canonical metadata table name.
pub const TABLE_METADATA: &str = "metadata";
/// Canonical buses table name.
pub const TABLE_BUSES: &str = "buses";
/// Canonical branches table name.
pub const TABLE_BRANCHES: &str = "branches";
/// Canonical generators table name.
pub const TABLE_GENERATORS: &str = "generators";
/// Canonical loads table name.
pub const TABLE_LOADS: &str = "loads";
/// Canonical fixed shunts table name.
pub const TABLE_FIXED_SHUNTS: &str = "fixed_shunts";
/// Canonical switched shunts table name.
pub const TABLE_SWITCHED_SHUNTS: &str = "switched_shunts";
/// Canonical two-winding transformers table name.
pub const TABLE_TRANSFORMERS_2W: &str = "transformers_2w";
/// Canonical three-winding transformers table name.
pub const TABLE_TRANSFORMERS_3W: &str = "transformers_3w";
/// Canonical areas table name.
pub const TABLE_AREAS: &str = "areas";
/// Canonical zones table name.
pub const TABLE_ZONES: &str = "zones";
/// Canonical owners table name.
pub const TABLE_OWNERS: &str = "owners";
/// Canonical contingencies table name.
pub const TABLE_CONTINGENCIES: &str = "contingencies";
/// Canonical interfaces table name.
pub const TABLE_INTERFACES: &str = "interfaces";
/// Canonical dynamics models table name.
pub const TABLE_DYNAMICS_MODELS: &str = "dynamics_models";
/// Optional FACTS devices table name.
pub const TABLE_FACTS_DEVICES: &str = "facts_devices";
/// Optional detail table emitted only when connectivity-detail mode is enabled.
pub const TABLE_CONNECTIVITY_GROUPS: &str = "connectivity_groups";
/// Optional detail table emitted only when node-breaker detail mode is enabled.
pub const TABLE_NODE_BREAKER_DETAIL: &str = "node_breaker_detail";
/// Optional detail table emitted only when node-breaker detail mode is enabled.
pub const TABLE_SWITCH_DETAIL: &str = "switch_detail";
/// Optional detail table emitted only when node-breaker detail mode is enabled.
pub const TABLE_CONNECTIVITY_NODES: &str = "connectivity_nodes";
/// Optional diagram layout table emitted only when CIM DiagramObject rows resolve.
pub const TABLE_DIAGRAM_OBJECTS: &str = "diagram_objects";
/// Optional diagram layout table emitted only when CIM DiagramObjectPoint rows resolve.
pub const TABLE_DIAGRAM_POINTS: &str = "diagram_points";
/// Backward-compatible alias for older callers.
pub const TABLE_DYNAMICS: &str = "dynamics";
/// Optional solved-state table emitted only when case_mode=solved_snapshot.
/// Contains per-bus post-solve voltage magnitude, angle, and injections.
pub const TABLE_BUSES_SOLVED: &str = "buses_solved";
/// Optional solved-state table emitted only when case_mode=solved_snapshot.
/// Contains per-generator post-solve real/reactive output and PV→PQ switch flag.
pub const TABLE_GENERATORS_SOLVED: &str = "generators_solved";
/// Optional solved-state table emitted only when case_mode=solved_snapshot.
/// Contains per-bank post-solve switched-shunt step and susceptance (v0.8.5+).
pub const TABLE_SWITCHED_SHUNTS_SOLVED: &str = "switched_shunts_solved";
/// Optional solved-state FACTS table emitted for solved snapshot replay (v0.8.6+).
pub const TABLE_FACTS_SOLVED: &str = "facts_solved";

/// Optional column required on export-side solved-result tables.
pub const COLUMN_CONTINGENCY_ID: &str = "contingency_id";

/// Canonical FACTS device token for SmartValve devices.
pub const FACTS_DEVICE_TYPE_SMARTVALVE: &str = "smartvalve";

/// Accepts FACTS device aliases and returns the canonical token.
pub fn normalize_facts_device_type(value: &str) -> Option<&'static str> {
    let token = value.trim();
    if token.eq_ignore_ascii_case(FACTS_DEVICE_TYPE_SMARTVALVE)
        || token.eq_ignore_ascii_case("smart_valve")
        || token.eq_ignore_ascii_case("sv")
    {
        return Some(FACTS_DEVICE_TYPE_SMARTVALVE);
    }
    None
}

fn dict_utf8() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn dict_utf8_u32() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

fn map_string_string() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn map_string_f64() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn contingencies_elements_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "element",
        DataType::Struct(
            vec![
                Field::new("element_type", dict_utf8(), false),
                Field::new("branch_id", DataType::Int32, true),
                Field::new("bus_id", DataType::Int32, true),
                Field::new("gen_id", dict_utf8(), true),
                Field::new("load_id", dict_utf8(), true),
                Field::new("amount_mw", DataType::Float64, true),
                Field::new("status_change", DataType::Boolean, false),
                Field::new("equipment_kind", dict_utf8(), true),
                Field::new("equipment_id", dict_utf8(), true),
            ]
            .into(),
        ),
        false,
    )))
}

/// Standard nullable contingency id field for solved/export result tables.
pub fn solved_results_contingency_id_field() -> Field {
    Field::new(COLUMN_CONTINGENCY_ID, dict_utf8(), true)
}

/// File-level metadata applied to each table schema.
pub fn schema_metadata() -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(METADATA_KEY_BRANDING.to_string(), BRANDING.to_string());
    metadata.insert(METADATA_KEY_VERSION.to_string(), SCHEMA_VERSION.to_string());
    metadata.insert(
        METADATA_KEY_RPF_VERSION.to_string(),
        SCHEMA_VERSION.to_string(),
    );
    metadata
}

/// `metadata` table schema.
///
/// v0.8.4 adds planning-vs-solved semantics fields:
/// - `case_mode`: flat_start_planning | warm_start_planning | solved_snapshot
/// - `solved_state_presence`: actual_solved | not_available | not_computed
/// - Solver provenance fields (all nullable): solver_version, solver_iterations,
///   solver_accuracy, solver_mode. Populated only when solved_state_presence=actual_solved.
pub fn metadata_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("base_mva", DataType::Float64, false),
            Field::new("frequency_hz", DataType::Float64, false),
            Field::new("psse_version", DataType::Int32, false),
            Field::new("study_name", dict_utf8(), false),
            Field::new("timestamp_utc", DataType::Utf8, false),
            Field::new("raptrix_version", DataType::Utf8, false),
            Field::new("is_planning_case", DataType::Boolean, false),
            Field::new("source_case_id", dict_utf8(), false),
            Field::new("snapshot_timestamp_utc", DataType::Utf8, false),
            Field::new("case_fingerprint", DataType::Utf8, false),
            Field::new("validation_mode", dict_utf8(), false),
            Field::new("custom_metadata", map_string_string(), true),
            // v0.8.4: planning-vs-solved semantics
            Field::new("case_mode", dict_utf8(), false),
            Field::new("solved_state_presence", dict_utf8(), true),
            Field::new("solver_version", DataType::Utf8, true),
            Field::new("solver_iterations", DataType::Int32, true),
            Field::new("solver_accuracy", DataType::Float64, true),
            Field::new("solver_mode", dict_utf8(), true),
            // v0.8.5: angle-reference frame and shunt provenance
            // bus_id of the angle reference (slack) bus used in the solve.
            Field::new("slack_bus_id_solved", DataType::Int32, true),
            // Angle reference value in degrees applied at the slack bus (typically 0.0).
            Field::new("angle_reference_deg", DataType::Float64, true),
            // Indicates whether switched-shunt solved state (step + susceptance) is
            // present in switched_shunts_solved: actual_solved | not_available.
            Field::new("solved_shunt_state_presence", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// `buses` table schema.
pub fn buses_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
            Field::new("type", DataType::Int8, false),
            Field::new("p_sched", DataType::Float64, false),
            Field::new("q_sched", DataType::Float64, false),
            Field::new("v_mag_set", DataType::Float64, false),
            Field::new("v_ang_set", DataType::Float64, false),
            Field::new("q_min", DataType::Float64, false),
            Field::new("q_max", DataType::Float64, false),
            Field::new("g_shunt", DataType::Float64, false),
            Field::new("b_shunt", DataType::Float64, false),
            Field::new("area", DataType::Int32, false),
            Field::new("zone", DataType::Int32, false),
            Field::new("owner", DataType::Int32, false),
            Field::new("v_min", DataType::Float64, false),
            Field::new("v_max", DataType::Float64, false),
            Field::new("p_min_agg", DataType::Float64, false),
            Field::new("p_max_agg", DataType::Float64, false),
            Field::new("nominal_kv", DataType::Float64, true),
            Field::new("bus_uuid", dict_utf8(), false),
        ],
        schema_metadata(),
    )
}

/// `branches` table schema.
pub fn branches_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("branch_id", DataType::Int32, false),
            Field::new("from_bus_id", DataType::Int32, false),
            Field::new("to_bus_id", DataType::Int32, false),
            Field::new("ckt", dict_utf8(), false),
            Field::new("r", DataType::Float64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("b_shunt", DataType::Float64, false),
            Field::new("tap", DataType::Float64, false),
            Field::new("phase", DataType::Float64, false),
            Field::new("rate_a", DataType::Float64, false),
            Field::new("rate_b", DataType::Float64, false),
            Field::new("rate_c", DataType::Float64, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("name", dict_utf8_u32(), true),
            Field::new("from_nominal_kv", DataType::Float64, true),
            Field::new("to_nominal_kv", DataType::Float64, true),
            // v0.8.6: additive generic FACTS control metadata.
            Field::new("device_type", dict_utf8(), true),
            Field::new("control_mode", dict_utf8(), true),
            Field::new("control_target_flow_mw", DataType::Float64, true),
            Field::new("x_min_pu", DataType::Float64, true),
            Field::new("x_max_pu", DataType::Float64, true),
            Field::new("injected_voltage_mag_pu", DataType::Float64, true),
            Field::new("injected_voltage_angle_deg", DataType::Float64, true),
            Field::new("facts_params", map_string_f64(), true),
        ],
        schema_metadata(),
    )
}

/// `generators` table schema.
pub fn generators_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("id", dict_utf8(), false),
            Field::new("p_sched_pu", DataType::Float64, false),
            Field::new("p_min_pu", DataType::Float64, false),
            Field::new("p_max_pu", DataType::Float64, false),
            Field::new("q_min_pu", DataType::Float64, false),
            Field::new("q_max_pu", DataType::Float64, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("mbase_mva", DataType::Float64, false),
            Field::new("H", DataType::Float64, false),
            Field::new("xd_prime", DataType::Float64, false),
            Field::new("D", DataType::Float64, false),
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// `loads` table schema.
pub fn loads_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("id", dict_utf8(), false),
            Field::new("status", DataType::Boolean, false),
            Field::new("p_pu", DataType::Float64, false),
            Field::new("q_pu", DataType::Float64, false),
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// `fixed_shunts` table schema.
pub fn fixed_shunts_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("id", dict_utf8(), false),
            Field::new("status", DataType::Boolean, false),
            Field::new("g_pu", DataType::Float64, false),
            Field::new("b_pu", DataType::Float64, false),
        ],
        schema_metadata(),
    )
}

/// `switched_shunts` table schema.
pub fn switched_shunts_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("v_low", DataType::Float64, false),
            Field::new("v_high", DataType::Float64, false),
            Field::new(
                "b_steps",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                false,
            ),
            Field::new("current_step", DataType::Int32, false),
            // v0.8.3: authoritative initial susceptance (BINIT/base_mva for PSS/E;
            // sum of energised steps for CIM).  Nullable so v0.8.2 files remain
            // readable; writers MUST populate this field going forward.
            Field::new("b_init_pu", DataType::Float64, true),
            // v0.8.5: stable per-bank identity to disambiguate multiple banks at
            // the same bus.  CIM path: ShuntCompensator mRID.  PSS/E path:
            // synthesized as "{bus_id}_shunt_{n}" (1-indexed).  Nullable for
            // backward compatibility; writers must populate when available.
            Field::new("shunt_id", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// `transformers_2w` table schema.
pub fn transformers_2w_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("from_bus_id", DataType::Int32, false),
            Field::new("to_bus_id", DataType::Int32, false),
            Field::new("ckt", dict_utf8(), false),
            Field::new("r", DataType::Float64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("winding1_r", DataType::Float64, false),
            Field::new("winding1_x", DataType::Float64, false),
            Field::new("winding2_r", DataType::Float64, false),
            Field::new("winding2_x", DataType::Float64, false),
            Field::new("g", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("tap_ratio", DataType::Float64, false),
            Field::new("nominal_tap_ratio", DataType::Float64, false),
            Field::new("phase_shift", DataType::Float64, false),
            Field::new("vector_group", dict_utf8(), false),
            Field::new("rate_a", DataType::Float64, false),
            Field::new("rate_b", DataType::Float64, false),
            Field::new("rate_c", DataType::Float64, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("name", dict_utf8_u32(), true),
            Field::new("from_nominal_kv", DataType::Float64, true),
            Field::new("to_nominal_kv", DataType::Float64, true),
        ],
        schema_metadata(),
    )
}

/// `transformers_3w` table schema.
pub fn transformers_3w_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_h_id", DataType::Int32, false),
            Field::new("bus_m_id", DataType::Int32, false),
            Field::new("bus_l_id", DataType::Int32, false),
            Field::new("star_bus_id", DataType::Int32, true),
            Field::new("ckt", dict_utf8(), false),
            Field::new("r_hm", DataType::Float64, false),
            Field::new("x_hm", DataType::Float64, false),
            Field::new("r_hl", DataType::Float64, false),
            Field::new("x_hl", DataType::Float64, false),
            Field::new("r_ml", DataType::Float64, false),
            Field::new("x_ml", DataType::Float64, false),
            Field::new("tap_h", DataType::Float64, false),
            Field::new("tap_m", DataType::Float64, false),
            Field::new("tap_l", DataType::Float64, false),
            Field::new("phase_shift", DataType::Float64, false),
            Field::new("vector_group", dict_utf8(), false),
            Field::new("rate_a", DataType::Float64, false),
            Field::new("rate_b", DataType::Float64, false),
            Field::new("rate_c", DataType::Float64, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("name", dict_utf8_u32(), true),
            Field::new("nominal_kv_h", DataType::Float64, true),
            Field::new("nominal_kv_m", DataType::Float64, true),
            Field::new("nominal_kv_l", DataType::Float64, true),
        ],
        schema_metadata(),
    )
}

/// `areas` lookup table schema.
pub fn areas_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("area_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
            Field::new("interchange_mw", DataType::Float64, true),
        ],
        schema_metadata(),
    )
}

/// `zones` lookup table schema.
pub fn zones_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("zone_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
        ],
        schema_metadata(),
    )
}

/// `owners` lookup table schema.
pub fn owners_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("owner_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
        ],
        schema_metadata(),
    )
}

/// `contingencies` table schema.
pub fn contingencies_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("contingency_id", dict_utf8(), false),
            Field::new("elements", contingencies_elements_type(), false),
        ],
        schema_metadata(),
    )
}

/// `interfaces` table schema.
pub fn interfaces_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("interface_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
            Field::new(
                "monitored_branches",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
            Field::new("transfer_limit_mw", DataType::Float64, false),
        ],
        schema_metadata(),
    )
}

/// `dynamics_models` table schema.
pub fn dynamics_models_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("gen_id", dict_utf8(), false),
            Field::new("model_type", dict_utf8(), false),
            Field::new("params", map_string_f64(), false),
        ],
        schema_metadata(),
    )
}

/// Optional `facts_devices` table schema (v0.8.6+).
pub fn facts_devices_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("device_id", dict_utf8(), false),
            Field::new("branch_id", DataType::Int32, true),
            Field::new("bus_id", DataType::Int32, true),
            Field::new("device_type", dict_utf8(), false),
            Field::new("status", DataType::Boolean, false),
            Field::new("control_mode", dict_utf8(), true),
            Field::new("target_flow_mw", DataType::Float64, true),
            Field::new("x_min_pu", DataType::Float64, true),
            Field::new("x_max_pu", DataType::Float64, true),
            Field::new("voltage_injection_mag_pu", DataType::Float64, true),
            Field::new("voltage_injection_angle_deg", DataType::Float64, true),
            Field::new("response_time_ms", DataType::Float64, true),
            Field::new("rating_mva", DataType::Float64, true),
            Field::new("dynamics_model_ref", dict_utf8(), true),
            Field::new("params", map_string_f64(), true),
            Field::new("name", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `connectivity_groups` table schema.
pub fn connectivity_groups_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("topological_bus_id", DataType::Int32, false),
            Field::new("topological_node_mrid", dict_utf8(), false),
            Field::new(
                "connectivity_node_mrids",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new("connectivity_count", DataType::Int32, false),
        ],
        schema_metadata(),
    )
}

/// Optional `node_breaker_detail` table schema.
pub fn node_breaker_detail_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("switch_id", dict_utf8(), false),
            Field::new("switch_type", dict_utf8(), false),
            Field::new("from_bus_id", DataType::Int32, true),
            Field::new("to_bus_id", DataType::Int32, true),
            Field::new("connectivity_node_a", dict_utf8(), true),
            Field::new("connectivity_node_b", dict_utf8(), true),
            Field::new("is_open", DataType::Boolean, true),
            Field::new("normal_open", DataType::Boolean, true),
            Field::new("status", DataType::Boolean, true),
        ],
        schema_metadata(),
    )
}

/// Optional `switch_detail` table schema.
pub fn switch_detail_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("switch_id", dict_utf8(), false),
            Field::new("name", dict_utf8_u32(), true),
            Field::new("switch_type", dict_utf8(), false),
            Field::new("is_open", DataType::Boolean, true),
            Field::new("normal_open", DataType::Boolean, true),
            Field::new("retained", DataType::Boolean, true),
        ],
        schema_metadata(),
    )
}

/// Optional `connectivity_nodes` table schema.
pub fn connectivity_nodes_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("connectivity_node_mrid", dict_utf8(), false),
            Field::new("topological_node_mrid", dict_utf8(), true),
            Field::new("bus_id", DataType::Int32, true),
        ],
        schema_metadata(),
    )
}

/// Optional `diagram_objects` table schema.
pub fn diagram_objects_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("element_id", DataType::Utf8, false),
            Field::new("element_type", DataType::Utf8, false),
            Field::new("diagram_id", DataType::Utf8, false),
            Field::new("rotation", DataType::Float32, true),
            Field::new("visible", DataType::Boolean, false),
            Field::new("draw_order", DataType::Int32, true),
        ],
        schema_metadata(),
    )
}

/// Optional `diagram_points` table schema.
pub fn diagram_points_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("element_id", DataType::Utf8, false),
            Field::new("diagram_id", DataType::Utf8, false),
            Field::new("seq", DataType::Int32, false),
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ],
        schema_metadata(),
    )
}

/// Returns optional node-breaker detail table schemas in deterministic order.
pub fn node_breaker_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_NODE_BREAKER_DETAIL, node_breaker_detail_schema()),
        (TABLE_SWITCH_DETAIL, switch_detail_schema()),
        (TABLE_CONNECTIVITY_NODES, connectivity_nodes_schema()),
    ]
}

/// Returns optional diagram layout table schemas in deterministic order.
pub fn diagram_layout_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_DIAGRAM_OBJECTS, diagram_objects_schema()),
        (TABLE_DIAGRAM_POINTS, diagram_points_schema()),
    ]
}

/// Optional `buses_solved` table schema (v0.8.4+).
///
/// Emitted only when `case_mode = solved_snapshot`.  All value columns are
/// nullable so a partial solve or a bus with no result can be represented
/// honestly.  `provenance` encodes per-row data origin:
/// `actual_solved` | `not_available` | `not_computed`.
pub fn buses_solved_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            // Foreign key into buses.bus_id — must be present for every row.
            Field::new("bus_id", DataType::Int32, false),
            // Post-solve voltage magnitude in per-unit.
            Field::new("v_mag_pu", DataType::Float64, true),
            // Post-solve voltage angle in degrees.
            Field::new("v_ang_deg", DataType::Float64, true),
            // Total net real injection at bus in per-unit (positive = generation).
            Field::new("p_inj_pu", DataType::Float64, true),
            // Total net reactive injection at bus in per-unit.
            Field::new("q_inj_pu", DataType::Float64, true),
            // Effective bus type after Newton-Raphson (may differ from planning
            // intent when PV → PQ switching occurred): 1=PQ, 2=PV, 3=slack.
            Field::new("bus_type_solved", DataType::Int8, true),
            // Per-row data provenance.
            Field::new("provenance", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `generators_solved` table schema (v0.8.4+).
///
/// Emitted only when `case_mode = solved_snapshot`.  Captures post-solve
/// real and reactive output from each generating unit, plus the PV→PQ
/// switching flag which must never be back-propagated into planning fields.
pub fn generators_solved_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            // Foreign key into generators.bus_id — must be present.
            Field::new("bus_id", DataType::Int32, false),
            // Foreign key into generators.id — must be present.
            Field::new("id", dict_utf8(), false),
            // Actual solved real power output in per-unit.
            Field::new("p_actual_pu", DataType::Float64, true),
            // Actual solved reactive power output in per-unit.
            Field::new("q_actual_pu", DataType::Float64, true),
            // v0.8.5: actual solved real power in MW (= p_actual_pu * base_mva).
            // Provided for solver-native unit convenience; always consistent with p_actual_pu.
            Field::new("p_mw", DataType::Float64, true),
            // v0.8.5: actual solved reactive power in MVAR (= q_actual_pu * base_mva).
            Field::new("q_mvar", DataType::Float64, true),
            // v0.8.5: in-service status at solve time.  A generator can be in-service
            // in the planning case but excluded from the solve (e.g., forced off by
            // unit commitment).  Null means status unknown.
            Field::new("status", DataType::Boolean, true),
            // True when this unit's bus was switched from PV to PQ during solve.
            // This flag must never be written back to generators.p_sched_pu.
            Field::new("pv_to_pq", DataType::Boolean, true),
            // Per-row data provenance.
            Field::new("provenance", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `switched_shunts_solved` table schema (v0.8.5+).
///
/// Emitted only when `case_mode = solved_snapshot`.  One row per switched-shunt
/// bank in service after Newton-Raphson convergence.  Uses `shunt_id` for
/// stable cross-table identity when multiple banks exist at the same bus.
pub fn switched_shunts_solved_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            // Foreign key into switched_shunts.bus_id — must be present.
            Field::new("bus_id", DataType::Int32, false),
            // Stable per-bank identifier matching switched_shunts.shunt_id.
            // Nullable when source data lacks a stable bank mRID; bus_id alone
            // is insufficient for disambiguation when multiple banks exist at a bus.
            Field::new("shunt_id", dict_utf8(), true),
            // Energized step index after Newton-Raphson convergence (1-indexed).
            // Corresponds to switched_shunts.b_steps[current_step_solved - 1].
            Field::new("current_step_solved", DataType::Int32, true),
            // Post-solve total susceptance in per-unit.  Matches
            // b_steps[current_step_solved - 1] for well-formed cases.
            Field::new("b_pu_solved", DataType::Float64, true),
            // Per-row data provenance.
            Field::new("provenance", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `facts_solved` table schema (v0.8.6+).
pub fn facts_solved_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("device_id", dict_utf8(), false),
            Field::new("effective_x_pu", DataType::Float64, true),
            Field::new("injected_voltage_mag_pu", DataType::Float64, true),
            Field::new("injected_voltage_angle_deg", DataType::Float64, true),
            Field::new("p_effect_mw", DataType::Float64, true),
            Field::new("q_effect_mvar", DataType::Float64, true),
            Field::new("status", DataType::Boolean, true),
        ],
        schema_metadata(),
    )
}

/// Returns optional FACTS table schemas in deterministic order (v0.8.6+).
pub fn facts_table_schemas(include_facts_solved: bool) -> Vec<(&'static str, Schema)> {
    let mut tables = vec![(TABLE_FACTS_DEVICES, facts_devices_schema())];
    if include_facts_solved {
        tables.push((TABLE_FACTS_SOLVED, facts_solved_schema()));
    }
    tables
}

/// Returns optional solved-state table schemas in deterministic order (v0.8.4+).
///
/// These tables are appended after all other optional root columns when
/// `case_mode = solved_snapshot`.
pub fn solved_state_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_BUSES_SOLVED, buses_solved_schema()),
        (TABLE_GENERATORS_SOLVED, generators_solved_schema()),
        (TABLE_SWITCHED_SHUNTS_SOLVED, switched_shunts_solved_schema()),
    ]
}

/// Returns all required table schemas in canonical v0.7.1 order.
pub fn all_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_METADATA, metadata_schema()),
        (TABLE_BUSES, buses_schema()),
        (TABLE_BRANCHES, branches_schema()),
        (TABLE_GENERATORS, generators_schema()),
        (TABLE_LOADS, loads_schema()),
        (TABLE_FIXED_SHUNTS, fixed_shunts_schema()),
        (TABLE_SWITCHED_SHUNTS, switched_shunts_schema()),
        (TABLE_TRANSFORMERS_2W, transformers_2w_schema()),
        (TABLE_TRANSFORMERS_3W, transformers_3w_schema()),
        (TABLE_AREAS, areas_schema()),
        (TABLE_ZONES, zones_schema()),
        (TABLE_OWNERS, owners_schema()),
        (TABLE_CONTINGENCIES, contingencies_schema()),
        (TABLE_INTERFACES, interfaces_schema()),
        (TABLE_DYNAMICS_MODELS, dynamics_models_schema()),
    ]
}

/// Returns the schema for a known table name.
pub fn table_schema(table_name: &str) -> Option<Schema> {
    match table_name {
        TABLE_METADATA => Some(metadata_schema()),
        TABLE_BUSES => Some(buses_schema()),
        TABLE_BRANCHES => Some(branches_schema()),
        TABLE_GENERATORS => Some(generators_schema()),
        TABLE_LOADS => Some(loads_schema()),
        TABLE_FIXED_SHUNTS => Some(fixed_shunts_schema()),
        TABLE_SWITCHED_SHUNTS => Some(switched_shunts_schema()),
        TABLE_TRANSFORMERS_2W => Some(transformers_2w_schema()),
        TABLE_TRANSFORMERS_3W => Some(transformers_3w_schema()),
        TABLE_AREAS => Some(areas_schema()),
        TABLE_ZONES => Some(zones_schema()),
        TABLE_OWNERS => Some(owners_schema()),
        TABLE_CONTINGENCIES => Some(contingencies_schema()),
        TABLE_INTERFACES => Some(interfaces_schema()),
        TABLE_DYNAMICS_MODELS => Some(dynamics_models_schema()),
        TABLE_FACTS_DEVICES => Some(facts_devices_schema()),
        TABLE_CONNECTIVITY_GROUPS => Some(connectivity_groups_schema()),
        TABLE_NODE_BREAKER_DETAIL => Some(node_breaker_detail_schema()),
        TABLE_SWITCH_DETAIL => Some(switch_detail_schema()),
        TABLE_CONNECTIVITY_NODES => Some(connectivity_nodes_schema()),
        TABLE_DIAGRAM_OBJECTS => Some(diagram_objects_schema()),
        TABLE_DIAGRAM_POINTS => Some(diagram_points_schema()),
        TABLE_DYNAMICS => Some(dynamics_models_schema()),
        TABLE_BUSES_SOLVED => Some(buses_solved_schema()),
        TABLE_GENERATORS_SOLVED => Some(generators_solved_schema()),
        TABLE_SWITCHED_SHUNTS_SOLVED => Some(switched_shunts_solved_schema()),
        TABLE_FACTS_SOLVED => Some(facts_solved_schema()),
        _ => None,
    }
}

/// Backward-compatible alias retained for older call sites.
pub fn dynamics_schema() -> Schema {
    dynamics_models_schema()
}

/// Backward-compatible alias retained for older call sites.
pub fn powerflow_schema() -> Schema {
    buses_schema()
}

/// Backward-compatible alias retained for older call sites.
pub fn branch_schema() -> Schema {
    branches_schema()
}

#[cfg(test)]
mod tests {
    use super::{
        branches_schema, contingencies_schema, diagram_objects_schema, diagram_points_schema,
        facts_devices_schema, facts_solved_schema, normalize_facts_device_type,
    };
    use arrow::datatypes::DataType;

    #[test]
    fn diagram_object_and_point_schemas_match_contract() {
        let objects = diagram_objects_schema();
        assert_eq!(objects.fields().len(), 6);
        assert_eq!(objects.field(0).name(), "element_id");
        assert_eq!(objects.field(0).data_type(), &DataType::Utf8);
        assert!(!objects.field(0).is_nullable());
        assert_eq!(objects.field(1).name(), "element_type");
        assert_eq!(objects.field(1).data_type(), &DataType::Utf8);
        assert!(!objects.field(1).is_nullable());
        assert_eq!(objects.field(2).name(), "diagram_id");
        assert_eq!(objects.field(2).data_type(), &DataType::Utf8);
        assert!(!objects.field(2).is_nullable());
        assert_eq!(objects.field(3).name(), "rotation");
        assert_eq!(objects.field(3).data_type(), &DataType::Float32);
        assert!(objects.field(3).is_nullable());
        assert_eq!(objects.field(4).name(), "visible");
        assert_eq!(objects.field(4).data_type(), &DataType::Boolean);
        assert!(!objects.field(4).is_nullable());
        assert_eq!(objects.field(5).name(), "draw_order");
        assert_eq!(objects.field(5).data_type(), &DataType::Int32);
        assert!(objects.field(5).is_nullable());

        let points = diagram_points_schema();
        assert_eq!(points.fields().len(), 5);
        assert_eq!(points.field(0).name(), "element_id");
        assert_eq!(points.field(0).data_type(), &DataType::Utf8);
        assert!(!points.field(0).is_nullable());
        assert_eq!(points.field(1).name(), "diagram_id");
        assert_eq!(points.field(1).data_type(), &DataType::Utf8);
        assert!(!points.field(1).is_nullable());
        assert_eq!(points.field(2).name(), "seq");
        assert_eq!(points.field(2).data_type(), &DataType::Int32);
        assert!(!points.field(2).is_nullable());
        assert_eq!(points.field(3).name(), "x");
        assert_eq!(points.field(3).data_type(), &DataType::Float64);
        assert!(!points.field(3).is_nullable());
        assert_eq!(points.field(4).name(), "y");
        assert_eq!(points.field(4).data_type(), &DataType::Float64);
        assert!(!points.field(4).is_nullable());
    }

    #[test]
    fn branches_schema_appends_facts_columns() {
        let branches = branches_schema();
        assert_eq!(branches.fields().len(), 24);
        assert_eq!(branches.field(16).name(), "device_type");
        assert_eq!(branches.field(17).name(), "control_mode");
        assert_eq!(branches.field(18).name(), "control_target_flow_mw");
        assert_eq!(branches.field(19).name(), "x_min_pu");
        assert_eq!(branches.field(20).name(), "x_max_pu");
        assert_eq!(branches.field(21).name(), "injected_voltage_mag_pu");
        assert_eq!(branches.field(22).name(), "injected_voltage_angle_deg");
        assert_eq!(branches.field(23).name(), "facts_params");
    }

    #[test]
    fn facts_tables_match_contract() {
        let devices = facts_devices_schema();
        assert_eq!(devices.fields().len(), 16);
        assert_eq!(devices.field(0).name(), "device_id");
        assert!(!devices.field(0).is_nullable());
        assert_eq!(devices.field(3).name(), "device_type");
        assert!(!devices.field(3).is_nullable());

        let solved = facts_solved_schema();
        assert_eq!(solved.fields().len(), 7);
        assert_eq!(solved.field(0).name(), "device_id");
        assert_eq!(solved.field(1).name(), "effective_x_pu");
        assert_eq!(solved.field(6).name(), "status");
    }

    #[test]
    fn contingency_elements_include_generic_equipment_identity() {
        let contingencies = contingencies_schema();
        let elements_field = contingencies.field(1);
        let DataType::List(element_field) = elements_field.data_type() else {
            panic!("contingencies.elements must be a list");
        };
        let DataType::Struct(child_fields) = element_field.data_type() else {
            panic!("contingencies.elements child must be a struct");
        };
        assert!(child_fields.iter().any(|field| field.name() == "equipment_kind"));
        assert!(child_fields.iter().any(|field| field.name() == "equipment_id"));
    }

    #[test]
    fn smartvalve_alias_normalization_is_canonical() {
        assert_eq!(normalize_facts_device_type("smartvalve"), Some("smartvalve"));
        assert_eq!(normalize_facts_device_type("SV"), Some("smartvalve"));
        assert_eq!(normalize_facts_device_type("sv"), Some("smartvalve"));
        assert_eq!(normalize_facts_device_type("smart_valve"), Some("smartvalve"));
        assert_eq!(normalize_facts_device_type("svc"), None);
    }
}
