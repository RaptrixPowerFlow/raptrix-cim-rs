// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow schema definitions for the Raptrix PowerFlow Interchange v0.8.0 profile.
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
pub const BRANDING: &str = "Raptrix CIM-Arrow / PowerFlow Interchange v0.8.0 - High-performance open CIM profile (CGMES 3.0+) by Musto Technologies LLC. Copyright (c) 2026 Musto Technologies LLC.";

/// Canonical RPF format version tag embedded as file-level metadata.
pub const RPF_VERSION: &str = "0.8.0";

/// Supported RPF versions accepted by generic Arrow IPC readers.
/// v0.8.0 introduces diagram layout support and drops CGMES 2.4.x compatibility.
pub const SUPPORTED_RPF_VERSIONS: &[&str] = &["0.8.0", "0.7.1", "0.7.0"];

/// Backward-compatible alias retained for older call sites.
pub const SCHEMA_VERSION: &str = RPF_VERSION;

/// File-level metadata key for branding string.
pub const METADATA_KEY_BRANDING: &str = "raptrix.branding";
/// File-level metadata key for schema version.
pub const METADATA_KEY_VERSION: &str = "raptrix.version";
/// File-level metadata key for RPF version alias.
pub const METADATA_KEY_RPF_VERSION: &str = "rpf_version";
/// Optional metadata key indicating node-breaker optional tables are emitted.
pub const METADATA_KEY_FEATURE_NODE_BREAKER: &str = "raptrix.features.node_breaker";
/// Optional metadata key indicating diagram layout optional tables are emitted.
pub const METADATA_KEY_FEATURE_DIAGRAM_LAYOUT: &str = "raptrix.features.diagram_layout";
/// Optional metadata key indicating contingencies table uses placeholder rows.
pub const METADATA_KEY_FEATURE_CONTINGENCIES_STUB: &str = "raptrix.features.contingencies_stub";
/// Optional metadata key indicating dynamics_models table uses placeholder rows.
pub const METADATA_KEY_FEATURE_DYNAMICS_STUB: &str = "raptrix.features.dynamics_stub";

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

/// Optional column required on export-side solved-result tables.
pub const COLUMN_CONTINGENCY_ID: &str = "contingency_id";

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
            Field::new("custom_metadata", map_string_string(), true),
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
            Field::new("p_sched_mw", DataType::Float64, false),
            Field::new("p_min_mw", DataType::Float64, false),
            Field::new("p_max_mw", DataType::Float64, false),
            Field::new("q_min_mvar", DataType::Float64, false),
            Field::new("q_max_mvar", DataType::Float64, false),
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
            Field::new("p_mw", DataType::Float64, false),
            Field::new("q_mvar", DataType::Float64, false),
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
            Field::new("g_mw", DataType::Float64, false),
            Field::new("b_mvar", DataType::Float64, false),
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
        TABLE_CONNECTIVITY_GROUPS => Some(connectivity_groups_schema()),
        TABLE_NODE_BREAKER_DETAIL => Some(node_breaker_detail_schema()),
        TABLE_SWITCH_DETAIL => Some(switch_detail_schema()),
        TABLE_CONNECTIVITY_NODES => Some(connectivity_nodes_schema()),
        TABLE_DIAGRAM_OBJECTS => Some(diagram_objects_schema()),
        TABLE_DIAGRAM_POINTS => Some(diagram_points_schema()),
        TABLE_DYNAMICS => Some(dynamics_models_schema()),
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
    use super::{diagram_objects_schema, diagram_points_schema};
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
}
