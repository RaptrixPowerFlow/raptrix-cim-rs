// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow schema definitions for the Raptrix PowerFlow Interchange v0.5.2 profile.
//!
//! This module exposes one exact Arrow schema per required table in the locked
//! `.rpf` contract, plus a deterministic table registry helper.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Human-readable branding string embedded as file-level metadata.
pub const BRANDING: &str = "Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC\nCopyright (c) 2026 Musto Technologies LLC";

/// Schema version tag embedded as file-level metadata.
pub const SCHEMA_VERSION: &str = "0.5.2";

pub const TABLE_METADATA: &str = "metadata";
pub const TABLE_BUSES: &str = "buses";
pub const TABLE_BRANCHES: &str = "branches";
pub const TABLE_GENERATORS: &str = "generators";
pub const TABLE_LOADS: &str = "loads";
pub const TABLE_FIXED_SHUNTS: &str = "fixed_shunts";
pub const TABLE_SWITCHED_SHUNTS: &str = "switched_shunts";
pub const TABLE_TRANSFORMERS_2W: &str = "transformers_2w";
pub const TABLE_TRANSFORMERS_3W: &str = "transformers_3w";
pub const TABLE_AREAS: &str = "areas";
pub const TABLE_ZONES: &str = "zones";
pub const TABLE_OWNERS: &str = "owners";
pub const TABLE_CONTINGENCIES: &str = "contingencies";
pub const TABLE_INTERFACES: &str = "interfaces";
pub const TABLE_DYNAMICS_MODELS: &str = "dynamics_models";
/// Optional detail table emitted only when connectivity-detail mode is enabled.
pub const TABLE_CONNECTIVITY_GROUPS: &str = "connectivity_groups";
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
            DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]
            .into()),
            false,
        )),
        false,
    )
}

fn map_string_f64() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]
            .into()),
            false,
        )),
        false,
    )
}

fn contingencies_elements_type() -> DataType {
    // Allowed element_type values are explicitly constrained by contract docs:
    // branch_outage, gen_trip, load_shed, shunt_switch.
    DataType::List(Arc::new(Field::new(
        "element",
        DataType::Struct(vec![
            Field::new("element_type", dict_utf8(), false),
            Field::new("branch_id", DataType::Int32, true),
            Field::new("bus_id", DataType::Int32, true),
            Field::new("gen_id", dict_utf8(), true),
            Field::new("load_id", dict_utf8(), true),
            Field::new("amount_mw", DataType::Float64, true),
            Field::new("status_change", DataType::Boolean, false),
        ]
        .into()),
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
    metadata.insert("raptrix.branding".to_string(), BRANDING.to_string());
    metadata.insert("raptrix.version".to_string(), SCHEMA_VERSION.to_string());
    metadata.insert("rpf_version".to_string(), SCHEMA_VERSION.to_string());
    metadata
}

/// v0.5 `metadata` table schema (exactly one row at write time).
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

/// v0.5 `buses` table schema.
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
        ],
        schema_metadata(),
    )
}

/// v0.5 `branches` table schema.
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
            // Optional operator-friendly label; additive in v0.5.2.
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// v0.5 `generators` table schema.
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
            // Optional operator-friendly label; additive in v0.5.2.
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// v0.5 `loads` table schema.
pub fn loads_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("id", dict_utf8(), false),
            Field::new("status", DataType::Boolean, false),
            Field::new("p_mw", DataType::Float64, false),
            Field::new("q_mvar", DataType::Float64, false),
            // Optional operator-friendly label; additive in v0.5.2.
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// v0.5 `fixed_shunts` table schema.
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

/// v0.5 `switched_shunts` table schema.
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

/// v0.5 `transformers_2w` table schema.
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
            // Optional operator-friendly label; additive in v0.5.2.
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// v0.5 `transformers_3w` table schema.
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
            // Optional operator-friendly label; additive in v0.5.2.
            Field::new("name", dict_utf8_u32(), true),
        ],
        schema_metadata(),
    )
}

/// v0.5 `areas` lookup table schema.
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

/// v0.5 `zones` lookup table schema.
pub fn zones_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("zone_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
        ],
        schema_metadata(),
    )
}

/// v0.5 `owners` lookup table schema.
pub fn owners_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("owner_id", DataType::Int32, false),
            Field::new("name", dict_utf8(), false),
        ],
        schema_metadata(),
    )
}

/// v0.5 `contingencies` table schema.
pub fn contingencies_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("contingency_id", dict_utf8(), false),
            Field::new("elements", contingencies_elements_type(), false),
        ],
        schema_metadata(),
    )
}

/// v0.5 `interfaces` table schema.
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

/// v0.5 `dynamics_models` table schema.
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
///
/// This table preserves TP split-bus membership while core `buses` may be
/// collapsed to TopologicalNode level for interoperability.
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

/// Returns all required table schemas in canonical v0.5 order.
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

/// Returns the v0.5 schema for a known table name.
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
