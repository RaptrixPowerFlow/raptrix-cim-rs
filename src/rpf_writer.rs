// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! End-to-end Raptrix PowerFlow Interchange (.rpf) writer.
//!
//! Tenet references:
//! - Tenet 1 (zero-copy first): parser helpers and dictionary/string builders
//!   are used in a way that avoids unnecessary string cloning in hot paths.
//! - Tenet 2 (locked schema): table schemas are sourced only from
//!   `arrow_schema::all_table_schemas()` and written in canonical order.
//! - Tenet 3 (separation of concerns): this module performs parsing/mapping/
//!   serialization only; no solver math is implemented here.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder, StringDictionaryBuilder,
    StructBuilder, new_null_array,
};
use arrow::datatypes::{DataType, Field, Int32Type, UInt32Type};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use raptrix_cim_arrow::{RootWriteOptions, write_root_rpf_with_metadata};
pub use raptrix_cim_arrow::{
    RpfSummary, TableSummary, read_rpf_tables, rpf_file_metadata, summarize_rpf,
};
use sha2::{Digest, Sha256};

use crate::arrow_schema::{
    METADATA_KEY_CASE_FINGERPRINT, METADATA_KEY_CASE_MODE, METADATA_KEY_FEATURE_TOPOLOGY_ONLY,
    METADATA_KEY_FEATURE_ZERO_INJECTION_STUB, METADATA_KEY_SOLVED_SHUNT_STATE_PRESENCE,
    METADATA_KEY_SOLVED_STATE_PRESENCE, METADATA_KEY_SOLVER_ACCURACY,
    METADATA_KEY_SOLVER_ANGLE_REFERENCE_DEG, METADATA_KEY_SOLVER_ITERATIONS,
    METADATA_KEY_SOLVER_MODE, METADATA_KEY_SOLVER_SLACK_BUS_ID, METADATA_KEY_SOLVER_VERSION,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_GENERATION_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_LOAD_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_NETWORK_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ISLANDS_PRESENT, METADATA_KEY_TOPOLOGY_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT, METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE,
    METADATA_KEY_VALIDATION_MODE, SCHEMA_VERSION, TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES,
    TABLE_CONNECTIVITY_NODES, TABLE_CONTINGENCIES, TABLE_DC_LINES_2W, TABLE_DIAGRAM_OBJECTS,
    TABLE_DIAGRAM_POINTS, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS,
    TABLE_INTERFACES, TABLE_LOADS, TABLE_METADATA, TABLE_MULTI_SECTION_LINES,
    TABLE_NODE_BREAKER_DETAIL, TABLE_OWNERS, TABLE_SWITCH_DETAIL, TABLE_SWITCHED_SHUNT_BANKS,
    TABLE_SWITCHED_SHUNTS, TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES, areas_schema,
    branches_schema, buses_schema, connectivity_groups_schema, connectivity_nodes_schema,
    contingencies_schema, dc_lines_2w_schema, diagram_objects_schema, diagram_points_schema,
    dynamics_models_schema, fixed_shunts_schema, generators_schema, interfaces_schema,
    loads_schema, metadata_schema, multi_section_lines_schema, node_breaker_detail_schema,
    owners_schema, switch_detail_schema, switched_shunt_banks_schema, switched_shunts_schema,
    transformers_2w_schema, transformers_3w_schema, zones_schema,
};
use crate::parser;

/// Declares how 3-winding `PowerTransformer` objects are represented in the exported RPF file.
///
/// The canonical metadata key `rpf.transformer_representation_mode` is stamped on every
/// exported file with the corresponding string value so readers can inspect it without
/// understanding CIM source structure.
///
/// **Dual materialization** — active rows in both `transformers_3w` and synthetic star-leg
/// `transformers_2w` for the same physical unit — is always a hard error regardless of mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransformerRepresentationMode {
    /// Physical 3W units appear only as native rows in `transformers_3w`.
    ///
    /// No synthetic star buses are allocated.  This is the CIM-native representation and
    /// the recommended default for CIM converter exports.  Metadata value: `"native_3w"`.
    #[default]
    Native3W,
    /// Physical 3W units are star-expanded into three synthetic 2W legs in `transformers_2w`
    /// via delta-to-wye impedance conversion.  `transformers_3w` has zero active rows.
    ///
    /// Star bus IDs are allocated deterministically from the bus-triple hash in the range
    /// `> 10_000_000` to avoid conflicts with real network bus IDs.  Metadata value: `"expanded"`.
    Expanded,
}

impl TransformerRepresentationMode {
    /// Returns the canonical string value written to file-level metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            TransformerRepresentationMode::Native3W => "native_3w",
            TransformerRepresentationMode::Expanded => "expanded",
        }
    }
}

/// Bus resolution mode for EQ+TP merges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusResolutionMode {
    /// Default interoperability mode: collapse to TP `TopologicalNode`.
    Topological,
    /// Detailed mode: keep EQ `ConnectivityNode` granularity.
    ConnectivityDetail,
}

/// Writer options for profile merge behavior.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub bus_resolution_mode: BusResolutionMode,
    pub detached_island_policy: DetachedIslandPolicy,
    pub emit_connectivity_groups: bool,
    pub emit_node_breaker_detail: bool,
    pub emit_diagram_layout: bool,
    pub contingencies_are_stub: bool,
    pub dynamics_are_stub: bool,
    pub base_mva: f64,
    pub frequency_hz: f64,
    pub study_name: Option<String>,
    pub timestamp_utc: Option<String>,
    /// v0.8.8: required modern-grid contract toggle on metadata row.
    pub modern_grid_profile: bool,
    /// v0.8.8: optional IBR penetration percentage metadata.
    pub ibr_penetration_pct: Option<f64>,
    /// v0.8.8: optional study-purpose metadata.
    pub study_purpose: Option<String>,
    /// v0.8.8: optional scenario tags metadata.
    pub scenario_tags: Vec<String>,
    /// Explicit case mode written to the metadata row and file-level metadata.
    /// CIM EQ/TP exports are always `FlatStartPlanning` (default).
    pub case_mode: CaseMode,
    /// Solver provenance written when `case_mode = SolvedSnapshot`.
    /// Must be `None` for planning cases; must be `Some` when `case_mode = SolvedSnapshot`.
    pub solver_provenance: Option<SolverProvenance>,
    /// How 3-winding transformers are represented in the exported file (v0.8.7+).
    ///
    /// `Native3W` (default): physical 3W units appear as native rows in `transformers_3w`.
    /// `Expanded`: physical 3W units are star-expanded into three 2W legs in `transformers_2w`.
    pub transformer_representation_mode: TransformerRepresentationMode,
}

/// Policy for handling detached electrical islands at export time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachedIslandPolicy {
    /// Preserve all islands and emit topology metadata diagnostics.
    Permissive,
    /// Fail export when detached islands have in-service network/load/generation.
    Strict,
    /// Keep only the largest island and prune detached islands from exported tables.
    PruneDetached,
}

/// Explicit case mode written into the exported RPF metadata row (v0.8.4+).
///
/// This is the single authoritative declaration of what kind of state the
/// exported case represents.  Solvers and downstream consumers use this field
/// to decide whether solved-state tables are expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CaseMode {
    /// All bus voltages initialised to 1.0 pu / 0°.  No solved-state data.
    /// This is the default for all CIM EQ/TP exports.
    #[default]
    FlatStartPlanning,
    /// Planning setpoints copied from a prior solved state (warm start), but
    /// the file is still a planning case — not a solved snapshot.
    WarmStartPlanning,
    /// Post-solve snapshot from the solver.  Solved-state tables
    /// (`buses_solved`, `generators_solved`) must be present and populated.
    SolvedSnapshot,
}

impl CaseMode {
    /// Returns the canonical string value written to metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            CaseMode::FlatStartPlanning => "flat_start_planning",
            CaseMode::WarmStartPlanning => "warm_start_planning",
            CaseMode::SolvedSnapshot => "solved_snapshot",
        }
    }
}

/// Per-export tag indicating whether solved-state field values are present
/// and what their origin is (v0.8.4+).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SolvedStatePresence {
    /// Solver ran successfully and produced results written into solved-state tables.
    ActualSolved,
    /// Solved data was not obtainable for this export (e.g., converter-only path).
    NotAvailable,
    /// No solve has been run; this is a planning-only case.  Default for CIM exports.
    NotComputed,
}

impl SolvedStatePresence {
    /// Returns the canonical string value written to metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            SolvedStatePresence::ActualSolved => "actual_solved",
            SolvedStatePresence::NotAvailable => "not_available",
            SolvedStatePresence::NotComputed => "not_computed",
        }
    }
}

/// Solver provenance block populated when `solved_state_presence = actual_solved` (v0.8.4+).
///
/// All fields are optional; populate only what the solver provides.
/// Written as nullable columns in the `metadata` table and also as
/// file-level metadata keys (`rpf.solver.*`).
#[derive(Debug, Clone, Default)]
pub struct SolverProvenance {
    /// Solver software version string, e.g. `"raptrix-core 1.4.2"`.
    pub solver_version: Option<String>,
    /// Number of Newton-Raphson iterations until convergence.
    pub solver_iterations: Option<i32>,
    /// Final mismatch accuracy (absolute MW/MVAR or per-unit residual norm).
    pub solver_accuracy: Option<f64>,
    /// Bus control mode after convergence, e.g. `"PV"`, `"PV_to_PQ"`.
    pub solver_mode: Option<String>,
    /// The bus_id used as the angle reference (slack bus) in the solve.
    /// Prevents silent reference-frame mismatch when snapshots are re-used.
    /// Written to metadata table column `slack_bus_id_solved` and
    /// file-level key `rpf.solver.slack_bus_id`.
    pub slack_bus_id_solved: Option<i32>,
    /// Angle reference value in degrees applied at the slack bus (typically 0.0).
    /// Written to metadata table column `angle_reference_deg` and
    /// file-level key `rpf.solver.angle_reference_deg`.
    pub angle_reference_deg: Option<f64>,
    /// Whether the `switched_shunts_solved` table contains actual post-solve
    /// shunt state or was unavailable.  Lets loaders fail-fast if a
    /// solved snapshot claims solved but lacks full shunt state.
    /// Written to metadata table column `solved_shunt_state_presence` and
    /// file-level key `rpf.solver.solved_shunt_state_presence`.
    pub solved_shunt_state_presence: Option<SolvedShuntStatePresence>,
}

/// Provenance tag for switched-shunt post-solve state (v0.8.5+).
///
/// Written into the `metadata` table column `solved_shunt_state_presence` and
/// the file-level key `rpf.solver.solved_shunt_state_presence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SolvedShuntStatePresence {
    /// Solver produced per-bank solved step and susceptance.  The
    /// `switched_shunts_solved` table is populated and authoritative.
    ActualSolved,
    /// Solved shunt state was not available (e.g., solver did not track
    /// discrete shunt steps).  The `switched_shunts_solved` table is absent
    /// or empty; loaders should warn rather than fail.
    NotAvailable,
}

impl SolvedShuntStatePresence {
    /// Returns the canonical string value written to metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            SolvedShuntStatePresence::ActualSolved => "actual_solved",
            SolvedShuntStatePresence::NotAvailable => "not_available",
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            bus_resolution_mode: BusResolutionMode::Topological,
            detached_island_policy: DetachedIslandPolicy::Permissive,
            emit_connectivity_groups: false,
            emit_node_breaker_detail: false,
            emit_diagram_layout: true,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
            modern_grid_profile: true,
            ibr_penetration_pct: None,
            study_purpose: None,
            scenario_tags: Vec::new(),
            case_mode: CaseMode::FlatStartPlanning,
            solver_provenance: None,
            transformer_representation_mode: TransformerRepresentationMode::Native3W,
        }
    }
}

/// Summary emitted by the writer for CLI/reporting.
#[derive(Debug, Clone, Default)]
pub struct WriteSummary {
    pub connectivity_bus_count: usize,
    pub final_bus_count: usize,
    pub tp_merged: bool,
    pub connectivity_groups_rows: usize,
    pub node_breaker_rows: usize,
    pub switch_detail_rows: usize,
    pub connectivity_node_rows: usize,
    pub diagram_object_rows: usize,
    pub diagram_point_rows: usize,
    pub dynamics_rows_total: usize,
    pub dynamics_rows_dy_linked: usize,
    pub dynamics_rows_eq_fallback: usize,
}

#[derive(Debug, Clone)]
struct MetadataRow<'a> {
    base_mva: f64,
    frequency_hz: f64,
    psse_version: i32,
    study_name: Cow<'a, str>,
    timestamp_utc: Cow<'a, str>,
    raptrix_version: Cow<'a, str>,
    is_planning_case: bool,
    source_case_id: Cow<'a, str>,
    snapshot_timestamp_utc: Cow<'a, str>,
    case_fingerprint: Cow<'a, str>,
    validation_mode: Cow<'a, str>,
    // v0.8.4 planning-vs-solved semantics
    case_mode: Cow<'a, str>,
    solved_state_presence: Option<Cow<'a, str>>,
    solver_version: Option<Cow<'a, str>>,
    solver_iterations: Option<i32>,
    solver_accuracy: Option<f64>,
    solver_mode: Option<Cow<'a, str>>,
    // v0.8.5 angle-reference frame and shunt provenance
    slack_bus_id_solved: Option<i32>,
    angle_reference_deg: Option<f64>,
    solved_shunt_state_presence: Option<Cow<'a, str>>,
    // v0.8.8 modern-grid metadata
    modern_grid_profile: bool,
    ibr_penetration_pct: Option<f64>,
    has_ibr: bool,
    has_smart_valve: bool,
    has_multi_terminal_dc: bool,
    study_purpose: Option<Cow<'a, str>>,
    scenario_tags: Vec<Cow<'a, str>>,
}

#[derive(Debug, Clone)]
struct BusRow<'a> {
    bus_id: i32,
    name: Cow<'a, str>,
    bus_type: i8,
    p_sched: f64,
    q_sched: f64,
    v_mag_set: f64,
    v_ang_set: f64,
    q_min: f64,
    q_max: f64,
    g_shunt: f64,
    b_shunt: f64,
    area: i32,
    zone: i32,
    owner_id: Option<i32>,
    v_min: f64,
    v_max: f64,
    p_min_agg: f64,
    p_max_agg: f64,
    nominal_kv: Option<f64>,
    bus_uuid: Cow<'a, str>,
}

const VALIDATION_MODE_TOPOLOGY_ONLY: &str = "topology_only";
const VALIDATION_MODE_SOLVED_READY: &str = "solved_ready";

#[derive(Debug, Clone)]
struct BranchRow<'a> {
    branch_id: i32,
    from_bus_id: i32,
    to_bus_id: i32,
    ckt: Cow<'a, str>,
    name: Cow<'a, str>,
    r: f64,
    x: f64,
    b_shunt: f64,
    tap: f64,
    phase: f64,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
    owner_id: Option<i32>,
    from_nominal_kv: Option<f64>,
    to_nominal_kv: Option<f64>,
}

#[derive(Debug, Clone)]
struct GenRow<'a> {
    generator_id: i32,
    bus_id: i32,
    id: Cow<'a, str>,
    name: Cow<'a, str>,
    unit_type: Cow<'a, str>,
    hierarchy_level: Cow<'a, str>,
    parent_generator_id: Option<i32>,
    aggregation_count: Option<i32>,
    p_sched_mw: f64,
    p_min_mw: f64,
    p_max_mw: f64,
    q_min_mvar: f64,
    q_max_mvar: f64,
    status: bool,
    mbase_mva: f64,
    uol_mw: Option<f64>,
    lol_mw: Option<f64>,
    ramp_rate_up_mw_min: Option<f64>,
    ramp_rate_down_mw_min: Option<f64>,
    is_ibr: bool,
    ibr_subtype: Option<Cow<'a, str>>,
    owner_id: Option<i32>,
    market_resource_id: Option<Cow<'a, str>>,
    h: f64,
    xd_prime: f64,
    d: f64,
}

#[derive(Debug, Clone)]
struct LoadRow<'a> {
    bus_id: i32,
    id: Cow<'a, str>,
    name: Cow<'a, str>,
    status: bool,
    p_mw: f64,
    q_mvar: f64,
}

#[derive(Debug, Clone)]
struct FixedShuntRow<'a> {
    bus_id: i32,
    id: Cow<'a, str>,
    status: bool,
    g_mw: f64,
    b_mvar: f64,
}

#[derive(Debug, Clone)]
struct SwitchedShuntRow {
    bus_id: i32,
    status: bool,
    v_low: f64,
    v_high: f64,
    b_steps: Vec<f64>,
    current_step: i32,
    /// Authoritative initial susceptance in per-unit (v0.8.3+).
    /// For CIM: sum of the first `current_step` cumulative b_steps values.
    /// For PSS/E: BINIT / base_mva written directly by the psse-rs converter.
    b_init_pu: f64,
    /// Stable per-bank identity (v0.8.5+).  CIM mRID or PSS/E-synthesized id.
    /// None when not available from source.
    shunt_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SwitchedShuntBankRow {
    shunt_id: i32,
    bank_id: i32,
    b_pu: f64,
    status: bool,
    step: i32,
    bus_id: i32,
}

#[derive(Debug, Clone)]
struct Transformer2WRow<'a> {
    from_bus_id: i32,
    to_bus_id: i32,
    ckt: Cow<'a, str>,
    name: Cow<'a, str>,
    r: f64,
    x: f64,
    winding1_r: f64,
    winding1_x: f64,
    winding2_r: f64,
    winding2_x: f64,
    g: f64,
    b: f64,
    tap_ratio: f64,
    nominal_tap_ratio: f64,
    phase_shift: f64,
    vector_group: Cow<'a, str>,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
    from_nominal_kv: Option<f64>,
    to_nominal_kv: Option<f64>,
}

#[derive(Debug, Clone)]
struct Transformer3WRow<'a> {
    bus_h_id: i32,
    bus_m_id: i32,
    bus_l_id: i32,
    ckt: Cow<'a, str>,
    name: Cow<'a, str>,
    r_hm: f64,
    x_hm: f64,
    r_hl: f64,
    x_hl: f64,
    r_ml: f64,
    x_ml: f64,
    tap_h: f64,
    tap_m: f64,
    tap_l: f64,
    phase_shift: f64,
    vector_group: Cow<'a, str>,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
    nominal_kv_h: Option<f64>,
    nominal_kv_m: Option<f64>,
    nominal_kv_l: Option<f64>,
}

#[derive(Debug, Clone)]
struct AreaRow<'a> {
    area_id: i32,
    name: Cow<'a, str>,
    interchange_mw: Option<f64>,
}

#[derive(Debug, Clone)]
struct ZoneRow<'a> {
    zone_id: i32,
    name: Cow<'a, str>,
}

#[derive(Debug, Clone)]
struct OwnerRow<'a> {
    owner_id: i32,
    name: Cow<'a, str>,
    short_name: Option<Cow<'a, str>>,
    owner_type: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone)]
struct ConnectivityGroupRow<'a> {
    topological_bus_id: i32,
    topological_node_mrid: Cow<'a, str>,
    connectivity_node_mrids: Vec<Cow<'a, str>>,
    connectivity_count: i32,
}

#[derive(Debug, Clone)]
struct NodeBreakerDetailRow<'a> {
    switch_id: Cow<'a, str>,
    switch_type: Cow<'a, str>,
    from_bus_id: Option<i32>,
    to_bus_id: Option<i32>,
    connectivity_node_a: Option<Cow<'a, str>>,
    connectivity_node_b: Option<Cow<'a, str>>,
    is_open: Option<bool>,
    normal_open: Option<bool>,
    status: Option<bool>,
}

#[derive(Debug, Clone)]
struct SwitchDetailRow<'a> {
    switch_id: Cow<'a, str>,
    name: Option<Cow<'a, str>>,
    switch_type: Cow<'a, str>,
    is_open: Option<bool>,
    normal_open: Option<bool>,
    retained: Option<bool>,
}

#[derive(Debug, Clone)]
struct ConnectivityNodeDetailRow<'a> {
    connectivity_node_mrid: Cow<'a, str>,
    topological_node_mrid: Option<Cow<'a, str>>,
    bus_id: Option<i32>,
}

#[derive(Debug, Clone)]
struct DiagramObjectRow<'a> {
    element_id: Cow<'a, str>,
    element_type: Cow<'a, str>,
    diagram_id: Cow<'a, str>,
    rotation: Option<f32>,
    visible: bool,
    draw_order: Option<i32>,
}

#[derive(Debug, Clone)]
struct DiagramPointRow<'a> {
    element_id: Cow<'a, str>,
    diagram_id: Cow<'a, str>,
    seq: i32,
    x: f64,
    y: f64,
}

#[derive(Debug, Clone)]
struct DiagramElementResolution {
    element_type: &'static str,
    element_id: String,
}

#[derive(Debug, Clone)]
struct ContingencyRow<'a> {
    contingency_id: Cow<'a, str>,
    elements: Vec<ContingencyElement<'a>>,
}

#[derive(Debug, Clone)]
struct ContingencyElement<'a> {
    element_type: Cow<'a, str>,
    branch_id: Option<i32>,
    bus_id: Option<i32>,
    status_change: bool,
    equipment_kind: Option<Cow<'a, str>>,
    equipment_id: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone)]
struct DynamicsModelRow<'a> {
    bus_id: i32,
    gen_id: Cow<'a, str>,
    model_type: Cow<'a, str>,
    params: Vec<(Cow<'a, str>, f64)>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Endpoints {
    from_terminal_idx: Option<usize>,
    to_terminal_idx: Option<usize>,
}

fn non_empty_name(name: &str) -> Option<&str> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn shortened_mrid(mrid: &str) -> &str {
    let len = mrid.len().min(8);
    &mrid[..len]
}

fn format_kv_label(nominal_kv: f64) -> String {
    if (nominal_kv.fract()).abs() < f64::EPSILON {
        format!("{}", nominal_kv as i32)
    } else {
        format!("{nominal_kv:.1}")
    }
}

fn make_diagram_element_id(element_type: &str, table_key: &str) -> String {
    format!("{element_type}:{table_key}")
}

fn bus_fallback_name(bus_key: &str, voltage_label: Option<&str>) -> String {
    let voltage = voltage_label.unwrap_or("unknown");
    format!("Bus {voltage}kV {}", shortened_mrid(bus_key))
}

fn equipment_fallback_name(prefix: &str, mrid: &str, voltage_label: Option<&str>) -> String {
    let voltage = voltage_label.unwrap_or("unknown");
    format!("{prefix} {voltage}kV {}", shortened_mrid(mrid))
}

fn branch_constructed_name(
    voltage_label: &str,
    from_bus_name: &str,
    to_bus_name: &str,
    ckt: &str,
) -> String {
    if ckt.trim().is_empty() || ckt == "1" {
        format!("{voltage_label} kV - {from_bus_name} to {to_bus_name}")
    } else {
        format!("{voltage_label} kV - {from_bus_name} to {to_bus_name} (Circuit {ckt})")
    }
}

fn voltage_label_from_name(name: Option<&str>) -> String {
    let Some(text) = name else {
        return "unknown".to_string();
    };
    infer_voltage_kv(text).unwrap_or_else(|| "unknown".to_string())
}

fn infer_voltage_kv(text: &str) -> Option<String> {
    let mut token = String::new();
    let mut best_value: Option<f64> = None;

    let mut commit_token = |candidate: &str| {
        let Ok(value) = candidate.parse::<f64>() else {
            return;
        };
        if !(10.0..=1200.0).contains(&value) {
            return;
        }
        match best_value {
            Some(existing) if value <= existing => {}
            _ => best_value = Some(value),
        }
    };

    for ch in text.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            token.push(ch);
        } else if !token.is_empty() {
            commit_token(&token);
            token.clear();
        }
    }
    if !token.is_empty() {
        commit_token(&token);
    }

    let value = best_value?;
    if (value.fract()).abs() < f64::EPSILON {
        Some(format!("{}", value as i32))
    } else {
        Some(format!("{value:.1}"))
    }
}

fn infer_study_name(cgmes_paths: &[&str]) -> String {
    let Some(first_path) = cgmes_paths.first() else {
        return "cgmes_import".to_string();
    };
    Path::new(first_path)
        .file_stem()
        .and_then(|value| value.to_str())
        .map(|value| value.to_string())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "cgmes_import".to_string())
}

fn current_timestamp_utc() -> String {
    Utc::now().to_rfc3339()
}

fn compute_case_fingerprint(
    cgmes_paths: &[&str],
    source_case_id: &str,
    snapshot_timestamp_utc: &str,
) -> Result<String> {
    let mut normalized_paths: Vec<&str> = cgmes_paths.to_vec();
    normalized_paths.sort_unstable();

    let mut hasher = Sha256::new();
    hasher.update(b"rpf_case_fingerprint_v1\n");
    hasher.update(source_case_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(snapshot_timestamp_utc.as_bytes());
    hasher.update(b"\n");

    for path in normalized_paths {
        let path_obj = Path::new(path);
        let metadata = std::fs::metadata(path_obj)
            .with_context(|| format!("failed to read metadata for fingerprint input: {path}"))?;
        let modified_epoch_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let canonical_path = path_obj
            .canonicalize()
            .unwrap_or_else(|_| path_obj.to_path_buf())
            .to_string_lossy()
            .into_owned();

        hasher.update(canonical_path.as_bytes());
        hasher.update(b"|");
        hasher.update(metadata.len().to_string().as_bytes());
        hasher.update(b"|");
        hasher.update(modified_epoch_ns.to_string().as_bytes());
        hasher.update(b"\n");
    }

    let digest = hasher.finalize();
    Ok(format!("{:x}", digest))
}

// ---------------------------------------------------------------------------
// v0.8.7  Transformer representation contract
// ---------------------------------------------------------------------------

/// Deterministic star bus ID for a 3-winding transformer's synthetic internal node.
///
/// Derived from the bus triple and circuit ID so the same physical 3W transformer
/// always gets the same star bus ID across serialization calls.  The result is
/// guaranteed to be in the range `[10_000_001, i32::MAX]` so it never collides
/// with real CIM bus IDs (which start at 1 and rarely exceed a few hundred thousand
/// even for the largest North American / European models).
fn star_bus_id_for_3w(bus_h: i32, bus_m: i32, bus_l: i32, ckt: &str) -> i32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    bus_h.hash(&mut hasher);
    bus_m.hash(&mut hasher);
    bus_l.hash(&mut hasher);
    ckt.hash(&mut hasher);
    let h = hasher.finish();
    // Map to [10_000_001, i32::MAX].  Range is ~2.1 billion slots; collision
    // probability for a realistic CIM file (< 10_000 3W units) is negligible.
    let range = (i32::MAX as u64) - 10_000_001;
    10_000_001i32 + (h % range) as i32
}

/// Star-expand a slice of active 3W transformer rows into three synthetic 2W legs each.
///
/// For each active `Transformer3WRow` the function:
/// 1. Applies delta-to-wye impedance conversion for all six (r, x) pairwise fields.
/// 2. Allocates a deterministic star bus ID via `star_bus_id_for_3w`.
/// 3. Returns three `Transformer2WRow` values: H→star, M→star, L→star.
///
/// Inactive rows (`status == false`) are preserved as-is in the 3W list and skipped
/// here — their impedance values are unchanged and they do not generate star legs.
fn star_expand_3w_transformers<'a>(
    rows_3w: &[Transformer3WRow<'a>],
    existing_star_ids: &HashSet<i32>,
) -> Vec<Transformer2WRow<'a>> {
    let mut out: Vec<Transformer2WRow<'a>> = Vec::with_capacity(rows_3w.len() * 3);
    let mut allocated: HashSet<i32> = existing_star_ids.clone();

    for row in rows_3w {
        if !row.status {
            continue;
        }

        // Delta-to-wye impedance conversion.
        let r_h = (row.r_hm + row.r_hl - row.r_ml) / 2.0;
        let r_m = (row.r_hm + row.r_ml - row.r_hl) / 2.0;
        let r_l = (row.r_hl + row.r_ml - row.r_hm) / 2.0;
        let x_h = (row.x_hm + row.x_hl - row.x_ml) / 2.0;
        let x_m = (row.x_hm + row.x_ml - row.x_hl) / 2.0;
        let x_l = (row.x_hl + row.x_ml - row.x_hm) / 2.0;

        // Deterministic star bus ID with collision avoidance.
        let mut star_id = star_bus_id_for_3w(row.bus_h_id, row.bus_m_id, row.bus_l_id, &row.ckt);
        // Probe forward on collision (extremely rare; keeps code simple).
        while allocated.contains(&star_id) {
            star_id = star_id.saturating_add(1);
            if star_id <= 10_000_000 {
                star_id = 10_000_001;
            }
        }
        allocated.insert(star_id);

        // H → star leg.
        out.push(Transformer2WRow {
            from_bus_id: row.bus_h_id,
            to_bus_id: star_id,
            ckt: row.ckt.clone(),
            name: Cow::Owned(format!("{}_H", row.name)),
            r: r_h,
            x: x_h,
            winding1_r: r_h,
            winding1_x: x_h,
            winding2_r: 0.0,
            winding2_x: 0.0,
            g: 0.0,
            b: 0.0,
            tap_ratio: row.tap_h,
            nominal_tap_ratio: 1.0,
            phase_shift: row.phase_shift,
            vector_group: row.vector_group.clone(),
            rate_a: row.rate_a,
            rate_b: row.rate_b,
            rate_c: row.rate_c,
            status: true,
            from_nominal_kv: row.nominal_kv_h,
            to_nominal_kv: None,
        });

        // M → star leg.
        out.push(Transformer2WRow {
            from_bus_id: row.bus_m_id,
            to_bus_id: star_id,
            ckt: row.ckt.clone(),
            name: Cow::Owned(format!("{}_M", row.name)),
            r: r_m,
            x: x_m,
            winding1_r: r_m,
            winding1_x: x_m,
            winding2_r: 0.0,
            winding2_x: 0.0,
            g: 0.0,
            b: 0.0,
            tap_ratio: row.tap_m,
            nominal_tap_ratio: 1.0,
            phase_shift: 0.0,
            vector_group: row.vector_group.clone(),
            rate_a: row.rate_a,
            rate_b: row.rate_b,
            rate_c: row.rate_c,
            status: true,
            from_nominal_kv: row.nominal_kv_m,
            to_nominal_kv: None,
        });

        // L → star leg.
        out.push(Transformer2WRow {
            from_bus_id: row.bus_l_id,
            to_bus_id: star_id,
            ckt: row.ckt.clone(),
            name: Cow::Owned(format!("{}_L", row.name)),
            r: r_l,
            x: x_l,
            winding1_r: r_l,
            winding1_x: x_l,
            winding2_r: 0.0,
            winding2_x: 0.0,
            g: 0.0,
            b: 0.0,
            tap_ratio: row.tap_l,
            nominal_tap_ratio: 1.0,
            phase_shift: 0.0,
            vector_group: row.vector_group.clone(),
            rate_a: row.rate_a,
            rate_b: row.rate_b,
            rate_c: row.rate_c,
            status: true,
            from_nominal_kv: row.nominal_kv_l,
            to_nominal_kv: None,
        });
    }
    out
}

/// Normalize `transformer_2w_rows` and `transformer_3w_rows` in-place to match `mode`.
///
/// - `Native3W`: removes any rows in `transformers_2w` that use a synthetic star bus
///   (bus_id > 10_000_000).  This is a safety-net; normal CIM exports should not have
///   these rows at all, but importing external files may.
/// - `Expanded`: star-expands all active 3W rows into synthetic 2W legs and clears
///   `transformer_3w_rows`.
fn normalize_transformer_representation<'a>(
    transformer_2w_rows: &mut Vec<Transformer2WRow<'a>>,
    transformer_3w_rows: &mut Vec<Transformer3WRow<'a>>,
    mode: TransformerRepresentationMode,
) {
    match mode {
        TransformerRepresentationMode::Native3W => {
            // Remove any previously allocated synthetic star-leg rows.
            transformer_2w_rows
                .retain(|row| row.from_bus_id <= 10_000_000 && row.to_bus_id <= 10_000_000);
        }
        TransformerRepresentationMode::Expanded => {
            // Collect existing real bus IDs to seed collision avoidance.
            let existing: HashSet<i32> = transformer_2w_rows
                .iter()
                .flat_map(|row| [row.from_bus_id, row.to_bus_id])
                .collect();
            let star_legs = star_expand_3w_transformers(transformer_3w_rows, &existing);
            transformer_2w_rows.extend(star_legs);
            transformer_3w_rows.clear();
        }
    }
}

/// Validates that `transformer_2w_rows` and `transformer_3w_rows` conform to `mode`.
///
/// Fails fast with a deterministic diagnostic when:
/// - `Expanded` mode has active rows in `transformers_3w`.
/// - `Native3W` mode has active rows in `transformers_2w` with synthetic star bus IDs.
fn validate_transformer_representation_mode(
    transformer_2w_rows: &[Transformer2WRow<'_>],
    transformer_3w_rows: &[Transformer3WRow<'_>],
    mode: TransformerRepresentationMode,
) -> Result<()> {
    match mode {
        TransformerRepresentationMode::Expanded => {
            let active_3w: Vec<_> = transformer_3w_rows
                .iter()
                .filter(|row| row.status)
                .collect();
            if !active_3w.is_empty() {
                let examples: Vec<String> = active_3w
                    .iter()
                    .take(3)
                    .map(|row| {
                        format!(
                            "(bus_h={} bus_m={} bus_l={} ckt={})",
                            row.bus_h_id, row.bus_m_id, row.bus_l_id, row.ckt
                        )
                    })
                    .collect();
                bail!(
                    "transformer representation contract violation: mode=expanded requires \
                     zero active rows in transformers_3w, found {} active row(s). \
                     Examples: {}",
                    active_3w.len(),
                    examples.join(", ")
                );
            }
        }
        TransformerRepresentationMode::Native3W => {
            let star_legs: Vec<_> = transformer_2w_rows
                .iter()
                .filter(|row| {
                    row.status && (row.from_bus_id > 10_000_000 || row.to_bus_id > 10_000_000)
                })
                .collect();
            if !star_legs.is_empty() {
                let examples: Vec<String> = star_legs
                    .iter()
                    .take(3)
                    .map(|row| {
                        format!(
                            "(from={} to={} ckt={})",
                            row.from_bus_id, row.to_bus_id, row.ckt
                        )
                    })
                    .collect();
                bail!(
                    "transformer representation contract violation: mode=native_3w must have \
                     no synthetic star-leg rows (bus_id > 10_000_000) in transformers_2w, \
                     found {} row(s). Examples: {}",
                    star_legs.len(),
                    examples.join(", ")
                );
            }
        }
    }
    Ok(())
}

fn validate_pre_write_contract(
    bus_rows: &[BusRow<'_>],
    branch_rows: &[BranchRow<'_>],
    gen_rows: &[GenRow<'_>],
    load_rows: &[LoadRow<'_>],
    transformer_2w_rows: &[Transformer2WRow<'_>],
    transformer_3w_rows: &[Transformer3WRow<'_>],
) -> Result<()> {
    for row in bus_rows {
        if row.bus_id <= 0 {
            bail!("pre-write contract violation: buses.bus_id must be > 0")
        }
    }
    for row in branch_rows {
        if row.branch_id <= 0 {
            bail!("pre-write contract violation: branches.branch_id must be > 0")
        }
        if row.from_bus_id <= 0 || row.to_bus_id <= 0 {
            bail!("pre-write contract violation: branches.from_bus_id/to_bus_id must be > 0")
        }
    }
    for row in gen_rows {
        if row.generator_id <= 0 || row.bus_id <= 0 {
            bail!("pre-write contract violation: generators.generator_id/bus_id must be present")
        }
    }
    for row in load_rows {
        if row.bus_id <= 0 || row.id.is_empty() {
            bail!("pre-write contract violation: loads.bus_id/id must be present")
        }
    }
    for row in transformer_2w_rows {
        if row.from_bus_id <= 0 || row.to_bus_id <= 0 {
            bail!("pre-write contract violation: transformers_2w.from_bus_id/to_bus_id must be > 0")
        }
    }
    for row in transformer_3w_rows {
        if row.bus_h_id <= 0 || row.bus_m_id <= 0 || row.bus_l_id <= 0 {
            bail!(
                "pre-write contract violation: transformers_3w.bus_h_id/bus_m_id/bus_l_id must be > 0"
            )
        }
    }
    Ok(())
}

/// Validates that required planning-state fields on every bus row are finite
/// and physically plausible.  Called as part of the pre-write contract.
fn validate_planning_fields_finite(bus_rows: &[BusRow<'_>]) -> Result<()> {
    for row in bus_rows {
        if !row.v_mag_set.is_finite() || row.v_mag_set <= 0.0 {
            bail!(
                "planning contract violation: buses.v_mag_set must be finite and > 0; \
                 bus_id={} v_mag_set={}",
                row.bus_id,
                row.v_mag_set
            );
        }
        if !row.v_ang_set.is_finite() {
            bail!(
                "planning contract violation: buses.v_ang_set must be finite; \
                 bus_id={} v_ang_set={}",
                row.bus_id,
                row.v_ang_set
            );
        }
        if !row.v_min.is_finite() || !row.v_max.is_finite() {
            bail!(
                "planning contract violation: buses.v_min/v_max must be finite; bus_id={}",
                row.bus_id
            );
        }
        if row.v_min > row.v_max {
            bail!(
                "planning contract violation: buses.v_min > v_max; \
                 bus_id={} v_min={} v_max={}",
                row.bus_id,
                row.v_min,
                row.v_max
            );
        }
    }
    Ok(())
}

/// Validates that case_mode and solved_state_presence are mutually consistent.
///
/// Rules:
/// - `solved_snapshot` requires `actual_solved`.
/// - `flat_start_planning` / `warm_start_planning` must not claim `actual_solved`.
///   Solvers must use `solved_snapshot` for post-solve results.
fn validate_case_mode_consistency(
    case_mode: CaseMode,
    solved_state_presence: SolvedStatePresence,
) -> Result<()> {
    match (case_mode, solved_state_presence) {
        (CaseMode::SolvedSnapshot, SolvedStatePresence::NotComputed)
        | (CaseMode::SolvedSnapshot, SolvedStatePresence::NotAvailable) => {
            bail!(
                "metadata consistency violation: case_mode=solved_snapshot requires \
                 solved_state_presence=actual_solved, got '{}'",
                solved_state_presence.as_str()
            );
        }
        (CaseMode::FlatStartPlanning, SolvedStatePresence::ActualSolved)
        | (CaseMode::WarmStartPlanning, SolvedStatePresence::ActualSolved) => {
            bail!(
                "metadata consistency violation: case_mode='{}' cannot have \
                 solved_state_presence=actual_solved; use case_mode=solved_snapshot \
                 for post-solve exports",
                case_mode.as_str()
            );
        }
        _ => {}
    }
    Ok(())
}

fn network_island_components(
    bus_rows: &[BusRow<'_>],
    branch_rows: &[BranchRow<'_>],
    transformer_2w_rows: &[Transformer2WRow<'_>],
    transformer_3w_rows: &[Transformer3WRow<'_>],
) -> Vec<Vec<i32>> {
    let mut adj: HashMap<i32, Vec<i32>> = HashMap::with_capacity(bus_rows.len());
    for row in bus_rows {
        adj.entry(row.bus_id).or_default();
    }

    let mut add_edge = |from_bus_id: i32, to_bus_id: i32| {
        if from_bus_id <= 0 || to_bus_id <= 0 || from_bus_id == to_bus_id {
            return;
        }
        if !(adj.contains_key(&from_bus_id) && adj.contains_key(&to_bus_id)) {
            return;
        }
        adj.entry(from_bus_id).or_default().push(to_bus_id);
        adj.entry(to_bus_id).or_default().push(from_bus_id);
    };

    for row in branch_rows {
        if row.status {
            add_edge(row.from_bus_id, row.to_bus_id);
        }
    }
    for row in transformer_2w_rows {
        if row.status {
            add_edge(row.from_bus_id, row.to_bus_id);
        }
    }
    for row in transformer_3w_rows {
        if row.status {
            add_edge(row.bus_h_id, row.bus_m_id);
            add_edge(row.bus_m_id, row.bus_l_id);
            add_edge(row.bus_h_id, row.bus_l_id);
        }
    }

    let mut bus_ids: Vec<i32> = adj.keys().copied().collect();
    bus_ids.sort_unstable();

    let mut visited: HashSet<i32> = HashSet::with_capacity(bus_ids.len());
    let mut islands: Vec<Vec<i32>> = Vec::new();
    for seed in bus_ids {
        if visited.contains(&seed) {
            continue;
        }
        let mut stack = vec![seed];
        visited.insert(seed);
        let mut component: Vec<i32> = Vec::new();
        while let Some(node) = stack.pop() {
            component.push(node);
            if let Some(neighbors) = adj.get(&node) {
                for neighbor in neighbors {
                    if visited.insert(*neighbor) {
                        stack.push(*neighbor);
                    }
                }
            }
        }
        islands.push(component);
    }
    islands.sort_unstable_by(|left, right| right.len().cmp(&left.len()));
    islands
}

#[derive(Debug, Clone, Copy, Default)]
struct IslandClassification {
    has_in_service_network: bool,
    has_in_service_load: bool,
    has_in_service_generation: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct TopologyDiagnostics {
    island_count: usize,
    main_island_bus_count: usize,
    detached_islands_present: bool,
    detached_active_network_island_count: usize,
    detached_active_load_island_count: usize,
    detached_active_generation_island_count: usize,
}

fn classify_islands(
    bus_rows: &[BusRow<'_>],
    branch_rows: &[BranchRow<'_>],
    transformer_2w_rows: &[Transformer2WRow<'_>],
    transformer_3w_rows: &[Transformer3WRow<'_>],
    load_rows: &[LoadRow<'_>],
    gen_rows: &[GenRow<'_>],
) -> TopologyDiagnostics {
    let islands = network_island_components(
        bus_rows,
        branch_rows,
        transformer_2w_rows,
        transformer_3w_rows,
    );

    let mut network_pairs: HashSet<(i32, i32)> = HashSet::new();
    let mut add_pair = |from_bus_id: i32, to_bus_id: i32| {
        if from_bus_id <= 0 || to_bus_id <= 0 || from_bus_id == to_bus_id {
            return;
        }
        let pair = if from_bus_id < to_bus_id {
            (from_bus_id, to_bus_id)
        } else {
            (to_bus_id, from_bus_id)
        };
        network_pairs.insert(pair);
    };

    for row in branch_rows {
        if row.status {
            add_pair(row.from_bus_id, row.to_bus_id);
        }
    }
    for row in transformer_2w_rows {
        if row.status {
            add_pair(row.from_bus_id, row.to_bus_id);
        }
    }
    for row in transformer_3w_rows {
        if row.status {
            add_pair(row.bus_h_id, row.bus_m_id);
            add_pair(row.bus_m_id, row.bus_l_id);
            add_pair(row.bus_h_id, row.bus_l_id);
        }
    }

    let mut load_buses: HashSet<i32> = HashSet::new();
    for row in load_rows {
        if row.status {
            load_buses.insert(row.bus_id);
        }
    }
    let mut gen_buses: HashSet<i32> = HashSet::new();
    for row in gen_rows {
        if row.status {
            gen_buses.insert(row.bus_id);
        }
    }

    let mut diagnostics = TopologyDiagnostics {
        island_count: islands.len(),
        main_island_bus_count: islands.first().map_or(0, Vec::len),
        detached_islands_present: islands.len() > 1,
        detached_active_network_island_count: 0,
        detached_active_load_island_count: 0,
        detached_active_generation_island_count: 0,
    };

    for island in islands.iter().skip(1) {
        let bus_set: HashSet<i32> = island.iter().copied().collect();
        let mut class = IslandClassification::default();

        class.has_in_service_load = bus_set.iter().any(|bus_id| load_buses.contains(bus_id));
        class.has_in_service_generation = bus_set.iter().any(|bus_id| gen_buses.contains(bus_id));
        class.has_in_service_network = network_pairs
            .iter()
            .any(|(left, right)| bus_set.contains(left) && bus_set.contains(right));

        if class.has_in_service_network {
            diagnostics.detached_active_network_island_count += 1;
        }
        if class.has_in_service_load {
            diagnostics.detached_active_load_island_count += 1;
        }
        if class.has_in_service_generation {
            diagnostics.detached_active_generation_island_count += 1;
        }
    }

    diagnostics
}

fn enforce_detached_island_policy(
    policy: DetachedIslandPolicy,
    bus_rows: &mut Vec<BusRow<'static>>,
    branch_rows: &mut Vec<BranchRow<'static>>,
    gen_rows: &mut Vec<GenRow<'static>>,
    load_rows: &mut Vec<LoadRow<'static>>,
    transformer_2w_rows: &mut Vec<Transformer2WRow<'static>>,
    transformer_3w_rows: &mut Vec<Transformer3WRow<'static>>,
    fixed_shunt_rows: &mut Vec<FixedShuntRow<'static>>,
    switched_shunt_rows: &mut Vec<SwitchedShuntRow>,
    switched_shunt_bank_rows: &mut Vec<SwitchedShuntBankRow>,
    node_breaker_rows: &mut Vec<NodeBreakerDetailRow<'static>>,
    connectivity_node_rows: &mut Vec<ConnectivityNodeDetailRow<'static>>,
    split_bus_stub_elements: &mut Vec<ContingencyElement<'static>>,
) -> Result<()> {
    let diagnostics = classify_islands(
        bus_rows,
        branch_rows,
        transformer_2w_rows,
        transformer_3w_rows,
        load_rows,
        gen_rows,
    );

    match policy {
        DetachedIslandPolicy::Permissive => return Ok(()),
        DetachedIslandPolicy::Strict => {
            let has_detached_active = diagnostics.detached_active_network_island_count > 0
                || diagnostics.detached_active_load_island_count > 0
                || diagnostics.detached_active_generation_island_count > 0;
            if has_detached_active {
                bail!(
                    "detached island policy=strict rejected export: islands={} detached_active_network={} detached_active_load={} detached_active_generation={}",
                    diagnostics.island_count,
                    diagnostics.detached_active_network_island_count,
                    diagnostics.detached_active_load_island_count,
                    diagnostics.detached_active_generation_island_count,
                );
            }
            return Ok(());
        }
        DetachedIslandPolicy::PruneDetached => {}
    }

    let islands = network_island_components(
        bus_rows,
        branch_rows,
        transformer_2w_rows,
        transformer_3w_rows,
    );
    let Some(main_island) = islands.first() else {
        return Ok(());
    };
    let main_set: HashSet<i32> = main_island.iter().copied().collect();

    bus_rows.retain(|row| main_set.contains(&row.bus_id));
    branch_rows
        .retain(|row| main_set.contains(&row.from_bus_id) && main_set.contains(&row.to_bus_id));
    gen_rows.retain(|row| main_set.contains(&row.bus_id));
    load_rows.retain(|row| main_set.contains(&row.bus_id));
    transformer_2w_rows
        .retain(|row| main_set.contains(&row.from_bus_id) && main_set.contains(&row.to_bus_id));
    transformer_3w_rows.retain(|row| {
        main_set.contains(&row.bus_h_id)
            && main_set.contains(&row.bus_m_id)
            && main_set.contains(&row.bus_l_id)
    });
    fixed_shunt_rows.retain(|row| main_set.contains(&row.bus_id));
    switched_shunt_rows.retain(|row| main_set.contains(&row.bus_id));
    switched_shunt_bank_rows.retain(|row| main_set.contains(&row.bus_id));
    node_breaker_rows.retain(|row| {
        row.from_bus_id
            .map(|bus_id| main_set.contains(&bus_id))
            .unwrap_or(true)
            && row
                .to_bus_id
                .map(|bus_id| main_set.contains(&bus_id))
                .unwrap_or(true)
    });
    connectivity_node_rows.retain(|row| {
        row.bus_id
            .map(|bus_id| main_set.contains(&bus_id))
            .unwrap_or(true)
    });
    split_bus_stub_elements.retain(|row| {
        row.bus_id
            .map(|bus_id| main_set.contains(&bus_id))
            .unwrap_or(true)
    });

    Ok(())
}

fn is_topology_only_zero_injection_case(
    bus_rows: &[BusRow<'_>],
    load_rows: &[LoadRow<'_>],
    gen_rows: &[GenRow<'_>],
) -> bool {
    let eps = 1e-9;

    let buses_zero = bus_rows
        .iter()
        .all(|row| row.p_sched.abs() <= eps && row.q_sched.abs() <= eps);
    let loads_zero = load_rows
        .iter()
        .filter(|row| row.status)
        .all(|row| row.p_mw.abs() <= eps && row.q_mvar.abs() <= eps);
    let gens_zero = gen_rows
        .iter()
        .filter(|row| row.status)
        .all(|row| row.p_sched_mw.abs() <= eps);

    buses_zero && loads_zero && gens_zero
}

/// Writes a complete Raptrix v0.8.1 `.rpf` Arrow IPC file.
///
/// The writer materializes all required canonical tables (empty tables
/// allowed) as struct columns in one root record batch.
///
/// Notes:
/// - Multi-profile merges are profile-aware by filename token (`_EQ`, `_TP`,
///   `_SV`, `_SSH`, `_DY`, `_DL`) and safely ignore unsupported payload
///   classes per profile.
/// - Output path must end in `.rpf`.
pub fn write_complete_rpf(cgmes_paths: &[&str], output_path: &str) -> Result<()> {
    write_complete_rpf_with_options(cgmes_paths, output_path, &WriteOptions::default())?;
    Ok(())
}

/// Writes a complete Raptrix v0.8.0 `.rpf` IPC file with merge options.
pub fn write_complete_rpf_with_options(
    cgmes_paths: &[&str],
    output_path: &str,
    options: &WriteOptions,
) -> Result<WriteSummary> {
    if cgmes_paths.is_empty() {
        bail!("cgmes_paths is empty; provide at least one CGMES XML file path");
    }
    if !output_path.ends_with(".rpf") {
        bail!("output_path must end with .rpf for Arrow IPC interchange output; got {output_path}");
    }

    let topology = parse_eq_topology_rows(
        cgmes_paths,
        options.bus_resolution_mode,
        options.emit_node_breaker_detail,
    )
    .with_context(|| {
        format!(
            "failed while parsing profile-aware CGMES content from {} input path(s)",
            cgmes_paths.len()
        )
    })?;
    let (
        mut bus_rows,
        mut branch_rows,
        mut gen_rows,
        mut load_rows,
        mut transformer_2w_rows,
        mut transformer_3w_rows,
        area_rows,
        zone_rows,
        owner_rows,
        mut fixed_shunt_rows,
        mut switched_shunt_rows,
        mut switched_shunt_bank_rows,
        connectivity_group_rows,
        mut node_breaker_rows,
        switch_detail_rows,
        mut connectivity_node_rows,
        diagram_object_rows,
        diagram_point_rows,
        mut split_bus_stub_elements,
        dy_model_specs,
    ) = topology;

    enforce_detached_island_policy(
        options.detached_island_policy,
        &mut bus_rows,
        &mut branch_rows,
        &mut gen_rows,
        &mut load_rows,
        &mut transformer_2w_rows,
        &mut transformer_3w_rows,
        &mut fixed_shunt_rows,
        &mut switched_shunt_rows,
        &mut switched_shunt_bank_rows,
        &mut node_breaker_rows,
        &mut connectivity_node_rows,
        &mut split_bus_stub_elements,
    )?;

    validate_pre_write_contract(
        &bus_rows,
        &branch_rows,
        &gen_rows,
        &load_rows,
        &transformer_2w_rows,
        &transformer_3w_rows,
    )?;

    // v0.8.7: normalize transformer representation mode (star-expansion or cleanup),
    // then validate contract invariants before writing any bytes.
    normalize_transformer_representation(
        &mut transformer_2w_rows,
        &mut transformer_3w_rows,
        options.transformer_representation_mode,
    );
    validate_transformer_representation_mode(
        &transformer_2w_rows,
        &transformer_3w_rows,
        options.transformer_representation_mode,
    )?;

    // v0.8.4: validate planning fields are finite before any solved-state checks.
    validate_planning_fields_finite(&bus_rows)?;

    // v0.8.4: determine solved_state_presence from case_mode and options.
    let solved_state_presence = match options.case_mode {
        CaseMode::SolvedSnapshot => SolvedStatePresence::ActualSolved,
        CaseMode::FlatStartPlanning | CaseMode::WarmStartPlanning => {
            SolvedStatePresence::NotComputed
        }
    };

    // v0.8.4: fail fast on contradictory case_mode / solved_state_presence.
    validate_case_mode_consistency(options.case_mode, solved_state_presence)?;

    // v0.8.4: SolvedSnapshot requires solver provenance; planning cases must not carry it.
    if options.case_mode == CaseMode::SolvedSnapshot && options.solver_provenance.is_none() {
        bail!(
            "export contract violation: case_mode=solved_snapshot requires solver_provenance \
             to be set in WriteOptions so the metadata row carries accurate solver attribution"
        );
    }
    if options.case_mode != CaseMode::SolvedSnapshot && options.solver_provenance.is_some() {
        bail!(
            "export contract violation: solver_provenance must only be set when \
             case_mode=solved_snapshot; got case_mode='{}'",
            options.case_mode.as_str()
        );
    }

    let study_name = options
        .study_name
        .clone()
        .unwrap_or_else(|| infer_study_name(cgmes_paths));
    let snapshot_timestamp_utc = options
        .timestamp_utc
        .clone()
        .unwrap_or_else(current_timestamp_utc);
    let case_fingerprint =
        compute_case_fingerprint(cgmes_paths, &study_name, &snapshot_timestamp_utc)?;
    let topology_only_zero_injection =
        is_topology_only_zero_injection_case(&bus_rows, &load_rows, &gen_rows);
    let validation_mode = if topology_only_zero_injection {
        VALIDATION_MODE_TOPOLOGY_ONLY
    } else {
        VALIDATION_MODE_SOLVED_READY
    };

    // v0.8.4: extract solver provenance fields (all None for planning cases).
    // v0.8.5: also extract angle-reference and shunt state provenance.
    let (
        sv_version,
        sv_iterations,
        sv_accuracy,
        sv_mode,
        sv_slack_bus_id,
        sv_angle_ref_deg,
        sv_shunt_state_presence,
    ) = if let Some(ref prov) = options.solver_provenance {
        (
            prov.solver_version.as_deref().map(Cow::Borrowed),
            prov.solver_iterations,
            prov.solver_accuracy,
            prov.solver_mode.as_deref().map(Cow::Borrowed),
            prov.slack_bus_id_solved,
            prov.angle_reference_deg,
            prov.solved_shunt_state_presence
                .map(|s| Cow::Borrowed(s.as_str())),
        )
    } else {
        (None, None, None, None, None, None, None)
    };

    let metadata_row = MetadataRow {
        base_mva: options.base_mva,
        frequency_hz: options.frequency_hz,
        psse_version: 35,
        study_name: Cow::Owned(study_name.clone()),
        timestamp_utc: Cow::Owned(snapshot_timestamp_utc.clone()),
        raptrix_version: Cow::Borrowed(SCHEMA_VERSION),
        is_planning_case: options.case_mode != CaseMode::SolvedSnapshot,
        source_case_id: Cow::Owned(study_name),
        snapshot_timestamp_utc: Cow::Owned(snapshot_timestamp_utc),
        case_fingerprint: Cow::Owned(case_fingerprint.clone()),
        validation_mode: Cow::Borrowed(validation_mode),
        case_mode: Cow::Borrowed(options.case_mode.as_str()),
        solved_state_presence: Some(Cow::Borrowed(solved_state_presence.as_str())),
        solver_version: sv_version.map(|v| Cow::Owned(v.into_owned())),
        solver_iterations: sv_iterations,
        solver_accuracy: sv_accuracy,
        solver_mode: sv_mode.map(|v| Cow::Owned(v.into_owned())),
        // v0.8.5
        slack_bus_id_solved: sv_slack_bus_id,
        angle_reference_deg: sv_angle_ref_deg,
        solved_shunt_state_presence: sv_shunt_state_presence.map(|v| Cow::Owned(v.into_owned())),
        // v0.8.8
        modern_grid_profile: options.modern_grid_profile,
        ibr_penetration_pct: options.ibr_penetration_pct,
        has_ibr: false,
        has_smart_valve: false,
        has_multi_terminal_dc: false,
        study_purpose: options.study_purpose.as_deref().map(Cow::Borrowed),
        scenario_tags: options
            .scenario_tags
            .iter()
            .map(|tag| Cow::Borrowed(tag.as_str()))
            .collect(),
    };

    let metadata_batch = build_metadata_batch(&metadata_row)?;
    let buses_batch = build_buses_batch(&bus_rows)?;
    let branches_batch = build_branches_batch(&branch_rows)?;
    let generators_batch = build_generators_batch(&gen_rows, options.base_mva)?;
    let loads_batch = build_loads_batch(&load_rows, options.base_mva)?;
    let transformers_2w_batch = build_transformers_2w_batch(&transformer_2w_rows)?;
    let transformers_3w_batch = build_transformers_3w_batch(&transformer_3w_rows)?;
    let areas_batch = build_areas_batch(&area_rows)?;
    let zones_batch = build_zones_batch(&zone_rows)?;
    let owners_batch = build_owners_batch(&owner_rows)?;
    let fixed_shunts_batch = build_fixed_shunts_batch(&fixed_shunt_rows, options.base_mva)?;
    let switched_shunts_batch = build_switched_shunts_batch(&switched_shunt_rows)?;
    let switched_shunt_banks_batch =
        build_switched_shunt_banks_batch(&switched_shunt_bank_rows, options.base_mva)?;
    let (contingencies_rows, contingencies_are_stub) =
        contingency_rows_from_switches_and_stubs(&node_breaker_rows, split_bus_stub_elements);
    let (
        dynamics_models_rows,
        dynamics_are_stub,
        dynamics_rows_dy_linked,
        dynamics_rows_eq_fallback,
    ) = dynamics_rows_from_generators_and_dy(&gen_rows, &dy_model_specs);
    let contingencies_batch = build_contingencies_batch(&contingencies_rows)?;
    let dynamics_models_batch = build_dynamics_models_batch(&dynamics_models_rows)?;
    let _connectivity_groups_batch = build_connectivity_groups_batch(&connectivity_group_rows)?;
    let node_breaker_detail_batch = build_node_breaker_detail_batch(&node_breaker_rows)?;
    let switch_detail_batch = build_switch_detail_batch(&switch_detail_rows)?;
    let connectivity_nodes_batch = build_connectivity_nodes_batch(&connectivity_node_rows)?;
    let diagram_objects_batch = build_diagram_objects_batch(&diagram_object_rows)?;
    let diagram_points_batch = build_diagram_points_batch(&diagram_point_rows)?;

    let mut table_batches: HashMap<&'static str, RecordBatch> = HashMap::new();
    table_batches.insert(TABLE_METADATA, metadata_batch.clone());
    table_batches.insert(TABLE_BUSES, buses_batch.clone());
    table_batches.insert(TABLE_BRANCHES, branches_batch.clone());
    table_batches.insert(
        TABLE_MULTI_SECTION_LINES,
        RecordBatch::new_empty(Arc::new(multi_section_lines_schema())),
    );
    table_batches.insert(
        TABLE_DC_LINES_2W,
        RecordBatch::new_empty(Arc::new(dc_lines_2w_schema())),
    );
    table_batches.insert(TABLE_GENERATORS, generators_batch.clone());
    table_batches.insert(TABLE_LOADS, loads_batch.clone());
    table_batches.insert(TABLE_FIXED_SHUNTS, fixed_shunts_batch.clone());
    table_batches.insert(TABLE_SWITCHED_SHUNTS, switched_shunts_batch.clone());
    table_batches.insert(
        TABLE_SWITCHED_SHUNT_BANKS,
        switched_shunt_banks_batch.clone(),
    );
    table_batches.insert(TABLE_TRANSFORMERS_2W, transformers_2w_batch.clone());
    table_batches.insert(TABLE_TRANSFORMERS_3W, transformers_3w_batch.clone());
    table_batches.insert(TABLE_AREAS, areas_batch.clone());
    table_batches.insert(TABLE_ZONES, zones_batch.clone());
    table_batches.insert(TABLE_OWNERS, owners_batch.clone());
    table_batches.insert(TABLE_CONTINGENCIES, contingencies_batch.clone());
    table_batches.insert(
        TABLE_INTERFACES,
        RecordBatch::new_empty(Arc::new(interfaces_schema())),
    );
    table_batches.insert(TABLE_DYNAMICS_MODELS, dynamics_models_batch.clone());

    if options.emit_node_breaker_detail {
        table_batches.insert(TABLE_NODE_BREAKER_DETAIL, node_breaker_detail_batch.clone());
        table_batches.insert(TABLE_SWITCH_DETAIL, switch_detail_batch.clone());
        table_batches.insert(TABLE_CONNECTIVITY_NODES, connectivity_nodes_batch.clone());
    }
    if options.emit_diagram_layout && !diagram_object_rows.is_empty() {
        table_batches.insert(TABLE_DIAGRAM_OBJECTS, diagram_objects_batch.clone());
        table_batches.insert(TABLE_DIAGRAM_POINTS, diagram_points_batch.clone());
    }

    let topology_diagnostics = classify_islands(
        &bus_rows,
        &branch_rows,
        &transformer_2w_rows,
        &transformer_3w_rows,
        &load_rows,
        &gen_rows,
    );
    let mut additional_root_metadata: HashMap<String, String> = HashMap::new();
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_ISLAND_COUNT.to_string(),
        topology_diagnostics.island_count.to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT.to_string(),
        topology_diagnostics.main_island_bus_count.to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_DETACHED_ISLANDS_PRESENT.to_string(),
        topology_diagnostics.detached_islands_present.to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_NETWORK_ISLAND_COUNT.to_string(),
        topology_diagnostics
            .detached_active_network_island_count
            .to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_LOAD_ISLAND_COUNT.to_string(),
        topology_diagnostics
            .detached_active_load_island_count
            .to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_GENERATION_ISLAND_COUNT.to_string(),
        topology_diagnostics
            .detached_active_generation_island_count
            .to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_FEATURE_TOPOLOGY_ONLY.to_string(),
        topology_only_zero_injection.to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_FEATURE_ZERO_INJECTION_STUB.to_string(),
        topology_only_zero_injection.to_string(),
    );
    additional_root_metadata.insert(METADATA_KEY_CASE_FINGERPRINT.to_string(), case_fingerprint);
    additional_root_metadata.insert(
        METADATA_KEY_VALIDATION_MODE.to_string(),
        validation_mode.to_string(),
    );
    // v0.8.4: planning-vs-solved metadata keys.
    additional_root_metadata.insert(
        METADATA_KEY_CASE_MODE.to_string(),
        options.case_mode.as_str().to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_SOLVED_STATE_PRESENCE.to_string(),
        solved_state_presence.as_str().to_string(),
    );
    // v0.8.7: transformer representation contract.
    additional_root_metadata.insert(
        METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE.to_string(),
        options.transformer_representation_mode.as_str().to_string(),
    );
    if let Some(ref prov) = options.solver_provenance {
        if let Some(ref ver) = prov.solver_version {
            additional_root_metadata.insert(METADATA_KEY_SOLVER_VERSION.to_string(), ver.clone());
        }
        if let Some(iters) = prov.solver_iterations {
            additional_root_metadata.insert(
                METADATA_KEY_SOLVER_ITERATIONS.to_string(),
                iters.to_string(),
            );
        }
        if let Some(acc) = prov.solver_accuracy {
            additional_root_metadata
                .insert(METADATA_KEY_SOLVER_ACCURACY.to_string(), acc.to_string());
        }
        if let Some(ref mode) = prov.solver_mode {
            additional_root_metadata.insert(METADATA_KEY_SOLVER_MODE.to_string(), mode.clone());
        }
        // v0.8.5: angle-reference and shunt-state provenance keys.
        if let Some(slack_id) = prov.slack_bus_id_solved {
            additional_root_metadata.insert(
                METADATA_KEY_SOLVER_SLACK_BUS_ID.to_string(),
                slack_id.to_string(),
            );
        }
        if let Some(ang_ref) = prov.angle_reference_deg {
            additional_root_metadata.insert(
                METADATA_KEY_SOLVER_ANGLE_REFERENCE_DEG.to_string(),
                ang_ref.to_string(),
            );
        }
        if let Some(shunt_state) = prov.solved_shunt_state_presence {
            additional_root_metadata.insert(
                METADATA_KEY_SOLVED_SHUNT_STATE_PRESENCE.to_string(),
                shunt_state.as_str().to_string(),
            );
        }
    }

    write_root_rpf_with_metadata(
        output_path,
        &table_batches,
        &RootWriteOptions {
            include_node_breaker_detail: options.emit_node_breaker_detail,
            include_diagram_layout: options.emit_diagram_layout && !diagram_object_rows.is_empty(),
            contingencies_are_stub: options.contingencies_are_stub || contingencies_are_stub,
            dynamics_are_stub: options.dynamics_are_stub || dynamics_are_stub,
            // CIM exporter produces planning cases only; solver core sets this when
            // assembling solved_snapshot files with buses_solved/generators_solved.
            include_solved_state: false,
            include_facts_devices: false,
            include_facts_solved: false,
        },
        &additional_root_metadata,
    )?;

    let tp_merged = options.bus_resolution_mode == BusResolutionMode::Topological
        && !connectivity_group_rows.is_empty();
    let connectivity_bus_count = if tp_merged {
        connectivity_group_rows
            .iter()
            .flat_map(|row| row.connectivity_node_mrids.iter())
            .count()
    } else {
        bus_rows.len()
    };

    Ok(WriteSummary {
        connectivity_bus_count,
        final_bus_count: bus_rows.len(),
        tp_merged,
        connectivity_groups_rows: connectivity_group_rows.len(),
        node_breaker_rows: node_breaker_rows.len(),
        switch_detail_rows: switch_detail_rows.len(),
        connectivity_node_rows: connectivity_node_rows.len(),
        diagram_object_rows: diagram_object_rows.len(),
        diagram_point_rows: diagram_point_rows.len(),
        dynamics_rows_total: dynamics_models_rows.len(),
        dynamics_rows_dy_linked,
        dynamics_rows_eq_fallback,
    })
}

fn contingency_rows_from_switches_and_stubs(
    node_breaker_rows: &[NodeBreakerDetailRow<'_>],
    split_bus_stub_elements: Vec<ContingencyElement<'static>>,
) -> (Vec<ContingencyRow<'static>>, bool) {
    let mut rows: Vec<ContingencyRow<'static>> = Vec::new();

    for row in node_breaker_rows {
        // Derive contingency candidates only when switch state is explicitly present.
        let has_switch_state = row.is_open.is_some() || row.normal_open.is_some();
        if !has_switch_state {
            continue;
        }

        let bus_id = row.from_bus_id.or(row.to_bus_id);
        rows.push(ContingencyRow {
            contingency_id: Cow::Owned(format!("SWITCH-{}", row.switch_id.as_ref())),
            elements: vec![ContingencyElement {
                element_type: Cow::Borrowed("shunt_switch"),
                branch_id: None,
                bus_id,
                status_change: true,
                equipment_kind: Some(Cow::Borrowed("switch")),
                equipment_id: Some(Cow::Owned(row.switch_id.as_ref().to_owned())),
            }],
        });
    }

    let mut contains_stub = false;
    if rows.is_empty() {
        contains_stub = true;
        rows = vec![
            ContingencyRow {
                contingency_id: Cow::Borrowed("N-1 Line1"),
                elements: vec![ContingencyElement {
                    element_type: Cow::Borrowed("branch_outage"),
                    branch_id: Some(1),
                    bus_id: None,
                    status_change: true,
                    equipment_kind: None,
                    equipment_id: None,
                }],
            },
            ContingencyRow {
                contingency_id: Cow::Borrowed("N-1 Bus2"),
                elements: vec![
                    ContingencyElement {
                        element_type: Cow::Borrowed("branch_outage"),
                        branch_id: Some(1),
                        bus_id: None,
                        status_change: true,
                        equipment_kind: None,
                        equipment_id: None,
                    },
                    ContingencyElement {
                        element_type: Cow::Borrowed("shunt_switch"),
                        branch_id: None,
                        bus_id: Some(2),
                        status_change: true,
                        equipment_kind: None,
                        equipment_id: None,
                    },
                ],
            },
        ];
    }

    if !split_bus_stub_elements.is_empty() {
        contains_stub = true;
        rows.push(ContingencyRow {
            contingency_id: Cow::Borrowed("split-bus-stub"),
            elements: split_bus_stub_elements,
        });
    }

    (rows, contains_stub)
}

fn stub_dynamics_model_rows() -> Vec<DynamicsModelRow<'static>> {
    vec![DynamicsModelRow {
        bus_id: 1,
        gen_id: Cow::Borrowed("G1"),
        model_type: Cow::Borrowed("GENROU"),
        params: vec![
            (Cow::Borrowed("H"), 5.0),
            (Cow::Borrowed("xd_prime"), 0.3),
            (Cow::Borrowed("source_stub"), 1.0),
        ],
    }]
}

fn dynamics_rows_from_generators_and_dy(
    gen_rows: &[GenRow<'_>],
    dy_specs: &[parser::DyModelSpec],
) -> (Vec<DynamicsModelRow<'static>>, bool, usize, usize) {
    let mut dy_linked_rows = 0_usize;
    let mut eq_fallback_rows = 0_usize;

    if !dy_specs.is_empty() && !gen_rows.is_empty() {
        let mut bus_by_gen_id: HashMap<&str, i32> = HashMap::new();
        for generator in gen_rows {
            bus_by_gen_id.insert(generator.id.as_ref(), generator.bus_id);
        }

        let mut rows = Vec::new();
        let mut matched_generators: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        for spec in dy_specs {
            let Some(bus_id) = bus_by_gen_id.get(spec.equipment_mrid.as_str()).copied() else {
                continue;
            };
            matched_generators.insert(spec.equipment_mrid.as_str());

            let params = if spec.params.is_empty() {
                vec![(Cow::Borrowed("dy_present"), 1.0)]
            } else {
                spec.params
                    .iter()
                    .map(|(key, value)| (Cow::Owned(key.clone()), *value))
                    .collect()
            };

            rows.push(DynamicsModelRow {
                bus_id,
                gen_id: Cow::Owned(spec.equipment_mrid.clone()),
                model_type: Cow::Owned(spec.model_type.clone()),
                params: {
                    let mut with_source = params;
                    with_source.push((Cow::Borrowed("source_dy"), 1.0));
                    with_source
                },
            });
            dy_linked_rows += 1;
        }

        // Keep generator coverage complete even when DY is partial by filling
        // unmatched machines from EQ-derived parameters.
        for generator in gen_rows {
            if matched_generators.contains(generator.id.as_ref()) {
                continue;
            }
            rows.push(DynamicsModelRow {
                bus_id: generator.bus_id,
                gen_id: Cow::Owned(generator.id.as_ref().to_owned()),
                model_type: Cow::Borrowed(infer_dynamics_model_type(generator)),
                params: vec![
                    (Cow::Borrowed("H"), generator.h),
                    (Cow::Borrowed("xd_prime"), generator.xd_prime),
                    (Cow::Borrowed("D"), generator.d),
                    (Cow::Borrowed("mbase_mva"), generator.mbase_mva),
                    (Cow::Borrowed("source_eq_fallback"), 1.0),
                ],
            });
            eq_fallback_rows += 1;
        }

        rows.sort_unstable_by(|left, right| {
            left.bus_id
                .cmp(&right.bus_id)
                .then_with(|| left.gen_id.as_ref().cmp(right.gen_id.as_ref()))
                .then_with(|| left.model_type.as_ref().cmp(right.model_type.as_ref()))
        });

        if !rows.is_empty() {
            return (rows, false, dy_linked_rows, eq_fallback_rows);
        }
    }

    if gen_rows.is_empty() {
        return (stub_dynamics_model_rows(), true, 0, 0);
    }

    let rows = gen_rows
        .iter()
        .map(|generator| DynamicsModelRow {
            bus_id: generator.bus_id,
            gen_id: Cow::Owned(generator.id.as_ref().to_owned()),
            model_type: Cow::Borrowed(infer_dynamics_model_type(generator)),
            params: vec![
                (Cow::Borrowed("H"), generator.h),
                (Cow::Borrowed("xd_prime"), generator.xd_prime),
                (Cow::Borrowed("D"), generator.d),
                (Cow::Borrowed("mbase_mva"), generator.mbase_mva),
                (Cow::Borrowed("source_eq_fallback"), 1.0),
            ],
        })
        .collect();

    (rows, false, 0, gen_rows.len())
}

fn infer_dynamics_model_type(generator: &GenRow<'_>) -> &'static str {
    if generator.xd_prime > 0.0 && generator.h > 0.0 {
        "GENROU"
    } else if generator.h > 0.0 {
        "GENCLS"
    } else {
        // Conservative fallback: preserve that this row came from EQ-side machine data,
        // not a fully parsed DY dynamic model exchange payload.
        "SYNC_MACHINE_EQ"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CgmesProfileKind {
    Eq,
    Tp,
    Sv,
    Ssh,
    Dy,
    Dl,
    Unknown,
}

fn infer_profile_kind_from_path(path: &str) -> CgmesProfileKind {
    let Some(file_name) = Path::new(path).file_name().and_then(|value| value.to_str()) else {
        return CgmesProfileKind::Unknown;
    };
    let upper = file_name.to_ascii_uppercase();

    if upper.contains("_EQ") {
        CgmesProfileKind::Eq
    } else if upper.contains("_TP") {
        CgmesProfileKind::Tp
    } else if upper.contains("_SV") {
        CgmesProfileKind::Sv
    } else if upper.contains("_SSH") {
        CgmesProfileKind::Ssh
    } else if upper.contains("_DY") {
        CgmesProfileKind::Dy
    } else if upper.contains("_DL") {
        CgmesProfileKind::Dl
    } else {
        CgmesProfileKind::Unknown
    }
}

fn parse_eq_components_for_path(
    path: &str,
) -> Result<(
    Vec<crate::models::ACLineSegment<'static>>,
    Vec<crate::models::SynchronousMachine<'static>>,
    Vec<crate::models::EnergyConsumer<'static>>,
    Vec<parser::PowerTransformer<'static>>,
    Vec<crate::models::Area<'static>>,
    Vec<crate::models::Zone<'static>>,
    Vec<crate::models::Owner<'static>>,
    Vec<parser::FixedShuntSpec>,
    Vec<crate::models::SvShuntCompensator<'static>>,
    Vec<parser::TerminalLink>,
    Vec<crate::models::TopologicalNode<'static>>,
    Vec<crate::models::ConnectivityNodeGroup<'static>>,
    Vec<parser::SwitchSpec>,
    Vec<parser::ConnectivityNodeSpec>,
    Vec<parser::BaseVoltageSpec>,
    Vec<parser::EquipmentBaseVoltageRef>,
    Vec<parser::DiagramRecord>,
    Vec<parser::DiagramObjectRecord>,
    Vec<parser::DiagramPointRecord>,
    Vec<parser::DyModelSpec>,
)> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to open CGMES input file at {path}"))?;

    let profile_kind = infer_profile_kind_from_path(path);

    let mut lines = Vec::new();
    let mut machines = Vec::new();
    let mut loads = Vec::new();
    let mut transformers = Vec::new();
    let mut areas = Vec::new();
    let mut zones = Vec::new();
    let mut owners = Vec::new();
    let mut fixed_shunts = Vec::new();
    let mut switched_shunts = Vec::new();
    let mut terminals = Vec::new();
    let mut topological_nodes = Vec::new();
    let mut connectivity_groups = Vec::new();
    let mut switches = Vec::new();
    let mut connectivity_nodes = Vec::new();
    let mut base_voltages = Vec::new();
    let mut equipment_base_voltage_refs = Vec::new();
    let mut diagrams = Vec::new();
    let mut diagram_objects = Vec::new();
    let mut diagram_points = Vec::new();
    let mut dy_model_specs = Vec::new();

    // Route parsing by profile so strict multi-profile runs can include SSH/DY
    // inputs without forcing EQ extractors over incompatible payloads.
    match profile_kind {
        CgmesProfileKind::Eq | CgmesProfileKind::Unknown => {
            let (parsed_lines, parsed_machines, parsed_terminals) =
                parser::eq_lines_machines_and_terminals_from_reader(Cursor::new(&bytes))
                    .with_context(|| {
                        format!(
                            "failed to extract ACLineSegment/SynchronousMachine/Terminal elements from CGMES input file at {path}"
                        )
                    })?;
            lines = parsed_lines;
            machines = parsed_machines;
            terminals = parsed_terminals;

            loads =
                parser::energy_consumers_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract EnergyConsumer elements from CGMES input file at {path}"
                    )
                })?;

            transformers =
                parser::power_transformers_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!("failed to extract PowerTransformer elements from CGMES input file at {path}")
                })?;

            areas = parser::areas_from_reader(Cursor::new(&bytes)).with_context(|| {
                format!("failed to extract ControlArea elements from CGMES input file at {path}")
            })?;

            zones = parser::zones_from_reader(Cursor::new(&bytes)).with_context(|| {
                format!(
                    "failed to extract SubGeographicalRegion elements from CGMES input file at {path}"
                )
            })?;

            owners = parser::owners_from_reader(Cursor::new(&bytes)).with_context(|| {
                format!("failed to extract Organisation elements from CGMES input file at {path}")
            })?;

            fixed_shunts =
                parser::fixed_shunts_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract LinearShuntCompensator elements from CGMES input file at {path}"
                    )
                })?;

            switches =
                parser::switch_specs_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!("failed to extract switch elements from CGMES input file at {path}")
                })?;

            connectivity_nodes =
                parser::connectivity_nodes_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract ConnectivityNode elements from CGMES input file at {path}"
                    )
                })?;

            base_voltages = parser::base_voltage_specs_from_reader(Cursor::new(&bytes))
                .with_context(|| {
                    format!(
                        "failed to extract BaseVoltage elements from CGMES input file at {path}"
                    )
                })?;

            equipment_base_voltage_refs = parser::equipment_base_voltage_refs_from_reader(
                Cursor::new(&bytes),
            )
            .with_context(|| {
                format!(
                    "failed to extract ConductingEquipment.BaseVoltage links from CGMES input file at {path}"
                )
            })?;

            // Some data providers ship TP/diagram payload in EQ-named files;
            // parse opportunistically so valid mixed files are not ignored.
            connectivity_groups =
                parser::connectivity_node_groups_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract TopologicalNode/ConnectivityNode group links from CGMES input file at {path}"
                    )
                })?;
            topological_nodes = parser::topological_nodes_from_reader(Cursor::new(&bytes))
                .with_context(|| {
                    format!(
                        "failed to extract TopologicalNode elements from CGMES input file at {path}"
                    )
                })?;

            let (parsed_diagrams, parsed_diagram_objects, parsed_diagram_points) =
                parser::diagram_layout_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract DiagramLayout elements from CGMES input file at {path}"
                    )
                })?;
            diagrams = parsed_diagrams;
            diagram_objects = parsed_diagram_objects;
            diagram_points = parsed_diagram_points;
        }
        CgmesProfileKind::Tp => {
            connectivity_groups =
                parser::connectivity_node_groups_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract TopologicalNode/ConnectivityNode group links from CGMES input file at {path}"
                    )
                })?;

            topological_nodes = parser::topological_nodes_from_reader(Cursor::new(&bytes))
                .with_context(|| {
                    format!(
                        "failed to extract TopologicalNode elements from CGMES input file at {path}"
                    )
                })?;
        }
        CgmesProfileKind::Sv => {
            switched_shunts =
                parser::sv_shunt_compensators_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract SvShuntCompensator elements from CGMES input file at {path}"
                    )
                })?;
        }
        CgmesProfileKind::Dl => {
            let (parsed_diagrams, parsed_diagram_objects, parsed_diagram_points) =
                parser::diagram_layout_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract DiagramLayout elements from CGMES input file at {path}"
                    )
                })?;
            diagrams = parsed_diagrams;
            diagram_objects = parsed_diagram_objects;
            diagram_points = parsed_diagram_points;
        }
        CgmesProfileKind::Ssh => {
            fixed_shunts = parser::fixed_shunts_from_reader(Cursor::new(&bytes)).with_context(|| {
                format!(
                    "failed to extract LinearShuntCompensator elements from CGMES input file at {path}"
                )
            })?;
            switches =
                parser::switch_specs_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!("failed to extract switch elements from CGMES input file at {path}")
                })?;
        }
        CgmesProfileKind::Dy => {
            dy_model_specs =
                parser::dy_model_specs_from_reader(Cursor::new(&bytes)).with_context(|| {
                    format!(
                        "failed to extract dynamic model payload from CGMES input file at {path}"
                    )
                })?;
        }
    }

    Ok((
        lines,
        machines,
        loads,
        transformers,
        areas,
        zones,
        owners,
        fixed_shunts,
        switched_shunts,
        terminals,
        topological_nodes,
        connectivity_groups,
        switches,
        connectivity_nodes,
        base_voltages,
        equipment_base_voltage_refs,
        diagrams,
        diagram_objects,
        diagram_points,
        dy_model_specs,
    ))
}

/// Parses EQ data and deterministically maps terminal connectivity into
/// dense bus IDs and concrete branch rows.
///
/// Tenet 1: uses parsed row data directly and avoids string reallocation in
/// hot loops where possible.
/// Tenet 2: prepares rows that map exactly to locked `buses` and `branches`
/// schemas without mutation.
/// Tenet 3: EQ ingestion ownership remains in Rust by joining `EnergyConsumer`
/// and `SvShuntCompensator` through `Terminal` references in this function.
fn parse_eq_topology_rows(
    cgmes_paths: &[&str],
    bus_resolution_mode: BusResolutionMode,
    emit_node_breaker_detail: bool,
) -> Result<(
    Vec<BusRow<'static>>,
    Vec<BranchRow<'static>>,
    Vec<GenRow<'static>>,
    Vec<LoadRow<'static>>,
    Vec<Transformer2WRow<'static>>,
    Vec<Transformer3WRow<'static>>,
    Vec<AreaRow<'static>>,
    Vec<ZoneRow<'static>>,
    Vec<OwnerRow<'static>>,
    Vec<FixedShuntRow<'static>>,
    Vec<SwitchedShuntRow>,
    Vec<SwitchedShuntBankRow>,
    Vec<ConnectivityGroupRow<'static>>,
    Vec<NodeBreakerDetailRow<'static>>,
    Vec<SwitchDetailRow<'static>>,
    Vec<ConnectivityNodeDetailRow<'static>>,
    Vec<DiagramObjectRow<'static>>,
    Vec<DiagramPointRow<'static>>,
    Vec<ContingencyElement<'static>>,
    Vec<parser::DyModelSpec>,
)> {
    let mut lines = Vec::new();
    let mut machines = Vec::new();
    let mut loads = Vec::new();
    let mut transformers = Vec::new();
    let mut areas = Vec::new();
    let mut zones = Vec::new();
    let mut owners = Vec::new();
    let mut fixed_shunts = Vec::new();
    let mut switched_shunts = Vec::new();
    let mut terminals = Vec::new();
    let mut topological_nodes = Vec::new();
    let mut connectivity_groups = Vec::new();
    let mut switches = Vec::new();
    let mut connectivity_nodes = Vec::new();
    let mut base_voltages = Vec::new();
    let mut equipment_base_voltage_refs = Vec::new();
    let mut diagrams = Vec::new();
    let mut diagram_objects = Vec::new();
    let mut diagram_points = Vec::new();
    let mut dy_model_specs = Vec::new();

    for path in cgmes_paths {
        let (
            mut parsed_lines,
            mut parsed_machines,
            mut parsed_loads,
            mut parsed_transformers,
            mut parsed_areas,
            mut parsed_zones,
            mut parsed_owners,
            mut parsed_fixed_shunts,
            mut parsed_switched_shunts,
            mut parsed_terminals,
            mut parsed_topological_nodes,
            mut parsed_connectivity_groups,
            mut parsed_switches,
            mut parsed_connectivity_nodes,
            mut parsed_base_voltages,
            mut parsed_equipment_base_voltage_refs,
            mut parsed_diagrams,
            mut parsed_diagram_objects,
            mut parsed_diagram_points,
            mut parsed_dy_model_specs,
        ) = parse_eq_components_for_path(path)?;
        if !parsed_lines.is_empty() {
            lines.append(&mut parsed_lines);
        }
        if !parsed_machines.is_empty() {
            machines.append(&mut parsed_machines);
        }
        if !parsed_loads.is_empty() {
            loads.append(&mut parsed_loads);
        }
        if !parsed_transformers.is_empty() {
            transformers.append(&mut parsed_transformers);
        }
        if !parsed_areas.is_empty() {
            areas.append(&mut parsed_areas);
        }
        if !parsed_zones.is_empty() {
            zones.append(&mut parsed_zones);
        }
        if !parsed_owners.is_empty() {
            owners.append(&mut parsed_owners);
        }
        if !parsed_fixed_shunts.is_empty() {
            fixed_shunts.append(&mut parsed_fixed_shunts);
        }
        if !parsed_switched_shunts.is_empty() {
            switched_shunts.append(&mut parsed_switched_shunts);
        }
        if !parsed_terminals.is_empty() {
            terminals.append(&mut parsed_terminals);
        }
        if !parsed_topological_nodes.is_empty() {
            topological_nodes.append(&mut parsed_topological_nodes);
        }
        if !parsed_connectivity_groups.is_empty() {
            connectivity_groups.append(&mut parsed_connectivity_groups);
        }
        if !parsed_switches.is_empty() {
            switches.append(&mut parsed_switches);
        }
        if !parsed_connectivity_nodes.is_empty() {
            connectivity_nodes.append(&mut parsed_connectivity_nodes);
        }
        if !parsed_base_voltages.is_empty() {
            base_voltages.append(&mut parsed_base_voltages);
        }
        if !parsed_equipment_base_voltage_refs.is_empty() {
            equipment_base_voltage_refs.append(&mut parsed_equipment_base_voltage_refs);
        }
        if !parsed_diagrams.is_empty() {
            diagrams.append(&mut parsed_diagrams);
        }
        if !parsed_diagram_objects.is_empty() {
            diagram_objects.append(&mut parsed_diagram_objects);
        }
        if !parsed_diagram_points.is_empty() {
            diagram_points.append(&mut parsed_diagram_points);
        }
        if !parsed_dy_model_specs.is_empty() {
            dy_model_specs.append(&mut parsed_dy_model_specs);
        }
    }

    if lines.is_empty() {
        bail!("no ACLineSegment elements found across supplied CGMES paths")
    }
    if terminals.is_empty() {
        bail!("no Terminal elements found across supplied CGMES paths")
    }

    // Merge duplicate equipment payloads across EQ/SSH paths with last-wins
    // semantics based on input path order.
    let mut machine_by_mrid: HashMap<String, crate::models::SynchronousMachine<'static>> =
        HashMap::new();
    for machine in machines {
        machine_by_mrid.insert(machine.base.m_rid.as_ref().to_owned(), machine);
    }
    let mut machines: Vec<crate::models::SynchronousMachine<'static>> =
        machine_by_mrid.into_values().collect();

    let mut load_by_mrid: HashMap<String, crate::models::EnergyConsumer<'static>> = HashMap::new();
    for load in loads {
        load_by_mrid.insert(load.base.m_rid.as_ref().to_owned(), load);
    }
    let mut loads: Vec<crate::models::EnergyConsumer<'static>> =
        load_by_mrid.into_values().collect();

    let mut fixed_shunt_by_mrid: HashMap<String, parser::FixedShuntSpec> = HashMap::new();
    for shunt in fixed_shunts {
        fixed_shunt_by_mrid.insert(shunt.equipment_mrid.clone(), shunt);
    }
    let mut fixed_shunts: Vec<parser::FixedShuntSpec> = fixed_shunt_by_mrid.into_values().collect();

    let mut switch_by_mrid: HashMap<String, parser::SwitchSpec> = HashMap::new();
    for switch in switches {
        switch_by_mrid.insert(switch.switch_mrid.clone(), switch);
    }
    let mut switches: Vec<parser::SwitchSpec> = switch_by_mrid.into_values().collect();

    let mut owner_by_mrid: HashMap<String, crate::models::Owner<'static>> = HashMap::new();
    for owner in owners {
        owner_by_mrid.insert(owner.base.m_rid.as_ref().to_owned(), owner);
    }
    let mut owners: Vec<crate::models::Owner<'static>> = owner_by_mrid.into_values().collect();
    owners.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let owner_id_by_mrid: HashMap<String, i32> = owners
        .iter()
        .enumerate()
        .map(|(idx, owner)| (owner.base.m_rid.as_ref().to_owned(), (idx as i32) + 1))
        .collect();
    let default_owner_id = if owners.len() == 1 { Some(1) } else { None };

    let mut conn_to_topo: HashMap<&str, &str> = HashMap::new();
    for group in &connectivity_groups {
        let topological_mrid = group.topological_node_mrid.as_ref();
        for connectivity_mrid in &group.connectivity_node_mrids {
            conn_to_topo.insert(connectivity_mrid.as_ref(), topological_mrid);
        }
    }

    let use_topological =
        !conn_to_topo.is_empty() && bus_resolution_mode == BusResolutionMode::Topological;

    let mut base_voltage_by_mrid: HashMap<String, f64> = HashMap::new();
    for entry in base_voltages {
        base_voltage_by_mrid
            .entry(entry.base_voltage_mrid)
            .or_insert(entry.nominal_kv);
    }

    let mut equipment_base_voltage_by_mrid: HashMap<String, String> = HashMap::new();
    for entry in equipment_base_voltage_refs {
        equipment_base_voltage_by_mrid
            .entry(entry.equipment_mrid)
            .or_insert(entry.base_voltage_mrid);
    }

    let mut equipment_voltage_label_by_mrid: HashMap<String, String> = HashMap::new();
    for (equipment_mrid, base_voltage_mrid) in &equipment_base_voltage_by_mrid {
        if let Some(nominal_kv) = base_voltage_by_mrid.get(base_voltage_mrid) {
            equipment_voltage_label_by_mrid
                .insert(equipment_mrid.clone(), format_kv_label(*nominal_kv));
        }
    }

    let mut sorted_bus_keys: Vec<&str> = terminals
        .iter()
        .map(|terminal| {
            if use_topological {
                conn_to_topo
                    .get(terminal.connectivity_node_mrid.as_str())
                    .copied()
                    .with_context(|| {
                        format!(
                            "missing TP TopologicalNode mapping for ConnectivityNode '{}'",
                            terminal.connectivity_node_mrid
                        )
                    })
            } else {
                Ok(terminal.connectivity_node_mrid.as_str())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    sorted_bus_keys.sort_unstable();
    sorted_bus_keys.dedup();

    let mut bus_kv_candidates: HashMap<&str, Vec<f64>> = HashMap::new();
    for terminal in &terminals {
        let Some(base_voltage_mrid) = equipment_base_voltage_by_mrid.get(&terminal.line_mrid)
        else {
            continue;
        };
        let Some(nominal_kv) = base_voltage_by_mrid.get(base_voltage_mrid).copied() else {
            continue;
        };

        let bus_key = if use_topological {
            match conn_to_topo
                .get(terminal.connectivity_node_mrid.as_str())
                .copied()
            {
                Some(value) => value,
                None => continue,
            }
        } else {
            terminal.connectivity_node_mrid.as_str()
        };

        bus_kv_candidates
            .entry(bus_key)
            .or_default()
            .push(nominal_kv);
    }

    let mut bus_nominal_kv_by_key: HashMap<&str, f64> = HashMap::new();
    let mut bus_voltage_label_by_key: HashMap<&str, String> = HashMap::new();
    for (bus_key, kvs) in bus_kv_candidates {
        let Some(best) = kvs.into_iter().max_by(|left, right| left.total_cmp(right)) else {
            continue;
        };
        bus_nominal_kv_by_key.insert(bus_key, best);
        bus_voltage_label_by_key.insert(bus_key, format_kv_label(best));
    }

    let mut bus_key_to_bus_id: HashMap<&str, i32> = HashMap::with_capacity(sorted_bus_keys.len());
    for (idx, bus_key) in sorted_bus_keys.iter().enumerate() {
        bus_key_to_bus_id.insert(*bus_key, (idx as i32) + 1);
    }

    let mut diagram_element_by_cim_mrid: HashMap<String, DiagramElementResolution> = HashMap::new();
    if use_topological {
        for (bus_key, bus_id) in &bus_key_to_bus_id {
            diagram_element_by_cim_mrid.insert(
                (*bus_key).to_owned(),
                DiagramElementResolution {
                    element_type: "bus",
                    element_id: make_diagram_element_id("bus", &bus_id.to_string()),
                },
            );
        }
    }

    let mut topology_name_by_mrid: HashMap<&str, &str> = HashMap::new();
    for node in &topological_nodes {
        if let Some(name) = node.base.name.as_deref().and_then(non_empty_name) {
            topology_name_by_mrid.insert(node.base.m_rid.as_ref(), name);
        }
    }

    let mut bus_name_by_key: HashMap<&str, String> = HashMap::with_capacity(sorted_bus_keys.len());
    for bus_key in &sorted_bus_keys {
        let resolved = if use_topological {
            topology_name_by_mrid
                .get(*bus_key)
                .copied()
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    bus_fallback_name(
                        bus_key,
                        bus_voltage_label_by_key.get(*bus_key).map(String::as_str),
                    )
                })
        } else {
            bus_fallback_name(
                bus_key,
                bus_voltage_label_by_key.get(*bus_key).map(String::as_str),
            )
        };
        bus_name_by_key.insert(*bus_key, resolved);
    }

    let bus_rows: Vec<BusRow<'static>> = sorted_bus_keys
        .iter()
        .map(|bus_key| BusRow {
            bus_id: bus_key_to_bus_id[bus_key],
            name: Cow::Owned(bus_name_by_key.get(*bus_key).cloned().unwrap_or_else(|| {
                bus_fallback_name(
                    bus_key,
                    bus_voltage_label_by_key.get(*bus_key).map(String::as_str),
                )
            })),
            bus_type: 1,
            p_sched: 0.0,
            q_sched: 0.0,
            v_mag_set: 1.0,
            v_ang_set: 0.0,
            q_min: -9999.0,
            q_max: 9999.0,
            g_shunt: 0.0,
            b_shunt: 0.0,
            area: 1,
            zone: 1,
            owner_id: default_owner_id,
            v_min: 0.9,
            v_max: 1.1,
            p_min_agg: 0.0,
            p_max_agg: 0.0,
            nominal_kv: bus_nominal_kv_by_key.get(*bus_key).copied(),
            bus_uuid: Cow::Owned((*bus_key).to_owned()),
        })
        .collect();

    let mut endpoints_by_line: HashMap<&str, Endpoints> = HashMap::new();
    for (terminal_idx, terminal) in terminals.iter().enumerate() {
        let entry = endpoints_by_line
            .entry(terminal.line_mrid.as_str())
            .or_default();
        match terminal.sequence_number {
            1 => entry.from_terminal_idx = Some(terminal_idx),
            2 => entry.to_terminal_idx = Some(terminal_idx),
            _ => {}
        }
    }

    lines.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    for (idx, line) in lines.iter().enumerate() {
        let branch_id = (idx as i32) + 1;
        diagram_element_by_cim_mrid.insert(
            line.base.m_rid.as_ref().to_owned(),
            DiagramElementResolution {
                element_type: "branch",
                element_id: make_diagram_element_id("branch", &branch_id.to_string()),
            },
        );
    }

    let mut branch_rows = Vec::with_capacity(lines.len());
    for (idx, line) in lines.iter().enumerate() {
        let line_mrid = line.base.m_rid.as_ref();
        let endpoints = endpoints_by_line.get(line_mrid).copied().with_context(|| {
            format!("missing Terminal linkage for ACLineSegment mRID '{line_mrid}'")
        })?;

        let from_idx = endpoints.from_terminal_idx.with_context(|| {
            format!("missing Terminal sequenceNumber=1 for ACLineSegment mRID '{line_mrid}'")
        })?;
        let to_idx = endpoints.to_terminal_idx.with_context(|| {
            format!("missing Terminal sequenceNumber=2 for ACLineSegment mRID '{line_mrid}'")
        })?;

        let from_node = terminals[from_idx].connectivity_node_mrid.as_str();
        let to_node = terminals[to_idx].connectivity_node_mrid.as_str();

        let from_bus_key = if use_topological {
            conn_to_topo.get(from_node).copied().with_context(|| {
                format!("failed to resolve ConnectivityNode '{from_node}' to TopologicalNode")
            })?
        } else {
            from_node
        };
        let to_bus_key = if use_topological {
            conn_to_topo.get(to_node).copied().with_context(|| {
                format!("failed to resolve ConnectivityNode '{to_node}' to TopologicalNode")
            })?
        } else {
            to_node
        };

        let from_bus_id = bus_key_to_bus_id
            .get(from_bus_key)
            .copied()
            .with_context(|| {
                format!("failed to resolve bus key '{from_bus_key}' to dense bus_id")
            })?;
        let to_bus_id = bus_key_to_bus_id
            .get(to_bus_key)
            .copied()
            .with_context(|| format!("failed to resolve bus key '{to_bus_key}' to dense bus_id"))?;

        let from_bus_name = bus_name_by_key
            .get(from_bus_key)
            .map(String::as_str)
            .unwrap_or(from_bus_key);
        let to_bus_name = bus_name_by_key
            .get(to_bus_key)
            .map(String::as_str)
            .unwrap_or(to_bus_key);

        let ckt = "1";
        let name = if let Some(existing) = line.base.name.as_deref().and_then(non_empty_name) {
            existing.to_owned()
        } else {
            let kv = equipment_voltage_label_by_mrid
                .get(line_mrid)
                .cloned()
                .unwrap_or_else(|| voltage_label_from_name(line.base.name.as_deref()));
            branch_constructed_name(&kv, from_bus_name, to_bus_name, ckt)
        };

        branch_rows.push(BranchRow {
            branch_id: (idx as i32) + 1,
            from_bus_id,
            to_bus_id,
            ckt: Cow::Borrowed(ckt),
            name: Cow::Owned(name),
            r: line.r.unwrap_or(0.0),
            x: line.x.unwrap_or(0.0),
            b_shunt: line.bch.unwrap_or(0.0),
            tap: 1.0,
            phase: 0.0,
            rate_a: 9999.0,
            rate_b: 9999.0,
            rate_c: 9999.0,
            status: true,
            owner_id: default_owner_id,
            from_nominal_kv: bus_nominal_kv_by_key.get(from_bus_key).copied(),
            to_nominal_kv: bus_nominal_kv_by_key.get(to_bus_key).copied(),
        });
    }

    machines.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    let mut terminals_by_equipment: HashMap<&str, Vec<&parser::TerminalLink>> = HashMap::new();
    for terminal in &terminals {
        terminals_by_equipment
            .entry(terminal.line_mrid.as_str())
            .or_default()
            .push(terminal);
    }

    let mut gen_rows = Vec::with_capacity(machines.len());
    for (generator_index, machine) in machines.into_iter().enumerate() {
        let machine_mrid = machine.base.m_rid.as_ref();
        let machine_terminals = terminals_by_equipment.get(machine_mrid).with_context(|| {
            format!("missing Terminal linkage for SynchronousMachine mRID '{machine_mrid}'")
        })?;

        let selected_terminal = machine_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for SynchronousMachine mRID '{machine_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for SynchronousMachine mRID '{machine_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for SynchronousMachine mRID '{machine_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for SynchronousMachine mRID '{machine_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        let machine_id = machine.base.m_rid;
        let machine_id_text = machine_id.as_ref().to_owned();
        diagram_element_by_cim_mrid.insert(
            machine_id_text.clone(),
            DiagramElementResolution {
                element_type: "generator",
                element_id: make_diagram_element_id("generator", &machine_id_text),
            },
        );
        gen_rows.push(GenRow {
            generator_id: (generator_index as i32) + 1,
            bus_id,
            id: machine_id,
            name: machine
                .base
                .name
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_owned()))
                .unwrap_or_else(|| {
                    Cow::Owned(equipment_fallback_name(
                        "Gen",
                        machine_id_text.as_ref(),
                        equipment_voltage_label_by_mrid
                            .get(machine_id_text.as_str())
                            .map(String::as_str),
                    ))
                }),
            unit_type: machine
                .unit_type
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_ascii_lowercase()))
                .unwrap_or_else(|| Cow::Borrowed("synchronous_machine")),
            hierarchy_level: Cow::Borrowed("unit"),
            parent_generator_id: None,
            aggregation_count: None,
            p_sched_mw: machine.p_sched_mw.unwrap_or(0.0),
            p_min_mw: machine.p_min_mw.unwrap_or(0.0),
            p_max_mw: machine.p_max_mw.unwrap_or(0.0),
            q_min_mvar: machine.q_min_mvar.unwrap_or(0.0),
            q_max_mvar: machine.q_max_mvar.unwrap_or(0.0),
            status: true,
            mbase_mva: machine.mbase_mva.unwrap_or(100.0),
            uol_mw: machine.uol_mw.or(machine.p_max_mw),
            lol_mw: machine.lol_mw.or(machine.p_min_mw),
            ramp_rate_up_mw_min: None,
            ramp_rate_down_mw_min: None,
            is_ibr: machine.is_ibr.unwrap_or(false),
            ibr_subtype: machine
                .ibr_subtype
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_owned())),
            owner_id: machine
                .owner_mrid
                .as_deref()
                .and_then(|mrid| owner_id_by_mrid.get(mrid).copied())
                .or(default_owner_id),
            market_resource_id: machine
                .market_resource_id
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_owned())),
            h: machine.h.unwrap_or(0.0),
            xd_prime: machine.xd_prime.unwrap_or(0.0),
            d: machine.d.unwrap_or(0.0),
        });
    }

    loads.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    let mut load_rows = Vec::with_capacity(loads.len());
    for load in loads {
        let load_mrid = load.base.m_rid.as_ref();
        let load_terminals = terminals_by_equipment.get(load_mrid).with_context(|| {
            format!("missing Terminal linkage for EnergyConsumer mRID '{load_mrid}'")
        })?;

        let selected_terminal = load_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for EnergyConsumer mRID '{load_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for EnergyConsumer mRID '{load_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for EnergyConsumer mRID '{load_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for EnergyConsumer mRID '{load_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        let load_id = load.base.m_rid;
        let load_id_text = load_id.as_ref().to_owned();
        diagram_element_by_cim_mrid.insert(
            load_id_text.clone(),
            DiagramElementResolution {
                element_type: "load",
                element_id: make_diagram_element_id("load", &load_id_text),
            },
        );
        load_rows.push(LoadRow {
            bus_id,
            id: load_id,
            name: load
                .base
                .name
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_owned()))
                .unwrap_or_else(|| {
                    Cow::Owned(equipment_fallback_name(
                        "Load",
                        load_id_text.as_ref(),
                        equipment_voltage_label_by_mrid
                            .get(load_id_text.as_str())
                            .map(String::as_str),
                    ))
                }),
            status: load.status.unwrap_or(true),
            p_mw: load.p_mw.unwrap_or(0.0),
            q_mvar: load.q_mvar.unwrap_or(0.0),
        });
    }

    transformers.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let mut transformer_2w_rows = Vec::new();
    let mut transformer_3w_rows = Vec::new();
    for transformer in transformers {
        let transformer_mrid = transformer.base.m_rid.as_ref();
        let transformer_terminals =
            terminals_by_equipment
                .get(transformer_mrid)
                .with_context(|| {
                    format!(
                        "missing Terminal linkage for PowerTransformer mRID '{transformer_mrid}'"
                    )
                })?;

        let mut unique_terminals: Vec<&parser::TerminalLink> = Vec::new();
        for terminal in transformer_terminals {
            if unique_terminals
                .iter()
                .all(|existing| existing.connectivity_node_mrid != terminal.connectivity_node_mrid)
            {
                unique_terminals.push(*terminal);
            }
        }
        unique_terminals.sort_by_key(|terminal| {
            (
                terminal.sequence_number,
                terminal.connectivity_node_mrid.as_str(),
            )
        });

        let winding_count = transformer.ends.len().max(unique_terminals.len());
        if winding_count < 2 {
            bail!(
                "PowerTransformer mRID '{transformer_mrid}' has fewer than 2 winding/terminal endpoints"
            );
        }

        let resolve_terminal_bus_key = |terminal: &parser::TerminalLink| -> Result<String> {
            if use_topological {
                conn_to_topo
                    .get(terminal.connectivity_node_mrid.as_str())
                    .copied()
                    .map(str::to_owned)
                    .with_context(|| {
                        format!(
                            "failed to resolve ConnectivityNode '{}' to TopologicalNode for PowerTransformer mRID '{transformer_mrid}'",
                            terminal.connectivity_node_mrid
                        )
                    })
            } else {
                Ok(terminal.connectivity_node_mrid.clone())
            }
        };

        let resolve_terminal_bus_id = |terminal: &parser::TerminalLink| -> Result<i32> {
            let bus_key = resolve_terminal_bus_key(terminal)?;
            bus_key_to_bus_id.get(bus_key.as_str()).copied().with_context(|| {
                format!(
                    "failed to resolve bus key '{}' to dense bus_id for PowerTransformer mRID '{transformer_mrid}'",
                    bus_key
                )
            })
        };

        let vector_group = transformer
            .vector_group
            .map(|value| Cow::Owned(value.into_owned()))
            .unwrap_or_else(|| Cow::Borrowed(""));
        let rate_a = transformer
            .rate_a
            .or_else(|| transformer.ends.iter().find_map(|end| end.rate))
            .unwrap_or(0.0);
        let rate_b = transformer.rate_b.unwrap_or(rate_a);
        let rate_c = transformer.rate_c.unwrap_or(rate_b);
        let status = transformer.status.unwrap_or(true);

        if winding_count == 2 {
            let from_terminal = unique_terminals.first().copied().with_context(|| {
                format!(
                    "missing Terminal endpoint #1 for PowerTransformer mRID '{transformer_mrid}'"
                )
            })?;
            let to_terminal = unique_terminals.get(1).copied().with_context(|| {
                format!(
                    "missing Terminal endpoint #2 for PowerTransformer mRID '{transformer_mrid}'"
                )
            })?;

            let from_bus_key = resolve_terminal_bus_key(from_terminal)?;
            let to_bus_key = resolve_terminal_bus_key(to_terminal)?;
            let from_bus_id = resolve_terminal_bus_id(from_terminal)?;
            let to_bus_id = resolve_terminal_bus_id(to_terminal)?;

            let winding1 = transformer.ends.first();
            let winding2 = transformer.ends.get(1);
            let winding1_r = winding1.and_then(|end| end.r).unwrap_or(0.0);
            let winding1_x = winding1.and_then(|end| end.x).unwrap_or(0.0);
            let winding2_r = winding2.and_then(|end| end.r).unwrap_or(0.0);
            let winding2_x = winding2.and_then(|end| end.x).unwrap_or(0.0);

            transformer_2w_rows.push(Transformer2WRow {
                from_bus_id,
                to_bus_id,
                ckt: Cow::Borrowed("1"),
                name: transformer
                    .base
                    .name
                    .as_deref()
                    .and_then(non_empty_name)
                    .map(|value| Cow::Owned(value.to_owned()))
                    .unwrap_or_else(|| {
                        Cow::Owned(equipment_fallback_name(
                            "Xfmr2W",
                            transformer.base.m_rid.as_ref(),
                            equipment_voltage_label_by_mrid
                                .get(transformer.base.m_rid.as_ref())
                                .map(String::as_str),
                        ))
                    }),
                r: winding1_r + winding2_r,
                x: winding1_x + winding2_x,
                winding1_r,
                winding1_x,
                winding2_r,
                winding2_x,
                g: transformer.ends.iter().filter_map(|end| end.g).sum(),
                b: transformer.ends.iter().filter_map(|end| end.b).sum(),
                tap_ratio: winding1.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                nominal_tap_ratio: 1.0,
                phase_shift: winding1.and_then(|end| end.phase_shift).unwrap_or(0.0),
                vector_group,
                rate_a,
                rate_b,
                rate_c,
                status,
                from_nominal_kv: bus_nominal_kv_by_key.get(from_bus_key.as_str()).copied(),
                to_nominal_kv: bus_nominal_kv_by_key.get(to_bus_key.as_str()).copied(),
            });
        } else {
            let terminal_h = unique_terminals.first().copied().with_context(|| {
                format!(
                    "missing Terminal endpoint #1 for PowerTransformer mRID '{transformer_mrid}'"
                )
            })?;
            let terminal_m = unique_terminals.get(1).copied().with_context(|| {
                format!(
                    "missing Terminal endpoint #2 for PowerTransformer mRID '{transformer_mrid}'"
                )
            })?;
            let terminal_l = unique_terminals.get(2).copied().with_context(|| {
                format!(
                    "missing Terminal endpoint #3 for PowerTransformer mRID '{transformer_mrid}'"
                )
            })?;

            let bus_h_key = resolve_terminal_bus_key(terminal_h)?;
            let bus_m_key = resolve_terminal_bus_key(terminal_m)?;
            let bus_l_key = resolve_terminal_bus_key(terminal_l)?;
            let bus_h_id = resolve_terminal_bus_id(terminal_h)?;
            let bus_m_id = resolve_terminal_bus_id(terminal_m)?;
            let bus_l_id = resolve_terminal_bus_id(terminal_l)?;

            let end_h = transformer.ends.first();
            let end_m = transformer.ends.get(1);
            let end_l = transformer.ends.get(2);

            transformer_3w_rows.push(Transformer3WRow {
                bus_h_id,
                bus_m_id,
                bus_l_id,
                ckt: Cow::Borrowed("1"),
                name: transformer
                    .base
                    .name
                    .as_deref()
                    .and_then(non_empty_name)
                    .map(|value| Cow::Owned(value.to_owned()))
                    .unwrap_or_else(|| {
                        Cow::Owned(equipment_fallback_name(
                            "Xfmr3W",
                            transformer.base.m_rid.as_ref(),
                            equipment_voltage_label_by_mrid
                                .get(transformer.base.m_rid.as_ref())
                                .map(String::as_str),
                        ))
                    }),
                r_hm: end_h.and_then(|end| end.r).unwrap_or(0.0),
                x_hm: end_h.and_then(|end| end.x).unwrap_or(0.0),
                r_hl: end_m.and_then(|end| end.r).unwrap_or(0.0),
                x_hl: end_m.and_then(|end| end.x).unwrap_or(0.0),
                r_ml: end_l.and_then(|end| end.r).unwrap_or(0.0),
                x_ml: end_l.and_then(|end| end.x).unwrap_or(0.0),
                tap_h: end_h.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                tap_m: end_m.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                tap_l: end_l.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                phase_shift: end_h.and_then(|end| end.phase_shift).unwrap_or(0.0),
                vector_group,
                rate_a,
                rate_b,
                rate_c,
                status,
                nominal_kv_h: bus_nominal_kv_by_key.get(bus_h_key.as_str()).copied(),
                nominal_kv_m: bus_nominal_kv_by_key.get(bus_m_key.as_str()).copied(),
                nominal_kv_l: bus_nominal_kv_by_key.get(bus_l_key.as_str()).copied(),
            });
        }
    }

    areas.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let area_rows: Vec<AreaRow<'static>> = areas
        .into_iter()
        .enumerate()
        .map(|(idx, area)| AreaRow {
            area_id: (idx as i32) + 1,
            name: area
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(area.base.m_rid.as_ref().to_owned())),
            interchange_mw: area.interchange_mw,
        })
        .collect();

    zones.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let zone_rows: Vec<ZoneRow<'static>> = zones
        .into_iter()
        .enumerate()
        .map(|(idx, zone)| ZoneRow {
            zone_id: (idx as i32) + 1,
            name: zone
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(zone.base.m_rid.as_ref().to_owned())),
        })
        .collect();

    let owner_rows: Vec<OwnerRow<'static>> = owners
        .into_iter()
        .enumerate()
        .map(|(idx, owner)| OwnerRow {
            owner_id: (idx as i32) + 1,
            name: owner
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(owner.base.m_rid.as_ref().to_owned())),
            short_name: owner
                .base
                .description
                .as_deref()
                .and_then(non_empty_name)
                .map(|value| Cow::Owned(value.to_owned())),
            owner_type: None,
        })
        .collect();

    fixed_shunts.sort_unstable_by(|left, right| left.equipment_mrid.cmp(&right.equipment_mrid));
    let mut fixed_shunt_rows = Vec::with_capacity(fixed_shunts.len());
    for shunt in fixed_shunts {
        let shunt_mrid = shunt.equipment_mrid.as_str();
        let shunt_terminals = terminals_by_equipment.get(shunt_mrid).with_context(|| {
            format!("missing Terminal linkage for LinearShuntCompensator mRID '{shunt_mrid}'")
        })?;

        let selected_terminal = shunt_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for LinearShuntCompensator mRID '{shunt_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for LinearShuntCompensator mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for LinearShuntCompensator mRID '{shunt_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for LinearShuntCompensator mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        fixed_shunt_rows.push(FixedShuntRow {
            bus_id,
            id: Cow::Owned(shunt.equipment_mrid),
            status: shunt.status.unwrap_or(true),
            g_mw: shunt.g_mw.unwrap_or(0.0),
            b_mvar: shunt.b_mvar.unwrap_or(0.0),
        });
        let shunt_key = fixed_shunt_rows
            .last()
            .map(|row| row.id.as_ref().to_owned())
            .unwrap_or_default();
        if !shunt_key.is_empty() {
            diagram_element_by_cim_mrid.insert(
                shunt_key.clone(),
                DiagramElementResolution {
                    element_type: "fixed_shunt",
                    element_id: make_diagram_element_id("fixed_shunt", &shunt_key),
                },
            );
        }
    }

    switched_shunts.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let mut switched_shunt_rows = Vec::with_capacity(switched_shunts.len());
    let mut switched_shunt_bank_rows = Vec::new();
    for shunt in switched_shunts {
        let shunt_mrid = shunt.base.m_rid.as_ref();
        let shunt_terminals = terminals_by_equipment.get(shunt_mrid).with_context(|| {
            format!("missing Terminal linkage for SvShuntCompensator mRID '{shunt_mrid}'")
        })?;

        let selected_terminal = shunt_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for SvShuntCompensator mRID '{shunt_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for switched shunt mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for switched shunt mRID '{shunt_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for switched shunt mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        let source_steps = shunt.b_steps.unwrap_or_default();
        let mut capacitive_steps = Vec::new();
        let numeric_shunt_id = (switched_shunt_rows.len() as i32) + 1;

        for (index, value) in source_steps.iter().copied().enumerate() {
            if value > 0.0 {
                // v0.8.8: switched_shunts.b_steps is strictly capacitive.
                capacitive_steps.push(value);
            } else if value < 0.0 {
                // v0.8.8: inductive steps are emitted in switched_shunt_banks.
                switched_shunt_bank_rows.push(SwitchedShuntBankRow {
                    shunt_id: numeric_shunt_id,
                    bank_id: (index as i32) + 1,
                    b_pu: value,
                    status: true,
                    step: (index as i32) + 1,
                    bus_id,
                });
            }
        }

        let raw_current_step = shunt.current_step.unwrap_or(0).max(0);
        let current_step = if capacitive_steps.is_empty() {
            0
        } else {
            raw_current_step.min(capacitive_steps.len() as i32)
        };
        // For CIM, b_steps are cumulative susceptance values (b_steps[i] = per_section*(i+1)).
        // b_init_pu is the susceptance at the active step: b_steps[current_step - 1].
        let b_init_pu = if current_step > 0 && (current_step as usize) <= capacitive_steps.len() {
            capacitive_steps[(current_step as usize) - 1]
        } else {
            0.0
        };

        switched_shunt_rows.push(SwitchedShuntRow {
            bus_id,
            status: true,
            v_low: shunt.v_low.unwrap_or(0.95),
            v_high: shunt.v_high.unwrap_or(1.05),
            b_steps: capacitive_steps,
            current_step,
            b_init_pu,
            // v0.8.5: CIM path — use ShuntCompensator mRID as stable bank identity.
            shunt_id: Some(shunt.base.m_rid.to_string()),
        });
    }

    let mut connectivity_group_rows: Vec<ConnectivityGroupRow<'static>> = Vec::new();
    let mut node_breaker_rows: Vec<NodeBreakerDetailRow<'static>> = Vec::new();
    let mut switch_detail_rows: Vec<SwitchDetailRow<'static>> = Vec::new();
    let mut connectivity_node_rows: Vec<ConnectivityNodeDetailRow<'static>> = Vec::new();
    let mut split_bus_stub_elements: Vec<ContingencyElement<'static>> = Vec::new();
    for group in &connectivity_groups {
        let topological_mrid = group.topological_node_mrid.as_ref().to_owned();
        let bus_key = if use_topological {
            topological_mrid.as_str()
        } else {
            continue;
        };
        let Some(topological_bus_id) = bus_key_to_bus_id.get(bus_key).copied() else {
            continue;
        };

        let mut connectivity_node_mrids: Vec<Cow<'static, str>> = group
            .connectivity_node_mrids
            .iter()
            .map(|value| Cow::Owned(value.as_ref().to_owned()))
            .collect();
        connectivity_node_mrids.sort_unstable();
        connectivity_node_mrids.dedup();

        let display_topological = topology_name_by_mrid
            .get(topological_mrid.as_str())
            .copied()
            .unwrap_or(topological_mrid.as_str())
            .to_owned();

        if connectivity_node_mrids.len() > 1 {
            let split_id = format!(
                "topological_node_id={topological_bus_id};connectivity_node_a={};connectivity_node_b={};breaker_mrid=stub",
                connectivity_node_mrids[0], connectivity_node_mrids[1]
            );
            split_bus_stub_elements.push(ContingencyElement {
                element_type: Cow::Borrowed("split_bus"),
                branch_id: None,
                bus_id: Some(topological_bus_id),
                status_change: true,
                equipment_kind: Some(Cow::Borrowed("split_bus")),
                equipment_id: Some(Cow::Owned(split_id)),
            });
        }

        connectivity_group_rows.push(ConnectivityGroupRow {
            topological_bus_id,
            topological_node_mrid: Cow::Owned(display_topological),
            connectivity_count: connectivity_node_mrids.len() as i32,
            connectivity_node_mrids,
        });
    }

    if !switches.is_empty() {
        switches.sort_unstable_by(|left, right| left.switch_mrid.cmp(&right.switch_mrid));
        for switch in switches {
            let switch_terminals = terminals_by_equipment
                .get(switch.switch_mrid.as_str())
                .cloned()
                .unwrap_or_default();

            let mut unique_nodes: Vec<&str> = Vec::new();
            for terminal in &switch_terminals {
                let node = terminal.connectivity_node_mrid.as_str();
                if !unique_nodes.iter().any(|existing| *existing == node) {
                    unique_nodes.push(node);
                }
            }
            unique_nodes.sort_unstable();

            let connectivity_node_a = unique_nodes
                .first()
                .map(|value| Cow::Owned((*value).to_string()));
            let connectivity_node_b = unique_nodes
                .get(1)
                .map(|value| Cow::Owned((*value).to_string()));

            let resolve_bus_id = |node: &str| -> Option<i32> {
                if use_topological {
                    conn_to_topo
                        .get(node)
                        .and_then(|topological| bus_key_to_bus_id.get(*topological))
                        .copied()
                } else {
                    bus_key_to_bus_id.get(node).copied()
                }
            };

            let from_bus_id = unique_nodes.first().and_then(|node| resolve_bus_id(node));
            let to_bus_id = unique_nodes.get(1).and_then(|node| resolve_bus_id(node));
            let status = switch.is_open.map(|value| !value);

            switch_detail_rows.push(SwitchDetailRow {
                switch_id: Cow::Owned(switch.switch_mrid.clone()),
                name: switch.name.map(Cow::Owned),
                switch_type: Cow::Owned(switch.switch_type.clone()),
                is_open: switch.is_open,
                normal_open: switch.normal_open,
                retained: switch.retained,
            });

            if emit_node_breaker_detail {
                diagram_element_by_cim_mrid.insert(
                    switch.switch_mrid.clone(),
                    DiagramElementResolution {
                        element_type: "breaker",
                        element_id: make_diagram_element_id("breaker", &switch.switch_mrid),
                    },
                );
            }

            node_breaker_rows.push(NodeBreakerDetailRow {
                switch_id: Cow::Owned(switch.switch_mrid),
                switch_type: Cow::Owned(switch.switch_type),
                from_bus_id,
                to_bus_id,
                connectivity_node_a,
                connectivity_node_b,
                is_open: switch.is_open,
                normal_open: switch.normal_open,
                status,
            });
        }
    }

    if !connectivity_nodes.is_empty() {
        for node in connectivity_nodes {
            let bus_id = if use_topological {
                node.topological_node_mrid
                    .as_deref()
                    .and_then(|topological| bus_key_to_bus_id.get(topological))
                    .copied()
            } else {
                bus_key_to_bus_id
                    .get(node.connectivity_node_mrid.as_str())
                    .copied()
            };

            connectivity_node_rows.push(ConnectivityNodeDetailRow {
                connectivity_node_mrid: Cow::Owned(node.connectivity_node_mrid),
                topological_node_mrid: node.topological_node_mrid.map(Cow::Owned),
                bus_id,
            });
            let connectivity_key = connectivity_node_rows
                .last()
                .map(|row| row.connectivity_node_mrid.as_ref().to_owned())
                .unwrap_or_default();
            if emit_node_breaker_detail && !connectivity_key.is_empty() {
                diagram_element_by_cim_mrid.insert(
                    connectivity_key.clone(),
                    DiagramElementResolution {
                        element_type: "connectivity_node",
                        element_id: make_diagram_element_id("connectivity_node", &connectivity_key),
                    },
                );
            }
        }
    }

    let diagram_name_by_rdf_id: HashMap<String, String> = diagrams
        .into_iter()
        .map(|diagram| {
            let diagram_id = diagram
                .name
                .as_deref()
                .and_then(non_empty_name)
                .unwrap_or(diagram.diagram_rdf_id.as_str())
                .to_owned();
            (diagram.diagram_rdf_id, diagram_id)
        })
        .collect();

    let mut points_by_object_id: HashMap<&str, Vec<&parser::DiagramPointRecord>> = HashMap::new();
    for point in &diagram_points {
        points_by_object_id
            .entry(point.obj_rdf_id.as_str())
            .or_default()
            .push(point);
    }

    let mut diagram_object_rows: Vec<DiagramObjectRow<'static>> = Vec::new();
    let mut diagram_point_rows: Vec<DiagramPointRow<'static>> = Vec::new();
    for object in diagram_objects {
        let Some(diagram_id) = diagram_name_by_rdf_id.get(&object.diagram_rdf_id) else {
            continue;
        };
        let Some(resolution) =
            diagram_element_by_cim_mrid.get(object.identified_object_rdf_id.as_str())
        else {
            continue;
        };

        let element_id = resolution.element_id.clone();
        let diagram_id_text = diagram_id.clone();
        diagram_object_rows.push(DiagramObjectRow {
            element_id: Cow::Owned(element_id.clone()),
            element_type: Cow::Borrowed(resolution.element_type),
            diagram_id: Cow::Owned(diagram_id_text.clone()),
            rotation: object.rotation,
            visible: true,
            draw_order: object.drawing_order,
        });

        if let Some(points) = points_by_object_id.get(object.obj_rdf_id.as_str()) {
            let mut ordered_points = points.clone();
            ordered_points.sort_unstable_by(|left, right| left.seq.cmp(&right.seq));
            for point in ordered_points {
                diagram_point_rows.push(DiagramPointRow {
                    element_id: Cow::Owned(element_id.clone()),
                    diagram_id: Cow::Owned(diagram_id_text.clone()),
                    seq: point.seq,
                    x: point.x,
                    y: point.y,
                });
            }
        }
    }

    diagram_object_rows.sort_unstable_by(|left, right| {
        left.diagram_id
            .cmp(&right.diagram_id)
            .then_with(|| left.element_type.cmp(&right.element_type))
            .then_with(|| left.element_id.cmp(&right.element_id))
    });
    diagram_object_rows.dedup_by(|left, right| {
        left.diagram_id == right.diagram_id
            && left.element_type == right.element_type
            && left.element_id == right.element_id
    });
    diagram_point_rows.sort_unstable_by(|left, right| {
        left.diagram_id
            .cmp(&right.diagram_id)
            .then_with(|| left.element_id.cmp(&right.element_id))
            .then_with(|| left.seq.cmp(&right.seq))
    });

    Ok((
        bus_rows,
        branch_rows,
        gen_rows,
        load_rows,
        transformer_2w_rows,
        transformer_3w_rows,
        area_rows,
        zone_rows,
        owner_rows,
        fixed_shunt_rows,
        switched_shunt_rows,
        switched_shunt_bank_rows,
        connectivity_group_rows,
        node_breaker_rows,
        switch_detail_rows,
        connectivity_node_rows,
        diagram_object_rows,
        diagram_point_rows,
        split_bus_stub_elements,
        dy_model_specs,
    ))
}

/// Builds the one-row `metadata` table batch using the locked v0.5 schema.
fn build_metadata_batch(row: &MetadataRow<'_>) -> Result<RecordBatch> {
    let schema = Arc::new(metadata_schema());

    let mut base_mva_b = Float64Builder::new();
    let mut frequency_b = Float64Builder::new();
    let mut psse_b = Int32Builder::new();
    let mut study_name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut timestamp_b = StringBuilder::new();
    let mut raptrix_version_b = StringBuilder::new();
    let mut planning_b = BooleanBuilder::new();
    let mut source_case_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut snapshot_timestamp_utc_b = StringBuilder::new();
    let mut case_fingerprint_b = StringBuilder::new();
    let mut validation_mode_b = StringDictionaryBuilder::<Int32Type>::new();
    // v0.8.4 builders
    let mut case_mode_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut solved_state_presence_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut solver_version_b = StringBuilder::new();
    let mut solver_iterations_b = Int32Builder::new();
    let mut solver_accuracy_b = Float64Builder::new();
    let mut solver_mode_b = StringDictionaryBuilder::<Int32Type>::new();
    // v0.8.5 builders
    let mut slack_bus_id_solved_b = Int32Builder::new();
    let mut angle_reference_deg_b = Float64Builder::new();
    let mut solved_shunt_state_presence_b = StringDictionaryBuilder::<Int32Type>::new();
    // v0.8.8 builders
    let mut modern_grid_profile_b = BooleanBuilder::new();
    let mut ibr_penetration_pct_b = Float64Builder::new();
    let mut has_ibr_b = BooleanBuilder::new();
    let mut has_smart_valve_b = BooleanBuilder::new();
    let mut has_multi_terminal_dc_b = BooleanBuilder::new();
    let mut study_purpose_b = StringBuilder::new();
    let list_field = schema.field(27).data_type().clone();
    let mut scenario_tags_b = ListBuilder::new(StringBuilder::new()).with_field(match list_field {
        DataType::List(field) => field,
        _ => Arc::new(Field::new("item", DataType::Utf8, false)),
    });
    // v0.9.0 builders
    let mut hour_ahead_uncertainty_band_b = Float64Builder::new();
    let mut commitment_source_b = StringBuilder::new();
    let mut solver_q_limit_infeasible_count_b = Int32Builder::new();
    let mut pv_to_pq_switch_count_b = Int32Builder::new();
    let mut real_time_discovery_b = BooleanBuilder::new();

    base_mva_b.append_value(row.base_mva);
    frequency_b.append_value(row.frequency_hz);
    psse_b.append_value(row.psse_version);
    study_name_b.append(row.study_name.as_ref())?;
    timestamp_b.append_value(row.timestamp_utc.as_ref());
    raptrix_version_b.append_value(row.raptrix_version.as_ref());
    planning_b.append_value(row.is_planning_case);
    source_case_id_b.append(row.source_case_id.as_ref())?;
    snapshot_timestamp_utc_b.append_value(row.snapshot_timestamp_utc.as_ref());
    case_fingerprint_b.append_value(row.case_fingerprint.as_ref());
    validation_mode_b.append(row.validation_mode.as_ref())?;
    // v0.8.4
    case_mode_b.append(row.case_mode.as_ref())?;
    match &row.solved_state_presence {
        Some(v) => {
            solved_state_presence_b.append(v.as_ref())?;
        }
        None => solved_state_presence_b.append_null(),
    }
    match &row.solver_version {
        Some(v) => solver_version_b.append_value(v.as_ref()),
        None => solver_version_b.append_null(),
    }
    match row.solver_iterations {
        Some(v) => solver_iterations_b.append_value(v),
        None => solver_iterations_b.append_null(),
    }
    match row.solver_accuracy {
        Some(v) => solver_accuracy_b.append_value(v),
        None => solver_accuracy_b.append_null(),
    }
    match &row.solver_mode {
        Some(v) => {
            solver_mode_b.append(v.as_ref())?;
        }
        None => solver_mode_b.append_null(),
    }
    // v0.8.5
    match row.slack_bus_id_solved {
        Some(v) => slack_bus_id_solved_b.append_value(v),
        None => slack_bus_id_solved_b.append_null(),
    }
    match row.angle_reference_deg {
        Some(v) => angle_reference_deg_b.append_value(v),
        None => angle_reference_deg_b.append_null(),
    }
    match &row.solved_shunt_state_presence {
        Some(v) => {
            solved_shunt_state_presence_b.append(v.as_ref())?;
        }
        None => solved_shunt_state_presence_b.append_null(),
    }
    modern_grid_profile_b.append_value(row.modern_grid_profile);
    match row.ibr_penetration_pct {
        Some(v) => ibr_penetration_pct_b.append_value(v),
        None => ibr_penetration_pct_b.append_null(),
    }
    has_ibr_b.append_value(row.has_ibr);
    has_smart_valve_b.append_value(row.has_smart_valve);
    has_multi_terminal_dc_b.append_value(row.has_multi_terminal_dc);
    match &row.study_purpose {
        Some(v) => study_purpose_b.append_value(v.as_ref()),
        None => study_purpose_b.append_null(),
    }
    for tag in &row.scenario_tags {
        scenario_tags_b.values().append_value(tag.as_ref());
    }
    scenario_tags_b.append(true);
    // v0.9.0 — all nullable; null in standard planning files
    hour_ahead_uncertainty_band_b.append_null();
    commitment_source_b.append_null();
    solver_q_limit_infeasible_count_b.append_null();
    pv_to_pq_switch_count_b.append_null();
    real_time_discovery_b.append_null();

    let custom_metadata_type = schema.field(11).data_type().clone();
    let custom_metadata_array = new_null_array(&custom_metadata_type, 1);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(base_mva_b.finish()) as ArrayRef,
        Arc::new(frequency_b.finish()) as ArrayRef,
        Arc::new(psse_b.finish()) as ArrayRef,
        Arc::new(study_name_b.finish()) as ArrayRef,
        Arc::new(timestamp_b.finish()) as ArrayRef,
        Arc::new(raptrix_version_b.finish()) as ArrayRef,
        Arc::new(planning_b.finish()) as ArrayRef,
        Arc::new(source_case_id_b.finish()) as ArrayRef,
        Arc::new(snapshot_timestamp_utc_b.finish()) as ArrayRef,
        Arc::new(case_fingerprint_b.finish()) as ArrayRef,
        Arc::new(validation_mode_b.finish()) as ArrayRef,
        custom_metadata_array,
        // v0.8.4
        Arc::new(case_mode_b.finish()) as ArrayRef,
        Arc::new(solved_state_presence_b.finish()) as ArrayRef,
        Arc::new(solver_version_b.finish()) as ArrayRef,
        Arc::new(solver_iterations_b.finish()) as ArrayRef,
        Arc::new(solver_accuracy_b.finish()) as ArrayRef,
        Arc::new(solver_mode_b.finish()) as ArrayRef,
        // v0.8.5
        Arc::new(slack_bus_id_solved_b.finish()) as ArrayRef,
        Arc::new(angle_reference_deg_b.finish()) as ArrayRef,
        Arc::new(solved_shunt_state_presence_b.finish()) as ArrayRef,
        Arc::new(modern_grid_profile_b.finish()) as ArrayRef,
        Arc::new(ibr_penetration_pct_b.finish()) as ArrayRef,
        Arc::new(has_ibr_b.finish()) as ArrayRef,
        Arc::new(has_smart_valve_b.finish()) as ArrayRef,
        Arc::new(has_multi_terminal_dc_b.finish()) as ArrayRef,
        Arc::new(study_purpose_b.finish()) as ArrayRef,
        Arc::new(scenario_tags_b.finish()) as ArrayRef,
        // v0.9.0
        Arc::new(hour_ahead_uncertainty_band_b.finish()) as ArrayRef,
        Arc::new(commitment_source_b.finish()) as ArrayRef,
        Arc::new(solver_q_limit_infeasible_count_b.finish()) as ArrayRef,
        Arc::new(pv_to_pq_switch_count_b.finish()) as ArrayRef,
        Arc::new(real_time_discovery_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build metadata record batch")
}

/// Builds the `buses` table batch with dictionary encoding on low-cardinality
/// string fields (`name`) and fixed, schema-ordered columns.
fn build_buses_batch(rows: &[BusRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(buses_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut type_b = Int8Builder::new();
    let mut p_sched_b = Float64Builder::new();
    let mut q_sched_b = Float64Builder::new();
    let mut v_mag_set_b = Float64Builder::new();
    let mut v_ang_set_b = Float64Builder::new();
    let mut q_min_b = Float64Builder::new();
    let mut q_max_b = Float64Builder::new();
    let mut g_shunt_b = Float64Builder::new();
    let mut b_shunt_b = Float64Builder::new();
    let mut area_b = Int32Builder::new();
    let mut zone_b = Int32Builder::new();
    let mut owner_id_b = Int32Builder::new();
    let mut v_min_b = Float64Builder::new();
    let mut v_max_b = Float64Builder::new();
    let mut p_min_agg_b = Float64Builder::new();
    let mut p_max_agg_b = Float64Builder::new();
    let mut nominal_kv_b = Float64Builder::new();
    let mut bus_uuid_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        name_b.append(row.name.as_ref())?;
        type_b.append_value(row.bus_type);
        p_sched_b.append_value(row.p_sched);
        q_sched_b.append_value(row.q_sched);
        v_mag_set_b.append_value(row.v_mag_set);
        v_ang_set_b.append_value(row.v_ang_set);
        q_min_b.append_value(row.q_min);
        q_max_b.append_value(row.q_max);
        g_shunt_b.append_value(row.g_shunt);
        b_shunt_b.append_value(row.b_shunt);
        area_b.append_value(row.area);
        zone_b.append_value(row.zone);
        if let Some(owner_id) = row.owner_id {
            owner_id_b.append_value(owner_id);
        } else {
            owner_id_b.append_null();
        }
        v_min_b.append_value(row.v_min);
        v_max_b.append_value(row.v_max);
        p_min_agg_b.append_value(row.p_min_agg);
        p_max_agg_b.append_value(row.p_max_agg);
        if let Some(nominal_kv) = row.nominal_kv {
            nominal_kv_b.append_value(nominal_kv);
        } else {
            nominal_kv_b.append_null();
        }
        bus_uuid_b.append(row.bus_uuid.as_ref())?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(type_b.finish()) as ArrayRef,
        Arc::new(p_sched_b.finish()) as ArrayRef,
        Arc::new(q_sched_b.finish()) as ArrayRef,
        Arc::new(v_mag_set_b.finish()) as ArrayRef,
        Arc::new(v_ang_set_b.finish()) as ArrayRef,
        Arc::new(q_min_b.finish()) as ArrayRef,
        Arc::new(q_max_b.finish()) as ArrayRef,
        Arc::new(g_shunt_b.finish()) as ArrayRef,
        Arc::new(b_shunt_b.finish()) as ArrayRef,
        Arc::new(area_b.finish()) as ArrayRef,
        Arc::new(zone_b.finish()) as ArrayRef,
        Arc::new(owner_id_b.finish()) as ArrayRef,
        Arc::new(v_min_b.finish()) as ArrayRef,
        Arc::new(v_max_b.finish()) as ArrayRef,
        Arc::new(p_min_agg_b.finish()) as ArrayRef,
        Arc::new(p_max_agg_b.finish()) as ArrayRef,
        Arc::new(nominal_kv_b.finish()) as ArrayRef,
        Arc::new(bus_uuid_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build buses record batch")
}

fn build_branches_batch(rows: &[BranchRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(branches_schema());

    let mut branch_id_b = Int32Builder::new();
    let mut from_bus_id_b = Int32Builder::new();
    let mut to_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_b = Float64Builder::new();
    let mut x_b = Float64Builder::new();
    let mut b_shunt_b = Float64Builder::new();
    let mut tap_b = Float64Builder::new();
    let mut phase_b = Float64Builder::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut owner_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut from_nominal_kv_b = Float64Builder::new();
    let mut to_nominal_kv_b = Float64Builder::new();

    for row in rows {
        branch_id_b.append_value(row.branch_id);
        from_bus_id_b.append_value(row.from_bus_id);
        to_bus_id_b.append_value(row.to_bus_id);
        ckt_b.append(row.ckt.as_ref())?;
        r_b.append_value(row.r);
        x_b.append_value(row.x);
        b_shunt_b.append_value(row.b_shunt);
        tap_b.append_value(row.tap);
        phase_b.append_value(row.phase);
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
        if let Some(owner_id) = row.owner_id {
            owner_id_b.append_value(owner_id);
        } else {
            owner_id_b.append_null();
        }
        name_b.append(row.name.as_ref())?;
        if let Some(from_nominal_kv) = row.from_nominal_kv {
            from_nominal_kv_b.append_value(from_nominal_kv);
        } else {
            from_nominal_kv_b.append_null();
        }
        if let Some(to_nominal_kv) = row.to_nominal_kv {
            to_nominal_kv_b.append_value(to_nominal_kv);
        } else {
            to_nominal_kv_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(branch_id_b.finish()) as ArrayRef,
        Arc::new(from_bus_id_b.finish()) as ArrayRef,
        Arc::new(to_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_b.finish()) as ArrayRef,
        Arc::new(x_b.finish()) as ArrayRef,
        Arc::new(b_shunt_b.finish()) as ArrayRef,
        Arc::new(tap_b.finish()) as ArrayRef,
        Arc::new(phase_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(owner_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(from_nominal_kv_b.finish()) as ArrayRef,
        Arc::new(to_nominal_kv_b.finish()) as ArrayRef,
        // v0.8.6 additive FACTS columns default to null for CIM exports that do
        // not currently materialize explicit FACTS rows.
        new_null_array(schema.field(17).data_type(), rows.len()),
        new_null_array(schema.field(18).data_type(), rows.len()),
        new_null_array(schema.field(19).data_type(), rows.len()),
        new_null_array(schema.field(20).data_type(), rows.len()),
        new_null_array(schema.field(21).data_type(), rows.len()),
        new_null_array(schema.field(22).data_type(), rows.len()),
        new_null_array(schema.field(23).data_type(), rows.len()),
        new_null_array(schema.field(24).data_type(), rows.len()),
        new_null_array(schema.field(25).data_type(), rows.len()),
        new_null_array(schema.field(26).data_type(), rows.len()),
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build branches record batch")
}

fn build_generators_batch(rows: &[GenRow<'_>], _base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(generators_schema());

    let mut generator_id_b = Int32Builder::new();
    let mut bus_id_b = Int32Builder::new();
    let mut name_b = StringBuilder::new();
    let mut unit_type_b = StringBuilder::new();
    let mut hierarchy_level_b = StringBuilder::new();
    let mut parent_generator_id_b = Int32Builder::new();
    let mut aggregation_count_b = Int32Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut p_sched_mw_b = Float64Builder::new();
    let mut p_min_mw_b = Float64Builder::new();
    let mut p_max_mw_b = Float64Builder::new();
    let mut q_min_mvar_b = Float64Builder::new();
    let mut q_max_mvar_b = Float64Builder::new();
    let mut mbase_mva_b = Float64Builder::new();
    let mut uol_mw_b = Float64Builder::new();
    let mut lol_mw_b = Float64Builder::new();
    let mut ramp_rate_up_mw_min_b = Float64Builder::new();
    let mut ramp_rate_down_mw_min_b = Float64Builder::new();
    let mut is_ibr_b = BooleanBuilder::new();
    let mut ibr_subtype_b = StringBuilder::new();
    let mut owner_id_b = Int32Builder::new();
    let mut market_resource_id_b = StringBuilder::new();
    let mut params_b = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));

    for row in rows {
        generator_id_b.append_value(row.generator_id);
        bus_id_b.append_value(row.bus_id);
        name_b.append_value(row.name.as_ref());
        unit_type_b.append_value(row.unit_type.as_ref());
        hierarchy_level_b.append_value(row.hierarchy_level.as_ref());
        if let Some(parent_generator_id) = row.parent_generator_id {
            parent_generator_id_b.append_value(parent_generator_id);
        } else {
            parent_generator_id_b.append_null();
        }
        if let Some(aggregation_count) = row.aggregation_count {
            aggregation_count_b.append_value(aggregation_count);
        } else {
            aggregation_count_b.append_null();
        }
        status_b.append_value(row.status);
        p_sched_mw_b.append_value(row.p_sched_mw);
        p_min_mw_b.append_value(row.p_min_mw);
        p_max_mw_b.append_value(row.p_max_mw);
        q_min_mvar_b.append_value(row.q_min_mvar);
        q_max_mvar_b.append_value(row.q_max_mvar);
        mbase_mva_b.append_value(row.mbase_mva);
        if let Some(uol_mw) = row.uol_mw {
            uol_mw_b.append_value(uol_mw);
        } else {
            uol_mw_b.append_null();
        }
        if let Some(lol_mw) = row.lol_mw {
            lol_mw_b.append_value(lol_mw);
        } else {
            lol_mw_b.append_null();
        }
        if let Some(ramp_rate_up_mw_min) = row.ramp_rate_up_mw_min {
            ramp_rate_up_mw_min_b.append_value(ramp_rate_up_mw_min);
        } else {
            ramp_rate_up_mw_min_b.append_null();
        }
        if let Some(ramp_rate_down_mw_min) = row.ramp_rate_down_mw_min {
            ramp_rate_down_mw_min_b.append_value(ramp_rate_down_mw_min);
        } else {
            ramp_rate_down_mw_min_b.append_null();
        }
        is_ibr_b.append_value(row.is_ibr);
        if let Some(ibr_subtype) = row.ibr_subtype.as_deref() {
            ibr_subtype_b.append_value(ibr_subtype);
        } else {
            ibr_subtype_b.append_null();
        }
        if let Some(owner_id) = row.owner_id {
            owner_id_b.append_value(owner_id);
        } else {
            owner_id_b.append_null();
        }
        if let Some(market_resource_id) = row.market_resource_id.as_deref() {
            market_resource_id_b.append_value(market_resource_id);
        } else {
            market_resource_id_b.append_null();
        }

        if row.h > 0.0 {
            params_b.keys().append_value("H");
            params_b.values().append_value(row.h);
        }
        if row.xd_prime > 0.0 {
            params_b.keys().append_value("xd_prime");
            params_b.values().append_value(row.xd_prime);
        }
        if row.d != 0.0 {
            params_b.keys().append_value("D");
            params_b.values().append_value(row.d);
        }
        params_b
            .append(true)
            .context("failed to append generators.params map row")?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(generator_id_b.finish()) as ArrayRef,
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(unit_type_b.finish()) as ArrayRef,
        Arc::new(hierarchy_level_b.finish()) as ArrayRef,
        Arc::new(parent_generator_id_b.finish()) as ArrayRef,
        Arc::new(aggregation_count_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(is_ibr_b.finish()) as ArrayRef,
        Arc::new(ibr_subtype_b.finish()) as ArrayRef,
        Arc::new(p_sched_mw_b.finish()) as ArrayRef,
        Arc::new(p_min_mw_b.finish()) as ArrayRef,
        Arc::new(p_max_mw_b.finish()) as ArrayRef,
        Arc::new(q_min_mvar_b.finish()) as ArrayRef,
        Arc::new(q_max_mvar_b.finish()) as ArrayRef,
        Arc::new(mbase_mva_b.finish()) as ArrayRef,
        Arc::new(uol_mw_b.finish()) as ArrayRef,
        Arc::new(lol_mw_b.finish()) as ArrayRef,
        Arc::new(ramp_rate_up_mw_min_b.finish()) as ArrayRef,
        Arc::new(ramp_rate_down_mw_min_b.finish()) as ArrayRef,
        Arc::new(owner_id_b.finish()) as ArrayRef,
        Arc::new(market_resource_id_b.finish()) as ArrayRef,
        Arc::new(params_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build generators record batch")
}

/// Builds the `loads` table batch from EQ `EnergyConsumer` joins.
///
/// Tenet 1: preserves borrowed IDs until Arrow append points.
/// Tenet 2: writes exact locked v0.5 `loads` schema ordering.
fn build_loads_batch(rows: &[LoadRow<'_>], base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(loads_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut status_b = BooleanBuilder::new();
    let mut p_pu_b = Float64Builder::new();
    let mut q_pu_b = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_ref())?;
        status_b.append_value(row.status);
        p_pu_b.append_value(row.p_mw / base_mva);
        q_pu_b.append_value(row.q_mvar / base_mva);
        name_b.append(row.name.as_ref())?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(p_pu_b.finish()) as ArrayRef,
        Arc::new(q_pu_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build loads record batch")
}

/// Builds the `transformers_2w` table batch from resolved EQ transformer rows.
///
/// Tenet 2: preserves exact v0.5 schema ordering and primitive types.
fn build_transformers_2w_batch(rows: &[Transformer2WRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(transformers_2w_schema());

    let mut from_bus_id_b = Int32Builder::new();
    let mut to_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_b = Float64Builder::new();
    let mut x_b = Float64Builder::new();
    let mut winding1_r_b = Float64Builder::new();
    let mut winding1_x_b = Float64Builder::new();
    let mut winding2_r_b = Float64Builder::new();
    let mut winding2_x_b = Float64Builder::new();
    let mut g_b = Float64Builder::new();
    let mut b_b = Float64Builder::new();
    let mut tap_ratio_b = Float64Builder::new();
    let mut nominal_tap_ratio_b = Float64Builder::new();
    let mut phase_shift_b = Float64Builder::new();
    let mut vector_group_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut from_nominal_kv_b = Float64Builder::new();
    let mut to_nominal_kv_b = Float64Builder::new();

    for row in rows {
        from_bus_id_b.append_value(row.from_bus_id);
        to_bus_id_b.append_value(row.to_bus_id);
        ckt_b.append(row.ckt.as_ref())?;
        r_b.append_value(row.r);
        x_b.append_value(row.x);
        winding1_r_b.append_value(row.winding1_r);
        winding1_x_b.append_value(row.winding1_x);
        winding2_r_b.append_value(row.winding2_r);
        winding2_x_b.append_value(row.winding2_x);
        g_b.append_value(row.g);
        b_b.append_value(row.b);
        tap_ratio_b.append_value(row.tap_ratio);
        nominal_tap_ratio_b.append_value(row.nominal_tap_ratio);
        phase_shift_b.append_value(row.phase_shift);
        vector_group_b.append(row.vector_group.as_ref())?;
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
        name_b.append(row.name.as_ref())?;
        if let Some(from_nominal_kv) = row.from_nominal_kv {
            from_nominal_kv_b.append_value(from_nominal_kv);
        } else {
            from_nominal_kv_b.append_null();
        }
        if let Some(to_nominal_kv) = row.to_nominal_kv {
            to_nominal_kv_b.append_value(to_nominal_kv);
        } else {
            to_nominal_kv_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(from_bus_id_b.finish()) as ArrayRef,
        Arc::new(to_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_b.finish()) as ArrayRef,
        Arc::new(x_b.finish()) as ArrayRef,
        Arc::new(winding1_r_b.finish()) as ArrayRef,
        Arc::new(winding1_x_b.finish()) as ArrayRef,
        Arc::new(winding2_r_b.finish()) as ArrayRef,
        Arc::new(winding2_x_b.finish()) as ArrayRef,
        Arc::new(g_b.finish()) as ArrayRef,
        Arc::new(b_b.finish()) as ArrayRef,
        Arc::new(tap_ratio_b.finish()) as ArrayRef,
        Arc::new(nominal_tap_ratio_b.finish()) as ArrayRef,
        Arc::new(phase_shift_b.finish()) as ArrayRef,
        Arc::new(vector_group_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(from_nominal_kv_b.finish()) as ArrayRef,
        Arc::new(to_nominal_kv_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build transformers_2w record batch")
}

/// Builds the `transformers_3w` table batch from resolved EQ transformer rows.
///
/// Tenet 2: preserves exact v0.5 schema ordering and primitive types.
fn build_transformers_3w_batch(rows: &[Transformer3WRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(transformers_3w_schema());

    let mut bus_h_id_b = Int32Builder::new();
    let mut bus_m_id_b = Int32Builder::new();
    let mut bus_l_id_b = Int32Builder::new();
    let mut star_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_hm_b = Float64Builder::new();
    let mut x_hm_b = Float64Builder::new();
    let mut r_hl_b = Float64Builder::new();
    let mut x_hl_b = Float64Builder::new();
    let mut r_ml_b = Float64Builder::new();
    let mut x_ml_b = Float64Builder::new();
    let mut tap_h_b = Float64Builder::new();
    let mut tap_m_b = Float64Builder::new();
    let mut tap_l_b = Float64Builder::new();
    let mut phase_shift_b = Float64Builder::new();
    let mut vector_group_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut nominal_kv_h_b = Float64Builder::new();
    let mut nominal_kv_m_b = Float64Builder::new();
    let mut nominal_kv_l_b = Float64Builder::new();

    for row in rows {
        bus_h_id_b.append_value(row.bus_h_id);
        bus_m_id_b.append_value(row.bus_m_id);
        bus_l_id_b.append_value(row.bus_l_id);
        star_bus_id_b.append_null();
        ckt_b.append(row.ckt.as_ref())?;
        r_hm_b.append_value(row.r_hm);
        x_hm_b.append_value(row.x_hm);
        r_hl_b.append_value(row.r_hl);
        x_hl_b.append_value(row.x_hl);
        r_ml_b.append_value(row.r_ml);
        x_ml_b.append_value(row.x_ml);
        tap_h_b.append_value(row.tap_h);
        tap_m_b.append_value(row.tap_m);
        tap_l_b.append_value(row.tap_l);
        phase_shift_b.append_value(row.phase_shift);
        vector_group_b.append(row.vector_group.as_ref())?;
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
        name_b.append(row.name.as_ref())?;
        if let Some(nominal_kv_h) = row.nominal_kv_h {
            nominal_kv_h_b.append_value(nominal_kv_h);
        } else {
            nominal_kv_h_b.append_null();
        }
        if let Some(nominal_kv_m) = row.nominal_kv_m {
            nominal_kv_m_b.append_value(nominal_kv_m);
        } else {
            nominal_kv_m_b.append_null();
        }
        if let Some(nominal_kv_l) = row.nominal_kv_l {
            nominal_kv_l_b.append_value(nominal_kv_l);
        } else {
            nominal_kv_l_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_h_id_b.finish()) as ArrayRef,
        Arc::new(bus_m_id_b.finish()) as ArrayRef,
        Arc::new(bus_l_id_b.finish()) as ArrayRef,
        Arc::new(star_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_hm_b.finish()) as ArrayRef,
        Arc::new(x_hm_b.finish()) as ArrayRef,
        Arc::new(r_hl_b.finish()) as ArrayRef,
        Arc::new(x_hl_b.finish()) as ArrayRef,
        Arc::new(r_ml_b.finish()) as ArrayRef,
        Arc::new(x_ml_b.finish()) as ArrayRef,
        Arc::new(tap_h_b.finish()) as ArrayRef,
        Arc::new(tap_m_b.finish()) as ArrayRef,
        Arc::new(tap_l_b.finish()) as ArrayRef,
        Arc::new(phase_shift_b.finish()) as ArrayRef,
        Arc::new(vector_group_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(nominal_kv_h_b.finish()) as ArrayRef,
        Arc::new(nominal_kv_m_b.finish()) as ArrayRef,
        Arc::new(nominal_kv_l_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build transformers_3w record batch")
}

fn build_areas_batch(rows: &[AreaRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(areas_schema());

    let mut area_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut interchange_mw_b = Float64Builder::new();

    for row in rows {
        area_id_b.append_value(row.area_id);
        name_b.append(row.name.as_ref())?;
        if let Some(interchange_mw) = row.interchange_mw {
            interchange_mw_b.append_value(interchange_mw);
        } else {
            interchange_mw_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(area_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(interchange_mw_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build areas record batch")
}

fn build_zones_batch(rows: &[ZoneRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(zones_schema());

    let mut zone_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        zone_id_b.append_value(row.zone_id);
        name_b.append(row.name.as_ref())?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(zone_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build zones record batch")
}

fn build_owners_batch(rows: &[OwnerRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(owners_schema());

    let mut owner_id_b = Int32Builder::new();
    let mut name_b = StringBuilder::new();
    let mut short_name_b = StringBuilder::new();
    let mut owner_type_b = StringBuilder::new();

    for row in rows {
        owner_id_b.append_value(row.owner_id);
        name_b.append_value(row.name.as_ref());
        if let Some(short_name) = row.short_name.as_deref() {
            short_name_b.append_value(short_name);
        } else {
            short_name_b.append_null();
        }
        if let Some(owner_type) = row.owner_type.as_deref() {
            owner_type_b.append_value(owner_type);
        } else {
            owner_type_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(owner_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(short_name_b.finish()) as ArrayRef,
        Arc::new(owner_type_b.finish()) as ArrayRef,
        new_null_array(schema.field(4).data_type(), rows.len()),
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build owners record batch")
}

fn build_fixed_shunts_batch(rows: &[FixedShuntRow<'_>], base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(fixed_shunts_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut status_b = BooleanBuilder::new();
    let mut g_pu_b = Float64Builder::new();
    let mut b_pu_b = Float64Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_ref())?;
        status_b.append_value(row.status);
        g_pu_b.append_value(row.g_mw / base_mva);
        b_pu_b.append_value(row.b_mvar / base_mva);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(g_pu_b.finish()) as ArrayRef,
        Arc::new(b_pu_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build fixed_shunts record batch")
}

/// Builds the `switched_shunts` table batch from `SvShuntCompensator` joins.
///
/// Tenet 1: appends list values directly from parsed row vectors.
/// Tenet 2: preserves locked v0.5 switched_shunts schema ordering and types.
fn build_switched_shunts_batch(rows: &[SwitchedShuntRow]) -> Result<RecordBatch> {
    let schema = Arc::new(switched_shunts_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut v_low_b = Float64Builder::new();
    let mut v_high_b = Float64Builder::new();
    let list_field = schema.field(4).data_type().clone();
    let mut b_steps_b = ListBuilder::new(Float64Builder::new()).with_field(match list_field {
        DataType::List(field) => field,
        _ => Arc::new(Field::new("item", DataType::Float64, false)),
    });
    let mut current_step_b = Int32Builder::new();
    let mut b_init_pu_b = Float64Builder::new();
    let mut shunt_id_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        status_b.append_value(row.status);
        v_low_b.append_value(row.v_low);
        v_high_b.append_value(row.v_high);

        for value in &row.b_steps {
            b_steps_b.values().append_value(*value);
        }
        b_steps_b.append(true);

        current_step_b.append_value(row.current_step.max(0));
        b_init_pu_b.append_value(row.b_init_pu);
        match row.shunt_id.as_deref() {
            Some(id) => {
                shunt_id_b.append(id)?;
            }
            None => shunt_id_b.append_null(),
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(v_low_b.finish()) as ArrayRef,
        Arc::new(v_high_b.finish()) as ArrayRef,
        Arc::new(b_steps_b.finish()) as ArrayRef,
        Arc::new(current_step_b.finish()) as ArrayRef,
        Arc::new(b_init_pu_b.finish()) as ArrayRef,
        Arc::new(shunt_id_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build switched_shunts record batch")
}

fn build_switched_shunt_banks_batch(
    rows: &[SwitchedShuntBankRow],
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema = Arc::new(switched_shunt_banks_schema());

    let mut shunt_id_b = Int32Builder::new();
    let mut bank_id_b = Int32Builder::new();
    let mut b_mvar_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut step_b = Int32Builder::new();

    for row in rows {
        shunt_id_b.append_value(row.shunt_id);
        bank_id_b.append_value(row.bank_id);
        b_mvar_b.append_value(row.b_pu * base_mva);
        status_b.append_value(row.status);
        step_b.append_value(row.step);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(shunt_id_b.finish()) as ArrayRef,
        Arc::new(bank_id_b.finish()) as ArrayRef,
        Arc::new(b_mvar_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(step_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays)
        .context("failed to build switched_shunt_banks record batch")
}

fn build_connectivity_groups_batch(rows: &[ConnectivityGroupRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(connectivity_groups_schema());

    let mut topological_bus_id_b = Int32Builder::new();
    let mut topological_node_mrid_b = StringDictionaryBuilder::<Int32Type>::new();
    let list_field = schema.field(2).data_type().clone();
    let mut connectivity_node_mrids_b =
        ListBuilder::new(StringBuilder::new()).with_field(match list_field {
            DataType::List(field) => field,
            _ => Arc::new(Field::new("item", DataType::Utf8, false)),
        });
    let mut connectivity_count_b = Int32Builder::new();

    for row in rows {
        topological_bus_id_b.append_value(row.topological_bus_id);
        topological_node_mrid_b.append(row.topological_node_mrid.as_ref())?;

        for connectivity_node_mrid in &row.connectivity_node_mrids {
            connectivity_node_mrids_b
                .values()
                .append_value(connectivity_node_mrid.as_ref());
        }
        connectivity_node_mrids_b.append(true);

        connectivity_count_b.append_value(row.connectivity_count);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(topological_bus_id_b.finish()) as ArrayRef,
        Arc::new(topological_node_mrid_b.finish()) as ArrayRef,
        Arc::new(connectivity_node_mrids_b.finish()) as ArrayRef,
        Arc::new(connectivity_count_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build connectivity_groups record batch")
}

fn build_node_breaker_detail_batch(rows: &[NodeBreakerDetailRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(node_breaker_detail_schema());

    let mut switch_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut switch_type_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut from_bus_id_b = Int32Builder::new();
    let mut to_bus_id_b = Int32Builder::new();
    let mut connectivity_node_a_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut connectivity_node_b_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut is_open_b = BooleanBuilder::new();
    let mut normal_open_b = BooleanBuilder::new();
    let mut status_b = BooleanBuilder::new();

    for row in rows {
        switch_id_b.append(row.switch_id.as_ref())?;
        switch_type_b.append(row.switch_type.as_ref())?;

        if let Some(value) = row.from_bus_id {
            from_bus_id_b.append_value(value);
        } else {
            from_bus_id_b.append_null();
        }
        if let Some(value) = row.to_bus_id {
            to_bus_id_b.append_value(value);
        } else {
            to_bus_id_b.append_null();
        }

        if let Some(value) = &row.connectivity_node_a {
            connectivity_node_a_b.append(value.as_ref())?;
        } else {
            connectivity_node_a_b.append_null();
        }
        if let Some(value) = &row.connectivity_node_b {
            connectivity_node_b_b.append(value.as_ref())?;
        } else {
            connectivity_node_b_b.append_null();
        }

        if let Some(value) = row.is_open {
            is_open_b.append_value(value);
        } else {
            is_open_b.append_null();
        }
        if let Some(value) = row.normal_open {
            normal_open_b.append_value(value);
        } else {
            normal_open_b.append_null();
        }
        if let Some(value) = row.status {
            status_b.append_value(value);
        } else {
            status_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(switch_id_b.finish()) as ArrayRef,
        Arc::new(switch_type_b.finish()) as ArrayRef,
        Arc::new(from_bus_id_b.finish()) as ArrayRef,
        Arc::new(to_bus_id_b.finish()) as ArrayRef,
        Arc::new(connectivity_node_a_b.finish()) as ArrayRef,
        Arc::new(connectivity_node_b_b.finish()) as ArrayRef,
        Arc::new(is_open_b.finish()) as ArrayRef,
        Arc::new(normal_open_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build node_breaker_detail record batch")
}

fn build_switch_detail_batch(rows: &[SwitchDetailRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(switch_detail_schema());

    let mut switch_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut switch_type_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut is_open_b = BooleanBuilder::new();
    let mut normal_open_b = BooleanBuilder::new();
    let mut retained_b = BooleanBuilder::new();

    for row in rows {
        switch_id_b.append(row.switch_id.as_ref())?;
        if let Some(name) = &row.name {
            name_b.append(name.as_ref())?;
        } else {
            name_b.append_null();
        }
        switch_type_b.append(row.switch_type.as_ref())?;

        if let Some(value) = row.is_open {
            is_open_b.append_value(value);
        } else {
            is_open_b.append_null();
        }
        if let Some(value) = row.normal_open {
            normal_open_b.append_value(value);
        } else {
            normal_open_b.append_null();
        }
        if let Some(value) = row.retained {
            retained_b.append_value(value);
        } else {
            retained_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(switch_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(switch_type_b.finish()) as ArrayRef,
        Arc::new(is_open_b.finish()) as ArrayRef,
        Arc::new(normal_open_b.finish()) as ArrayRef,
        Arc::new(retained_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build switch_detail record batch")
}

fn build_connectivity_nodes_batch(rows: &[ConnectivityNodeDetailRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(connectivity_nodes_schema());

    let mut connectivity_node_mrid_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut topological_node_mrid_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut bus_id_b = Int32Builder::new();

    for row in rows {
        connectivity_node_mrid_b.append(row.connectivity_node_mrid.as_ref())?;
        if let Some(topological) = &row.topological_node_mrid {
            topological_node_mrid_b.append(topological.as_ref())?;
        } else {
            topological_node_mrid_b.append_null();
        }

        if let Some(bus_id) = row.bus_id {
            bus_id_b.append_value(bus_id);
        } else {
            bus_id_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(connectivity_node_mrid_b.finish()) as ArrayRef,
        Arc::new(topological_node_mrid_b.finish()) as ArrayRef,
        Arc::new(bus_id_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build connectivity_nodes record batch")
}

fn build_diagram_objects_batch(rows: &[DiagramObjectRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(diagram_objects_schema());

    let mut element_id_b = StringBuilder::new();
    let mut element_type_b = StringBuilder::new();
    let mut diagram_id_b = StringBuilder::new();
    let mut rotation_b = Float32Builder::new();
    let mut visible_b = BooleanBuilder::new();
    let mut draw_order_b = Int32Builder::new();

    for row in rows {
        element_id_b.append_value(row.element_id.as_ref());
        element_type_b.append_value(row.element_type.as_ref());
        diagram_id_b.append_value(row.diagram_id.as_ref());

        if let Some(rotation) = row.rotation {
            rotation_b.append_value(rotation);
        } else {
            rotation_b.append_null();
        }

        visible_b.append_value(row.visible);

        if let Some(draw_order) = row.draw_order {
            draw_order_b.append_value(draw_order);
        } else {
            draw_order_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(element_id_b.finish()) as ArrayRef,
        Arc::new(element_type_b.finish()) as ArrayRef,
        Arc::new(diagram_id_b.finish()) as ArrayRef,
        Arc::new(rotation_b.finish()) as ArrayRef,
        Arc::new(visible_b.finish()) as ArrayRef,
        Arc::new(draw_order_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build diagram_objects record batch")
}

fn build_diagram_points_batch(rows: &[DiagramPointRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(diagram_points_schema());

    let mut element_id_b = StringBuilder::new();
    let mut diagram_id_b = StringBuilder::new();
    let mut seq_b = Int32Builder::new();
    let mut x_b = Float64Builder::new();
    let mut y_b = Float64Builder::new();

    for row in rows {
        element_id_b.append_value(row.element_id.as_ref());
        diagram_id_b.append_value(row.diagram_id.as_ref());
        seq_b.append_value(row.seq);
        x_b.append_value(row.x);
        y_b.append_value(row.y);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(element_id_b.finish()) as ArrayRef,
        Arc::new(diagram_id_b.finish()) as ArrayRef,
        Arc::new(seq_b.finish()) as ArrayRef,
        Arc::new(x_b.finish()) as ArrayRef,
        Arc::new(y_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build diagram_points record batch")
}

/// Builds the `contingencies` table batch with list-of-struct payloads.
///
/// Tenet 2: this uses the exact locked `contingencies_schema()` ordering and
/// nested field layout.
fn build_contingencies_batch(rows: &[ContingencyRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(contingencies_schema());

    let mut contingency_id_b = StringDictionaryBuilder::<Int32Type>::new();

    let element_fields = vec![
        Field::new(
            "element_type",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("branch_id", DataType::Int32, true),
        Field::new("bus_id", DataType::Int32, true),
        Field::new(
            "gen_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "load_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("amount_mw", DataType::Float64, true),
        Field::new("status_change", DataType::Boolean, false),
        Field::new(
            "equipment_kind",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "equipment_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ];
    let element_field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(Int32Builder::new()),
        Box::new(Int32Builder::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(Float64Builder::new()),
        Box::new(BooleanBuilder::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
    ];
    let element_struct_b = StructBuilder::new(element_fields, element_field_builders);
    let elements_field = match schema.field(1).data_type() {
        DataType::List(field) => field.clone(),
        other => {
            bail!("contingencies.elements field must be List<Struct>, found {other:?}")
        }
    };
    let mut elements_b = ListBuilder::new(element_struct_b).with_field(elements_field);

    for row in rows {
        contingency_id_b.append(row.contingency_id.as_ref())?;

        for element in &row.elements {
            let struct_b = elements_b.values();

            struct_b
                .field_builder::<StringDictionaryBuilder<Int32Type>>(0)
                .context("missing element_type builder")?
                .append(element.element_type.as_ref())?;

            if let Some(branch_id) = element.branch_id {
                struct_b
                    .field_builder::<Int32Builder>(1)
                    .context("missing branch_id builder")?
                    .append_value(branch_id);
            } else {
                struct_b
                    .field_builder::<Int32Builder>(1)
                    .context("missing branch_id builder")?
                    .append_null();
            }

            if let Some(bus_id) = element.bus_id {
                struct_b
                    .field_builder::<Int32Builder>(2)
                    .context("missing bus_id builder")?
                    .append_value(bus_id);
            } else {
                struct_b
                    .field_builder::<Int32Builder>(2)
                    .context("missing bus_id builder")?
                    .append_null();
            }

            struct_b
                .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                .context("missing gen_id builder")?
                .append_null();
            struct_b
                .field_builder::<StringDictionaryBuilder<Int32Type>>(4)
                .context("missing load_id builder")?
                .append_null();
            struct_b
                .field_builder::<Float64Builder>(5)
                .context("missing amount_mw builder")?
                .append_null();
            struct_b
                .field_builder::<BooleanBuilder>(6)
                .context("missing status_change builder")?
                .append_value(element.status_change);

            if let Some(equipment_kind) = &element.equipment_kind {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(7)
                    .context("missing equipment_kind builder")?
                    .append(equipment_kind.as_ref())?;
            } else {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(7)
                    .context("missing equipment_kind builder")?
                    .append_null();
            }

            if let Some(equipment_id) = &element.equipment_id {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(8)
                    .context("missing equipment_id builder")?
                    .append(equipment_id.as_ref())?;
            } else {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(8)
                    .context("missing equipment_id builder")?
                    .append_null();
            }
            struct_b.append(true);
        }

        elements_b.append(true);
    }

    let n = rows.len();

    // v0.9.0: 6 nullable operational-outcome columns — null in planning/stub files.
    let risk_score_arr = new_null_array(&DataType::Float64, n);
    let cleared_by_reserves_arr = new_null_array(&DataType::Boolean, n);
    let voltage_collapse_flag_arr = new_null_array(&DataType::Boolean, n);
    let recovery_possible_arr = new_null_array(&DataType::Boolean, n);
    let recovery_time_min_arr = new_null_array(&DataType::Float64, n);
    let greedy_reserve_summary_arr = new_null_array(&DataType::Utf8, n);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(contingency_id_b.finish()) as ArrayRef,
        Arc::new(elements_b.finish()) as ArrayRef,
        risk_score_arr,
        cleared_by_reserves_arr,
        voltage_collapse_flag_arr,
        recovery_possible_arr,
        recovery_time_min_arr,
        greedy_reserve_summary_arr,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build contingencies record batch")
}

/// Builds the `dynamics_models` table batch with map-typed parameter payloads.
///
/// Tenet 3: this is currently stubbed ingestion data only; no dynamic model
/// solving behavior exists here.
fn build_dynamics_models_batch(rows: &[DynamicsModelRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(dynamics_models_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut gen_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut model_type_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut params_b = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        gen_id_b.append(row.gen_id.as_ref())?;
        model_type_b.append(row.model_type.as_ref())?;

        for (key, value) in &row.params {
            params_b.keys().append_value(key.as_ref());
            params_b.values().append_value(*value);
        }
        params_b
            .append(true)
            .context("failed to append params map row")?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(gen_id_b.finish()) as ArrayRef,
        Arc::new(model_type_b.finish()) as ArrayRef,
        Arc::new(params_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build dynamics_models record batch")
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::fs;
    use std::time::Instant;

    use anyhow::{Context, Result};
    use arrow::array::{
        Array, DictionaryArray, Float64Array, Int32Array, ListArray, MapArray, StringArray,
        StructArray,
    };
    use arrow::datatypes::Schema;
    use arrow::datatypes::{Int32Type, UInt32Type};

    use crate::arrow_schema::{
        BRANDING, SCHEMA_VERSION, all_table_schemas, branches_schema, buses_schema,
        connectivity_nodes_schema, contingencies_schema, diagram_objects_schema,
        diagram_points_schema, dynamics_models_schema, metadata_schema, node_breaker_detail_schema,
        switch_detail_schema,
    };

    use super::{
        BusResolutionMode, DetachedIslandPolicy, GenRow, Transformer2WRow, Transformer3WRow,
        TransformerRepresentationMode, WriteOptions, infer_dynamics_model_type,
        normalize_transformer_representation, read_rpf_tables, rpf_file_metadata,
        star_expand_3w_transformers, summarize_rpf, validate_transformer_representation_mode,
        write_complete_rpf, write_complete_rpf_with_options,
    };

    fn assert_schema_shape_matches(actual: &Schema, expected: &Schema) {
        assert_eq!(actual.fields().len(), expected.fields().len());
        for idx in 0..actual.fields().len() {
            let actual_field = actual.field(idx);
            let expected_field = expected.field(idx);
            assert_eq!(actual_field.name(), expected_field.name());
            assert_eq!(actual_field.data_type(), expected_field.data_type());
            assert_eq!(actual_field.is_nullable(), expected_field.is_nullable());
        }
    }

    fn generate_eq_fixture_branch_count(n_branches: usize) -> String {
        let mut xml = String::from(
            r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">"#,
        );

        for idx in 0..n_branches {
            let line_id = format!("Line{idx}");
            let from_node = format!("Node{}", idx * 2 + 1);
            let to_node = format!("Node{}", idx * 2 + 2);
            xml.push_str(&format!(
                r##"<cim:ACLineSegment rdf:ID="{line_id}"><IdentifiedObject.name>Line {idx}</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x><ACLineSegment.bch>0.001</ACLineSegment.bch></cim:ACLineSegment><cim:Terminal rdf:ID="T{idx}_1"><Terminal.ConductingEquipment rdf:resource="#{line_id}"/><Terminal.ConnectivityNode rdf:resource="#{from_node}"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal><cim:Terminal rdf:ID="T{idx}_2"><Terminal.ConductingEquipment rdf:resource="#{line_id}"/><Terminal.ConnectivityNode rdf:resource="#{to_node}"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>"##
            ));
        }

        xml.push_str("</rdf:RDF>");
        xml
    }

    fn generate_eq_fixture_with_breaker() -> String {
        String::from(
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:ConnectivityNode rdf:ID="N1" />
<cim:ConnectivityNode rdf:ID="N2" />
<cim:ACLineSegment rdf:ID="L1"><IdentifiedObject.name>Line 1</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x><ACLineSegment.bch>0.001</ACLineSegment.bch></cim:ACLineSegment>
<cim:Terminal rdf:ID="LT1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="LT2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Breaker rdf:ID="BR1"><IdentifiedObject.name>Breaker 1</IdentifiedObject.name><Switch.open>false</Switch.open><Switch.normalOpen>false</Switch.normalOpen><Switch.retained>true</Switch.retained></cim:Breaker>
<cim:Terminal rdf:ID="BT1"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="BT2"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##,
        )
    }

    fn generate_eq_fixture_with_base_voltage_fallbacks() -> String {
        String::from(
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:BaseVoltage rdf:ID="BV230"><BaseVoltage.nominalVoltage>230</BaseVoltage.nominalVoltage></cim:BaseVoltage>
<cim:ACLineSegment rdf:ID="L1"><ConductingEquipment.BaseVoltage rdf:resource="#BV230"/><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x></cim:ACLineSegment>
<cim:Terminal rdf:ID="T1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="T2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##,
        )
    }

    fn generate_eq_fixture_with_generator_dynamics() -> String {
        String::from(
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:ACLineSegment rdf:ID="L1"><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x></cim:ACLineSegment>
<cim:Terminal rdf:ID="LT1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="LT2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:SynchronousMachine rdf:ID="G1"><RotatingMachine.ratedS>150.0</RotatingMachine.ratedS><SynchronousMachine.H>4.5</SynchronousMachine.H><SynchronousMachine.xdPrime>0.25</SynchronousMachine.xdPrime><SynchronousMachine.D>1.2</SynchronousMachine.D></cim:SynchronousMachine>
<cim:Terminal rdf:ID="GT1"><Terminal.ConductingEquipment rdf:resource="#G1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##,
        )
    }

    fn generate_eq_fixture_with_diagram_layout() -> String {
        String::from(
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:ConnectivityNode rdf:ID="N1" />
<cim:ConnectivityNode rdf:ID="N2" />
<cim:ACLineSegment rdf:ID="L1"><IdentifiedObject.name>Line 1</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x><ACLineSegment.bch>0.001</ACLineSegment.bch></cim:ACLineSegment>
<cim:Terminal rdf:ID="LT1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="LT2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Breaker rdf:ID="BR1"><IdentifiedObject.name>Breaker 1</IdentifiedObject.name><Switch.open>false</Switch.open><Switch.normalOpen>false</Switch.normalOpen><Switch.retained>true</Switch.retained></cim:Breaker>
<cim:Terminal rdf:ID="BT1"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="BT2"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Diagram rdf:ID="D1"><IdentifiedObject.name>overview</IdentifiedObject.name></cim:Diagram>
<cim:DiagramObject rdf:ID="DO1"><DiagramObject.Diagram rdf:resource="#D1"/><DiagramObject.IdentifiedObject rdf:resource="#L1"/><DiagramObject.rotation>0.0</DiagramObject.rotation><DiagramObject.drawingOrder>1</DiagramObject.drawingOrder></cim:DiagramObject>
<cim:DiagramObject rdf:ID="DO2"><DiagramObject.Diagram rdf:resource="#D1"/><DiagramObject.IdentifiedObject rdf:resource="#BR1"/><DiagramObject.rotation>90.0</DiagramObject.rotation><DiagramObject.drawingOrder>2</DiagramObject.drawingOrder></cim:DiagramObject>
<cim:DiagramObjectPoint rdf:ID="P1"><DiagramObjectPoint.DiagramObject rdf:resource="#DO1"/><DiagramObjectPoint.sequenceNumber>0</DiagramObjectPoint.sequenceNumber><DiagramObjectPoint.xPosition>10.0</DiagramObjectPoint.xPosition><DiagramObjectPoint.yPosition>20.0</DiagramObjectPoint.yPosition></cim:DiagramObjectPoint>
<cim:DiagramObjectPoint rdf:ID="P2"><DiagramObjectPoint.DiagramObject rdf:resource="#DO1"/><DiagramObjectPoint.sequenceNumber>1</DiagramObjectPoint.sequenceNumber><DiagramObjectPoint.xPosition>40.0</DiagramObjectPoint.xPosition><DiagramObjectPoint.yPosition>20.0</DiagramObjectPoint.yPosition></cim:DiagramObjectPoint>
<cim:DiagramObjectPoint rdf:ID="P3"><DiagramObjectPoint.DiagramObject rdf:resource="#DO2"/><DiagramObjectPoint.sequenceNumber>0</DiagramObjectPoint.sequenceNumber><DiagramObjectPoint.xPosition>22.0</DiagramObjectPoint.xPosition><DiagramObjectPoint.yPosition>18.0</DiagramObjectPoint.yPosition></cim:DiagramObjectPoint>
<cim:DiagramObjectPoint rdf:ID="P4"><DiagramObjectPoint.DiagramObject rdf:resource="#DO2"/><DiagramObjectPoint.sequenceNumber>1</DiagramObjectPoint.sequenceNumber><DiagramObjectPoint.xPosition>28.0</DiagramObjectPoint.xPosition><DiagramObjectPoint.yPosition>18.0</DiagramObjectPoint.yPosition></cim:DiagramObjectPoint>
</rdf:RDF>"##,
        )
    }

    #[test]
    fn summarize_rpf_reports_expected_counts() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_summary_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("summary_fixture_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_branch_count(2))?;

        let output_path = tmp_dir.join("summary_fixture.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let summary = summarize_rpf(&output_path)?;
        assert!(summary.has_all_canonical_tables);
        assert_eq!(summary.tables.len(), all_table_schemas().len());
        assert_eq!(summary.total_batches, all_table_schemas().len());
        assert_eq!(summary.table_rows("branches"), Some(2));
        assert_eq!(summary.table_rows("buses"), Some(4));

        Ok(())
    }

    #[test]
    #[ignore = "skeleton test for incremental .rpf IPC validation"]
    fn write_complete_rpf_skeleton() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_writer_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("minimal_eq.xml");
        fs::write(
            &eq_path,
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
        <cim:ACLineSegment rdf:ID="L1"><IdentifiedObject.name>Line 1</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x></cim:ACLineSegment>
        <cim:Terminal rdf:ID="T1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
        <cim:Terminal rdf:ID="T2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
    </rdf:RDF>"##,
        )?;

        let output_path = tmp_dir.join("case.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let tables = read_rpf_tables(&output_path)?;
        assert_eq!(tables.len(), all_table_schemas().len());

        let expected_table_names: Vec<String> = all_table_schemas()
            .into_iter()
            .map(|(name, _)| name.to_string())
            .collect();
        let actual_table_names: Vec<String> = tables.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(actual_table_names, expected_table_names);

        // Spot-check metadata segment and required file metadata keys.
        assert_schema_shape_matches(tables[0].1.schema().as_ref(), &metadata_schema());
        let schema_ref = tables[0].1.schema();
        let meta = schema_ref.metadata();
        assert_eq!(meta.get("raptrix.branding"), Some(&BRANDING.to_string()));
        assert_eq!(
            meta.get("raptrix.version"),
            Some(&SCHEMA_VERSION.to_string())
        );
        assert_eq!(meta.get("rpf_version"), Some(&SCHEMA_VERSION.to_string()));

        assert_schema_shape_matches(tables[1].1.schema().as_ref(), &buses_schema());
        assert_schema_shape_matches(tables[2].1.schema().as_ref(), &branches_schema());
        assert_schema_shape_matches(tables[15].1.schema().as_ref(), &contingencies_schema());
        assert_schema_shape_matches(tables[17].1.schema().as_ref(), &dynamics_models_schema());

        let buses_batch = &tables[1].1;
        let bus_uuid_dict = buses_batch
            .column(19)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("buses.bus_uuid must be dictionary-encoded UTF8");
        assert_eq!(bus_uuid_dict.null_count(), 0);

        let branches_batch = &tables[2].1;
        assert_eq!(
            branches_batch.num_rows(),
            1,
            "fixture should produce one branch"
        );

        let branch_id = branches_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("branch_id must be Int32");
        let from_bus_id = branches_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("from_bus_id must be Int32");
        let to_bus_id = branches_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("to_bus_id must be Int32");
        let r = branches_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("r must be Float64");
        let x = branches_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("x must be Float64");

        assert_eq!(branch_id.value(0), 1);
        assert_eq!(from_bus_id.value(0), 1);
        assert_eq!(to_bus_id.value(0), 2);
        assert!((r.value(0) - 0.01).abs() < 1e-12);
        assert!((x.value(0) - 0.05).abs() < 1e-12);

        let contingencies_batch = &tables[12].1;
        assert!(
            contingencies_batch.num_rows() >= 1,
            "expected at least one contingency row"
        );
        let elements = contingencies_batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("contingencies.elements must be ListArray");
        assert!(
            elements.value_length(0) > 0,
            "expected first contingency to contain at least one element"
        );

        let dynamics_models_batch = &tables[14].1;
        assert_schema_shape_matches(
            dynamics_models_batch.schema().as_ref(),
            &dynamics_models_schema(),
        );
        assert!(
            dynamics_models_batch.num_rows() >= 1,
            "expected at least one dynamics model row"
        );

        let params_map = dynamics_models_batch
            .column(3)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("dynamics_models.params must be MapArray");
        assert!(
            params_map.value_length(0) > 0,
            "expected at least one params key/value pair"
        );
        let entries: &StructArray = params_map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("params key column must be Utf8");
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("params value column must be Float64");

        let mut saw_h = false;
        for index in 0..keys.len() {
            if keys.value(index) == "H" {
                assert!((values.value(index) - 5.0).abs() < 1e-12);
                saw_h = true;
                break;
            }
        }
        saw_h
            .then_some(())
            .context("expected dynamics params to contain key 'H' with value 5.0")?;

        Ok(())
    }

    #[test]
    #[ignore = "performance smoke test for RPF write/read throughput"]
    fn benchmark_write_complete_rpf_round_trip() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_perf_tests");
        fs::create_dir_all(&tmp_dir)?;

        let branch_count = 1_000usize;
        let eq_path = tmp_dir.join("perf_eq.xml");
        let output_path = tmp_dir.join("perf_case.rpf");
        fs::write(&eq_path, generate_eq_fixture_branch_count(branch_count))?;

        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        let write_start = Instant::now();
        write_complete_rpf(&[&eq_path_str], &output_path_str)?;
        let write_elapsed = write_start.elapsed();
        let write_ms = write_elapsed.as_millis();
        let file_size_bytes = fs::metadata(&output_path)?.len();
        let file_size_mib = file_size_bytes as f64 / (1024.0 * 1024.0);

        let read_start = Instant::now();
        let tables = read_rpf_tables(&output_path)?;
        let read_elapsed = read_start.elapsed();
        let read_ms = read_elapsed.as_millis();

        let branches_batch = tables
            .iter()
            .find(|(table_name, _)| table_name == "branches")
            .map(|(_, batch)| batch)
            .context("expected branches batch in perf round-trip output")?;

        assert_eq!(tables.len(), all_table_schemas().len());
        assert_eq!(branches_batch.num_rows(), branch_count);

        println!("RPF write: {} branches in {} ms", branch_count, write_ms,);
        println!("Wrote {} bytes ({:.2} MiB)", file_size_bytes, file_size_mib,);
        println!(
            "Write throughput: {:.0} branches/s",
            branch_count as f64 / write_elapsed.as_secs_f64()
        );
        println!("RPF read: {} tables in {} ms", tables.len(), read_ms,);
        println!(
            "Read throughput: {:.0} tables/s",
            tables.len() as f64 / read_elapsed.as_secs_f64()
        );

        Ok(())
    }

    #[test]
    #[ignore = "performance scaling test for 10k-branch RPF write/read throughput"]
    fn benchmark_write_complete_rpf_round_trip_10k() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_perf_tests");
        fs::create_dir_all(&tmp_dir)?;

        let branch_count = 10_000usize;
        let eq_path = tmp_dir.join("perf_eq_10k.xml");
        let output_path = tmp_dir.join("perf_case_10k.rpf");
        fs::write(&eq_path, generate_eq_fixture_branch_count(branch_count))?;

        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        let write_start = Instant::now();
        write_complete_rpf(&[&eq_path_str], &output_path_str)?;
        let write_elapsed = write_start.elapsed();
        let write_ms = write_elapsed.as_millis();
        let file_size_bytes = fs::metadata(&output_path)?.len();
        let file_size_mib = file_size_bytes as f64 / (1024.0 * 1024.0);

        let read_start = Instant::now();
        let tables = read_rpf_tables(&output_path)?;
        let read_elapsed = read_start.elapsed();
        let read_ms = read_elapsed.as_millis();

        let branches_batch = tables
            .iter()
            .find(|(table_name, _)| table_name == "branches")
            .map(|(_, batch)| batch)
            .context("expected branches batch in 10k perf round-trip output")?;

        assert_eq!(tables.len(), all_table_schemas().len());
        assert_eq!(branches_batch.num_rows(), branch_count);

        println!(
            "RPF 10k write: {} branches in {} ms",
            branch_count, write_ms,
        );
        println!("Wrote {} bytes ({:.2} MiB)", file_size_bytes, file_size_mib,);
        println!(
            "Write throughput: {:.0} branches/s",
            branch_count as f64 / write_elapsed.as_secs_f64()
        );
        println!("RPF 10k read: {} tables in {} ms", tables.len(), read_ms,);
        println!(
            "Read throughput: {:.0} tables/s",
            tables.len() as f64 / read_elapsed.as_secs_f64()
        );

        Ok(())
    }

    #[test]
    fn write_complete_rpf_with_node_breaker_tables_matches_optional_schemas() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_node_breaker_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("node_breaker_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_with_breaker())?;

        let output_path = tmp_dir.join("node_breaker_case.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf_with_options(
            &[&eq_path_str],
            &output_path_str,
            &WriteOptions {
                bus_resolution_mode: BusResolutionMode::Topological,
                detached_island_policy: DetachedIslandPolicy::Permissive,
                emit_connectivity_groups: false,
                emit_node_breaker_detail: true,
                emit_diagram_layout: true,
                contingencies_are_stub: false,
                dynamics_are_stub: false,
                base_mva: 100.0,
                frequency_hz: 60.0,
                study_name: None,
                timestamp_utc: None,
                ..Default::default()
            },
        )?;

        let tables = read_rpf_tables(&output_path)?;

        let node_breaker_detail = tables
            .iter()
            .find(|(name, _)| name == "node_breaker_detail")
            .map(|(_, batch)| batch)
            .context("expected node_breaker_detail table")?;
        assert_schema_shape_matches(
            node_breaker_detail.schema().as_ref(),
            &node_breaker_detail_schema(),
        );
        assert_eq!(node_breaker_detail.num_rows(), 1);

        let switch_detail = tables
            .iter()
            .find(|(name, _)| name == "switch_detail")
            .map(|(_, batch)| batch)
            .context("expected switch_detail table")?;
        assert_schema_shape_matches(switch_detail.schema().as_ref(), &switch_detail_schema());
        assert_eq!(switch_detail.num_rows(), 1);

        let connectivity_nodes = tables
            .iter()
            .find(|(name, _)| name == "connectivity_nodes")
            .map(|(_, batch)| batch)
            .context("expected connectivity_nodes table")?;
        assert_schema_shape_matches(
            connectivity_nodes.schema().as_ref(),
            &connectivity_nodes_schema(),
        );
        assert_eq!(connectivity_nodes.num_rows(), 2);

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get("raptrix.features.node_breaker"),
            Some(&"true".to_string())
        );
        assert_eq!(metadata.get("raptrix.features.contingencies_stub"), None);
        assert_eq!(
            metadata.get("raptrix.features.dynamics_stub"),
            Some(&"true".to_string())
        );
        assert_eq!(
            metadata.get("raptrix.version"),
            Some(&SCHEMA_VERSION.to_string())
        );
        assert_eq!(
            metadata.get("rpf.topology.island_count"),
            Some(&"1".to_string())
        );
        assert_eq!(
            metadata.get("rpf.topology.detached_islands_present"),
            Some(&"false".to_string())
        );
        assert_eq!(
            metadata.get("rpf.features.topology_only"),
            Some(&"true".to_string())
        );

        Ok(())
    }

    #[test]
    fn write_complete_rpf_emits_diagram_layout_tables_from_cim() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_diagram_layout_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("diagram_layout_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_with_diagram_layout())?;

        let output_path = tmp_dir.join("diagram_layout_case.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        let summary = write_complete_rpf_with_options(
            &[&eq_path_str],
            &output_path_str,
            &WriteOptions {
                bus_resolution_mode: BusResolutionMode::Topological,
                detached_island_policy: DetachedIslandPolicy::Permissive,
                emit_connectivity_groups: false,
                emit_node_breaker_detail: true,
                emit_diagram_layout: true,
                contingencies_are_stub: false,
                dynamics_are_stub: false,
                base_mva: 100.0,
                frequency_hz: 60.0,
                study_name: None,
                timestamp_utc: None,
                ..Default::default()
            },
        )?;

        assert_eq!(summary.diagram_object_rows, 2);
        assert_eq!(summary.diagram_point_rows, 4);

        let tables = read_rpf_tables(&output_path)?;
        let diagram_objects = tables
            .iter()
            .find(|(name, _)| name == "diagram_objects")
            .map(|(_, batch)| batch)
            .context("expected diagram_objects table")?;
        let diagram_points = tables
            .iter()
            .find(|(name, _)| name == "diagram_points")
            .map(|(_, batch)| batch)
            .context("expected diagram_points table")?;

        assert_schema_shape_matches(diagram_objects.schema().as_ref(), &diagram_objects_schema());
        assert_schema_shape_matches(diagram_points.schema().as_ref(), &diagram_points_schema());
        assert_eq!(diagram_objects.num_rows(), 2);
        assert_eq!(diagram_points.num_rows(), 4);

        let object_ids = diagram_objects
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("diagram_objects.element_id should be Utf8")?;
        let element_types = diagram_objects
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("diagram_objects.element_type should be Utf8")?;
        let diagram_ids = diagram_objects
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("diagram_objects.diagram_id should be Utf8")?;

        assert_eq!(object_ids.value(0), "branch:1");
        assert_eq!(element_types.value(0), "branch");
        assert_eq!(diagram_ids.value(0), "overview");
        assert_eq!(object_ids.value(1), "breaker:BR1");
        assert_eq!(element_types.value(1), "breaker");

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get("raptrix.features.diagram_layout"),
            Some(&"true".to_string())
        );

        Ok(())
    }

    #[test]
    fn write_complete_rpf_derives_real_dynamics_from_generators() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_dynamics_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("dynamics_fixture_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_with_generator_dynamics())?;

        let output_path = tmp_dir.join("dynamics_fixture.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let tables = read_rpf_tables(&output_path)?;
        let dynamics_batch = tables
            .iter()
            .find(|(name, _)| name == "dynamics_models")
            .map(|(_, batch)| batch)
            .context("expected dynamics_models table")?;
        assert_eq!(dynamics_batch.num_rows(), 1);

        let gen_id_dict = dynamics_batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .context("dynamics_models.gen_id should be dictionary-encoded UTF8")?;
        let gen_values = gen_id_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("dynamics_models.gen_id dictionary values should be Utf8")?;
        let gen_key = gen_id_dict
            .key(0)
            .context("missing dynamics gen_id dictionary key")? as usize;
        assert_eq!(gen_values.value(gen_key), "G1");

        let model_type_dict = dynamics_batch
            .column(2)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .context("dynamics_models.model_type should be dictionary-encoded UTF8")?;
        let model_type_values = model_type_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("dynamics_models.model_type dictionary values should be Utf8")?;
        let model_type_key = model_type_dict
            .key(0)
            .context("missing dynamics model_type dictionary key")?
            as usize;
        assert_eq!(model_type_values.value(model_type_key), "GENROU");

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(metadata.get("raptrix.features.dynamics_stub"), None);

        Ok(())
    }

    #[test]
    fn infer_dynamics_model_type_uses_conservative_rules() {
        let full = GenRow {
            generator_id: 1,
            bus_id: 1,
            id: Cow::Borrowed("G1"),
            name: Cow::Borrowed("Gen 1"),
            unit_type: Cow::Borrowed("synchronous_machine"),
            hierarchy_level: Cow::Borrowed("unit"),
            parent_generator_id: None,
            aggregation_count: None,
            p_sched_mw: 0.0,
            p_min_mw: 0.0,
            p_max_mw: 0.0,
            q_min_mvar: 0.0,
            q_max_mvar: 0.0,
            status: true,
            mbase_mva: 100.0,
            uol_mw: None,
            lol_mw: None,
            ramp_rate_up_mw_min: None,
            ramp_rate_down_mw_min: None,
            is_ibr: false,
            ibr_subtype: None,
            owner_id: None,
            market_resource_id: None,
            h: 4.0,
            xd_prime: 0.2,
            d: 0.5,
        };
        assert_eq!(infer_dynamics_model_type(&full), "GENROU");

        let inertial = GenRow {
            xd_prime: 0.0,
            ..full.clone()
        };
        assert_eq!(infer_dynamics_model_type(&inertial), "GENCLS");

        let minimal = GenRow {
            h: 0.0,
            xd_prime: 0.0,
            ..full
        };
        assert_eq!(infer_dynamics_model_type(&minimal), "SYNC_MACHINE_EQ");
    }

    #[test]
    fn write_complete_rpf_uses_base_voltage_for_fallback_names() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_base_voltage_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("base_voltage_fixture_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_with_base_voltage_fallbacks())?;

        let output_path = tmp_dir.join("base_voltage_fixture.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let tables = read_rpf_tables(&output_path)?;
        let buses_batch = tables
            .iter()
            .find(|(name, _)| name == "buses")
            .map(|(_, batch)| batch)
            .context("expected buses table")?;
        let branches_batch = tables
            .iter()
            .find(|(name, _)| name == "branches")
            .map(|(_, batch)| batch)
            .context("expected branches table")?;

        let bus_names = buses_batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .context("buses.name should be dictionary-encoded UTF8")?;
        let bus_values = bus_names
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("buses.name dictionary values should be Utf8")?;
        for idx in 0..bus_names.len() {
            let key = bus_names
                .key(idx)
                .context("missing bus name dictionary key")? as usize;
            let name = bus_values.value(key);
            assert!(
                name.contains("230kV"),
                "expected BaseVoltage-aware bus fallback name, got '{name}'"
            );
        }

        let bus_nominal_kv = buses_batch
            .column(18)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("buses.nominal_kv should be Float64")?;
        for idx in 0..bus_nominal_kv.len() {
            assert!(!bus_nominal_kv.is_null(idx));
            assert!((bus_nominal_kv.value(idx) - 230.0).abs() < 1e-12);
        }

        let branch_names = branches_batch
            .column(14)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .context("branches.name should be dictionary-encoded UTF8")?;
        let branch_values = branch_names
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("branches.name dictionary values should be Utf8")?;
        let branch_key = branch_names
            .key(0)
            .context("missing branch name dictionary key")? as usize;
        let branch_name = branch_values.value(branch_key);
        assert!(
            branch_name.contains("230 kV"),
            "expected BaseVoltage-aware branch fallback name, got '{branch_name}'"
        );

        let from_nominal_kv = branches_batch
            .column(15)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("branches.from_nominal_kv should be Float64")?;
        let to_nominal_kv = branches_batch
            .column(16)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("branches.to_nominal_kv should be Float64")?;
        assert!((from_nominal_kv.value(0) - 230.0).abs() < 1e-12);
        assert!((to_nominal_kv.value(0) - 230.0).abs() < 1e-12);

        Ok(())
    }

    #[test]
    fn write_complete_rpf_captures_switch_contingency_identity() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_contingency_identity_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("contingency_identity_fixture_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_with_breaker())?;

        let output_path = tmp_dir.join("contingency_identity_fixture.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let tables = read_rpf_tables(&output_path)?;
        let contingencies_batch = tables
            .iter()
            .find(|(name, _)| name == "contingencies")
            .map(|(_, batch)| batch)
            .context("expected contingencies table")?;

        let elements = contingencies_batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("contingencies.elements should be ListArray")?;
        let element_values = elements.value(0);
        let element_struct = element_values
            .as_any()
            .downcast_ref::<StructArray>()
            .context("contingency elements should downcast to StructArray")?;

        let equipment_kind = element_struct
            .column(7)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .context("contingencies.elements.equipment_kind should be dictionary-encoded UTF8")?;
        let equipment_kind_values = equipment_kind
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("contingencies.elements.equipment_kind values should be Utf8")?;
        let equipment_kind_key = equipment_kind
            .key(0)
            .context("missing equipment_kind dictionary key")?
            as usize;
        assert_eq!(equipment_kind_values.value(equipment_kind_key), "switch");

        let equipment_id = element_struct
            .column(8)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .context("contingencies.elements.equipment_id should be dictionary-encoded UTF8")?;
        let equipment_id_values = equipment_id
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("contingencies.elements.equipment_id values should be Utf8")?;
        let equipment_id_key = equipment_id
            .key(0)
            .context("missing equipment_id dictionary key")?
            as usize;
        assert_eq!(equipment_id_values.value(equipment_id_key), "BR1");

        Ok(())
    }

    #[test]
    fn write_complete_rpf_sets_contingencies_stub_flag_when_only_stub_rows_exist() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_contingency_stub_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("contingency_stub_fixture_eq.xml");
        fs::write(&eq_path, generate_eq_fixture_branch_count(1))?;

        let output_path = tmp_dir.join("contingency_stub_fixture.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get("raptrix.features.contingencies_stub"),
            Some(&"true".to_string())
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // v0.8.7  Transformer representation contract — unit tests
    // -------------------------------------------------------------------------

    fn make_3w_row<'a>(
        bus_h: i32,
        bus_m: i32,
        bus_l: i32,
        r_hm: f64,
        x_hm: f64,
        r_hl: f64,
        x_hl: f64,
        r_ml: f64,
        x_ml: f64,
        active: bool,
    ) -> Transformer3WRow<'a> {
        Transformer3WRow {
            bus_h_id: bus_h,
            bus_m_id: bus_m,
            bus_l_id: bus_l,
            ckt: Cow::Borrowed("1"),
            name: Cow::Borrowed("T3W"),
            r_hm,
            x_hm,
            r_hl,
            x_hl,
            r_ml,
            x_ml,
            tap_h: 1.0,
            tap_m: 1.0,
            tap_l: 1.0,
            phase_shift: 0.0,
            vector_group: Cow::Borrowed("unknown"),
            rate_a: 100.0,
            rate_b: 100.0,
            rate_c: 100.0,
            status: active,
            nominal_kv_h: None,
            nominal_kv_m: None,
            nominal_kv_l: None,
        }
    }

    fn make_2w_star_row<'a>(from: i32, to: i32) -> Transformer2WRow<'a> {
        Transformer2WRow {
            from_bus_id: from,
            to_bus_id: to,
            ckt: Cow::Borrowed("1"),
            name: Cow::Borrowed("STAR"),
            r: 0.01,
            x: 0.05,
            winding1_r: 0.01,
            winding1_x: 0.05,
            winding2_r: 0.0,
            winding2_x: 0.0,
            g: 0.0,
            b: 0.0,
            tap_ratio: 1.0,
            nominal_tap_ratio: 1.0,
            phase_shift: 0.0,
            vector_group: Cow::Borrowed("unknown"),
            rate_a: 100.0,
            rate_b: 100.0,
            rate_c: 100.0,
            status: true,
            from_nominal_kv: None,
            to_nominal_kv: None,
        }
    }

    // --- mode string helpers ---

    #[test]
    fn mode_as_str_native_3w() {
        assert_eq!(
            TransformerRepresentationMode::Native3W.as_str(),
            "native_3w"
        );
    }

    #[test]
    fn mode_as_str_expanded() {
        assert_eq!(TransformerRepresentationMode::Expanded.as_str(), "expanded");
    }

    #[test]
    fn mode_default_is_native_3w() {
        assert_eq!(
            TransformerRepresentationMode::default(),
            TransformerRepresentationMode::Native3W
        );
    }

    // --- validate_transformer_representation_mode ---

    #[test]
    fn validate_native3w_with_no_3w_rows_passes() {
        let rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let rows_3w: Vec<Transformer3WRow<'_>> = vec![];
        assert!(
            validate_transformer_representation_mode(
                &rows_2w,
                &rows_3w,
                TransformerRepresentationMode::Native3W
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_native3w_with_active_3w_rows_passes() {
        // Native3W allows active 3W rows — they are the native form.
        let rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let rows_3w = vec![make_3w_row(1, 2, 3, 0.01, 0.1, 0.01, 0.1, 0.01, 0.1, true)];
        assert!(
            validate_transformer_representation_mode(
                &rows_2w,
                &rows_3w,
                TransformerRepresentationMode::Native3W
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_native3w_rejects_synthetic_star_bus_in_2w() {
        // A 2W row whose to_bus_id is in the synthetic star range must be rejected in native_3w.
        let rows_2w = vec![make_2w_star_row(1, 10_000_001)];
        let rows_3w: Vec<Transformer3WRow<'_>> = vec![];
        let err = validate_transformer_representation_mode(
            &rows_2w,
            &rows_3w,
            TransformerRepresentationMode::Native3W,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("native_3w"),
            "diagnostic must mention mode; got: {msg}"
        );
        assert!(
            msg.contains("10_000_000") || msg.contains("star"),
            "diagnostic must mention star bus; got: {msg}"
        );
    }

    #[test]
    fn validate_expanded_with_no_3w_rows_passes() {
        let rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let rows_3w: Vec<Transformer3WRow<'_>> = vec![];
        assert!(
            validate_transformer_representation_mode(
                &rows_2w,
                &rows_3w,
                TransformerRepresentationMode::Expanded
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_expanded_rejects_active_3w_rows() {
        let rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let rows_3w = vec![make_3w_row(1, 2, 3, 0.01, 0.1, 0.01, 0.1, 0.01, 0.1, true)];
        let err = validate_transformer_representation_mode(
            &rows_2w,
            &rows_3w,
            TransformerRepresentationMode::Expanded,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("expanded"),
            "diagnostic must mention mode; got: {msg}"
        );
        assert!(
            msg.contains("bus_h=1") || msg.contains("1 active"),
            "diagnostic must include row identity; got: {msg}"
        );
    }

    #[test]
    fn validate_expanded_allows_inactive_3w_rows() {
        // Inactive (status=false) 3W rows do not violate expanded mode.
        let rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let rows_3w = vec![make_3w_row(1, 2, 3, 0.01, 0.1, 0.01, 0.1, 0.01, 0.1, false)];
        assert!(
            validate_transformer_representation_mode(
                &rows_2w,
                &rows_3w,
                TransformerRepresentationMode::Expanded
            )
            .is_ok()
        );
    }

    // --- star_expand_3w_transformers ---

    #[test]
    fn star_expand_single_3w_row_produces_three_legs() {
        let rows_3w = vec![make_3w_row(
            10, 20, 30, 0.02, 0.10, 0.02, 0.10, 0.02, 0.10, true,
        )];
        let existing = std::collections::HashSet::new();
        let legs = star_expand_3w_transformers(&rows_3w, &existing);
        assert_eq!(legs.len(), 3, "expected 3 star legs for 1 active 3W row");
    }

    #[test]
    fn star_expand_inactive_3w_row_produces_no_legs() {
        let rows_3w = vec![make_3w_row(
            10, 20, 30, 0.02, 0.10, 0.02, 0.10, 0.02, 0.10, false,
        )];
        let existing = std::collections::HashSet::new();
        let legs = star_expand_3w_transformers(&rows_3w, &existing);
        assert_eq!(legs.len(), 0);
    }

    #[test]
    fn star_expand_delta_to_wye_impedance_correctness() {
        // Known values: r_hm=0.04 r_hl=0.06 r_ml=0.02  x_hm=0.10 x_hl=0.14 x_ml=0.08
        // Wye:  r_h=(0.04+0.06-0.02)/2=0.04  r_m=(0.04+0.02-0.06)/2=0.0  r_l=(0.06+0.02-0.04)/2=0.02
        //       x_h=(0.10+0.14-0.08)/2=0.08  x_m=(0.10+0.08-0.14)/2=0.02 x_l=(0.14+0.08-0.10)/2=0.06
        let rows_3w = vec![make_3w_row(
            1, 2, 3, 0.04, 0.10, // r_hm, x_hm
            0.06, 0.14, // r_hl, x_hl
            0.02, 0.08, // r_ml, x_ml
            true,
        )];
        let existing = std::collections::HashSet::new();
        let legs = star_expand_3w_transformers(&rows_3w, &existing);
        assert_eq!(legs.len(), 3);

        // H leg (from_bus_id == 1)
        let h_leg = legs
            .iter()
            .find(|l| l.from_bus_id == 1)
            .expect("H leg missing");
        assert!((h_leg.r - 0.04).abs() < 1e-9, "r_h mismatch: {}", h_leg.r);
        assert!((h_leg.x - 0.08).abs() < 1e-9, "x_h mismatch: {}", h_leg.x);

        // M leg (from_bus_id == 2)
        let m_leg = legs
            .iter()
            .find(|l| l.from_bus_id == 2)
            .expect("M leg missing");
        assert!((m_leg.r - 0.0).abs() < 1e-9, "r_m mismatch: {}", m_leg.r);
        assert!((m_leg.x - 0.02).abs() < 1e-9, "x_m mismatch: {}", m_leg.x);

        // L leg (from_bus_id == 3)
        let l_leg = legs
            .iter()
            .find(|l| l.from_bus_id == 3)
            .expect("L leg missing");
        assert!((l_leg.r - 0.02).abs() < 1e-9, "r_l mismatch: {}", l_leg.r);
        assert!((l_leg.x - 0.06).abs() < 1e-9, "x_l mismatch: {}", l_leg.x);
    }

    #[test]
    fn star_expand_star_bus_id_is_deterministic() {
        let rows_3w = vec![make_3w_row(
            10, 20, 30, 0.01, 0.05, 0.01, 0.05, 0.01, 0.05, true,
        )];
        let existing = std::collections::HashSet::new();
        let legs_a = star_expand_3w_transformers(&rows_3w, &existing);
        let legs_b = star_expand_3w_transformers(&rows_3w, &existing);
        assert_eq!(
            legs_a[0].to_bus_id, legs_b[0].to_bus_id,
            "star bus ID must be deterministic across calls"
        );
    }

    #[test]
    fn star_expand_star_bus_id_is_in_safe_range() {
        let rows_3w = vec![make_3w_row(
            10, 20, 30, 0.01, 0.05, 0.01, 0.05, 0.01, 0.05, true,
        )];
        let existing = std::collections::HashSet::new();
        let legs = star_expand_3w_transformers(&rows_3w, &existing);
        let star_id = legs[0].to_bus_id;
        assert!(
            star_id > 10_000_000,
            "star bus ID must be > 10_000_000, got {star_id}"
        );
        assert!(star_id > 0, "star bus ID must be positive");
    }

    #[test]
    fn star_expand_all_three_legs_share_same_star_bus() {
        let rows_3w = vec![make_3w_row(
            10, 20, 30, 0.01, 0.05, 0.01, 0.05, 0.01, 0.05, true,
        )];
        let existing = std::collections::HashSet::new();
        let legs = star_expand_3w_transformers(&rows_3w, &existing);
        assert_eq!(legs.len(), 3);
        let star_ids: std::collections::HashSet<i32> = legs.iter().map(|l| l.to_bus_id).collect();
        assert_eq!(
            star_ids.len(),
            1,
            "all three legs must share the same star bus ID"
        );
    }

    // --- normalize_transformer_representation ---

    #[test]
    fn normalize_native3w_strips_synthetic_star_legs() {
        let mut rows_2w = vec![
            make_2w_star_row(1, 2),          // real 2W — keep
            make_2w_star_row(1, 10_000_001), // synthetic star leg — remove
        ];
        let mut rows_3w = vec![make_3w_row(1, 2, 3, 0.01, 0.1, 0.01, 0.1, 0.01, 0.1, true)];
        normalize_transformer_representation(
            &mut rows_2w,
            &mut rows_3w,
            TransformerRepresentationMode::Native3W,
        );
        assert_eq!(rows_2w.len(), 1, "star leg must be stripped");
        assert_eq!(rows_2w[0].to_bus_id, 2, "real 2W row must be kept");
        assert_eq!(rows_3w.len(), 1, "3W rows must be unchanged in native_3w");
    }

    #[test]
    fn normalize_expanded_clears_3w_rows_and_adds_legs() {
        let mut rows_2w: Vec<Transformer2WRow<'_>> = vec![];
        let mut rows_3w = vec![make_3w_row(
            1, 2, 3, 0.04, 0.10, 0.06, 0.14, 0.02, 0.08, true,
        )];
        normalize_transformer_representation(
            &mut rows_2w,
            &mut rows_3w,
            TransformerRepresentationMode::Expanded,
        );
        assert_eq!(rows_3w.len(), 0, "3W rows must be cleared in expanded mode");
        assert_eq!(rows_2w.len(), 3, "three star legs must be added");
    }

    // --- shared schema reader helper (raptrix-cim-arrow) ---

    #[test]
    fn schema_validates_known_mode_values() {
        use crate::arrow_schema::validate_transformer_representation_mode_value;
        assert!(validate_transformer_representation_mode_value("native_3w").is_ok());
        assert!(validate_transformer_representation_mode_value("expanded").is_ok());
    }

    #[test]
    fn schema_rejects_unknown_mode_values() {
        use crate::arrow_schema::validate_transformer_representation_mode_value;
        let err = validate_transformer_representation_mode_value("star_point").unwrap_err();
        assert!(
            err.contains("star_point"),
            "error must mention unknown value; got: {err}"
        );
        assert!(
            err.contains("native_3w"),
            "error must suggest valid values; got: {err}"
        );
    }

    // --- metadata key stamping (write round-trip) ---

    fn generate_eq_fixture_minimal_line() -> String {
        String::from(
            r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:ConnectivityNode rdf:ID="N1" /><cim:ConnectivityNode rdf:ID="N2" />
<cim:ACLineSegment rdf:ID="L1"><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x></cim:ACLineSegment>
<cim:Terminal rdf:ID="LT1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="LT2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##,
        )
    }

    #[test]
    fn write_complete_rpf_stamps_native3w_mode_key() -> Result<()> {
        use crate::arrow_schema::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_repr_mode_native3w");
        fs::create_dir_all(&tmp_dir)?;
        let eq_path = tmp_dir.join("eq.xml");
        let out_path = tmp_dir.join("out.rpf");
        fs::write(&eq_path, generate_eq_fixture_minimal_line())?;
        let eq_str = eq_path.to_string_lossy().into_owned();
        let out_str = out_path.to_string_lossy().into_owned();
        write_complete_rpf(&[&eq_str], &out_str)?;
        let metadata = rpf_file_metadata(&out_str)?;
        assert_eq!(
            metadata.get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE),
            Some(&"native_3w".to_string()),
            "default mode must be native_3w"
        );
        Ok(())
    }

    #[test]
    fn write_complete_rpf_stamps_expanded_mode_key() -> Result<()> {
        use crate::arrow_schema::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_repr_mode_expanded");
        fs::create_dir_all(&tmp_dir)?;
        let eq_path = tmp_dir.join("eq.xml");
        let out_path = tmp_dir.join("out.rpf");
        fs::write(&eq_path, generate_eq_fixture_minimal_line())?;
        let options = WriteOptions {
            transformer_representation_mode: TransformerRepresentationMode::Expanded,
            ..WriteOptions::default()
        };
        let eq_str = eq_path.to_string_lossy().into_owned();
        let out_str = out_path.to_string_lossy().into_owned();
        write_complete_rpf_with_options(&[&eq_str], &out_str, &options)?;
        let metadata = rpf_file_metadata(&out_str)?;
        assert_eq!(
            metadata.get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE),
            Some(&"expanded".to_string()),
            "expanded mode must stamp 'expanded'"
        );
        Ok(())
    }
}
