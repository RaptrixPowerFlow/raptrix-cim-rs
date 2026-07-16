/*
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
*/

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow schema definitions for the Raptrix Power Interchange v0.12.5 profile.
//!
//! ## v0.12.5 — optional bus GIS coordinates (additive)
//! Adds nullable trailing `buses.latitude` / `buses.longitude` (Float64, WGS84 degrees)
//! for operator-oriented relative layout. v0.12.4 files remain readable without re-export.
//!
//! ## v0.12.3 — baseline provenance (additive metadata + topology_changes)
//! Adds nullable baseline provenance fields to `metadata` (provenance, model upgrade tracking,
//! convergence stats) and nullable `change_source` / `applied_phase` dictionary columns to
//! optional `topology_changes`. v0.12.2 files remain readable without re-export.
//!
//! **CGMES 3.0+ Only**: This module targets CGMES v3.0 and later (v17+ CIM) merged profiles.
//! Support for legacy CGMES 2.4.x was dropped in this release for simplicity and performance.
//!
//! This module exposes one exact Arrow schema per required table in the locked
//! `.rpf` contract, plus deterministic schema registry helpers used by both
//! writers and readers.
//!
//! ## v0.12.1 — unified optional tables (canonical RAS + contingency topology audit)
//! Merges optional `remedial_action_schemes` (canonical RAS/SPS for new writes) with
//! optional `contingency_island_analysis` (contingency topology filter audit rows).
//! Optional file metadata keys: `raptrix.features.remedial_action_schemes`,
//! `raptrix.features.contingency_island_analysis`, and `rpf.ras.schema_mode`
//! (default `canonical_v12` when RAS rows are emitted). `SUPPORTED_RPF_VERSIONS`
//! accepts **only** v0.12.1 — prior contract files must be re-emitted.
//!
//! ## v0.11.0 — 18 canonical tables (additive: protection-informed contingencies)
//! Adds two optional root tables — `protection_contingencies` (logical protection-group
//! baseline plus optional breaker-level refinement) and `topology_changes` (declared, and
//! later solved, post-event topology deltas) — plus optional file metadata keys
//! `raptrix.features.protection_contingencies`, `raptrix.features.topology_changes`, and
//! `rpf.protection.fidelity`. The `contingencies.elements.element_type` vocabulary gains a
//! doc-level `protection_event` token. No required table or column shape changes.
//! `SUPPORTED_RPF_VERSIONS` accepts v0.11.0 and retains v0.10.0 for backward-compatible reads
//! (pure-additive optional tables — v0.10.0 files are valid v0.11.0 inputs). See
//! `docs/adr/0001-protection-informed-contingencies.md`.
//!
//! ## v0.10.0 — 18 canonical tables (additive: computational-load interchange)
//! Adds nullable `metadata.computational_load_mode`, optional root table `computational_load_profiles`,
//! and nullable `dynamics_models.perc1_params` struct for PERC1 baseline parameters.
//! `SUPPORTED_RPF_VERSIONS` accepts **only** v0.10.0 — prior contract files must be re-emitted.
//!
//! ## v0.9.6 — 18 canonical tables (additive: warm-start seed semantics)
//! Adds `solved_state_presence = "seed_only"` to mark warm-start RPF files that emit a
//! populated `buses_solved` table sourced from the original case's initial conditions
//! (e.g., PSS/E RAW VM/VA values) without claiming a true post-solve snapshot. Enables
//! warm-start parity with raw-case ingestion for solver consumers. No table or column
//! shape changes — only metadata vocabulary and emission policy.
//!
//! ## v0.9.5 — 18 canonical tables (additive: generators + metadata)
//! Adds `generators.controlled_bus_id` (remote voltage regulation / IREG denormalization) and
//! optional nullable `metadata.default_shunt_control_mode` for declarative planning ↔ real-time
//! shunt handoff. Prior v0.9.4 tables remain valid; readers synthesize missing `controlled_bus_id`
//! as `0` (local regulation).
//!
//! ## v0.9.4 — 18 canonical tables (breaking: buses gains 2 new required columns)
//! Adds explicit Q decomposition to the `buses` table: `qd_load_pu` (pure reactive load,
//! always ≥ 0) and `qg_sched_pu` (pure scheduled generator reactive, any sign). The existing
//! `q_sched` column retains its meaning as the net scheduled injection (`qg_sched_pu − qd_load_pu`)
//! for all bus types. This eliminates the overloaded-column issue where PV/slack buses could have
//! a different physical meaning written by different converters.
//!
//! ## v0.9.3 — 18 canonical tables
//! The `ibr_devices` table was removed. IBRs are now modeled exclusively in the unified
//! `generators` table using `is_ibr = true` + `ibr_subtype`. The `contingencies` table gains
//! 6 nullable operational-outcome columns for real-time solvers. The `metadata` table gains 5 nullable
//! solver-readiness fields. A new optional `scenario_context` table is introduced.
//! The `loads` table gains 4 nullable ZIP-fidelity columns:
//! `p_i_pu`, `q_i_pu`, `p_y_pu`, `q_y_pu`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Human-readable branding string embedded as file-level metadata.
pub const BRANDING: &str = "Raptrix CIM-Arrow / Raptrix Power Interchange v0.12.5 - High-performance open CIM profile (CGMES 3.0+) by Raptrix Power. Copyright (c) 2026 Raptrix Power.";

/// Canonical RPF format version tag embedded as file-level metadata.
pub const RPF_VERSION: &str = "v0.12.5";

/// Supported RPF versions accepted by generic Arrow IPC readers.
///
/// v0.12.5 is the current contract release. v0.12.4–v0.12.1 files remain
/// readable (additive trailing columns). Prior contract versions must be re-emitted.
pub const SUPPORTED_RPF_VERSIONS: &[&str] = &[
    "v0.12.5", "0.12.5", "v0.12.4", "0.12.4", "v0.12.3", "0.12.3", "v0.12.2", "0.12.2", "v0.12.1",
    "0.12.1",
];

/// Validates a nominal kV value for required network voltage fields.
pub fn validate_nominal_kv(value: f64, context: &str) -> Result<(), String> {
    if !value.is_finite() || value <= 0.0 {
        return Err(format!(
            "{context} must be finite and > 0.0 kV, got {value}"
        ));
    }
    Ok(())
}

/// Backward-compatible alias retained for older call sites.
pub const SCHEMA_VERSION: &str = RPF_VERSION;

/// File-level metadata key for branding string.
pub const METADATA_KEY_BRANDING: &str = "raptrix.branding";
/// File-level metadata key for schema version.
pub const METADATA_KEY_VERSION: &str = "raptrix.version";
/// File-level metadata key for RPF version alias.
pub const METADATA_KEY_RPF_VERSION: &str = "rpf_version";
/// Schema metadata key indicating stable equipment `mrid` column support (v0.12.2+).
pub const METADATA_KEY_MRID_SUPPORT: &str = "rpf.mrid_support";
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
/// Optional metadata key indicating the `feasibility_certificate_buses` table is present (v0.12.4+).
pub const METADATA_KEY_FEATURE_FEASIBILITY_CERTIFICATE: &str =
    "raptrix.features.feasibility_certificate";
/// Optional metadata key indicating export is a topology-only snapshot.
pub const METADATA_KEY_FEATURE_TOPOLOGY_ONLY: &str = "rpf.features.topology_only";
/// Optional metadata key indicating all injections were zeroed by export.
pub const METADATA_KEY_FEATURE_ZERO_INJECTION_STUB: &str = "rpf.features.zero_injection_stub";
/// Required metadata key describing the case mode (flat_start_planning | warm_start_planning | solved_snapshot).
/// Added in v0.8.4.
pub const METADATA_KEY_CASE_MODE: &str = "rpf.case_mode";
/// Optional metadata key and `metadata` table column: default shunt control mode for solver handoff.
/// Values: `planning_full` \| `real_time_hot_start` \| `real_time_frozen`. Added in v0.9.5.
pub const METADATA_KEY_DEFAULT_SHUNT_CONTROL_MODE: &str = "rpf.default_shunt_control_mode";
/// Required metadata key indicating presence/provenance of solved-state fields.
/// Values: actual_solved | not_available | not_computed | seed_only. Added in v0.8.4;
/// `seed_only` added in v0.9.6 to mark warm-start initial-condition seeding via a
/// populated `buses_solved` table without solver provenance.
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
/// Required metadata key declaring how 3-winding transformers are represented in this file.
/// Added in v0.8.7.  Allowed values:
/// - `"native_3w"` — physical 3W units appear only in `transformers_3w`; no synthetic star buses.
/// - `"expanded"` — physical 3W units are star-expanded into three 2W legs in `transformers_2w`
///   via delta-to-wye impedance conversion; `transformers_3w` has zero active rows.
///   Dual materialization (active rows in both tables for the same physical unit) is always a
///   hard error regardless of the declared mode.
pub const METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE: &str =
    "rpf.transformer_representation_mode";
/// Optional metadata key indicating total electrical island count.
pub const METADATA_KEY_TOPOLOGY_ISLAND_COUNT: &str = "rpf.topology.island_count";
/// Optional metadata key indicating largest-island bus count.
pub const METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT: &str = "rpf.topology.main_island_bus_count";
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
/// Optional metadata key indicating availability of ZIP load fidelity terms in `loads`.
/// Values: `not_available` | `partial` | `complete`.
/// Added in v0.9.1.
pub const METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE: &str = "rpf.loads.zip_fidelity_presence";
/// `metadata` table column name (Boolean, nullable): when true, consumers enforce the
/// computational-load validation contract (non-empty `computational_load_profiles`, etc.).
/// Added in v0.10.0. Same string is used as the Arrow field name for zero-copy C++ reads.
pub const METADATA_KEY_COMPUTATIONAL_LOAD_MODE: &str = "computational_load_mode";
/// File-level feature flag: optional `computational_load_profiles` root table is present.
pub const METADATA_KEY_FEATURE_COMPUTATIONAL_LOAD_PROFILES: &str =
    "raptrix.features.computational_load_profiles";
/// File-level feature flag: optional `protection_contingencies` root table is present (v0.11.0+).
pub const METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES: &str =
    "raptrix.features.protection_contingencies";
/// File-level feature flag: optional `topology_changes` root table is present (v0.11.0+).
pub const METADATA_KEY_FEATURE_TOPOLOGY_CHANGES: &str = "raptrix.features.topology_changes";
/// Optional metadata key declaring protection-data fidelity (v0.11.0+).
/// Values: `logical` (logical protection-group baseline only) | `breaker_level`
/// (breaker/switch-resolved) | `mixed` (both present across rows).
pub const METADATA_KEY_PROTECTION_FIDELITY: &str = "rpf.protection.fidelity";
/// File-level feature flag: optional `remedial_action_schemes` root table is present (v0.12.1+).
pub const METADATA_KEY_FEATURE_REMEDIAL_ACTION_SCHEMES: &str =
    "raptrix.features.remedial_action_schemes";
/// File-level feature flag: optional `contingency_island_analysis` root table is present (v0.12.1+).
pub const METADATA_KEY_FEATURE_CONTINGENCY_ISLAND_ANALYSIS: &str =
    "raptrix.features.contingency_island_analysis";
/// Optional metadata key declaring canonical RAS schema mode (v0.12.1+).
/// Current value: `canonical_v12`.
pub const METADATA_KEY_RAS_SCHEMA_MODE: &str = "rpf.ras.schema_mode";

/// Canonical metadata table name.
pub const TABLE_METADATA: &str = "metadata";
/// Canonical buses table name.
pub const TABLE_BUSES: &str = "buses";
/// Canonical branches table name.
pub const TABLE_BRANCHES: &str = "branches";
/// Canonical multi-section logical line table name (v0.8.8+).
pub const TABLE_MULTI_SECTION_LINES: &str = "multi_section_lines";
/// Canonical two-terminal DC line table name (v0.8.8+).
pub const TABLE_DC_LINES_2W: &str = "dc_lines_2w";
/// Canonical generators table name.
pub const TABLE_GENERATORS: &str = "generators";
/// Canonical loads table name.
pub const TABLE_LOADS: &str = "loads";
/// Canonical fixed shunts table name.
pub const TABLE_FIXED_SHUNTS: &str = "fixed_shunts";
/// Canonical switched shunts table name.
pub const TABLE_SWITCHED_SHUNTS: &str = "switched_shunts";
/// Canonical switched shunt per-bank detail table name (v0.8.8+).
pub const TABLE_SWITCHED_SHUNT_BANKS: &str = "switched_shunt_banks";
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
/// Optional contingency topology filter audit rows (v0.12.1+).
pub const TABLE_CONTINGENCY_ISLAND_ANALYSIS: &str = "contingency_island_analysis";
/// Canonical interfaces table name.
pub const TABLE_INTERFACES: &str = "interfaces";
/// Canonical dynamics models table name.
pub const TABLE_DYNAMICS_MODELS: &str = "dynamics_models";
/// Optional computational-load profile rows (v0.10.0+), appended when enabled in `RootWriteOptions`.
pub const TABLE_COMPUTATIONAL_LOAD_PROFILES: &str = "computational_load_profiles";
/// Optional FACTS devices table name.
pub const TABLE_FACTS_DEVICES: &str = "facts_devices";
/// Optional scenario context table name (v0.9.0+).
pub const TABLE_SCENARIO_CONTEXT: &str = "scenario_context";
/// Optional protection-informed contingency table name (v0.11.0+).
pub const TABLE_PROTECTION_CONTINGENCIES: &str = "protection_contingencies";
/// Optional post-event topology-change table name (v0.11.0+).
pub const TABLE_TOPOLOGY_CHANGES: &str = "topology_changes";
/// Optional canonical RAS/SPS table name (v0.12.1+).
pub const TABLE_REMEDIAL_ACTION_SCHEMES: &str = "remedial_action_schemes";
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
/// Optional solved-state table.
///
/// Emitted when:
/// - `case_mode = solved_snapshot` (post-solve operating point), or
/// - `case_mode = warm_start_planning` with `solved_state_presence = seed_only`
///   (v0.9.6+) — warm-start initial conditions copied from the source case.
///
/// Contains per-bus post-solve / seed voltage magnitude, angle, and injections.
pub const TABLE_BUSES_SOLVED: &str = "buses_solved";
/// Optional solved-state table emitted only when case_mode=solved_snapshot.
/// Contains per-generator post-solve real/reactive output and PV→PQ switch flag.
pub const TABLE_GENERATORS_SOLVED: &str = "generators_solved";
/// Optional solved-state table emitted only when case_mode=solved_snapshot.
/// Contains per-bank post-solve switched-shunt step and susceptance (v0.8.5+).
pub const TABLE_SWITCHED_SHUNTS_SOLVED: &str = "switched_shunts_solved";
/// Optional solved-state FACTS table emitted for solved snapshot replay (v0.8.6+).
pub const TABLE_FACTS_SOLVED: &str = "facts_solved";
/// Optional solved-state table emitted only when case_mode=solved_snapshot (v0.12.4+).
/// Contains per-bus reactive-limit targets recorded by the solver for buses whose
/// generators reached a Q limit during the solve.
pub const TABLE_Q_LIMITS_SOLVED: &str = "q_limits_solved";
/// Optional post-solve feasibility/complementarity certificate audit rows (v0.12.4+).
pub const TABLE_FEASIBILITY_CERTIFICATE_BUSES: &str = "feasibility_certificate_buses";

/// Ownership of a root table across solve → re-export (see `docs/schema-contract.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableOwnership {
    /// Always taken from the source `.rpf` when present; patch ignored.
    Converter,
    /// Taken from the patch when present; otherwise retained from source.
    Solver,
    /// Explicit merge (today: `metadata` only).
    Shared,
}

/// Returns table ownership for patch-based re-export.
///
/// Unknown table names default to [`TableOwnership::Converter`] so that an older
/// solver binary cannot drop newer enrichment tables it does not model.
pub fn table_ownership(table_name: &str) -> TableOwnership {
    match table_name {
        TABLE_BUSES_SOLVED
        | TABLE_GENERATORS_SOLVED
        | TABLE_SWITCHED_SHUNTS_SOLVED
        | TABLE_FACTS_SOLVED
        | TABLE_Q_LIMITS_SOLVED
        | TABLE_FEASIBILITY_CERTIFICATE_BUSES => TableOwnership::Solver,
        TABLE_METADATA => TableOwnership::Shared,
        _ => TableOwnership::Converter,
    }
}

/// File-level metadata keys owned by the solver patch (overlay onto source).
pub fn is_solver_root_metadata_key(key: &str) -> bool {
    key.starts_with("rpf.solver.")
        || key == METADATA_KEY_CASE_MODE
        || key == METADATA_KEY_SOLVED_STATE_PRESENCE
        || key == METADATA_KEY_SOLVED_SHUNT_STATE_PRESENCE
        || key == METADATA_KEY_FACTS_SOLVED_STATE_PRESENCE
        || key == METADATA_KEY_FEATURE_FACTS_SOLVED
        || key == METADATA_KEY_FEATURE_FEASIBILITY_CERTIFICATE
}

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

/// Validates the value of `rpf.transformer_representation_mode` read from file metadata.
///
/// Returns `Ok(())` for `"expanded"` or `"native_3w"`.  Returns an error for any other
/// value so readers can apply strict or compatibility semantics as appropriate.
///
/// # Usage
/// - **Strict reader**: propagate the error and reject the file.
/// - **Compatibility reader**: log a warning and assume `"native_3w"` as fallback.
pub fn validate_transformer_representation_mode_value(value: &str) -> Result<(), String> {
    match value {
        "expanded" | "native_3w" => Ok(()),
        other => Err(format!(
            "unknown rpf.transformer_representation_mode value '{}'; expected 'expanded' or \
             'native_3w'.  Files produced by converters older than v0.8.7 may lack this key \
             entirely — treat absence as 'native_3w' in compatibility mode.",
            other
        )),
    }
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

/// Inner struct fields for each element of `computational_load_profiles.seasonal_envelope`:
/// `season` (Utf8), `min_mw`, `max_mw`, `pf` (Float32).
pub fn seasonal_envelope_element_fields() -> Vec<Field> {
    vec![
        Field::new("season", DataType::Utf8, false),
        Field::new("min_mw", DataType::Float32, false),
        Field::new("max_mw", DataType::Float32, false),
        Field::new("pf", DataType::Float32, false),
    ]
}

/// `List<Struct{ season, min_mw, max_mw, pf }>` for seasonal MW / power-factor envelopes.
pub fn seasonal_envelope_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(seasonal_envelope_element_fields().into()),
        false,
    )))
}

/// Inner struct fields for each element of `computational_load_profiles.buildout_schedule`:
/// `year` (Int32), `mw` (Float32).
pub fn buildout_schedule_element_fields() -> Vec<Field> {
    vec![
        Field::new("year", DataType::Int32, false),
        Field::new("mw", DataType::Float32, false),
    ]
}

/// `List<Struct{ year, mw }>` for forecasted build-out MW by year.
pub fn buildout_schedule_list_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(buildout_schedule_element_fields().into()),
        false,
    )))
}

/// Named fields for nullable `dynamics_models.perc1_params` (PERC1 baseline model).
pub fn perc1_params_struct_fields() -> Vec<Field> {
    vec![
        Field::new("perc1_voltage_ride_through_pu", DataType::Float64, true),
        Field::new("perc1_frequency_ride_through_hz", DataType::Float64, true),
        Field::new("perc1_reactive_power_ceiling_pu", DataType::Float64, true),
        Field::new(
            "perc1_active_power_recovery_rate_pu_per_s",
            DataType::Float64,
            true,
        ),
        Field::new("perc1_voltage_support_time_sec", DataType::Float64, true),
        Field::new("perc1_frequency_support_time_sec", DataType::Float64, true),
    ]
}

/// Nullable struct type for `dynamics_models.perc1_params`.
pub fn perc1_params_struct_type() -> DataType {
    DataType::Struct(perc1_params_struct_fields().into())
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

/// `List<Utf8>` of breaker / switch identifiers for optional breaker-level refinement
/// (`protection_contingencies.breaker_ids`, v0.11.0+).
fn utf8_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
}

/// `List<Int32>` of bus identifiers (`topology_changes.affected_bus_ids`, v0.11.0+).
fn int32_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}

/// `List<Struct{ step, delay_ms, equipment_kind, equipment_id }>` describing an automatic
/// protection sequence (`protection_contingencies.sequence`, v0.11.0+).
fn protection_sequence_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "step_item",
        DataType::Struct(
            vec![
                Field::new("step", DataType::Int32, false),
                Field::new("delay_ms", DataType::Float64, true),
                Field::new("equipment_kind", dict_utf8(), true),
                Field::new("equipment_id", dict_utf8(), true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `List<Struct{ island_index, bus_ids, energized }>` describing islands produced by a
/// topology change (`topology_changes.resulting_islands`, v0.11.0+).
fn resulting_islands_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "island",
        DataType::Struct(
            vec![
                Field::new("island_index", DataType::Int32, false),
                Field::new("bus_ids", int32_list_type(), false),
                Field::new("energized", DataType::Boolean, true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `List<Struct{ target_type, target_id, operator, threshold, unit, source_table,
/// source_column }>` for reusable model filters in canonical RAS rows (v0.12.0+).
fn ras_model_filter_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "filter",
        DataType::Struct(
            vec![
                Field::new("target_type", dict_utf8(), false),
                Field::new("target_id", dict_utf8(), false),
                Field::new("operator", dict_utf8(), true),
                Field::new("threshold", DataType::Float64, true),
                Field::new("unit", dict_utf8(), true),
                Field::new("source_table", dict_utf8(), true),
                Field::new("source_column", dict_utf8(), true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `List<Struct{ condition_id, lhs_ref, comparator, rhs_kind, rhs_value, rhs_ref,
/// duration_ms }>` for trigger and arming conditions (v0.12.0+).
fn ras_model_condition_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "condition",
        DataType::Struct(
            vec![
                Field::new("condition_id", dict_utf8(), false),
                Field::new("lhs_ref", DataType::Utf8, false),
                Field::new("comparator", dict_utf8(), false),
                Field::new("rhs_kind", dict_utf8(), false),
                Field::new("rhs_value", DataType::Float64, true),
                Field::new("rhs_ref", DataType::Utf8, true),
                Field::new("duration_ms", DataType::Float64, true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `List<Struct{ element_id, action_type, target_type, target_id, amount_mw,
/// amount_pct, status_change, params }>` for RAS action targets (v0.12.0+).
fn ras_action_element_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "action",
        DataType::Struct(
            vec![
                Field::new("element_id", dict_utf8(), false),
                Field::new("action_type", dict_utf8(), false),
                Field::new("target_type", dict_utf8(), false),
                Field::new("target_id", dict_utf8(), false),
                Field::new("amount_mw", DataType::Float64, true),
                Field::new("amount_pct", DataType::Float64, true),
                Field::new("status_change", DataType::Boolean, true),
                Field::new("params", map_string_f64(), true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `List<Struct{ step_index, delay_ms, priority, merit_order, action_set,
/// rollback_on_fail }>` for executable RAS sequence ordering (v0.12.0+).
fn ras_sequence_step_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "step",
        DataType::Struct(
            vec![
                Field::new("step_index", DataType::Int32, false),
                Field::new("delay_ms", DataType::Float64, true),
                Field::new("priority", DataType::Int32, true),
                Field::new("merit_order", DataType::Int32, true),
                Field::new("action_set", ras_action_element_type(), false),
                Field::new("rollback_on_fail", DataType::Boolean, true),
            ]
            .into(),
        ),
        false,
    )))
}

/// `Struct{ armed, arm_delay_ms, disarm_delay_ms, rearm_delay_ms, max_operations }`
/// for RAS arming window policy (v0.12.0+).
fn ras_arming_window_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new("armed", DataType::Boolean, true),
            Field::new("arm_delay_ms", DataType::Float64, true),
            Field::new("disarm_delay_ms", DataType::Float64, true),
            Field::new("rearm_delay_ms", DataType::Float64, true),
            Field::new("max_operations", DataType::Int32, true),
        ]
        .into(),
    )
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
    metadata.insert(METADATA_KEY_MRID_SUPPORT.to_string(), "v1".to_string());
    metadata
}

/// `metadata` table schema.
///
/// v0.8.4 adds planning-vs-solved semantics fields:
/// - `case_mode`: flat_start_planning | warm_start_planning | solved_snapshot | hour_ahead_advisory
/// - `solved_state_presence`: actual_solved | not_available | not_computed | seed_only (v0.9.6+)
/// - Solver provenance fields (all nullable): solver_version, solver_iterations,
///   solver_accuracy, solver_mode. Populated only when solved_state_presence=actual_solved.
///   `seed_only` does NOT require solver provenance — it indicates `buses_solved` carries
///   warm-start initial conditions copied from the source case (no solve was executed).
///
/// v0.9.5 adds nullable `default_shunt_control_mode` (planning_full | real_time_hot_start |
/// real_time_frozen) for declarative shunt-mode handoff; uncoupled from `case_mode` semantics.
///
/// v0.10.0 adds nullable `computational_load_mode` (Boolean) for computational-load interchange mode.
///
/// v0.12.3 adds nullable baseline provenance fields: `original_sentinel_case_id`,
/// `original_model_version`, `target_baseline_version`, `is_sal_enhanced`,
/// `sal_enhancement_timestamp`, `cim_model_version_used`, `planning_ready`, `upgrade_summary`,
/// `convergence_time_ms`, and `convergence_iterations`. Populated when a source case
/// is upgraded to a self-describing baseline .rpf; null in standard CIM exports.
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
            // v0.8.8: modern-grid profile metadata.
            Field::new("modern_grid_profile", DataType::Boolean, false),
            Field::new("ibr_penetration_pct", DataType::Float64, true),
            Field::new("has_ibr", DataType::Boolean, false),
            Field::new("has_smart_valve", DataType::Boolean, false),
            Field::new("has_multi_terminal_dc", DataType::Boolean, false),
            Field::new("study_purpose", DataType::Utf8, true),
            Field::new(
                "scenario_tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                true,
            ),
            // v0.9.0: solver-readiness fields
            // case_mode now also accepts "hour_ahead_advisory" in addition to existing values
            Field::new("hour_ahead_uncertainty_band", DataType::Float64, true), // e.g. 2.0 = ±2% load forecast error
            Field::new("commitment_source", DataType::Utf8, true), // "day_ahead_market", "operator_plan"
            Field::new("solver_q_limit_infeasible_count", DataType::Int32, true),
            Field::new("pv_to_pq_switch_count", DataType::Int32, true),
            Field::new("real_time_discovery", DataType::Boolean, true), // true if from live SE analysis
            // v0.9.5: optional declarative shunt mode for planning ↔ real-time interchange
            Field::new("default_shunt_control_mode", dict_utf8(), true),
            // v0.10.0: computational-load interchange (see `computational_load_profiles` optional table)
            Field::new(
                METADATA_KEY_COMPUTATIONAL_LOAD_MODE,
                DataType::Boolean,
                true,
            ),
            // v0.12.3: baseline provenance
            Field::new("original_sentinel_case_id", DataType::Utf8, true),
            Field::new("original_model_version", DataType::Utf8, true),
            Field::new("target_baseline_version", DataType::Utf8, true),
            Field::new("is_sal_enhanced", DataType::Boolean, true),
            Field::new("sal_enhancement_timestamp", DataType::Utf8, true),
            Field::new("cim_model_version_used", DataType::Utf8, true),
            Field::new("planning_ready", DataType::Boolean, true),
            Field::new("upgrade_summary", DataType::Utf8, true),
            Field::new("convergence_time_ms", DataType::Float64, true),
            Field::new("convergence_iterations", DataType::Int32, true),
        ],
        schema_metadata(),
    )
}

/// `buses` table schema.
///
/// v0.9.4 adds `qd_load_pu` and `qg_sched_pu` at positions 20–21 (after `bus_uuid`).
/// v0.12.5 adds nullable trailing `latitude` / `longitude` (WGS84 degrees).
/// `q_sched` (pos 4) retains its meaning as `qg_sched_pu − qd_load_pu` for all bus types.
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
            Field::new("owner_id", DataType::Int32, true),
            Field::new("v_min", DataType::Float64, false),
            Field::new("v_max", DataType::Float64, false),
            Field::new("p_min_agg", DataType::Float64, false),
            Field::new("p_max_agg", DataType::Float64, false),
            Field::new("nominal_kv", DataType::Float64, false),
            Field::new("bus_uuid", dict_utf8(), false),
            // v0.9.4: explicit Q decomposition for unambiguous round-trip fidelity.
            // qd_load_pu: Σ(in-service load QL) / SBASE (signed; positive for inductive load,
            //             negative for capacitive reactive injection via load record)
            // qg_sched_pu: Σ(in-service generator QG) / SBASE (any sign)
            // Identity: q_sched == qg_sched_pu - qd_load_pu  (machine-checkable)
            Field::new("qd_load_pu", DataType::Float64, false),
            Field::new("qg_sched_pu", DataType::Float64, false),
            // v0.12.5: optional WGS84 bus coordinates for relative GIS ordering in viewers.
            // Not required for electrical fidelity; null when the source model lacks GL data.
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
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
            Field::new("owner_id", DataType::Int32, true),
            Field::new("name", dict_utf8_u32(), true),
            Field::new("from_nominal_kv", DataType::Float64, false),
            Field::new("to_nominal_kv", DataType::Float64, false),
            // v0.8.6: additive generic FACTS control metadata.
            Field::new("device_type", dict_utf8(), true),
            Field::new("control_mode", dict_utf8(), true),
            Field::new("control_target_flow_mw", DataType::Float64, true),
            Field::new("x_min_pu", DataType::Float64, true),
            Field::new("x_max_pu", DataType::Float64, true),
            Field::new("injected_voltage_mag_pu", DataType::Float64, true),
            Field::new("injected_voltage_angle_deg", DataType::Float64, true),
            Field::new("facts_params", map_string_f64(), true),
            // v0.8.8: multi-section logical-line linkage columns.
            Field::new("parent_line_id", DataType::Int32, true),
            Field::new("section_index", DataType::Int32, true),
            // v0.12.2: stable CIM mRID (ACLineSegment.base.m_rid etc.)
            Field::new("mrid", DataType::Utf8, true),
        ],
        schema_metadata(),
    )
}

/// `multi_section_lines` table schema (v0.8.8+).
pub fn multi_section_lines_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("line_id", DataType::Int32, false),
            Field::new("from_bus_id", DataType::Int32, false),
            Field::new("to_bus_id", DataType::Int32, false),
            Field::new("ckt", DataType::Utf8, false),
            Field::new(
                "section_branch_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
            Field::new("total_r_pu", DataType::Float64, false),
            Field::new("total_x_pu", DataType::Float64, false),
            Field::new("total_b_pu", DataType::Float64, false),
            Field::new("rate_a_mva", DataType::Float64, false),
            Field::new("rate_b_mva", DataType::Float64, true),
            Field::new("status", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ],
        schema_metadata(),
    )
}

/// `dc_lines_2w` table schema (v0.8.8+).
pub fn dc_lines_2w_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("dc_line_id", DataType::Int32, false),
            Field::new("from_bus_id", DataType::Int32, false),
            Field::new("to_bus_id", DataType::Int32, false),
            Field::new("ckt", DataType::Utf8, false),
            Field::new("r_ohm", DataType::Float64, false),
            Field::new("l_henry", DataType::Float64, true),
            Field::new("control_mode", DataType::Utf8, false),
            Field::new("p_setpoint_mw", DataType::Float64, true),
            Field::new("i_setpoint_ka", DataType::Float64, true),
            Field::new("v_setpoint_kv", DataType::Float64, true),
            Field::new("q_from_mvar", DataType::Float64, true),
            Field::new("q_to_mvar", DataType::Float64, true),
            Field::new("status", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("converter_type", DataType::Utf8, false),
        ],
        schema_metadata(),
    )
}

/// `generators` table schema.
pub fn generators_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("generator_id", DataType::Int32, false),
            Field::new("bus_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("unit_type", DataType::Utf8, false),
            Field::new("hierarchy_level", DataType::Utf8, false),
            Field::new("parent_generator_id", DataType::Int32, true),
            Field::new("aggregation_count", DataType::Int32, true),
            Field::new("status", DataType::Boolean, false),
            Field::new("is_ibr", DataType::Boolean, false),
            Field::new("ibr_subtype", DataType::Utf8, true),
            Field::new("p_sched_mw", DataType::Float64, false),
            Field::new("q_sched_mvar", DataType::Float64, false),
            Field::new("p_min_mw", DataType::Float64, false),
            Field::new("p_max_mw", DataType::Float64, false),
            Field::new("q_min_mvar", DataType::Float64, false),
            Field::new("q_max_mvar", DataType::Float64, false),
            Field::new("mbase_mva", DataType::Float64, false),
            Field::new("uol_mw", DataType::Float64, true),
            Field::new("lol_mw", DataType::Float64, true),
            Field::new("ramp_rate_up_mw_min", DataType::Float64, true),
            Field::new("ramp_rate_down_mw_min", DataType::Float64, true),
            Field::new("owner_id", DataType::Int32, true),
            Field::new("market_resource_id", DataType::Utf8, true),
            Field::new("params", map_string_f64(), true),
            // v0.9.5: remote voltage regulation target (PSS/E IREG; CIM RegulatingControl denormalized)
            Field::new("controlled_bus_id", DataType::Int32, false),
            // v0.12.2: stable CIM mRID (SynchronousMachine.base.m_rid etc.)
            Field::new("mrid", DataType::Utf8, true),
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
            // Constant-power (P) ZIP components (legacy fields; always populated by current writers).
            Field::new("p_pu", DataType::Float64, false),
            Field::new("q_pu", DataType::Float64, false),
            // v0.9.1: optional ZIP-fidelity components on system base.
            // Null means source data did not provide that ZIP term.
            Field::new("p_i_pu", DataType::Float64, true),
            Field::new("q_i_pu", DataType::Float64, true),
            Field::new("p_y_pu", DataType::Float64, true),
            Field::new("q_y_pu", DataType::Float64, true),
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

/// `switched_shunt_banks` table schema (v0.8.8+).
pub fn switched_shunt_banks_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("shunt_id", DataType::Int32, false),
            Field::new("bank_id", DataType::Int32, false),
            Field::new("b_mvar", DataType::Float64, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("step", DataType::Int32, false),
        ],
        schema_metadata(),
    )
}

/// `scenario_context` table schema (v0.9.0+, optional).
///
/// Stores rich structured context for every flagged/exported case.
/// Used by real-time solvers for intelligent contingency analysis
/// and rich `.rpf` export for planning feedback.
/// This is an optional table — present in real-time solver exports, absent in standard planning files.
pub fn scenario_context_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("scenario_context_id", DataType::Int32, false),
            // case_id links to metadata.case_fingerprint
            Field::new("case_id", DataType::Utf8, false),
            Field::new("source_type", DataType::Utf8, false), // "real_time", "hour_ahead_advisory", "planning_study"
            Field::new("priority", DataType::Utf8, false),    // "critical", "high", "medium", "low"
            Field::new("violation_type", DataType::Utf8, true), // "voltage_collapse", "q_limit_infeasible", "unrecoverable_n2", "limit_violation"
            Field::new("nerc_recovery_status", DataType::Utf8, true), // "recoverable_15min_lte", "not_recoverable", "unknown"
            Field::new("recovery_time_min", DataType::Float64, true),
            Field::new("cleared_by_reserves", DataType::Boolean, true),
            Field::new("planning_feedback_flag", DataType::Boolean, false),
            Field::new("planning_assumption_violated", DataType::Utf8, true),
            Field::new("recommended_action", DataType::Utf8, true),
            Field::new("investigation_summary", DataType::Utf8, true),
            Field::new("load_forecast_error_pct", DataType::Float64, true), // for hour-ahead cases
            Field::new("created_timestamp_utc", DataType::Utf8, false),
            Field::new("params", map_string_f64(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `protection_contingencies` table schema (v0.11.0+).
///
/// Deprecated for new RAS writes in v0.12.0+. Retained for backward-compatible reads
/// and deterministic migration into `remedial_action_schemes`.
///
/// One row per protection-driven contingency event, keyed to a `contingencies.contingency_id`.
/// Implements the layered model from
/// `docs/adr/0001-protection-informed-contingencies.md`: a logical protection-group baseline
/// (`tripped_elements`, `scheme_type`, `data_confidence`) with optional breaker-level
/// refinement (`breaker_ids`, joining `node_breaker_detail` / `switch_detail`).
///
/// `tripped_elements` reuses the exact element struct shape of `contingencies.elements`
/// (see `contingencies_elements_type()`) so consumers can apply the resulting outage set with
/// existing compound-contingency logic.
pub fn protection_contingencies_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            // FK to contingencies.contingency_id.
            Field::new("contingency_id", dict_utf8(), false),
            // Stable identity of the protection scheme / group.
            Field::new("protection_group_id", dict_utf8(), false),
            Field::new("name", DataType::Utf8, true),
            // Open vocabulary: breaker_failure, stuck_breaker, relay_misoperation,
            // bus_differential, zone_protection, line_protection, transfer_trip,
            // sympathetic_trip, auto_reclose. Consumers must tolerate unknown tokens.
            Field::new("scheme_type", dict_utf8(), false),
            // The fault / triggering element.
            Field::new("initiating_equipment_kind", dict_utf8(), true),
            Field::new("initiating_equipment_id", dict_utf8(), true),
            // Resulting outage set; identical struct shape to contingencies.elements.
            Field::new("tripped_elements", contingencies_elements_type(), false),
            // Optional automatic sequence ordering / timing.
            Field::new("sequence", protection_sequence_type(), true),
            // FK to topology_changes.topology_change_id when a topology change results.
            Field::new("topology_change_id", DataType::Int32, true),
            // Producer honesty about the outage set: modeled | inferred | assumed.
            Field::new("data_confidence", dict_utf8(), false),
            // Optional breaker-level refinement; references switch identifiers.
            Field::new("breaker_ids", utf8_list_type(), true),
            Field::new("params", map_string_f64(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `topology_changes` table schema (v0.11.0+).
///
/// Deprecated for new RAS writes in v0.12.0+. Retained for backward-compatible reads
/// and deterministic migration into `remedial_action_schemes` action sets.
///
/// One row per resulting topology delta. `provenance` discriminates planning intent
/// (`declared`, emitted by Phase 0 producers) from a future solver-derived delta (`solved`).
///
/// v0.12.3 adds nullable `change_source` and `applied_phase` dictionary columns for SAL
/// Baseline upgrade tracking (e.g. `SAL_CIM_Upgrade`, `Jan_to_June_Baseline`).
pub fn topology_changes_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("topology_change_id", DataType::Int32, false),
            // Optional back-reference to the contingency that produced the change.
            Field::new("contingency_id", dict_utf8(), true),
            // bus_split | island_formation | substation_isolation | partial_isolation |
            // element_isolation. Open vocabulary; consumers must tolerate unknown tokens.
            Field::new("change_type", dict_utf8(), false),
            Field::new("affected_bus_ids", int32_list_type(), false),
            Field::new("resulting_islands", resulting_islands_type(), true),
            Field::new("isolated_element_count", DataType::Int32, true),
            Field::new("summary", DataType::Utf8, true),
            // declared (planning intent, Phase 0) | solved (solver-derived, future).
            Field::new("provenance", dict_utf8(), true),
            Field::new("params", map_string_f64(), true),
            // v0.12.3: baseline change tracking
            Field::new("change_source", dict_utf8(), true),
            Field::new("applied_phase", dict_utf8(), true),
        ],
        schema_metadata(),
    )
}

/// Returns optional protection-informed table schemas in deterministic order (v0.11.0+).
///
/// Order matches root-column ordering: `protection_contingencies` before `topology_changes`.
pub fn protection_table_schemas(include_topology_changes: bool) -> Vec<(&'static str, Schema)> {
    let mut tables = vec![(
        TABLE_PROTECTION_CONTINGENCIES,
        protection_contingencies_schema(),
    )];
    if include_topology_changes {
        tables.push((TABLE_TOPOLOGY_CHANGES, topology_changes_schema()));
    }
    tables
}

/// Optional canonical `remedial_action_schemes` table schema (v0.12.0+).
///
/// This section implements the publicly documented WECC Common RAS Model format.
/// No proprietary data is included.
pub fn remedial_action_schemes_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("ras_id", dict_utf8(), false),
            Field::new("name", DataType::Utf8, true),
            Field::new("authority", DataType::Utf8, true),
            Field::new("model_version", DataType::Utf8, true),
            Field::new("enabled", DataType::Boolean, false),
            Field::new("arming_window", ras_arming_window_type(), true),
            Field::new("arming_filters", ras_model_filter_type(), true),
            Field::new("arming_conditions", ras_model_condition_type(), true),
            Field::new("trigger_filters", ras_model_filter_type(), true),
            Field::new("trigger_conditions", ras_model_condition_type(), false),
            Field::new("sequence_steps", ras_sequence_step_type(), false),
            Field::new("remedial_action", dict_utf8(), true),
            Field::new("scheme_kind", dict_utf8(), true),
            Field::new("remedial_action_elements", ras_action_element_type(), true),
            Field::new("applicable_contingency_ids", utf8_list_type(), true),
            Field::new("notes", DataType::Utf8, true),
            Field::new("data_confidence", dict_utf8(), false),
            Field::new("params", map_string_f64(), true),
        ],
        schema_metadata(),
    )
}

/// Returns optional canonical RAS table schemas in deterministic order (v0.12.1+).
pub fn remedial_action_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![(
        TABLE_REMEDIAL_ACTION_SCHEMES,
        remedial_action_schemes_schema(),
    )]
}

/// Returns optional contingency island analysis table schemas in deterministic order (v0.12.1+).
pub fn contingency_island_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![(
        TABLE_CONTINGENCY_ISLAND_ANALYSIS,
        contingency_island_analysis_schema(),
    )]
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
            Field::new("from_nominal_kv", DataType::Float64, false),
            Field::new("to_nominal_kv", DataType::Float64, false),
            // v0.12.2: stable CIM mRID (PowerTransformer.base.m_rid etc.)
            Field::new("mrid", DataType::Utf8, true),
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
            Field::new("nominal_kv_h", DataType::Float64, false),
            Field::new("nominal_kv_m", DataType::Float64, false),
            Field::new("nominal_kv_l", DataType::Float64, false),
            // v0.12.2: stable CIM mRID (PowerTransformer.base.m_rid etc.)
            Field::new("mrid", DataType::Utf8, true),
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
            Field::new("name", DataType::Utf8, false),
            Field::new("short_name", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("params", map_string_f64(), true),
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
            // v0.9.0: operational-outcome columns (nullable; null in planning/stub files)
            Field::new("risk_score", DataType::Float64, true),
            Field::new("cleared_by_reserves", DataType::Boolean, true),
            Field::new("voltage_collapse_flag", DataType::Boolean, true),
            Field::new("recovery_possible", DataType::Boolean, true),
            Field::new("recovery_time_min", DataType::Float64, true),
            Field::new("greedy_reserve_summary", DataType::Utf8, true),
        ],
        schema_metadata(),
    )
}

/// `contingency_island_analysis` table schema (v0.12.1+, optional).
///
/// Optional contingency topology filter audit rows keyed by `contingency_id`.
pub fn contingency_island_analysis_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("contingency_id", dict_utf8(), false),
            Field::new("classification", dict_utf8(), true),
            Field::new("filter_reason", dict_utf8(), true),
            Field::new("island_load_mw", DataType::Float64, true),
            Field::new("island_gen_mw", DataType::Float64, true),
            Field::new("bus_count", DataType::Int32, true),
            Field::new("max_kv", DataType::Float64, true),
            Field::new("is_main_island", DataType::Boolean, true),
            Field::new("excluded_from_events", DataType::Boolean, true),
            Field::new("params_snapshot_json", DataType::Utf8, true),
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
///
/// v0.10.0 adds nullable `perc1_params` struct for PERC1 baseline model parameters.
pub fn dynamics_models_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("gen_id", dict_utf8(), false),
            Field::new("model_type", dict_utf8(), false),
            Field::new("params", map_string_f64(), false),
            Field::new("perc1_params", perc1_params_struct_type(), true),
        ],
        schema_metadata(),
    )
}

/// Optional `computational_load_profiles` table (v0.10.0+).
///
/// One row per computational-load bus or load. Exactly one of `bus_id` or `load_id` should be
/// non-null for a valid interchange row; enforcement is a **runtime** contract when
/// `metadata.computational_load_mode` is true.
pub fn computational_load_profiles_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, true),
            Field::new("load_id", dict_utf8(), true),
            Field::new("seasonal_envelope", seasonal_envelope_list_type(), true),
            Field::new("buildout_schedule", buildout_schedule_list_type(), true),
            Field::new("ramp_rate_up_mw_per_min", DataType::Float32, true),
            Field::new("ramp_rate_down_mw_per_min", DataType::Float32, true),
            Field::new("it_load_percent", DataType::Float32, true),
            Field::new("non_it_load_percent", DataType::Float32, true),
            Field::new("it_allocation_mode", dict_utf8(), true),
            Field::new("ups_config", map_string_f64(), true),
            Field::new("pcc_relay_settings", map_string_f64(), true),
            Field::new("onsite_gen_bess_mw", DataType::Float32, true),
            Field::new("onsite_gen_parallel", DataType::Boolean, true),
            Field::new("bess_ramp_rate_mw_per_min", DataType::Float32, true),
            Field::new("facility_use_case_percent", map_string_f64(), true),
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
/// Emitted when `case_mode = solved_snapshot`, or when
/// `case_mode = warm_start_planning` with `solved_state_presence = seed_only`
/// (v0.9.6+).  All value columns are nullable so a partial solve or a bus with
/// no result can be represented honestly.  `provenance` encodes per-row data
/// origin: `actual_solved` | `not_available` | `not_computed` | `seed_only`
/// (v0.9.6+ — warm-start initial conditions, no solve executed).
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
            // This flag must never be written back to generators.p_sched_mw.
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

/// Optional `q_limits_solved` table schema (v0.12.4+).
///
/// Emitted only when `case_mode = solved_snapshot` and the solver recorded
/// per-bus reactive-limit targets (buses whose units reached a Q limit).
pub fn q_limits_solved_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            // Foreign key into buses.bus_id — must be present for every row.
            Field::new("bus_id", DataType::Int32, false),
            // Net reactive-power target at the bus in per-unit after limit enforcement.
            Field::new("q_net_target_pu", DataType::Float64, false),
        ],
        schema_metadata(),
    )
}

/// Optional `feasibility_certificate_buses` table schema (v0.12.4+).
///
/// Emitted when a post-solve feasibility/complementarity certificate is present.
/// Mirrors the certificate bus audit rows for typed consumers.
pub fn feasibility_certificate_buses_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("is_pv", DataType::Boolean, true),
            Field::new("switched_to_pq", DataType::Boolean, true),
            Field::new("q_gen_pu", DataType::Float64, true),
            Field::new("complementarity_ok", DataType::Boolean, true),
            Field::new("voltage_box_ok", DataType::Boolean, true),
            Field::new("violation_kind", dict_utf8(), true),
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

/// Returns optional `computational_load_profiles` table schema (v0.10.0+).
pub fn computational_load_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![(
        TABLE_COMPUTATIONAL_LOAD_PROFILES,
        computational_load_profiles_schema(),
    )]
}

/// Returns optional solved-state table schemas in deterministic order (v0.8.4+).
///
/// These tables are appended after all other optional root columns when
/// `case_mode = solved_snapshot`.
pub fn solved_state_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_BUSES_SOLVED, buses_solved_schema()),
        (TABLE_GENERATORS_SOLVED, generators_solved_schema()),
        (
            TABLE_SWITCHED_SHUNTS_SOLVED,
            switched_shunts_solved_schema(),
        ),
    ]
}

/// Returns all required table schemas in canonical v0.7.1 order.
pub fn all_table_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (TABLE_METADATA, metadata_schema()),
        (TABLE_BUSES, buses_schema()),
        (TABLE_BRANCHES, branches_schema()),
        (TABLE_MULTI_SECTION_LINES, multi_section_lines_schema()),
        (TABLE_DC_LINES_2W, dc_lines_2w_schema()),
        (TABLE_GENERATORS, generators_schema()),
        (TABLE_LOADS, loads_schema()),
        (TABLE_FIXED_SHUNTS, fixed_shunts_schema()),
        (TABLE_SWITCHED_SHUNTS, switched_shunts_schema()),
        (TABLE_SWITCHED_SHUNT_BANKS, switched_shunt_banks_schema()),
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
        TABLE_MULTI_SECTION_LINES => Some(multi_section_lines_schema()),
        TABLE_DC_LINES_2W => Some(dc_lines_2w_schema()),
        TABLE_GENERATORS => Some(generators_schema()),
        TABLE_SCENARIO_CONTEXT => Some(scenario_context_schema()),
        TABLE_LOADS => Some(loads_schema()),
        TABLE_FIXED_SHUNTS => Some(fixed_shunts_schema()),
        TABLE_SWITCHED_SHUNTS => Some(switched_shunts_schema()),
        TABLE_SWITCHED_SHUNT_BANKS => Some(switched_shunt_banks_schema()),
        TABLE_TRANSFORMERS_2W => Some(transformers_2w_schema()),
        TABLE_TRANSFORMERS_3W => Some(transformers_3w_schema()),
        TABLE_AREAS => Some(areas_schema()),
        TABLE_ZONES => Some(zones_schema()),
        TABLE_OWNERS => Some(owners_schema()),
        TABLE_CONTINGENCIES => Some(contingencies_schema()),
        TABLE_CONTINGENCY_ISLAND_ANALYSIS => Some(contingency_island_analysis_schema()),
        TABLE_INTERFACES => Some(interfaces_schema()),
        TABLE_DYNAMICS_MODELS => Some(dynamics_models_schema()),
        TABLE_PROTECTION_CONTINGENCIES => Some(protection_contingencies_schema()),
        TABLE_TOPOLOGY_CHANGES => Some(topology_changes_schema()),
        TABLE_REMEDIAL_ACTION_SCHEMES => Some(remedial_action_schemes_schema()),
        TABLE_COMPUTATIONAL_LOAD_PROFILES => Some(computational_load_profiles_schema()),
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
        TABLE_Q_LIMITS_SOLVED => Some(q_limits_solved_schema()),
        TABLE_FEASIBILITY_CERTIFICATE_BUSES => Some(feasibility_certificate_buses_schema()),
        _ => None,
    }
}

/// Solved-snapshot dialect of `multi_section_lines` (v0.12.4 read compatibility).
///
/// Current solved-snapshot exporters emit a per-section row layout for this table
/// instead of the canonical per-line layout. Generic readers accept both; writers
/// in this crate always emit the canonical layout.
pub fn multi_section_lines_snapshot_dialect_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("line_id", DataType::Int32, true),
            Field::new("parent_line_id", DataType::Int32, true),
            Field::new("section_index", DataType::Int32, true),
            Field::new("section_branch_id", DataType::Int32, true),
            Field::new("from_bus_id", DataType::Int32, true),
            Field::new("to_bus_id", DataType::Int32, true),
            Field::new("status", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
        ],
        schema_metadata(),
    )
}

/// Solved-snapshot dialect of `dc_lines_2w` (v0.12.4 read compatibility).
///
/// Current solved-snapshot exporters emit a solver-oriented MW-setpoint layout
/// for this table. Generic readers accept both; writers in this crate always
/// emit the canonical layout.
pub fn dc_lines_2w_snapshot_dialect_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("dc_line_id", DataType::Int32, true),
            Field::new("from_bus_id", DataType::Int32, true),
            Field::new("to_bus_id", DataType::Int32, true),
            Field::new("status", DataType::Boolean, true),
            Field::new("is_vsc", DataType::Boolean, true),
            Field::new("control_mode", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("p_setpoint_mw", DataType::Float64, true),
            Field::new("p_min_mw", DataType::Float64, true),
            Field::new("p_max_mw", DataType::Float64, true),
            Field::new("loss_factor", DataType::Float64, true),
        ],
        schema_metadata(),
    )
}

/// Solved-snapshot dialect of `switched_shunt_banks` (v0.12.4 read compatibility).
///
/// Current solved-snapshot exporters emit a per-bank control layout for this table.
/// Generic readers accept both; writers in this crate always emit the canonical layout.
pub fn switched_shunt_banks_snapshot_dialect_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bank_id", DataType::Int32, true),
            Field::new("bus_id", DataType::Int32, true),
            Field::new("status", DataType::Boolean, true),
            Field::new("v_low", DataType::Float64, true),
            Field::new("v_high", DataType::Float64, true),
            Field::new(
                "b_steps",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new("current_step", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ],
        schema_metadata(),
    )
}

/// Solved-snapshot dialect of `generators_solved` (v0.12.4 read compatibility).
///
/// Current solved-snapshot exporters omit the per-unit output columns and emit
/// solver-native MW/MVAR fields directly after the identity columns. Generic
/// readers accept both; writers in this crate always emit the canonical layout.
pub fn generators_solved_snapshot_dialect_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("bus_id", DataType::Int32, false),
            Field::new("id", dict_utf8(), false),
            Field::new("p_mw", DataType::Float64, true),
            Field::new("q_mvar", DataType::Float64, true),
            Field::new("status", DataType::Boolean, true),
        ],
        schema_metadata(),
    )
}

/// Returns accepted schema variants for a table, canonical layout first.
///
/// Most tables have a single canonical layout. Three optional extension tables
/// additionally accept a solved-snapshot dialect emitted by current solver
/// exports (v0.12.4). Readers match by field names against each variant in
/// order; writers must always use the canonical (first) layout.
pub fn table_schema_variants(table_name: &str) -> Vec<Schema> {
    let mut variants = Vec::new();
    if let Some(canonical) = table_schema(table_name) {
        variants.push(canonical);
    }
    match table_name {
        TABLE_MULTI_SECTION_LINES => {
            variants.push(multi_section_lines_snapshot_dialect_schema());
        }
        TABLE_DC_LINES_2W => variants.push(dc_lines_2w_snapshot_dialect_schema()),
        TABLE_SWITCHED_SHUNT_BANKS => {
            variants.push(switched_shunt_banks_snapshot_dialect_schema());
        }
        TABLE_GENERATORS_SOLVED => {
            variants.push(generators_solved_snapshot_dialect_schema());
        }
        _ => {}
    }
    variants
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
        SUPPORTED_RPF_VERSIONS, all_table_schemas, branches_schema, contingencies_schema,
        diagram_objects_schema, diagram_points_schema, dynamics_models_schema,
        facts_devices_schema, facts_solved_schema, generators_schema, loads_schema,
        normalize_facts_device_type, perc1_params_struct_type, table_schema,
    };
    use arrow::datatypes::DataType;

    #[test]
    fn v010_schema_contract_spot_check() {
        // contingencies must have exactly 8 fields (2 base + 6 operational outcome cols)
        let c = contingencies_schema();
        assert_eq!(c.fields().len(), 8, "contingencies should have 8 fields");
        assert_eq!(c.field(0).name(), "contingency_id");
        assert_eq!(c.field(2).name(), "risk_score");
        assert_eq!(c.field(7).name(), "greedy_reserve_summary");

        let dm = dynamics_models_schema();
        assert_eq!(dm.fields().len(), 5);
        assert_eq!(dm.field(4).name(), "perc1_params");
        assert_eq!(dm.field(4).data_type(), &perc1_params_struct_type());

        // scenario_context resolvable via table_schema() but absent from all_table_schemas()
        assert!(
            table_schema("scenario_context").is_some(),
            "scenario_context must resolve via table_schema()"
        );
        let all = all_table_schemas();
        assert!(
            !all.iter().any(|(n, _)| *n == "scenario_context"),
            "scenario_context must NOT appear in all_table_schemas()"
        );
        assert_eq!(
            all.len(),
            18,
            "all_table_schemas() must return 18 canonical tables"
        );

        assert!(
            table_schema("computational_load_profiles").is_some(),
            "computational_load_profiles must resolve via table_schema()"
        );

        // v0.11.0 optional protection tables resolve via table_schema() but stay out of
        // all_table_schemas() (optional, like scenario_context / facts_devices).
        assert!(
            table_schema("protection_contingencies").is_some(),
            "protection_contingencies must resolve via table_schema()"
        );
        assert!(
            table_schema("topology_changes").is_some(),
            "topology_changes must resolve via table_schema()"
        );
        assert!(
            table_schema("remedial_action_schemes").is_some(),
            "remedial_action_schemes must resolve via table_schema()"
        );
        assert!(
            table_schema("contingency_island_analysis").is_some(),
            "contingency_island_analysis must resolve via table_schema()"
        );
        assert!(
            !all.iter().any(|(n, _)| *n == "protection_contingencies"
                || *n == "topology_changes"
                || *n == "remedial_action_schemes"
                || *n == "contingency_island_analysis"),
            "optional RAS/protection/island tables must NOT appear in all_table_schemas()"
        );

        // version gate: v0.12.5 current; v0.12.4–v0.12.1 remain readable
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"v0.12.5"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"0.12.5"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"v0.12.4"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"0.12.4"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"v0.12.3"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"0.12.3"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"v0.12.2"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"0.12.2"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"v0.12.1"));
        assert!(SUPPORTED_RPF_VERSIONS.contains(&"0.12.1"));
        assert_eq!(SUPPORTED_RPF_VERSIONS.len(), 10);
    }

    #[test]
    fn v0124_optional_tables_resolve_via_table_schema() {
        let q = super::q_limits_solved_schema();
        assert_eq!(q.fields().len(), 2);
        assert_eq!(q.field(0).name(), "bus_id");
        assert!(!q.field(0).is_nullable());
        assert_eq!(q.field(1).name(), "q_net_target_pu");

        let fc = super::feasibility_certificate_buses_schema();
        assert_eq!(fc.fields().len(), 7);
        assert_eq!(fc.field(0).name(), "bus_id");
        assert!(!fc.field(0).is_nullable());
        assert_eq!(fc.field(6).name(), "violation_kind");

        assert!(table_schema("q_limits_solved").is_some());
        assert!(table_schema("feasibility_certificate_buses").is_some());
        let all = all_table_schemas();
        assert!(
            !all.iter()
                .any(|(n, _)| *n == "q_limits_solved" || *n == "feasibility_certificate_buses"),
            "v0.12.4 optional tables must NOT appear in all_table_schemas()"
        );
    }

    #[test]
    fn snapshot_dialect_variants_are_registered() {
        for table in [
            "multi_section_lines",
            "dc_lines_2w",
            "switched_shunt_banks",
            "generators_solved",
        ] {
            let variants = super::table_schema_variants(table);
            assert_eq!(variants.len(), 2, "{table} must expose canonical + dialect");
            assert_eq!(
                variants[0],
                table_schema(table).unwrap(),
                "{table} canonical variant must come first"
            );
        }
        // Single-layout tables expose exactly the canonical schema.
        assert_eq!(super::table_schema_variants("buses").len(), 1);
        assert!(super::table_schema_variants("nonexistent_table").is_empty());
    }

    #[test]
    fn remedial_action_schemes_match_v012_contract() {
        let ras = super::remedial_action_schemes_schema();
        assert_eq!(ras.fields().len(), 18);
        assert_eq!(ras.field(0).name(), "ras_id");
        assert!(!ras.field(0).is_nullable());
        assert_eq!(ras.field(4).name(), "enabled");
        assert!(!ras.field(4).is_nullable());
        assert_eq!(ras.field(9).name(), "trigger_conditions");
        assert!(!ras.field(9).is_nullable());
        assert_eq!(ras.field(10).name(), "sequence_steps");
        assert!(!ras.field(10).is_nullable());
        assert_eq!(ras.field(16).name(), "data_confidence");
        assert!(!ras.field(16).is_nullable());

        let optional = super::remedial_action_table_schemas();
        assert_eq!(optional.len(), 1);
        assert_eq!(optional[0].0, "remedial_action_schemes");
    }

    #[test]
    fn contingency_island_analysis_match_v0121_contract() {
        let cia = super::contingency_island_analysis_schema();
        assert_eq!(cia.fields().len(), 10);
        assert_eq!(cia.field(0).name(), "contingency_id");
        assert!(!cia.field(0).is_nullable());
        assert_eq!(cia.field(1).name(), "classification");
        assert!(cia.field(1).is_nullable());
        assert_eq!(cia.field(9).name(), "params_snapshot_json");
        assert!(cia.field(9).is_nullable());

        let optional = super::contingency_island_table_schemas();
        assert_eq!(optional.len(), 1);
        assert_eq!(optional[0].0, "contingency_island_analysis");
    }

    #[test]
    fn protection_tables_match_v011_contract() {
        let pc = super::protection_contingencies_schema();
        assert_eq!(pc.fields().len(), 12);
        assert_eq!(pc.field(0).name(), "contingency_id");
        assert!(!pc.field(0).is_nullable());
        assert_eq!(pc.field(1).name(), "protection_group_id");
        assert_eq!(pc.field(3).name(), "scheme_type");
        assert!(!pc.field(3).is_nullable());
        assert_eq!(pc.field(6).name(), "tripped_elements");
        assert!(!pc.field(6).is_nullable());
        // tripped_elements must reuse the exact contingencies.elements struct shape.
        assert_eq!(
            pc.field(6).data_type(),
            super::contingencies_schema().field(1).data_type()
        );
        assert_eq!(pc.field(9).name(), "data_confidence");
        assert!(!pc.field(9).is_nullable());
        assert_eq!(pc.field(10).name(), "breaker_ids");
        assert!(pc.field(10).is_nullable());

        let tc = super::topology_changes_schema();
        assert_eq!(tc.fields().len(), 11);
        assert_eq!(tc.field(0).name(), "topology_change_id");
        assert!(!tc.field(0).is_nullable());
        assert_eq!(tc.field(2).name(), "change_type");
        assert!(!tc.field(2).is_nullable());
        assert_eq!(tc.field(3).name(), "affected_bus_ids");
        assert!(!tc.field(3).is_nullable());
        assert_eq!(tc.field(7).name(), "provenance");
        assert!(tc.field(7).is_nullable());
        assert_eq!(tc.field(9).name(), "change_source");
        assert!(tc.field(9).is_nullable());
        assert_eq!(
            tc.field(9).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8),)
        );
        assert_eq!(tc.field(10).name(), "applied_phase");
        assert!(tc.field(10).is_nullable());
        assert_eq!(
            tc.field(10).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8),)
        );

        // helper ordering: protection_contingencies before topology_changes
        let with_topo = super::protection_table_schemas(true);
        assert_eq!(with_topo.len(), 2);
        assert_eq!(with_topo[0].0, "protection_contingencies");
        assert_eq!(with_topo[1].0, "topology_changes");
        let without_topo = super::protection_table_schemas(false);
        assert_eq!(without_topo.len(), 1);
        assert_eq!(without_topo[0].0, "protection_contingencies");
    }

    #[test]
    fn metadata_schema_v0123_sal_baseline_columns() {
        let meta = super::metadata_schema();
        assert_eq!(meta.fields().len(), 45);
        assert_eq!(meta.field(35).name(), "original_sentinel_case_id");
        assert_eq!(meta.field(35).data_type(), &DataType::Utf8);
        assert!(meta.field(35).is_nullable());
        assert_eq!(meta.field(36).name(), "original_model_version");
        assert_eq!(meta.field(37).name(), "target_baseline_version");
        assert_eq!(meta.field(38).name(), "is_sal_enhanced");
        assert_eq!(meta.field(38).data_type(), &DataType::Boolean);
        assert_eq!(meta.field(39).name(), "sal_enhancement_timestamp");
        assert_eq!(meta.field(40).name(), "cim_model_version_used");
        assert_eq!(meta.field(41).name(), "planning_ready");
        assert_eq!(meta.field(42).name(), "upgrade_summary");
        assert_eq!(meta.field(43).name(), "convergence_time_ms");
        assert_eq!(meta.field(43).data_type(), &DataType::Float64);
        assert_eq!(meta.field(44).name(), "convergence_iterations");
        assert_eq!(meta.field(44).data_type(), &DataType::Int32);
        assert!(meta.field(44).is_nullable());
    }

    #[test]
    fn topology_changes_v0123_change_tracking_columns() {
        let tc = super::topology_changes_schema();
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(tc.field(9).name(), "change_source");
        assert_eq!(tc.field(9).data_type(), &dict);
        assert_eq!(tc.field(10).name(), "applied_phase");
        assert_eq!(tc.field(10).data_type(), &dict);
    }

    #[test]
    fn table_ownership_classifier() {
        assert_eq!(
            super::table_ownership(super::TABLE_BUSES),
            super::TableOwnership::Converter
        );
        assert_eq!(
            super::table_ownership(super::TABLE_BUSES_SOLVED),
            super::TableOwnership::Solver
        );
        assert_eq!(
            super::table_ownership(super::TABLE_METADATA),
            super::TableOwnership::Shared
        );
        assert_eq!(
            super::table_ownership("future_unknown_table"),
            super::TableOwnership::Converter
        );
        assert!(super::is_solver_root_metadata_key(super::METADATA_KEY_CASE_MODE));
        assert!(!super::is_solver_root_metadata_key(
            super::METADATA_KEY_FEATURE_DIAGRAM_LAYOUT
        ));
    }

    #[test]
    fn buses_schema_v094_q_decomposition_columns() {
        let buses = super::buses_schema();
        // v0.12.5: 24 total columns (22 through qg_sched_pu + lat/lon)
        assert_eq!(buses.fields().len(), 24);
        // New columns at indices 20 and 21
        assert_eq!(buses.field(20).name(), "qd_load_pu");
        assert!(!buses.field(20).is_nullable());
        assert_eq!(buses.field(21).name(), "qg_sched_pu");
        assert!(!buses.field(21).is_nullable());
        assert_eq!(buses.field(22).name(), "latitude");
        assert!(buses.field(22).is_nullable());
        assert_eq!(buses.field(23).name(), "longitude");
        assert!(buses.field(23).is_nullable());
    }

    #[test]
    fn nominal_kv_columns_are_required() {
        let buses = super::buses_schema();
        assert!(!buses.field(18).is_nullable());

        let branches = super::branches_schema();
        assert!(!branches.field(15).is_nullable());
        assert!(!branches.field(16).is_nullable());

        let transformers_2w = super::transformers_2w_schema();
        assert!(!transformers_2w.field(20).is_nullable());
        assert!(!transformers_2w.field(21).is_nullable());

        let transformers_3w = super::transformers_3w_schema();
        assert!(!transformers_3w.field(21).is_nullable());
        assert!(!transformers_3w.field(22).is_nullable());
        assert!(!transformers_3w.field(23).is_nullable());
    }

    #[test]
    fn loads_schema_includes_optional_zip_columns() {
        let loads = loads_schema();
        assert_eq!(loads.fields().len(), 10);
        assert_eq!(loads.field(3).name(), "p_pu");
        assert!(!loads.field(3).is_nullable());
        assert_eq!(loads.field(4).name(), "q_pu");
        assert!(!loads.field(4).is_nullable());
        assert_eq!(loads.field(5).name(), "p_i_pu");
        assert!(loads.field(5).is_nullable());
        assert_eq!(loads.field(6).name(), "q_i_pu");
        assert!(loads.field(6).is_nullable());
        assert_eq!(loads.field(7).name(), "p_y_pu");
        assert!(loads.field(7).is_nullable());
        assert_eq!(loads.field(8).name(), "q_y_pu");
        assert!(loads.field(8).is_nullable());
    }

    #[test]
    fn generators_schema_includes_required_q_sched_mvar() {
        let generators = generators_schema();
        assert_eq!(generators.field(10).name(), "p_sched_mw");
        assert_eq!(generators.field(11).name(), "q_sched_mvar");
        assert!(!generators.field(11).is_nullable());
    }

    #[test]
    fn generators_schema_v095_controlled_bus_id() {
        let generators = generators_schema();
        assert_eq!(generators.fields().len(), 26);
        assert_eq!(generators.field(24).name(), "controlled_bus_id");
        assert_eq!(generators.field(24).data_type(), &DataType::Int32);
        assert!(!generators.field(24).is_nullable());
        assert_eq!(generators.field(25).name(), "mrid");
        assert_eq!(generators.field(25).data_type(), &DataType::Utf8);
        assert!(generators.field(25).is_nullable());
    }

    #[test]
    fn equipment_tables_v0122_mrid_columns() {
        for (table_name, schema) in [
            ("branches", branches_schema()),
            ("generators", generators_schema()),
            ("transformers_2w", super::transformers_2w_schema()),
            ("transformers_3w", super::transformers_3w_schema()),
        ] {
            let mrid = schema
                .field_with_name("mrid")
                .unwrap_or_else(|_| panic!("{table_name} must include mrid column"));
            assert_eq!(mrid.data_type(), &DataType::Utf8);
            assert!(mrid.is_nullable(), "{table_name}.mrid must be nullable");
        }
    }

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
        assert_eq!(branches.fields().len(), 28);
        assert_eq!(branches.field(17).name(), "device_type");
        assert_eq!(branches.field(18).name(), "control_mode");
        assert_eq!(branches.field(19).name(), "control_target_flow_mw");
        assert_eq!(branches.field(20).name(), "x_min_pu");
        assert_eq!(branches.field(21).name(), "x_max_pu");
        assert_eq!(branches.field(22).name(), "injected_voltage_mag_pu");
        assert_eq!(branches.field(23).name(), "injected_voltage_angle_deg");
        assert_eq!(branches.field(24).name(), "facts_params");
        assert_eq!(branches.field(25).name(), "parent_line_id");
        assert_eq!(branches.field(26).name(), "section_index");
        assert_eq!(branches.field(27).name(), "mrid");
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
        assert!(
            child_fields
                .iter()
                .any(|field| field.name() == "equipment_kind")
        );
        assert!(
            child_fields
                .iter()
                .any(|field| field.name() == "equipment_id")
        );
    }

    #[test]
    fn smartvalve_alias_normalization_is_canonical() {
        assert_eq!(
            normalize_facts_device_type("smartvalve"),
            Some("smartvalve")
        );
        assert_eq!(normalize_facts_device_type("SV"), Some("smartvalve"));
        assert_eq!(normalize_facts_device_type("sv"), Some("smartvalve"));
        assert_eq!(
            normalize_facts_device_type("smart_valve"),
            Some("smartvalve")
        );
        assert_eq!(normalize_facts_device_type("svc"), None);
    }
}
