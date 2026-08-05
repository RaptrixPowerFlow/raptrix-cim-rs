// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `enhance` — pure-authoring patch of an existing v0.13.0 `.rpf`.
//!
//! Reads an existing `.rpf`, applies a small JSON "enhancement spec" that can
//! add/replace `computational_load_profiles` rows, optionally
//! `dynamics_models` rows, and optionally scale/set load MW at buses via
//! `load_overrides`, then writes a new `.rpf`. This module invents no new
//! schema: it only calls the locked `raptrix-cim-arrow` contract APIs
//! (`read_rpf_tables`, `build_computational_load_profiles_batch`,
//! `patch_metadata_computational_load_mode`, `write_root_rpf_with_metadata`)
//! and a small local `dynamics_models` batch builder that mirrors the
//! existing (private) converter-side builder, extended to accept
//! `classical_params` from the spec. Load overrides mutate only the existing
//! `loads` table columns (`p_pu` / `q_pu` and optional ZIP terms).
//!
//! ## Enhancement spec JSON shape
//!
//! ```json
//! {
//!   "computational_load_profiles": [
//!     {
//!       "bus_id": 110013,
//!       "facility_class": "ai_hpc",
//!       "common_mode_group": "ashburn_campus_a",
//!       "priority": 1,
//!       "max_step_drop_mw": 800.0,
//!       "trip_study_percentiles": [60.0, 100.0],
//!       "transfer_to_backup_threshold_pu": 0.90,
//!       "transfer_delay_ms": 50.0,
//!       "poi_name": "Campus A POI 1"
//!     }
//!   ],
//!   "dynamics_models": [
//!     {
//!       "bus_id": 1,
//!       "gen_id": "1",
//!       "model_type": "GENCLS",
//!       "params": {},
//!       "classical_params": { "H": 5.0, "D": 0.0, "xd_prime": 0.25, "mbase_mva": 100.0 }
//!     }
//!   ],
//!   "load_overrides": [
//!     { "bus_id": 110013, "p_mw": 800.0, "q_mw": 200.0 },
//!     { "bus_id": 110123, "scale_p": 10.0 }
//!   ],
//!   "computational_load_mode": true
//! }
//! ```
//!
//! Rules:
//! - `computational_load_profiles` accepts every field on
//!   [`ComputationalLoadProfileRow`](crate::arrow_schema::ComputationalLoadProfileRow) by
//!   name. Omit fields you don't need; `bus_id` **xor** `load_id` must be set per row when
//!   `computational_load_mode` ends up `true` (enforced by
//!   `validate_computational_load_profiles_batch`).
//! - If `computational_load_profiles` is **omitted**, the input's existing table (if any) is
//!   preserved unchanged.
//! - If `computational_load_profiles` is **present** (including an empty array), it fully
//!   replaces the table — an empty array clears it.
//! - If `dynamics_models` is **omitted**, the input's existing `dynamics_models` table is
//!   preserved unchanged.
//! - If `dynamics_models` is **present**, it fully replaces the table. Each row requires
//!   `bus_id`, `gen_id`, `model_type`; `params` (string→f64 map) and `classical_params`
//!   (`{H, D, xd_prime, mbase_mva}`, all optional) default to empty/absent. `perc1_params` is
//!   not settable via `enhance` (always written null, matching upstream converter behavior).
//! - `load_overrides` (optional) scales and/or sets absolute load MW on the `loads` table for
//!   named `bus_id`s. `p_mw`/`q_mw` are **bus-total** campus injections (converted to
//!   `p_pu`/`q_pu` via `metadata.base_mva`, default 100): the first in-service load row
//!   at the bus receives the absolute value; any sibling rows at that bus are zeroed so
//!   multi-id buses do not multiply the campus MW. `scale_p`/`scale_q` still multiply
//!   every load row at the bus. Scale is applied first; absolute set then wins.
//!   Absolute set clears ZIP I/Y (null). If a bus has no load row, a minimal row
//!   (`status=true`, `id="DC1"`) is created only when absolute `p_mw` is provided.
//!   Core trusts `buses.p_sched` when the bus table already carries aggregate injections
//!   (it does **not** rebuild from loads in that case), so `enhance` also adjusts
//!   `buses.p_sched` / `q_sched` (and `qd_load_pu` when present) by the load delta at
//!   each overridden bus.
//! - `metadata.computational_load_mode` is set to `true` automatically whenever the resolved
//!   `computational_load_profiles` table is non-empty, unless the spec's top-level
//!   `computational_load_mode` explicitly overrides it (e.g. to author an unsized/staged
//!   skeleton without flipping the file into computational-load mode yet).
//! - Every other table (buses, branches, generators, contingencies, diagram layout,
//!   node-breaker detail, FACTS, RAS, etc.) is preserved byte-for-byte from the input.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, RecordBatch, StringArray, StringBuilder, StringDictionaryBuilder,
    new_null_array,
};
use arrow::datatypes::{DataType, Int32Type, UInt32Type};
use serde::Deserialize;

use crate::arrow_schema::{
    BuildoutEntry, ComputationalLoadProfileRow, DisturbanceCounter,
    METADATA_KEY_FEATURE_CONTINGENCIES_STUB, METADATA_KEY_FEATURE_DYNAMICS_STUB,
    ProtectionSettingsProvenance, ReconnectionParams, RootWriteOptions, SeasonalEnvelopeEntry,
    TABLE_BUSES, TABLE_BUSES_SOLVED, TABLE_COMPUTATIONAL_LOAD_PROFILES,
    TABLE_CONTINGENCY_ISLAND_ANALYSIS, TABLE_DIAGRAM_OBJECTS, TABLE_DYNAMICS_MODELS,
    TABLE_FACTS_DEVICES, TABLE_FACTS_SOLVED, TABLE_LOADS, TABLE_METADATA,
    TABLE_NODE_BREAKER_DETAIL, TABLE_PROTECTION_CONTINGENCIES, TABLE_REMEDIAL_ACTION_SCHEMES,
    TABLE_TOPOLOGY_CHANGES, VoltageMeasurement, VoltageTransferCurveStage,
    build_computational_load_profiles_batch, loads_schema,
    patch_metadata_computational_load_mode, read_rpf_tables, rpf_file_metadata,
    validate_computational_load_profiles_batch, write_root_rpf_with_metadata,
};

/// Human-readable summary returned after a successful `enhance` run.
#[derive(Debug, Clone)]
pub struct EnhanceSummary {
    /// Total distinct tables written to the output `.rpf`.
    pub tables_written: usize,
    /// Whether the spec's `dynamics_models` key was present (table replaced).
    pub dynamics_models_replaced: bool,
    /// Row count of the `dynamics_models` table in the output.
    pub dynamics_models_rows: usize,
    /// Whether the spec's `computational_load_profiles` key was present (table replaced).
    pub computational_load_profiles_replaced: bool,
    /// Whether `computational_load_profiles` is present at all in the output (replaced or
    /// carried over from the input).
    pub computational_load_profiles_included: bool,
    /// Row count of the `computational_load_profiles` table in the output (0 if absent).
    pub computational_load_profiles_rows: usize,
    /// Resolved value written to `metadata.computational_load_mode` (`None` clears it).
    pub computational_load_mode: Option<bool>,
    /// Number of `load_overrides` entries applied from the spec (`0` if omitted/empty).
    pub load_overrides_applied: usize,
    /// Number of new `loads` rows created because a bus had no existing load and `p_mw` was set.
    pub load_rows_created: usize,
}

/// Reads `input_path`, applies the enhancement spec at `spec_path`, and writes the result to
/// `output_path`. See the module docs for the JSON spec shape and replace/preserve rules.
pub fn run_enhance(
    input_path: impl AsRef<Path>,
    spec_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<EnhanceSummary> {
    let input_path = input_path.as_ref();
    let spec_path = spec_path.as_ref();
    let output_path = output_path.as_ref();

    let spec_text = fs::read_to_string(spec_path)
        .with_context(|| format!("failed to read enhancement spec at {}", spec_path.display()))?;
    let spec: EnhanceSpecFile = serde_json::from_str(&spec_text).with_context(|| {
        format!(
            "failed to parse enhancement spec JSON at {}",
            spec_path.display()
        )
    })?;

    let existing_tables = read_rpf_tables(input_path)
        .with_context(|| format!("failed to read input .rpf at {}", input_path.display()))?;
    let root_metadata = rpf_file_metadata(input_path).with_context(|| {
        format!(
            "failed to read input .rpf metadata at {}",
            input_path.display()
        )
    })?;

    let mut present_names: HashSet<String> = HashSet::new();
    let mut table_map: HashMap<&'static str, RecordBatch> = HashMap::new();
    for (name, batch) in existing_tables {
        present_names.insert(name.clone());
        // The table set is bounded and known ahead of time; leaking the (short, deduplicated)
        // name string for the lifetime of this one-shot CLI process is the same pattern used
        // by `raptrix-cim-arrow`'s own round-trip tests (see `io.rs`).
        let key: &'static str = Box::leak(name.into_boxed_str());
        table_map.insert(key, batch);
    }

    if !table_map.contains_key(TABLE_METADATA) {
        bail!(
            "input .rpf at {} is missing the required 'metadata' table",
            input_path.display()
        );
    }

    let EnhanceSpecFile {
        computational_load_profiles: clp_spec,
        dynamics_models: dynamics_spec,
        load_overrides,
        computational_load_mode: explicit_mode,
        _provenance: _,
    } = spec;

    let dynamics_models_replaced = dynamics_spec.is_some();
    if let Some(specs) = dynamics_spec {
        let batch = build_dynamics_models_batch_from_spec(&specs)
            .context("failed to build dynamics_models table from enhancement spec")?;
        table_map.insert(TABLE_DYNAMICS_MODELS, batch);
    }
    let dynamics_models_rows = table_map
        .get(TABLE_DYNAMICS_MODELS)
        .map(RecordBatch::num_rows)
        .unwrap_or(0);

    let load_overrides = load_overrides.unwrap_or_default();
    let load_overrides_applied = load_overrides.len();
    let mut load_rows_created = 0usize;
    if !load_overrides.is_empty() {
        let base_mva = metadata_base_mva(table_map.get(TABLE_METADATA))?;
        let loads_batch = table_map
            .remove(TABLE_LOADS)
            .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(loads_schema())));
        let old_load_pq = sum_active_load_pu_by_bus(&loads_batch)
            .context("failed to sum pre-override load injections by bus")?;
        let (patched_loads, created) =
            apply_load_overrides(&loads_batch, &load_overrides, base_mva)
                .context("failed to apply load_overrides to loads table")?;
        let new_load_pq = sum_active_load_pu_by_bus(&patched_loads)
            .context("failed to sum post-override load injections by bus")?;
        load_rows_created = created;
        table_map.insert(TABLE_LOADS, patched_loads);
        present_names.insert(TABLE_LOADS.to_string());

        // Keep buses.p_sched / q_sched consistent with the loads table. Core only
        // rebuilds bus schedules from loads when the bus-table L1 is ~0.
        if let Some(buses_batch) = table_map.remove(TABLE_BUSES) {
            let patched_buses = sync_bus_schedules_for_load_delta(
                &buses_batch,
                &old_load_pq,
                &new_load_pq,
            )
            .context("failed to sync buses.p_sched after load_overrides")?;
            table_map.insert(TABLE_BUSES, patched_buses);
        }
    }

    let had_clp_table = present_names.contains(TABLE_COMPUTATIONAL_LOAD_PROFILES);
    let computational_load_profiles_replaced = clp_spec.is_some();
    if let Some(specs) = clp_spec {
        let rows: Vec<ComputationalLoadProfileRow> =
            specs.into_iter().map(clp_spec_to_row).collect();
        let batch = build_computational_load_profiles_batch(&rows)
            .context("failed to build computational_load_profiles table from enhancement spec")?;
        table_map.insert(TABLE_COMPUTATIONAL_LOAD_PROFILES, batch);
    }

    let include_computational_load_profiles = computational_load_profiles_replaced || had_clp_table;
    let computational_load_profiles_rows = table_map
        .get(TABLE_COMPUTATIONAL_LOAD_PROFILES)
        .map(RecordBatch::num_rows)
        .unwrap_or(0);

    let resolved_mode = match explicit_mode {
        Some(explicit) => Some(explicit),
        None if computational_load_profiles_rows > 0 => Some(true),
        None => None,
    };

    if include_computational_load_profiles {
        let clp_batch = table_map
            .get(TABLE_COMPUTATIONAL_LOAD_PROFILES)
            .context("internal error: computational_load_profiles table missing after assembly")?;
        validate_computational_load_profiles_batch(clp_batch, resolved_mode)
            .context("computational_load_profiles failed contract validation")?;
    } else if resolved_mode == Some(true) {
        bail!(
            "computational_load_mode=true requires a non-empty 'computational_load_profiles' \
             table; the input has none and the spec did not add any rows"
        );
    }

    let metadata_batch = table_map
        .remove(TABLE_METADATA)
        .context("internal error: 'metadata' table missing during enhancement")?;
    let patched_metadata = patch_metadata_computational_load_mode(&metadata_batch, resolved_mode)
        .context("failed to patch metadata.computational_load_mode")?;
    table_map.insert(TABLE_METADATA, patched_metadata);

    let flag_true = |key: &str| {
        root_metadata
            .get(key)
            .map(|value| value == "true")
            .unwrap_or(false)
    };

    let options = RootWriteOptions {
        include_node_breaker_detail: present_names.contains(TABLE_NODE_BREAKER_DETAIL),
        include_diagram_layout: present_names.contains(TABLE_DIAGRAM_OBJECTS),
        contingencies_are_stub: flag_true(METADATA_KEY_FEATURE_CONTINGENCIES_STUB),
        // A freshly authored dynamics_models table is no longer stub-derived.
        dynamics_are_stub: if dynamics_models_replaced {
            false
        } else {
            flag_true(METADATA_KEY_FEATURE_DYNAMICS_STUB)
        },
        include_solved_state: present_names.contains(TABLE_BUSES_SOLVED),
        include_facts_devices: present_names.contains(TABLE_FACTS_DEVICES),
        include_facts_solved: present_names.contains(TABLE_FACTS_SOLVED),
        include_computational_load_profiles,
        include_protection_contingencies: present_names.contains(TABLE_PROTECTION_CONTINGENCIES),
        include_topology_changes: present_names.contains(TABLE_TOPOLOGY_CHANGES),
        include_remedial_action_schemes: present_names.contains(TABLE_REMEDIAL_ACTION_SCHEMES),
        include_contingency_island_analysis: present_names
            .contains(TABLE_CONTINGENCY_ISLAND_ANALYSIS),
    };

    // Carry over all other root-level file metadata (case fingerprint, topology diagnostics,
    // case_mode, solved-state provenance, custom keys, etc). Row-count keys are recomputed by
    // `write_root_rpf_with_metadata` from the actual table batches, so stale counts from the
    // input file must not be passed through.
    let mut additional_root_metadata: HashMap<String, String> = HashMap::new();
    for (key, value) in &root_metadata {
        if key.starts_with("rpf.rows.") {
            continue;
        }
        additional_root_metadata.insert(key.clone(), value.clone());
    }

    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    write_root_rpf_with_metadata(output_path, &table_map, &options, &additional_root_metadata)
        .with_context(|| format!("failed to write enhanced .rpf to {}", output_path.display()))?;

    Ok(EnhanceSummary {
        tables_written: table_map.len(),
        dynamics_models_replaced,
        dynamics_models_rows,
        computational_load_profiles_replaced,
        computational_load_profiles_included: include_computational_load_profiles,
        computational_load_profiles_rows,
        computational_load_mode: resolved_mode,
        load_overrides_applied,
        load_rows_created,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnhanceSpecFile {
    #[serde(default)]
    computational_load_profiles: Option<Vec<ComputationalLoadProfileSpec>>,
    #[serde(default)]
    dynamics_models: Option<Vec<DynamicsModelSpec>>,
    #[serde(default)]
    load_overrides: Option<Vec<LoadOverrideSpec>>,
    #[serde(default)]
    computational_load_mode: Option<bool>,
    /// Optional authoring notes; ignored by the enhancer.
    #[serde(default, rename = "_provenance")]
    _provenance: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadOverrideSpec {
    bus_id: i32,
    #[serde(default)]
    p_mw: Option<f64>,
    #[serde(default)]
    q_mw: Option<f64>,
    #[serde(default)]
    scale_p: Option<f64>,
    #[serde(default)]
    scale_q: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ComputationalLoadProfileSpec {
    #[serde(default)]
    bus_id: Option<i32>,
    #[serde(default)]
    load_id: Option<String>,
    #[serde(default)]
    seasonal_envelope: Option<Vec<SeasonalEnvelopeSpec>>,
    #[serde(default)]
    buildout_schedule: Option<Vec<BuildoutEntrySpec>>,
    #[serde(default)]
    ramp_rate_up_mw_per_min: Option<f32>,
    #[serde(default)]
    ramp_rate_down_mw_per_min: Option<f32>,
    #[serde(default)]
    it_load_percent: Option<f32>,
    #[serde(default)]
    non_it_load_percent: Option<f32>,
    #[serde(default)]
    it_allocation_mode: Option<String>,
    #[serde(default)]
    ups_config: Option<HashMap<String, f64>>,
    #[serde(default)]
    pcc_relay_settings: Option<HashMap<String, f64>>,
    #[serde(default)]
    onsite_gen_bess_mw: Option<f32>,
    #[serde(default)]
    onsite_gen_parallel: Option<bool>,
    #[serde(default)]
    bess_ramp_rate_mw_per_min: Option<f32>,
    #[serde(default)]
    facility_use_case_percent: Option<HashMap<String, f64>>,
    #[serde(default)]
    mrid: Option<String>,
    #[serde(default)]
    poi_name: Option<String>,
    #[serde(default)]
    facility_class: Option<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    max_step_drop_mw: Option<f32>,
    #[serde(default)]
    trip_study_percentiles: Option<Vec<f32>>,
    #[serde(default)]
    common_mode_group: Option<String>,
    #[serde(default)]
    voltage_sensitivity_hint: Option<f32>,
    #[serde(default)]
    transfer_to_backup_threshold_pu: Option<f32>,
    #[serde(default)]
    transfer_delay_ms: Option<f32>,
    #[serde(default)]
    reconnection_criteria: Option<HashMap<String, f64>>,
    #[serde(default)]
    ride_through_capability: Option<HashMap<String, f64>>,
    #[serde(default)]
    voltage_transfer_curve: Option<Vec<VoltageTransferCurveStageSpec>>,
    #[serde(default)]
    disturbance_counter: Option<DisturbanceCounterSpec>,
    #[serde(default)]
    reconnection_params: Option<ReconnectionParamsSpec>,
    #[serde(default)]
    voltage_measurement: Option<VoltageMeasurementSpec>,
    #[serde(default)]
    protection_settings_provenance: Option<ProtectionSettingsProvenanceSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoltageTransferCurveStageSpec {
    v_pu: f32,
    t_ms: f32,
    polarity: String,
    action: String,
    #[serde(default)]
    mw_fraction: Option<f32>,
    #[serde(default)]
    load_class: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DisturbanceCounterSpec {
    #[serde(default)]
    strike_limit: Option<i32>,
    #[serde(default)]
    window_sec: Option<f32>,
    #[serde(default)]
    qualifying_v_pu: Option<f32>,
    #[serde(default)]
    qualifying_duration_ms: Option<f32>,
    #[serde(default)]
    latch_permanent: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconnectionParamsSpec {
    #[serde(default)]
    v_recover_pu: Option<f32>,
    #[serde(default)]
    delay_ms: Option<f32>,
    #[serde(default)]
    ramp_mw_per_min: Option<f32>,
    #[serde(default)]
    manual_reset_required: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoltageMeasurementSpec {
    #[serde(default)]
    basis: Option<String>,
    #[serde(default)]
    filter_time_constant_ms: Option<f32>,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    reset_hysteresis_pu: Option<f32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtectionSettingsProvenanceSpec {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    profile_id: Option<String>,
    /// RFC3339 or epoch microseconds; parsed loosely via i64 when numeric.
    #[serde(default)]
    effective_date_us: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeasonalEnvelopeSpec {
    season: String,
    min_mw: f32,
    max_mw: f32,
    pf: f32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildoutEntrySpec {
    year: i32,
    mw: f32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DynamicsModelSpec {
    bus_id: i32,
    gen_id: String,
    model_type: String,
    #[serde(default)]
    params: HashMap<String, f64>,
    #[serde(default)]
    classical_params: Option<ClassicalParamsSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassicalParamsSpec {
    #[serde(rename = "H", default)]
    h: Option<f64>,
    #[serde(rename = "D", default)]
    d: Option<f64>,
    #[serde(default)]
    xd_prime: Option<f64>,
    #[serde(default)]
    mbase_mva: Option<f64>,
}

/// Reads `metadata.base_mva`, defaulting to 100 when missing/non-positive (Texas7k / PSS/E norm).
fn metadata_base_mva(metadata: Option<&RecordBatch>) -> Result<f64> {
    let Some(meta) = metadata else {
        return Ok(100.0);
    };
    if meta.num_rows() == 0 {
        return Ok(100.0);
    }
    let Ok(idx) = meta.schema().index_of("base_mva") else {
        return Ok(100.0);
    };
    let Some(arr) = meta.column(idx).as_any().downcast_ref::<Float64Array>() else {
        bail!("metadata.base_mva must be Float64");
    };
    if !arr.is_valid(0) {
        return Ok(100.0);
    }
    let value = arr.value(0);
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Ok(100.0)
    }
}

#[derive(Debug, Clone)]
struct LoadRowState {
    bus_id: i32,
    id: String,
    status: bool,
    p_pu: f64,
    q_pu: f64,
    p_i_pu: Option<f64>,
    q_i_pu: Option<f64>,
    p_y_pu: Option<f64>,
    q_y_pu: Option<f64>,
    name: Option<String>,
    mrid: Option<String>,
}

/// Sum in-service load `p_pu` / `q_pu` by bus_id (load convention: positive MW/MVAR).
fn sum_active_load_pu_by_bus(loads: &RecordBatch) -> Result<HashMap<i32, (f64, f64)>> {
    let mut out: HashMap<i32, (f64, f64)> = HashMap::new();
    if loads.num_rows() == 0 {
        return Ok(out);
    }
    let bus_id = required_i32_col(loads, "bus_id")?;
    let status = required_bool_col(loads, "status")?;
    let p_pu = required_f64_col(loads, "p_pu")?;
    let q_pu = required_f64_col(loads, "q_pu")?;
    for i in 0..loads.num_rows() {
        if !status.value(i) {
            continue;
        }
        let entry = out.entry(bus_id.value(i)).or_insert((0.0, 0.0));
        entry.0 += p_pu.value(i);
        entry.1 += q_pu.value(i);
    }
    Ok(out)
}

/// Adjust `buses.p_sched` / `q_sched` by −Δload when loads change.
///
/// Injection convention: `p_sched = gen_pu − load_pu`. Increasing load therefore
/// decreases `p_sched` by the same per-unit amount. Also refreshes `qd_load_pu`
/// when that column is present.
fn sync_bus_schedules_for_load_delta(
    buses: &RecordBatch,
    old_load_pq: &HashMap<i32, (f64, f64)>,
    new_load_pq: &HashMap<i32, (f64, f64)>,
) -> Result<RecordBatch> {
    let mut touched: HashSet<i32> = HashSet::new();
    touched.extend(old_load_pq.keys().copied());
    touched.extend(new_load_pq.keys().copied());
    if touched.is_empty() {
        return Ok(buses.clone());
    }

    let bus_id_idx = buses.schema().index_of("bus_id").context("buses missing bus_id")?;
    let p_idx = buses.schema().index_of("p_sched").context("buses missing p_sched")?;
    let q_idx = buses.schema().index_of("q_sched").context("buses missing q_sched")?;
    let qd_idx = buses.schema().index_of("qd_load_pu").ok();

    let bus_ids = buses
        .column(bus_id_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("buses.bus_id must be Int32")?;
    let p_sched = buses
        .column(p_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("buses.p_sched must be Float64")?;
    let q_sched = buses
        .column(q_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("buses.q_sched must be Float64")?;
    let qd_load = qd_idx.and_then(|idx| {
        buses
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
    });

    let n = buses.num_rows();
    let mut p_builder = Float64Builder::with_capacity(n);
    let mut q_builder = Float64Builder::with_capacity(n);
    let mut qd_builder = qd_idx.map(|_| Float64Builder::with_capacity(n));

    for i in 0..n {
        let bid = bus_ids.value(i);
        let (old_p, old_q) = old_load_pq.get(&bid).copied().unwrap_or((0.0, 0.0));
        let (new_p, new_q) = new_load_pq.get(&bid).copied().unwrap_or((0.0, 0.0));
        let dp = new_p - old_p;
        let dq = new_q - old_q;

        if p_sched.is_valid(i) {
            p_builder.append_value(p_sched.value(i) - dp);
        } else {
            p_builder.append_null();
        }
        if q_sched.is_valid(i) {
            q_builder.append_value(q_sched.value(i) - dq);
        } else {
            q_builder.append_null();
        }
        if let Some(ref mut qb) = qd_builder {
            match qd_load {
                Some(arr) if arr.is_valid(i) => {
                    // qd_load_pu tracks load Q; replace with the new aggregate when touched.
                    if touched.contains(&bid) {
                        qb.append_value(new_q);
                    } else {
                        qb.append_value(arr.value(i));
                    }
                }
                Some(_) => qb.append_null(),
                None => qb.append_null(),
            }
        }
    }

    let mut columns: Vec<ArrayRef> = buses.columns().to_vec();
    columns[p_idx] = Arc::new(p_builder.finish());
    columns[q_idx] = Arc::new(q_builder.finish());
    if let (Some(idx), Some(mut builder)) = (qd_idx, qd_builder) {
        columns[idx] = Arc::new(builder.finish());
    }
    RecordBatch::try_new(buses.schema(), columns)
        .context("failed to rebuild buses batch after load-schedule sync")
}

fn apply_load_overrides(
    loads: &RecordBatch,
    overrides: &[LoadOverrideSpec],
    base_mva: f64,
) -> Result<(RecordBatch, usize)> {
    if !(base_mva.is_finite() && base_mva > 0.0) {
        bail!("invalid base_mva for load_overrides: {base_mva}");
    }

    let mut rows = load_rows_from_batch(loads)?;
    let mut created = 0usize;

    for ov in overrides {
        if ov.p_mw.is_none() && ov.q_mw.is_none() && ov.scale_p.is_none() && ov.scale_q.is_none() {
            bail!(
                "load_overrides entry for bus_id={} must set at least one of p_mw, q_mw, scale_p, scale_q",
                ov.bus_id
            );
        }
        for scale in [ov.scale_p, ov.scale_q].into_iter().flatten() {
            if !scale.is_finite() {
                bail!(
                    "load_overrides scale for bus_id={} must be finite (got {scale})",
                    ov.bus_id
                );
            }
        }
        for mw in [ov.p_mw, ov.q_mw].into_iter().flatten() {
            if !mw.is_finite() {
                bail!(
                    "load_overrides MW for bus_id={} must be finite (got {mw})",
                    ov.bus_id
                );
            }
        }

        let mut indices: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.bus_id == ov.bus_id)
            .map(|(i, _)| i)
            .collect();

        if indices.is_empty() {
            if ov.p_mw.is_none() {
                bail!(
                    "load_overrides bus_id={} has no load rows; creating a row requires absolute p_mw",
                    ov.bus_id
                );
            }
            let mut row = LoadRowState {
                bus_id: ov.bus_id,
                id: "DC1".to_string(),
                status: true,
                p_pu: 0.0,
                q_pu: 0.0,
                p_i_pu: None,
                q_i_pu: None,
                p_y_pu: None,
                q_y_pu: None,
                name: None,
                mrid: None,
            };
            apply_override_to_row(&mut row, ov, base_mva);
            rows.push(row);
            created += 1;
            continue;
        }

        // Prefer an in-service row as the primary campus injection target.
        indices.sort_by_key(|&i| !rows[i].status);
        let primary = indices[0];

        // Absolute p_mw/q_mw are bus-total: apply to primary, zero siblings so
        // multi-id buses do not N× the campus MW. Scales still hit every row.
        let has_absolute = ov.p_mw.is_some() || ov.q_mw.is_some();
        if has_absolute {
            for &i in &indices {
                if i == primary {
                    apply_override_to_row(&mut rows[i], ov, base_mva);
                } else {
                    // Scale siblings first (if any), then clear absolute quantities
                    // that were set on the bus-total override.
                    let scale_only = LoadOverrideSpec {
                        bus_id: ov.bus_id,
                        p_mw: None,
                        q_mw: None,
                        scale_p: ov.scale_p,
                        scale_q: ov.scale_q,
                    };
                    if scale_only.scale_p.is_some() || scale_only.scale_q.is_some() {
                        apply_override_to_row(&mut rows[i], &scale_only, base_mva);
                    }
                    if ov.p_mw.is_some() {
                        rows[i].p_pu = 0.0;
                        rows[i].p_i_pu = None;
                        rows[i].p_y_pu = None;
                    }
                    if ov.q_mw.is_some() {
                        rows[i].q_pu = 0.0;
                        rows[i].q_i_pu = None;
                        rows[i].q_y_pu = None;
                    }
                }
            }
        } else {
            for &i in &indices {
                apply_override_to_row(&mut rows[i], ov, base_mva);
            }
        }
    }

    Ok((build_loads_batch_from_rows(&rows)?, created))
}

fn apply_override_to_row(row: &mut LoadRowState, ov: &LoadOverrideSpec, base_mva: f64) {
    // Scale first, then absolute set wins for that quantity.
    if let Some(scale_p) = ov.scale_p {
        row.p_pu *= scale_p;
        if let Some(v) = row.p_i_pu.as_mut() {
            *v *= scale_p;
        }
        if let Some(v) = row.p_y_pu.as_mut() {
            *v *= scale_p;
        }
    }
    if let Some(scale_q) = ov.scale_q {
        row.q_pu *= scale_q;
        if let Some(v) = row.q_i_pu.as_mut() {
            *v *= scale_q;
        }
        if let Some(v) = row.q_y_pu.as_mut() {
            *v *= scale_q;
        }
    }
    if let Some(p_mw) = ov.p_mw {
        row.p_pu = p_mw / base_mva;
        row.p_i_pu = None;
        row.p_y_pu = None;
    }
    if let Some(q_mw) = ov.q_mw {
        row.q_pu = q_mw / base_mva;
        row.q_i_pu = None;
        row.q_y_pu = None;
    }
}

fn load_rows_from_batch(loads: &RecordBatch) -> Result<Vec<LoadRowState>> {
    let n = loads.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }

    let bus_id = required_i32_col(loads, "bus_id")?;
    let status = required_bool_col(loads, "status")?;
    let p_pu = required_f64_col(loads, "p_pu")?;
    let q_pu = required_f64_col(loads, "q_pu")?;
    let id_col = loads
        .column(loads.schema().index_of("id").context("loads missing id")?)
        .clone();
    let id_utf8 =
        arrow::compute::cast(&id_col, &DataType::Utf8).context("casting loads.id to Utf8")?;
    let id_arr = id_utf8
        .as_any()
        .downcast_ref::<StringArray>()
        .context("loads.id cast did not yield Utf8")?;

    let name_utf8 = optional_utf8_col(loads, "name")?;
    let mrid = optional_utf8_col(loads, "mrid")?;
    let p_i_pu = optional_f64_col(loads, "p_i_pu")?;
    let q_i_pu = optional_f64_col(loads, "q_i_pu")?;
    let p_y_pu = optional_f64_col(loads, "p_y_pu")?;
    let q_y_pu = optional_f64_col(loads, "q_y_pu")?;

    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        rows.push(LoadRowState {
            bus_id: bus_id.value(i),
            id: id_arr.value(i).to_string(),
            status: status.value(i),
            p_pu: p_pu.value(i),
            q_pu: q_pu.value(i),
            p_i_pu: opt_f64_at(p_i_pu.as_ref(), i),
            q_i_pu: opt_f64_at(q_i_pu.as_ref(), i),
            p_y_pu: opt_f64_at(p_y_pu.as_ref(), i),
            q_y_pu: opt_f64_at(q_y_pu.as_ref(), i),
            name: name_utf8
                .as_ref()
                .and_then(|arr| (!arr.is_null(i)).then(|| arr.value(i).to_string())),
            mrid: mrid
                .as_ref()
                .and_then(|arr| (!arr.is_null(i)).then(|| arr.value(i).to_string())),
        });
    }
    Ok(rows)
}

fn build_loads_batch_from_rows(rows: &[LoadRowState]) -> Result<RecordBatch> {
    let schema = Arc::new(loads_schema());
    let n = rows.len();

    let mut bus_id_b = Int32Builder::with_capacity(n);
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut status_b = BooleanBuilder::with_capacity(n);
    let mut p_pu_b = Float64Builder::with_capacity(n);
    let mut q_pu_b = Float64Builder::with_capacity(n);
    let mut p_i_pu_b = Float64Builder::with_capacity(n);
    let mut q_i_pu_b = Float64Builder::with_capacity(n);
    let mut p_y_pu_b = Float64Builder::with_capacity(n);
    let mut q_y_pu_b = Float64Builder::with_capacity(n);
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut mrid_b = StringBuilder::with_capacity(n, n * 8);

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_str())
            .context("failed to append loads.id")?;
        status_b.append_value(row.status);
        p_pu_b.append_value(row.p_pu);
        q_pu_b.append_value(row.q_pu);
        append_opt_f64(&mut p_i_pu_b, row.p_i_pu);
        append_opt_f64(&mut q_i_pu_b, row.q_i_pu);
        append_opt_f64(&mut p_y_pu_b, row.p_y_pu);
        append_opt_f64(&mut q_y_pu_b, row.q_y_pu);
        match &row.name {
            Some(name) => {
                name_b
                    .append(name.as_str())
                    .context("failed to append loads.name")?;
            }
            None => {
                name_b.append_null();
            }
        }
        match &row.mrid {
            Some(mrid) => mrid_b.append_value(mrid),
            None => mrid_b.append_null(),
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id_b.finish()) as ArrayRef,
            Arc::new(id_b.finish()) as ArrayRef,
            Arc::new(status_b.finish()) as ArrayRef,
            Arc::new(p_pu_b.finish()) as ArrayRef,
            Arc::new(q_pu_b.finish()) as ArrayRef,
            Arc::new(p_i_pu_b.finish()) as ArrayRef,
            Arc::new(q_i_pu_b.finish()) as ArrayRef,
            Arc::new(p_y_pu_b.finish()) as ArrayRef,
            Arc::new(q_y_pu_b.finish()) as ArrayRef,
            Arc::new(name_b.finish()) as ArrayRef,
            Arc::new(mrid_b.finish()) as ArrayRef,
        ],
    )
    .context("failed to rebuild loads record batch after load_overrides")
}

fn append_opt_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn required_i32_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    let idx = batch.schema().index_of(name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .with_context(|| format!("loads.{name} must be Int32"))
}

fn required_bool_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
    let idx = batch.schema().index_of(name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| format!("loads.{name} must be Boolean"))
}

fn required_f64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    let idx = batch.schema().index_of(name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .with_context(|| format!("loads.{name} must be Float64"))
}

fn optional_f64_col(batch: &RecordBatch, name: &str) -> Result<Option<Float64Array>> {
    let Ok(idx) = batch.schema().index_of(name) else {
        return Ok(None);
    };
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .with_context(|| format!("loads.{name} must be Float64"))?;
    Ok(Some(arr.clone()))
}

fn optional_utf8_col(batch: &RecordBatch, name: &str) -> Result<Option<StringArray>> {
    let Ok(idx) = batch.schema().index_of(name) else {
        return Ok(None);
    };
    let col = batch.column(idx);
    let casted = arrow::compute::cast(col, &DataType::Utf8)
        .with_context(|| format!("casting loads.{name} to Utf8"))?;
    let arr = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("loads.{name} cast did not yield Utf8"))?
        .clone();
    Ok(Some(arr))
}

fn opt_f64_at(arr: Option<&Float64Array>, row: usize) -> Option<f64> {
    arr.and_then(|a| a.is_valid(row).then(|| a.value(row)))
}

fn clp_spec_to_row(spec: ComputationalLoadProfileSpec) -> ComputationalLoadProfileRow {
    ComputationalLoadProfileRow {
        bus_id: spec.bus_id,
        load_id: spec.load_id,
        seasonal_envelope: spec.seasonal_envelope.map(|items| {
            items
                .into_iter()
                .map(|item| SeasonalEnvelopeEntry {
                    season: item.season,
                    min_mw: item.min_mw,
                    max_mw: item.max_mw,
                    pf: item.pf,
                })
                .collect()
        }),
        buildout_schedule: spec.buildout_schedule.map(|items| {
            items
                .into_iter()
                .map(|item| BuildoutEntry {
                    year: item.year,
                    mw: item.mw,
                })
                .collect()
        }),
        ramp_rate_up_mw_per_min: spec.ramp_rate_up_mw_per_min,
        ramp_rate_down_mw_per_min: spec.ramp_rate_down_mw_per_min,
        it_load_percent: spec.it_load_percent,
        non_it_load_percent: spec.non_it_load_percent,
        it_allocation_mode: spec.it_allocation_mode,
        ups_config: spec.ups_config,
        pcc_relay_settings: spec.pcc_relay_settings,
        onsite_gen_bess_mw: spec.onsite_gen_bess_mw,
        onsite_gen_parallel: spec.onsite_gen_parallel,
        bess_ramp_rate_mw_per_min: spec.bess_ramp_rate_mw_per_min,
        facility_use_case_percent: spec.facility_use_case_percent,
        mrid: spec.mrid,
        poi_name: spec.poi_name,
        facility_class: spec.facility_class,
        priority: spec.priority,
        max_step_drop_mw: spec.max_step_drop_mw,
        trip_study_percentiles: spec.trip_study_percentiles,
        common_mode_group: spec.common_mode_group,
        voltage_sensitivity_hint: spec.voltage_sensitivity_hint,
        transfer_to_backup_threshold_pu: spec.transfer_to_backup_threshold_pu,
        transfer_delay_ms: spec.transfer_delay_ms,
        reconnection_criteria: spec.reconnection_criteria,
        ride_through_capability: spec.ride_through_capability,
        voltage_transfer_curve: spec.voltage_transfer_curve.map(|stages| {
            stages
                .into_iter()
                .map(|s| VoltageTransferCurveStage {
                    v_pu: s.v_pu,
                    t_ms: s.t_ms,
                    polarity: s.polarity,
                    action: s.action,
                    mw_fraction: s.mw_fraction,
                    load_class: s.load_class,
                })
                .collect()
        }),
        disturbance_counter: spec.disturbance_counter.map(|d| DisturbanceCounter {
            strike_limit: d.strike_limit,
            window_sec: d.window_sec,
            qualifying_v_pu: d.qualifying_v_pu,
            qualifying_duration_ms: d.qualifying_duration_ms,
            latch_permanent: d.latch_permanent,
        }),
        reconnection_params: spec.reconnection_params.map(|r| ReconnectionParams {
            v_recover_pu: r.v_recover_pu,
            delay_ms: r.delay_ms,
            ramp_mw_per_min: r.ramp_mw_per_min,
            manual_reset_required: r.manual_reset_required,
        }),
        voltage_measurement: spec.voltage_measurement.map(|v| VoltageMeasurement {
            basis: v.basis,
            filter_time_constant_ms: v.filter_time_constant_ms,
            location: v.location,
            reset_hysteresis_pu: v.reset_hysteresis_pu,
        }),
        protection_settings_provenance: spec.protection_settings_provenance.map(|p| {
            ProtectionSettingsProvenance {
                source: p.source,
                profile_id: p.profile_id,
                effective_date_us: p.effective_date_us,
            }
        }),
    }
}

/// Builds a `dynamics_models` table batch from enhancement-spec rows.
///
/// Mirrors the converter-internal `build_dynamics_models_batch` in `rpf_writer.rs`, extended to
/// accept `classical_params` values from the spec instead of always writing null. `perc1_params`
/// is not part of the `enhance` spec contract and is always written null, matching upstream.
/// Delegates to the shared authoring builder in `raptrix-cim-arrow::dynamics` (the same
/// full-fidelity path Studio's Dynamics editor uses) so this CLI and desktop authoring never
/// drift on `dynamics_models` wire encoding. `perc1_params` is left null: the enhancement spec
/// format does not yet accept PERC1 inputs (see module docs).
fn build_dynamics_models_batch_from_spec(specs: &[DynamicsModelSpec]) -> Result<RecordBatch> {
    let rows: Vec<raptrix_cim_arrow::dynamics::DynamicsModelRow> = specs
        .iter()
        .map(|spec| raptrix_cim_arrow::dynamics::DynamicsModelRow {
            bus_id: spec.bus_id,
            gen_id: spec.gen_id.clone(),
            model_type: spec.model_type.clone(),
            params: spec.params.clone(),
            perc1_params: None,
            classical_params: spec.classical_params.as_ref().map(|cp| {
                raptrix_cim_arrow::dynamics::ClassicalParams {
                    h: cp.h,
                    d: cp.d,
                    xd_prime: cp.xd_prime,
                    mbase_mva: cp.mbase_mva,
                }
            }),
        })
        .collect();
    raptrix_cim_arrow::dynamics::build_dynamics_models_batch(&rows)
        .context("failed to build dynamics_models record batch from enhancement spec")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_schema::{all_table_schemas, metadata_schema, write_root_rpf};
    use anyhow::Result;
    use arrow::array::Array;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn unique_temp_path(label: &str) -> std::path::PathBuf {
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "raptrix_cim_rs_enhance_test_{label}_{}_{seq}.rpf",
            std::process::id()
        ))
    }

    /// Every non-nullable scalar type used by `metadata_schema()` gets a structurally valid
    /// placeholder value; nullable columns are left null.
    fn default_scalar_array(data_type: &DataType, len: usize) -> Result<ArrayRef> {
        use arrow::array::{BooleanArray, Float64Array, StringArray, TimestampMicrosecondArray};
        use arrow::datatypes::TimeUnit;

        Ok(match data_type {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![false; len])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![0.0f64; len])),
            DataType::Utf8 => Arc::new(StringArray::from(vec!["test"; len])),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let array = TimestampMicrosecondArray::from(vec![0i64; len]);
                let array = match tz {
                    Some(tz) => array.with_timezone(tz.clone()),
                    None => array,
                };
                Arc::new(array)
            }
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
                let values: ArrayRef = Arc::new(StringArray::from(vec!["test"; len]));
                arrow::compute::cast(&values, data_type)
                    .context("casting default dictionary array")?
            }
            other => bail!("no default scalar builder for metadata field type {other:?}"),
        })
    }

    /// Real `.rpf` files always carry exactly one `metadata` row (the file-level record); a
    /// zero-row `metadata` table (fine for every other table in these tests) can never have its
    /// `computational_load_mode` patched. Build a structurally valid single row instead of
    /// hand-populating all 40+ metadata columns: nullable fields stay null, non-nullable
    /// (all scalar) fields get a placeholder value.
    fn one_row_null_metadata_batch() -> Result<RecordBatch> {
        let schema = Arc::new(metadata_schema());
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| {
                if field.is_nullable() {
                    Ok(new_null_array(field.data_type(), 1))
                } else {
                    default_scalar_array(field.data_type(), 1)
                }
            })
            .collect::<Result<_>>()?;
        RecordBatch::try_new(schema, columns).context("building minimal metadata batch")
    }

    fn write_minimal_base_rpf(path: &Path) -> Result<()> {
        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();
        table_batches.insert(TABLE_METADATA, one_row_null_metadata_batch()?);
        write_root_rpf(path, &table_batches, &RootWriteOptions::default())?;
        Ok(())
    }

    #[test]
    fn enhance_adds_clp_and_sets_mode() -> Result<()> {
        let base = unique_temp_path("base_ok");
        let out = unique_temp_path("out_ok");
        write_minimal_base_rpf(&base)?;

        let spec_path = unique_temp_path("spec_ok").with_extension("json");
        fs::write(
            &spec_path,
            r#"{
                "computational_load_profiles": [
                    {
                        "bus_id": 3,
                        "facility_class": "ai_hpc",
                        "common_mode_group": "campus_a",
                        "priority": 1,
                        "max_step_drop_mw": 50.0,
                        "trip_study_percentiles": [60.0, 100.0],
                        "transfer_to_backup_threshold_pu": 0.90,
                        "transfer_delay_ms": 50.0,
                        "poi_name": "POI-3"
                    }
                ]
            }"#,
        )?;

        let summary = run_enhance(&base, &spec_path, &out)?;
        assert!(summary.computational_load_profiles_replaced);
        assert_eq!(summary.computational_load_profiles_rows, 1);
        assert_eq!(summary.computational_load_mode, Some(true));

        let tables: HashMap<String, RecordBatch> = read_rpf_tables(&out)?.into_iter().collect();
        let clp = tables
            .get(TABLE_COMPUTATIONAL_LOAD_PROFILES)
            .expect("clp present");
        assert_eq!(clp.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn enhance_preserves_dynamics_when_omitted() -> Result<()> {
        let base = unique_temp_path("base_dyn");
        let out = unique_temp_path("out_dyn");
        write_minimal_base_rpf(&base)?;

        let spec_path = unique_temp_path("spec_dyn").with_extension("json");
        fs::write(&spec_path, r#"{ "computational_load_profiles": [] }"#)?;

        let summary = run_enhance(&base, &spec_path, &out)?;
        assert!(!summary.dynamics_models_replaced);
        assert_eq!(summary.dynamics_models_rows, 0);
        assert_eq!(summary.computational_load_mode, None);
        Ok(())
    }

    #[test]
    fn enhance_writes_dynamics_with_classical_params() -> Result<()> {
        let base = unique_temp_path("base_classical");
        let out = unique_temp_path("out_classical");
        write_minimal_base_rpf(&base)?;

        let spec_path = unique_temp_path("spec_classical").with_extension("json");
        fs::write(
            &spec_path,
            r#"{
                "dynamics_models": [
                    {
                        "bus_id": 1,
                        "gen_id": "1",
                        "model_type": "GENCLS",
                        "params": {},
                        "classical_params": { "H": 5.0, "D": 0.0, "xd_prime": 0.25, "mbase_mva": 100.0 }
                    }
                ]
            }"#,
        )?;

        let summary = run_enhance(&base, &spec_path, &out)?;
        assert!(summary.dynamics_models_replaced);
        assert_eq!(summary.dynamics_models_rows, 1);

        let tables: HashMap<String, RecordBatch> = read_rpf_tables(&out)?.into_iter().collect();
        let dyn_table = tables.get(TABLE_DYNAMICS_MODELS).expect("dynamics present");
        assert_eq!(dyn_table.num_rows(), 1);
        let classical_idx = dyn_table.schema().index_of("classical_params")?;
        let classical = dyn_table
            .column(classical_idx)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("classical_params struct");
        assert!(!classical.is_null(0));
        Ok(())
    }

    #[test]
    fn enhance_rejects_unknown_spec_fields() -> Result<()> {
        let base = unique_temp_path("base_unknown");
        let out = unique_temp_path("out_unknown");
        write_minimal_base_rpf(&base)?;

        let spec_path = unique_temp_path("spec_unknown").with_extension("json");
        fs::write(&spec_path, r#"{ "not_a_real_field": true }"#)?;

        let err = run_enhance(&base, &spec_path, &out).unwrap_err();
        assert!(format!("{err:#}").contains("unknown field"));
        Ok(())
    }

    fn write_base_rpf_with_one_load(path: &Path, bus_id: i32, p_pu: f64, q_pu: f64) -> Result<()> {
        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();
        table_batches.insert(TABLE_METADATA, one_row_null_metadata_batch()?);

        let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
        id_b.append("1")?;
        let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
        name_b.append_null();
        let loads_batch = RecordBatch::try_new(
            Arc::new(loads_schema()),
            vec![
                Arc::new(Int32Array::from(vec![bus_id])) as ArrayRef,
                Arc::new(id_b.finish()) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                Arc::new(Float64Array::from(vec![p_pu])) as ArrayRef,
                Arc::new(Float64Array::from(vec![q_pu])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(0.1)])) as ArrayRef, // p_i_pu
                Arc::new(Float64Array::from(vec![Some(0.05)])) as ArrayRef, // q_i_pu
                new_null_array(&DataType::Float64, 1),                     // p_y_pu
                new_null_array(&DataType::Float64, 1),                     // q_y_pu
                Arc::new(name_b.finish()) as ArrayRef,
                new_null_array(&DataType::Utf8, 1), // mrid
            ],
        )?;
        table_batches.insert(TABLE_LOADS, loads_batch);
        write_root_rpf(path, &table_batches, &RootWriteOptions::default())?;
        Ok(())
    }

    #[test]
    fn enhance_scales_load_and_round_trips() -> Result<()> {
        let base = unique_temp_path("base_load_scale");
        let out = unique_temp_path("out_load_scale");
        // p_pu=0.5 @ base_mva default 100 → 50 MW; scale_p=10 → 5.0 pu (500 MW).
        write_base_rpf_with_one_load(&base, 110013, 0.5, 0.1)?;

        let spec_path = unique_temp_path("spec_load_scale").with_extension("json");
        fs::write(
            &spec_path,
            r#"{
                "load_overrides": [
                    { "bus_id": 110013, "scale_p": 10.0, "scale_q": 2.0 }
                ]
            }"#,
        )?;

        let summary = run_enhance(&base, &spec_path, &out)?;
        assert_eq!(summary.load_overrides_applied, 1);
        assert_eq!(summary.load_rows_created, 0);

        let tables: HashMap<String, RecordBatch> = read_rpf_tables(&out)?.into_iter().collect();
        let loads = tables.get(TABLE_LOADS).expect("loads present");
        assert_eq!(loads.num_rows(), 1);
        let p_pu = loads
            .column(loads.schema().index_of("p_pu")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("p_pu");
        let q_pu = loads
            .column(loads.schema().index_of("q_pu")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("q_pu");
        let p_i = loads
            .column(loads.schema().index_of("p_i_pu")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("p_i_pu");
        assert!((p_pu.value(0) - 5.0).abs() < 1e-12);
        assert!((q_pu.value(0) - 0.2).abs() < 1e-12);
        // ZIP I term scaled with scale_p.
        assert!((p_i.value(0) - 1.0).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn enhance_sets_absolute_load_mw_and_creates_row() -> Result<()> {
        let base = unique_temp_path("base_load_abs");
        let out = unique_temp_path("out_load_abs");
        write_minimal_base_rpf(&base)?;

        let spec_path = unique_temp_path("spec_load_abs").with_extension("json");
        fs::write(
            &spec_path,
            r#"{
                "load_overrides": [
                    { "bus_id": 110123, "p_mw": 800.0, "q_mw": 200.0 }
                ]
            }"#,
        )?;

        let summary = run_enhance(&base, &spec_path, &out)?;
        assert_eq!(summary.load_overrides_applied, 1);
        assert_eq!(summary.load_rows_created, 1);

        let tables: HashMap<String, RecordBatch> = read_rpf_tables(&out)?.into_iter().collect();
        let loads = tables.get(TABLE_LOADS).expect("loads present");
        assert_eq!(loads.num_rows(), 1);
        let bus_id = loads
            .column(loads.schema().index_of("bus_id")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("bus_id");
        let p_pu = loads
            .column(loads.schema().index_of("p_pu")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("p_pu");
        let q_pu = loads
            .column(loads.schema().index_of("q_pu")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("q_pu");
        // metadata.base_mva placeholder is 0.0 in the test fixture → enhancer defaults to 100.
        assert_eq!(bus_id.value(0), 110123);
        assert!((p_pu.value(0) - 8.0).abs() < 1e-12);
        assert!((q_pu.value(0) - 2.0).abs() < 1e-12);
        Ok(())
    }
}
