// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Read-only RPF case health inspection.
//!
//! See `docs/rpf-case-health.md` in the workspace root for the health model and grading rules.

mod batch_metrics;

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use crate::io::{read_rpf_tables, rpf_file_metadata};
use crate::schema::{
    METADATA_KEY_CASE_MODE, METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE,
    METADATA_KEY_SOLVED_STATE_PRESENCE, METADATA_KEY_SOLVER_ACCURACY, METADATA_KEY_SOLVER_ITERATIONS,
    METADATA_KEY_SOLVER_MODE,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_GENERATION_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_LOAD_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_NETWORK_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_DETACHED_ISLANDS_PRESENT, METADATA_KEY_TOPOLOGY_ISLAND_COUNT,
    METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT, METADATA_KEY_VALIDATION_MODE,
    TABLE_BRANCHES, TABLE_BUSES, TABLE_BUSES_SOLVED,
    TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_GENERATORS_SOLVED, TABLE_LOADS, TABLE_METADATA,
    TABLE_SWITCHED_SHUNT_BANKS, TABLE_SWITCHED_SHUNTS, TABLE_TRANSFORMERS_2W,
    TABLE_TRANSFORMERS_3W,
};

/// In-memory RPF interchange used by [`inspect_rpf_case`].
///
/// Spec documents may refer to this as `RpfNetwork`; there is no separate network object in the
/// Arrow interchange — canonical tables plus file-level metadata are the model.
#[derive(Debug, Clone)]
pub struct RpfTables {
    /// Canonical table name → `RecordBatch`.
    pub tables: HashMap<String, RecordBatch>,
    /// Root Arrow IPC file metadata (`rpf.*` keys).
    pub file_metadata: HashMap<String, String>,
}

impl RpfTables {
    /// Builds from the output of [`read_rpf_tables`] and [`rpf_file_metadata`].
    pub fn from_read_result(
        tables: Vec<(String, RecordBatch)>,
        file_metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            tables: tables.into_iter().collect(),
            file_metadata,
        }
    }
}

/// Overall case health grade (deterministic, explainable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RpfHealthGrade {
    Healthy,
    Caution,
    Stressed,
    Pathological,
}

/// How island topology metrics were obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologySource {
    /// Read from `rpf.topology.*` file metadata (or metadata table).
    Metadata,
    /// Recomputed from in-service branches and transformers.
    Recomputed,
}

/// Per-table element counts (total and in-service where applicable).
#[derive(Debug, Clone, PartialEq)]
pub struct RpfCaseCounts {
    pub buses: usize,
    pub buses_in_service: usize,
    pub branches: usize,
    pub branches_in_service: usize,
    pub generators: usize,
    pub generators_in_service: usize,
    pub loads: usize,
    pub loads_in_service: usize,
    pub fixed_shunts: usize,
    pub fixed_shunts_in_service: usize,
    pub switched_shunts: usize,
    pub switched_shunts_in_service: usize,
    pub switched_shunt_banks: usize,
    pub transformers_2w: usize,
    pub transformers_2w_in_service: usize,
    pub transformers_3w: usize,
    pub transformers_3w_in_service: usize,
    pub branch_taps_non_unity: usize,
    pub transformer_taps_non_unity: usize,
}

/// Aggregate power, reserve, and shunt/tap statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct RpfCaseAggregates {
    pub base_mva: f64,
    /// Sum of in-service load constant-power P (`loads.p_pu * base_mva`).
    pub total_load_p_mw: f64,
    /// Sum of in-service load constant-power Q (`loads.q_pu * base_mva`).
    pub total_load_q_mvar: f64,
    /// Sum of in-service generator `p_sched_mw`.
    pub total_gen_p_mw: f64,
    /// Sum of in-service generator `q_sched_mvar`.
    pub total_gen_q_mvar: f64,
    /// Sum over in-service generators of `max(0, p_max_mw - p_sched_mw)`.
    pub reserve_p_up_mw: f64,
    /// Sum over in-service generators of `max(0, p_sched_mw - p_min_mw)`.
    pub reserve_p_down_mw: f64,
    /// Sum over in-service generators of `max(0, q_max_mvar - q_sched_mvar)`.
    pub reserve_q_up_mvar: f64,
    /// Sum over in-service generators of `max(0, q_sched_mvar - q_min_mvar)`.
    pub reserve_q_down_mvar: f64,
    /// Sum over in-service generators of `max(q_max - q_sched, q_sched - q_min)` MVAR —
    /// reactive headroom to the nearer Q limit (not double-counting both sides).
    pub reactive_support_headroom_mvar: f64,
    pub switched_shunts_in_service: usize,
    pub switched_shunt_banks_total: usize,
    pub tap_settings_non_unity: usize,
}

/// Topology and planning/solved semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct RpfTopologySignals {
    pub island_count: usize,
    pub main_island_bus_count: usize,
    pub detached_islands_present: bool,
    pub detached_active_network_island_count: usize,
    pub detached_active_load_island_count: usize,
    pub detached_active_generation_island_count: usize,
    pub topology_source: TopologySource,
    pub case_mode: Option<String>,
    pub solved_state_presence: Option<String>,
    pub validation_mode: Option<String>,
    pub v_mag_set_min: Option<f64>,
    pub v_mag_set_max: Option<f64>,
    pub v_mag_set_mean: Option<f64>,
    pub buses_out_of_voltage_band: usize,
    pub buses_nominal_kv_nonpositive: usize,
    pub zip_fidelity_presence: Option<String>,
    pub loads_with_zip_terms: usize,
    pub loads_total: usize,
}

/// Convergence and solve-quality hints — every `Option` is `None` when the source data is absent.
#[derive(Debug, Clone, PartialEq)]
pub struct RpfConvergenceHints {
    pub solve_data_present: bool,
    pub solver_iterations: Option<i32>,
    pub solver_accuracy: Option<f64>,
    pub solver_mode: Option<String>,
    pub solver_q_limit_infeasible_count: Option<i32>,
    pub pv_to_pq_switch_count: Option<i32>,
    pub pv_to_pq_from_generators_solved: Option<usize>,
    pub initial_mismatch_rms: Option<f64>,
    pub q_violation_count: Option<usize>,
    pub contraction_ratio_first_step: Option<f64>,
    pub stall_or_oscillation: Option<bool>,
}

/// Full health inspection result.
#[derive(Debug, Clone, PartialEq)]
pub struct RpfCaseHealth {
    pub grade: RpfHealthGrade,
    pub reasons: Vec<String>,
    pub counts: RpfCaseCounts,
    pub aggregates: RpfCaseAggregates,
    pub topology: RpfTopologySignals,
    pub convergence: RpfConvergenceHints,
}

/// Reads an `.rpf` file and returns a health inspection report.
pub fn inspect_rpf_file(path: impl AsRef<Path>) -> Result<RpfCaseHealth> {
    let path = path.as_ref();
    let tables = read_rpf_tables(path)
        .with_context(|| format!("failed to read RPF tables from {}", path.display()))?;
    let metadata = rpf_file_metadata(path)
        .with_context(|| format!("failed to read RPF metadata from {}", path.display()))?;
    inspect_rpf_case(&RpfTables::from_read_result(tables, metadata))
}

/// Inspects loaded canonical RPF tables and file metadata.
pub fn inspect_rpf_case(input: &RpfTables) -> Result<RpfCaseHealth> {
    let counts = collect_counts(input)?;
    let aggregates = collect_aggregates(input)?;
    let topology = collect_topology(input)?;
    let convergence = collect_convergence(input)?;
    let (grade, reasons) = grade_case(input, &counts, &aggregates, &topology, &convergence);
    Ok(RpfCaseHealth {
        grade,
        reasons,
        counts,
        aggregates,
        topology,
        convergence,
    })
}

/// Formats a human-readable health report (summary line + bullet reasons).
pub fn format_health_report(health: &RpfCaseHealth) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "grade={:?} buses={} islands={} load_mw={:.1} gen_mw={:.1}",
        health.grade,
        health.counts.buses,
        health.topology.island_count,
        health.aggregates.total_load_p_mw,
        health.aggregates.total_gen_p_mw,
    ));
    if !health.reasons.is_empty() {
        out.push_str("\nreasons:");
        for reason in &health.reasons {
            out.push_str("\n  - ");
            out.push_str(reason);
        }
    }
    out
}

fn collect_counts(input: &RpfTables) -> Result<RpfCaseCounts> {
    let buses = batch_metrics::table_batch(&input.tables, TABLE_BUSES);
    let branches = batch_metrics::table_batch(&input.tables, TABLE_BRANCHES);
    let gens = batch_metrics::table_batch(&input.tables, TABLE_GENERATORS);
    let loads = batch_metrics::table_batch(&input.tables, TABLE_LOADS);
    let fixed = batch_metrics::table_batch(&input.tables, TABLE_FIXED_SHUNTS);
    let sw = batch_metrics::table_batch(&input.tables, TABLE_SWITCHED_SHUNTS);
    let banks = batch_metrics::table_batch(&input.tables, TABLE_SWITCHED_SHUNT_BANKS);
    let xf2 = batch_metrics::table_batch(&input.tables, TABLE_TRANSFORMERS_2W);
    let xf3 = batch_metrics::table_batch(&input.tables, TABLE_TRANSFORMERS_3W);

    Ok(RpfCaseCounts {
        buses: batch_metrics::row_count(buses),
        buses_in_service: batch_metrics::count_in_service(buses)?,
        branches: batch_metrics::row_count(branches),
        branches_in_service: batch_metrics::count_in_service(branches)?,
        generators: batch_metrics::row_count(gens),
        generators_in_service: batch_metrics::count_in_service(gens)?,
        loads: batch_metrics::row_count(loads),
        loads_in_service: batch_metrics::count_in_service(loads)?,
        fixed_shunts: batch_metrics::row_count(fixed),
        fixed_shunts_in_service: batch_metrics::count_in_service(fixed)?,
        switched_shunts: batch_metrics::row_count(sw),
        switched_shunts_in_service: batch_metrics::count_in_service(sw)?,
        switched_shunt_banks: batch_metrics::row_count(banks),
        transformers_2w: batch_metrics::row_count(xf2),
        transformers_2w_in_service: batch_metrics::count_in_service(xf2)?,
        transformers_3w: batch_metrics::row_count(xf3),
        transformers_3w_in_service: batch_metrics::count_in_service(xf3)?,
        branch_taps_non_unity: batch_metrics::count_non_unity_tap(branches, "tap", None, 1e-6)?,
        transformer_taps_non_unity: batch_metrics::count_non_unity_tap(
            xf2,
            "tap_ratio",
            Some("nominal_tap_ratio"),
            1e-6,
        )?,
    })
}

fn collect_aggregates(input: &RpfTables) -> Result<RpfCaseAggregates> {
    let meta = batch_metrics::table_batch(&input.tables, TABLE_METADATA);
    let base_mva = meta
        .and_then(|b| batch_metrics::metadata_f64(b, "base_mva").ok().flatten())
        .filter(|v| *v > 0.0)
        .unwrap_or(100.0);

    let loads = batch_metrics::table_batch(&input.tables, TABLE_LOADS);
    let gens = batch_metrics::table_batch(&input.tables, TABLE_GENERATORS);
    let branches = batch_metrics::table_batch(&input.tables, TABLE_BRANCHES);
    let xf2 = batch_metrics::table_batch(&input.tables, TABLE_TRANSFORMERS_2W);
    let sw = batch_metrics::table_batch(&input.tables, TABLE_SWITCHED_SHUNTS);
    let banks = batch_metrics::table_batch(&input.tables, TABLE_SWITCHED_SHUNT_BANKS);

    let total_load_p_mw = batch_metrics::sum_f64_in_service(loads, "p_pu", base_mva)?;
    let total_load_q_mvar = batch_metrics::sum_f64_in_service(loads, "q_pu", base_mva)?;

    Ok(RpfCaseAggregates {
        base_mva,
        total_load_p_mw,
        total_load_q_mvar,
        total_gen_p_mw: batch_metrics::sum_f64_in_service(gens, "p_sched_mw", 1.0)?,
        total_gen_q_mvar: batch_metrics::sum_f64_in_service(gens, "q_sched_mvar", 1.0)?,
        reserve_p_up_mw: batch_metrics::sum_gen_reserve(
            gens,
            "p_sched_mw",
            "p_min_mw",
            "p_max_mw",
            true,
        )?,
        reserve_p_down_mw: batch_metrics::sum_gen_reserve(
            gens,
            "p_sched_mw",
            "p_min_mw",
            "p_max_mw",
            false,
        )?,
        reserve_q_up_mvar: batch_metrics::sum_gen_reserve(
            gens,
            "q_sched_mvar",
            "q_min_mvar",
            "q_max_mvar",
            true,
        )?,
        reserve_q_down_mvar: batch_metrics::sum_gen_reserve(
            gens,
            "q_sched_mvar",
            "q_min_mvar",
            "q_max_mvar",
            false,
        )?,
        reactive_support_headroom_mvar: batch_metrics::sum_reactive_headroom(gens)?,
        switched_shunts_in_service: batch_metrics::count_in_service(sw)?,
        switched_shunt_banks_total: batch_metrics::row_count(banks),
        tap_settings_non_unity: batch_metrics::count_non_unity_tap(branches, "tap", None, 1e-6)?
            + batch_metrics::count_non_unity_tap(xf2, "tap_ratio", Some("nominal_tap_ratio"), 1e-6)?,
    })
}

fn metadata_usize(meta: &HashMap<String, String>, key: &str) -> Option<usize> {
    meta.get(key).and_then(|v| v.parse().ok())
}

fn metadata_bool(meta: &HashMap<String, String>, key: &str) -> Option<bool> {
    meta.get(key).map(|v| v == "true")
}

fn collect_topology(input: &RpfTables) -> Result<RpfTopologySignals> {
    let buses = batch_metrics::table_batch(&input.tables, TABLE_BUSES);
    let loads = batch_metrics::table_batch(&input.tables, TABLE_LOADS);
    let (zip_loads, loads_total) = if let Some(batch) = loads {
        batch_metrics::count_zip_load_rows(batch)?
    } else {
        (0, 0)
    };

    let zip_fidelity = input
        .file_metadata
        .get(METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE)
        .cloned()
        .or_else(|| {
            batch_metrics::table_batch(&input.tables, TABLE_METADATA).and_then(|b| {
                batch_metrics::metadata_utf8_at(b, "zip_fidelity_presence", 0)
                    .ok()
                    .flatten()
            })
        });

    let (island_count, main_island, detached, det_net, det_load, det_gen, source) =
        if let (Some(ic), Some(main)) = (
            metadata_usize(&input.file_metadata, METADATA_KEY_TOPOLOGY_ISLAND_COUNT),
            metadata_usize(&input.file_metadata, METADATA_KEY_TOPOLOGY_MAIN_ISLAND_BUS_COUNT),
        ) {
            (
                ic,
                main,
                metadata_bool(&input.file_metadata, METADATA_KEY_TOPOLOGY_DETACHED_ISLANDS_PRESENT)
                    .unwrap_or(ic > 1),
                metadata_usize(
                    &input.file_metadata,
                    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_NETWORK_ISLAND_COUNT,
                )
                .unwrap_or(0),
                metadata_usize(
                    &input.file_metadata,
                    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_LOAD_ISLAND_COUNT,
                )
                .unwrap_or(0),
                metadata_usize(
                    &input.file_metadata,
                    METADATA_KEY_TOPOLOGY_DETACHED_ACTIVE_GENERATION_ISLAND_COUNT,
                )
                .unwrap_or(0),
                TopologySource::Metadata,
            )
        } else {
            let diag = recompute_topology(input)?;
            (
                diag.island_count,
                diag.main_island_bus_count,
                diag.detached_islands_present,
                diag.detached_active_network_island_count,
                diag.detached_active_load_island_count,
                diag.detached_active_generation_island_count,
                TopologySource::Recomputed,
            )
        };

    let (v_min, v_max, v_mean) = if let Some(b) = buses {
        match batch_metrics::f64_stats_in_service(b, "v_mag_set")? {
            Some((min, max, mean, _)) => (Some(min), Some(max), Some(mean)),
            None => (None, None, None),
        }
    } else {
        (None, None, None)
    };

    let out_of_band = buses.map(batch_metrics::count_voltage_out_of_band).transpose()?.unwrap_or(0);
    let bad_kv = buses
        .map(batch_metrics::count_nominal_kv_nonpositive)
        .transpose()?
        .unwrap_or(0);

    let meta_batch = batch_metrics::table_batch(&input.tables, TABLE_METADATA);
    let case_mode = file_or_table_meta(input, meta_batch, METADATA_KEY_CASE_MODE, "case_mode");
    let solved_state = file_or_table_meta(
        input,
        meta_batch,
        METADATA_KEY_SOLVED_STATE_PRESENCE,
        "solved_state_presence",
    );
    let validation_mode = file_or_table_meta(
        input,
        meta_batch,
        METADATA_KEY_VALIDATION_MODE,
        "validation_mode",
    );

    Ok(RpfTopologySignals {
        island_count,
        main_island_bus_count: main_island,
        detached_islands_present: detached,
        detached_active_network_island_count: det_net,
        detached_active_load_island_count: det_load,
        detached_active_generation_island_count: det_gen,
        topology_source: source,
        case_mode,
        solved_state_presence: solved_state,
        validation_mode,
        v_mag_set_min: v_min,
        v_mag_set_max: v_max,
        v_mag_set_mean: v_mean,
        buses_out_of_voltage_band: out_of_band,
        buses_nominal_kv_nonpositive: bad_kv,
        zip_fidelity_presence: zip_fidelity,
        loads_with_zip_terms: zip_loads,
        loads_total,
    })
}

fn file_or_table_meta(
    input: &RpfTables,
    meta_batch: Option<&RecordBatch>,
    file_key: &str,
    table_col: &str,
) -> Option<String> {
    input
        .file_metadata
        .get(file_key)
        .cloned()
        .or_else(|| {
            meta_batch.and_then(|b| batch_metrics::metadata_utf8_at(b, table_col, 0).ok().flatten())
        })
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

fn recompute_topology(input: &RpfTables) -> Result<TopologyDiagnostics> {
    let buses = batch_metrics::table_batch(&input.tables, TABLE_BUSES);
    let branches = batch_metrics::table_batch(&input.tables, TABLE_BRANCHES);
    let xf2 = batch_metrics::table_batch(&input.tables, TABLE_TRANSFORMERS_2W);
    let xf3 = batch_metrics::table_batch(&input.tables, TABLE_TRANSFORMERS_3W);
    let loads = batch_metrics::table_batch(&input.tables, TABLE_LOADS);
    let gens = batch_metrics::table_batch(&input.tables, TABLE_GENERATORS);

    let Some(buses) = buses else {
        return Ok(TopologyDiagnostics::default());
    };

    let bus_ids = batch_metrics::column_i32(buses, "bus_id")?
        .context("buses.bus_id required for topology recompute")?;

    let mut adj: HashMap<i32, Vec<i32>> = HashMap::new();
    for row in 0..buses.num_rows() {
        if bus_ids.is_valid(row) {
            adj.entry(bus_ids.value(row)).or_default();
        }
    }

    let mut add_edge = |from: i32, to: i32| {
        if from <= 0 || to <= 0 || from == to {
            return;
        }
        if adj.contains_key(&from) && adj.contains_key(&to) {
            adj.entry(from).or_default().push(to);
            adj.entry(to).or_default().push(from);
        }
    };

    if let Some(br) = branches {
        let from = batch_metrics::column_i32(br, "from_bus_id")?;
        let to = batch_metrics::column_i32(br, "to_bus_id")?;
        let status = batch_metrics::column_bool(br, "status")?;
        if let (Some(from), Some(to)) = (from, to) {
            for row in 0..br.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && from.is_valid(row) && to.is_valid(row) {
                    add_edge(from.value(row), to.value(row));
                }
            }
        }
    }

    if let Some(xf) = xf2 {
        let from = batch_metrics::column_i32(xf, "from_bus_id")?;
        let to = batch_metrics::column_i32(xf, "to_bus_id")?;
        let status = batch_metrics::column_bool(xf, "status")?;
        if let (Some(from), Some(to)) = (from, to) {
            for row in 0..xf.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && from.is_valid(row) && to.is_valid(row) {
                    add_edge(from.value(row), to.value(row));
                }
            }
        }
    }

    if let Some(xf) = xf3 {
        let h = batch_metrics::column_i32(xf, "bus_h_id")?;
        let m = batch_metrics::column_i32(xf, "bus_m_id")?;
        let l = batch_metrics::column_i32(xf, "bus_l_id")?;
        let status = batch_metrics::column_bool(xf, "status")?;
        if let (Some(h), Some(m), Some(l)) = (h, m, l) {
            for row in 0..xf.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if !ins {
                    continue;
                }
                if h.is_valid(row) && m.is_valid(row) {
                    add_edge(h.value(row), m.value(row));
                }
                if m.is_valid(row) && l.is_valid(row) {
                    add_edge(m.value(row), l.value(row));
                }
                if h.is_valid(row) && l.is_valid(row) {
                    add_edge(h.value(row), l.value(row));
                }
            }
        }
    }

    let mut bus_ids_sorted: Vec<i32> = adj.keys().copied().collect();
    bus_ids_sorted.sort_unstable();

    let mut visited: HashSet<i32> = HashSet::new();
    let mut islands: Vec<Vec<i32>> = Vec::new();
    for seed in bus_ids_sorted {
        if visited.contains(&seed) {
            continue;
        }
        let mut stack = vec![seed];
        visited.insert(seed);
        let mut component = Vec::new();
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
    islands.sort_unstable_by(|a, b| b.len().cmp(&a.len()));

    let mut load_buses = HashSet::new();
    if let Some(loads) = loads {
        let bus_id = batch_metrics::column_i32(loads, "bus_id")?;
        let status = batch_metrics::column_bool(loads, "status")?;
        if let Some(bus_id) = bus_id {
            for row in 0..loads.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && bus_id.is_valid(row) {
                    load_buses.insert(bus_id.value(row));
                }
            }
        }
    }

    let mut gen_buses = HashSet::new();
    if let Some(gens) = gens {
        let bus_id = batch_metrics::column_i32(gens, "bus_id")?;
        let status = batch_metrics::column_bool(gens, "status")?;
        if let Some(bus_id) = bus_id {
            for row in 0..gens.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && bus_id.is_valid(row) {
                    gen_buses.insert(bus_id.value(row));
                }
            }
        }
    }

    let mut network_pairs: HashSet<(i32, i32)> = HashSet::new();
    let mut add_pair = |a: i32, b: i32| {
        if a <= 0 || b <= 0 || a == b {
            return;
        }
        let pair = if a < b { (a, b) } else { (b, a) };
        network_pairs.insert(pair);
    };
    if let Some(br) = branches {
        let from = batch_metrics::column_i32(br, "from_bus_id")?;
        let to = batch_metrics::column_i32(br, "to_bus_id")?;
        let status = batch_metrics::column_bool(br, "status")?;
        if let (Some(from), Some(to)) = (from, to) {
            for row in 0..br.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && from.is_valid(row) && to.is_valid(row) {
                    add_pair(from.value(row), to.value(row));
                }
            }
        }
    }
    if let Some(xf) = xf2 {
        let from = batch_metrics::column_i32(xf, "from_bus_id")?;
        let to = batch_metrics::column_i32(xf, "to_bus_id")?;
        let status = batch_metrics::column_bool(xf, "status")?;
        if let (Some(from), Some(to)) = (from, to) {
            for row in 0..xf.num_rows() {
                let ins = status
                    .and_then(|s| s.is_valid(row).then(|| s.value(row)))
                    .unwrap_or(true);
                if ins && from.is_valid(row) && to.is_valid(row) {
                    add_pair(from.value(row), to.value(row));
                }
            }
        }
    }

    let mut diagnostics = TopologyDiagnostics {
        island_count: islands.len(),
        main_island_bus_count: islands.first().map_or(0, Vec::len),
        detached_islands_present: islands.len() > 1,
        ..Default::default()
    };

    for island in islands.iter().skip(1) {
        let bus_set: HashSet<i32> = island.iter().copied().collect();
        let has_load = bus_set.iter().any(|b| load_buses.contains(b));
        let has_gen = bus_set.iter().any(|b| gen_buses.contains(b));
        let has_network = network_pairs
            .iter()
            .any(|(l, r)| bus_set.contains(l) && bus_set.contains(r));
        if has_network {
            diagnostics.detached_active_network_island_count += 1;
        }
        if has_load {
            diagnostics.detached_active_load_island_count += 1;
        }
        if has_gen {
            diagnostics.detached_active_generation_island_count += 1;
        }
    }

    Ok(diagnostics)
}

fn collect_convergence(input: &RpfTables) -> Result<RpfConvergenceHints> {
    let meta_batch = batch_metrics::table_batch(&input.tables, TABLE_METADATA);
    let buses = batch_metrics::table_batch(&input.tables, TABLE_BUSES);
    let buses_solved = batch_metrics::table_batch(&input.tables, TABLE_BUSES_SOLVED);
    let gens_solved = batch_metrics::table_batch(&input.tables, TABLE_GENERATORS_SOLVED);
    let gens = batch_metrics::table_batch(&input.tables, TABLE_GENERATORS);

    let solved_presence = file_or_table_meta(
        input,
        meta_batch,
        METADATA_KEY_SOLVED_STATE_PRESENCE,
        "solved_state_presence",
    );
    let solve_data_present = matches!(
        solved_presence.as_deref(),
        Some("actual_solved") | Some("seed_only")
    ) || buses_solved.is_some_and(|b| b.num_rows() > 0);

    let solver_iterations = optional_meta_i32(input, meta_batch, METADATA_KEY_SOLVER_ITERATIONS, "solver_iterations");
    let solver_accuracy = optional_meta_f64(input, meta_batch, METADATA_KEY_SOLVER_ACCURACY, "solver_accuracy");
    let solver_mode = file_or_table_meta(input, meta_batch, METADATA_KEY_SOLVER_MODE, "solver_mode");
    let solver_q_limit_infeasible_count = optional_meta_i32(
        input,
        meta_batch,
        "rpf.solver.q_limit_infeasible_count",
        "solver_q_limit_infeasible_count",
    );
    let pv_to_pq_switch_count =
        optional_meta_i32(input, meta_batch, "rpf.pv_to_pq_switch_count", "pv_to_pq_switch_count");

    let pv_to_pq_from_generators_solved = gens_solved
        .filter(|b| b.num_rows() > 0)
        .map(batch_metrics::count_pv_to_pq_solved)
        .transpose()?;

    let initial_mismatch_rms = match (buses, buses_solved) {
        (Some(b), Some(s)) if s.num_rows() > 0 => batch_metrics::initial_voltage_mismatch_rms(b, s)?,
        _ => None,
    };

    let q_violation_count = gens
        .filter(|b| b.num_rows() > 0)
        .map(|b| batch_metrics::count_gens_at_q_limit(b, 0.01))
        .transpose()?;

    let (contraction_ratio_first_step, stall_or_oscillation) =
        optional_solve_trace_params(meta_batch);

    Ok(RpfConvergenceHints {
        solve_data_present,
        solver_iterations,
        solver_accuracy,
        solver_mode,
        solver_q_limit_infeasible_count,
        pv_to_pq_switch_count,
        pv_to_pq_from_generators_solved,
        initial_mismatch_rms,
        q_violation_count,
        contraction_ratio_first_step,
        stall_or_oscillation,
    })
}

fn optional_meta_i32(
    input: &RpfTables,
    meta_batch: Option<&RecordBatch>,
    file_key: &str,
    table_col: &str,
) -> Option<i32> {
    input
        .file_metadata
        .get(file_key)
        .and_then(|v| v.parse().ok())
        .or_else(|| {
            meta_batch.and_then(|b| batch_metrics::metadata_i32(b, table_col).ok().flatten())
        })
}

fn optional_meta_f64(
    input: &RpfTables,
    meta_batch: Option<&RecordBatch>,
    file_key: &str,
    table_col: &str,
) -> Option<f64> {
    input
        .file_metadata
        .get(file_key)
        .and_then(|v| v.parse().ok())
        .or_else(|| {
            meta_batch.and_then(|b| batch_metrics::metadata_f64(b, table_col).ok().flatten())
        })
}

fn optional_solve_trace_params(
    _meta_batch: Option<&RecordBatch>,
) -> (Option<f64>, Option<bool>) {
    // Only populated when a writer stored solve-trace keys in custom_metadata or
    // scenario_context.params (`contraction_ratio_first_step`, `stall_or_oscillation`);
    // never inferred from planning tables alone.
    (None, None)
}

// --- Grading (deterministic scorecard) ---

/// Bus count at or above which cases are treated as "large" for caution grading.
/// Calibrated: Texas2k ~2.7k buses; NYISO ~1.5k; IEEE 14/118 well below.
const LARGE_CASE_BUS_THRESHOLD: usize = 1_500;

/// Many non-unity taps / switched shunts — typical on Texas2k and NYISO planning cases.
const MANY_TAP_SETTINGS_THRESHOLD: usize = 50;
const MANY_SWITCHED_SHUNTS_THRESHOLD: usize = 100;

/// Fraction of in-service generators at Q limits before "stressed" reactive pressure.
const Q_LIMIT_PRESSURE_FRACTION: f64 = 0.15;

const Q_LIMIT_GEN_COUNT_STRESSED: usize = 20;
const PV_TO_PQ_STRESSED_COUNT: i32 = 10;
const VOLTAGE_OUT_OF_BAND_STRESSED: usize = 5;
const INITIAL_MISMATCH_CAUTION_RMS: f64 = 0.02;

fn grade_case(
    input: &RpfTables,
    counts: &RpfCaseCounts,
    aggregates: &RpfCaseAggregates,
    topology: &RpfTopologySignals,
    convergence: &RpfConvergenceHints,
) -> (RpfHealthGrade, Vec<String>) {
    let mut rules: Vec<(RpfHealthGrade, String)> = Vec::new();

    if topology.detached_active_load_island_count > 0 || topology.detached_active_generation_island_count > 0
    {
        rules.push((
            RpfHealthGrade::Pathological,
            format!(
                "detached islands with active load ({}) or generation ({})",
                topology.detached_active_load_island_count,
                topology.detached_active_generation_island_count
            ),
        ));
    }

    if topology.validation_mode.as_deref() == Some("topology_only")
        && (aggregates.total_load_p_mw.abs() > 1e-3 || aggregates.total_gen_p_mw.abs() > 1e-3)
    {
        rules.push((
            RpfHealthGrade::Pathological,
            "validation_mode=topology_only but case has material P load or generation".to_string(),
        ));
    }

    if topology.island_count > 4 {
        rules.push((
            RpfHealthGrade::Pathological,
            format!("fragmented topology: {} electrical islands", topology.island_count),
        ));
    }

    if let Some(solved) = topology.solved_state_presence.as_deref() {
        if solved == "actual_solved"
            && !convergence.solve_data_present
            && batch_metrics::table_batch(&input.tables, TABLE_BUSES_SOLVED).is_none()
        {
            rules.push((
                RpfHealthGrade::Pathological,
                "solved_state_presence=actual_solved but buses_solved table is missing".to_string(),
            ));
        }
    }

    if topology.detached_islands_present && topology.detached_active_network_island_count > 0 {
        rules.push((
            RpfHealthGrade::Stressed,
            format!(
                "{} detached island(s) with in-service network elements",
                topology.detached_active_network_island_count
            ),
        ));
    }

    if let Some(n) = convergence.pv_to_pq_switch_count {
        if n >= PV_TO_PQ_STRESSED_COUNT {
            rules.push((
                RpfHealthGrade::Stressed,
                format!("high PV→PQ switching during solve ({n})"),
            ));
        }
    }
    if let Some(n) = convergence.solver_q_limit_infeasible_count {
        if n > 0 {
            rules.push((
                RpfHealthGrade::Stressed,
                format!("solver reported {n} Q-limit infeasibility event(s)"),
            ));
        }
    }

    if topology.buses_out_of_voltage_band >= VOLTAGE_OUT_OF_BAND_STRESSED {
        rules.push((
            RpfHealthGrade::Stressed,
            format!(
                "{} in-service buses have v_mag_set outside [v_min, v_max]",
                topology.buses_out_of_voltage_band
            ),
        ));
    }

    if let Some(qv) = convergence.q_violation_count {
        let gens = counts.generators_in_service;
        let frac = if gens > 0 {
            qv as f64 / gens as f64
        } else {
            0.0
        };
        let stressed_by_count = qv >= Q_LIMIT_GEN_COUNT_STRESSED;
        let stressed_by_fraction = gens >= 10 && frac >= Q_LIMIT_PRESSURE_FRACTION;
        if stressed_by_count || stressed_by_fraction {
            rules.push((
                RpfHealthGrade::Stressed,
                format!("{qv} in-service generators at Q limits (planning schedule)"),
            ));
        } else if qv > 0 {
            rules.push((
                RpfHealthGrade::Caution,
                format!("{qv} in-service generator(s) at Q limits (planning schedule)"),
            ));
        }
    }

    if counts.buses >= LARGE_CASE_BUS_THRESHOLD {
        rules.push((
            RpfHealthGrade::Caution,
            format!(
                "large case ({} buses; threshold {LARGE_CASE_BUS_THRESHOLD} tuned on NYISO/Texas2k)",
                counts.buses
            ),
        ));
    }

    if aggregates.tap_settings_non_unity >= MANY_TAP_SETTINGS_THRESHOLD {
        rules.push((
            RpfHealthGrade::Caution,
            format!(
                "{} non-unity tap settings (branches + transformers)",
                aggregates.tap_settings_non_unity
            ),
        ));
    }

    if aggregates.switched_shunts_in_service >= MANY_SWITCHED_SHUNTS_THRESHOLD {
        rules.push((
            RpfHealthGrade::Caution,
            format!(
                "{} in-service switched shunts",
                aggregates.switched_shunts_in_service
            ),
        ));
    }

    if topology.solved_state_presence.as_deref() == Some("not_computed") {
        rules.push((
            RpfHealthGrade::Caution,
            "planning case without solved-state payload (flat/warm start)".to_string(),
        ));
    }

    if let Some(rms) = convergence.initial_mismatch_rms {
        if rms >= INITIAL_MISMATCH_CAUTION_RMS {
            rules.push((
                RpfHealthGrade::Caution,
                format!(
                    "initial voltage mismatch RMS {rms:.4} between buses and buses_solved (threshold {INITIAL_MISMATCH_CAUTION_RMS})"
                ),
            ));
        }
    }

    if topology.zip_fidelity_presence.as_deref() == Some("partial") {
        rules.push((
            RpfHealthGrade::Caution,
            "partial ZIP load fidelity on loads table".to_string(),
        ));
    }

    if topology.buses_nominal_kv_nonpositive > 0 {
        rules.push((
            RpfHealthGrade::Caution,
            format!(
                "{} in-service buses with non-positive nominal_kv",
                topology.buses_nominal_kv_nonpositive
            ),
        ));
    }

    if rules.is_empty() {
        rules.push((
            RpfHealthGrade::Healthy,
            "no elevated health rules triggered".to_string(),
        ));
    }

    rules.sort_by(|a, b| b.0.cmp(&a.0));
    let grade = rules[0].0;
    let reasons = rules.into_iter().map(|(_, r)| r).collect();
    (grade, reasons)
}
