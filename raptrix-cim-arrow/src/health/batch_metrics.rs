// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level Arrow column helpers for read-only RPF health metrics.

use anyhow::{Context, Result, bail};
use arrow::array::Array;
use arrow::array::{BooleanArray, DictionaryArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;

pub(crate) fn table_batch<'a>(
    tables: &'a std::collections::HashMap<String, RecordBatch>,
    name: &str,
) -> Option<&'a RecordBatch> {
    tables.get(name)
}

pub(crate) fn row_in_service(status: Option<&BooleanArray>, row: usize) -> bool {
    status
        .and_then(|s| s.is_valid(row).then(|| s.value(row)))
        .unwrap_or(true)
}

pub(crate) fn row_count(batch: Option<&RecordBatch>) -> usize {
    batch.map(RecordBatch::num_rows).unwrap_or(0)
}

pub(crate) fn column_f64<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a Float64Array>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    Ok(batch.column(idx).as_any().downcast_ref::<Float64Array>())
}

pub(crate) fn column_bool<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a BooleanArray>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    Ok(batch.column(idx).as_any().downcast_ref::<BooleanArray>())
}

pub(crate) fn column_i32<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a Int32Array>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    Ok(batch.column(idx).as_any().downcast_ref::<Int32Array>())
}

pub(crate) fn count_in_service(batch: Option<&RecordBatch>) -> Result<usize> {
    let Some(batch) = batch else {
        return Ok(0);
    };
    let status = column_bool(batch, "status")?;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if row_in_service(status, row) {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn sum_f64_in_service(
    batch: Option<&RecordBatch>,
    column: &str,
    scale: f64,
) -> Result<f64> {
    let Some(batch) = batch else {
        return Ok(0.0);
    };
    let values = column_f64(batch, column)?
        .with_context(|| format!("column '{column}' missing or wrong type"))?;
    let status = column_bool(batch, "status")?;
    let mut sum = 0.0;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row) || !values.is_valid(row) {
            continue;
        }
        sum += values.value(row) * scale;
    }
    Ok(sum)
}

pub(crate) fn sum_gen_reserve(
    batch: Option<&RecordBatch>,
    sched_col: &str,
    min_col: &str,
    max_col: &str,
    up: bool,
) -> Result<f64> {
    let Some(batch) = batch else {
        return Ok(0.0);
    };
    let sched =
        column_f64(batch, sched_col)?.with_context(|| format!("column '{sched_col}' missing"))?;
    let min_v =
        column_f64(batch, min_col)?.with_context(|| format!("column '{min_col}' missing"))?;
    let max_v =
        column_f64(batch, max_col)?.with_context(|| format!("column '{max_col}' missing"))?;
    let status = column_bool(batch, "status")?;
    let mut sum = 0.0;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row)
            || !sched.is_valid(row)
            || !min_v.is_valid(row)
            || !max_v.is_valid(row)
        {
            continue;
        }
        let sched_v = sched.value(row);
        let reserve = if up {
            max_v.value(row) - sched_v
        } else {
            sched_v - min_v.value(row)
        };
        if reserve.is_finite() && reserve > 0.0 {
            sum += reserve;
        }
    }
    Ok(sum)
}

pub(crate) fn sum_reactive_headroom(batch: Option<&RecordBatch>) -> Result<f64> {
    let Some(batch) = batch else {
        return Ok(0.0);
    };
    let q_sched =
        column_f64(batch, "q_sched_mvar")?.with_context(|| "column 'q_sched_mvar' missing")?;
    let q_min = column_f64(batch, "q_min_mvar")?.with_context(|| "column 'q_min_mvar' missing")?;
    let q_max = column_f64(batch, "q_max_mvar")?.with_context(|| "column 'q_max_mvar' missing")?;
    let status = column_bool(batch, "status")?;
    let mut sum = 0.0;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row)
            || !q_sched.is_valid(row)
            || !q_min.is_valid(row)
            || !q_max.is_valid(row)
        {
            continue;
        }
        let sched = q_sched.value(row);
        let headroom_up = q_max.value(row) - sched;
        let headroom_down = sched - q_min.value(row);
        let headroom = headroom_up.max(headroom_down);
        if headroom.is_finite() && headroom > 0.0 {
            sum += headroom;
        }
    }
    Ok(sum)
}

pub(crate) fn metadata_f64(batch: &RecordBatch, column: &str) -> Result<Option<f64>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let values = column_f64(batch, column)?;
    let Some(values) = values else {
        return Ok(None);
    };
    if !values.is_valid(0) {
        return Ok(None);
    }
    let v = values.value(0);
    if v.is_finite() { Ok(Some(v)) } else { Ok(None) }
}

pub(crate) fn metadata_i32(batch: &RecordBatch, column: &str) -> Result<Option<i32>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let values = column_i32(batch, column)?;
    let Some(values) = values else {
        return Ok(None);
    };
    if !values.is_valid(0) {
        return Ok(None);
    }
    Ok(Some(values.value(0)))
}

pub(crate) fn metadata_utf8_at(
    batch: &RecordBatch,
    column: &str,
    row: usize,
) -> Result<Option<String>> {
    if batch.num_rows() <= row {
        return Ok(None);
    }
    let idx = batch.schema().index_of(column)?;
    let array = batch.column(idx);
    match array.data_type() {
        DataType::Utf8 => {
            let s = array.as_any().downcast_ref::<StringArray>().unwrap();
            if s.is_valid(row) {
                Ok(Some(s.value(row).to_string()))
            } else {
                Ok(None)
            }
        }
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .with_context(|| format!("expected dictionary column '{column}'"))?;
            if !dict.is_valid(row) {
                return Ok(None);
            }
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .context("dictionary values must be Utf8")?;
            let key = dict.keys().value(row);
            Ok(Some(values.value(key as usize).to_string()))
        }
        other => bail!("unsupported metadata string type for '{column}': {other:?}"),
    }
}

pub(crate) fn f64_stats_in_service(
    batch: &RecordBatch,
    column: &str,
) -> Result<Option<(f64, f64, f64, usize)>> {
    let values =
        column_f64(batch, column)?.with_context(|| format!("column '{column}' missing"))?;
    let status = column_bool(batch, "status")?;
    let mut min_v = f64::INFINITY;
    let mut max_v = f64::NEG_INFINITY;
    let mut sum = 0.0;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row) || !values.is_valid(row) {
            continue;
        }
        let v = values.value(row);
        if !v.is_finite() {
            continue;
        }
        min_v = min_v.min(v);
        max_v = max_v.max(v);
        sum += v;
        count += 1;
    }
    if count == 0 {
        Ok(None)
    } else {
        Ok(Some((min_v, max_v, sum / count as f64, count)))
    }
}

pub(crate) fn count_voltage_out_of_band(batch: &RecordBatch) -> Result<usize> {
    let v_mag = column_f64(batch, "v_mag_set")?.with_context(|| "column 'v_mag_set' missing")?;
    let v_min = column_f64(batch, "v_min")?.with_context(|| "column 'v_min' missing")?;
    let v_max = column_f64(batch, "v_max")?.with_context(|| "column 'v_max' missing")?;
    let status = column_bool(batch, "status")?;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row)
            || !v_mag.is_valid(row)
            || !v_min.is_valid(row)
            || !v_max.is_valid(row)
        {
            continue;
        }
        let vm = v_mag.value(row);
        let vmin = v_min.value(row);
        let vmax = v_max.value(row);
        if vmin > 0.0 && vmax > vmin && (vm < vmin || vm > vmax) {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn count_nominal_kv_nonpositive(batch: &RecordBatch) -> Result<usize> {
    let kv = column_f64(batch, "nominal_kv")?.with_context(|| "column 'nominal_kv' missing")?;
    let status = column_bool(batch, "status")?;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row) || !kv.is_valid(row) {
            continue;
        }
        if kv.value(row) <= 0.0 {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn count_non_unity_tap(
    batch: Option<&RecordBatch>,
    tap_col: &str,
    nominal_col: Option<&str>,
    epsilon: f64,
) -> Result<usize> {
    let Some(batch) = batch else {
        return Ok(0);
    };
    let tap = column_f64(batch, tap_col)?.with_context(|| format!("column '{tap_col}' missing"))?;
    let nominal = nominal_col
        .map(|c| column_f64(batch, c))
        .transpose()?
        .flatten();
    let status = column_bool(batch, "status")?;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row) || !tap.is_valid(row) {
            continue;
        }
        let tap_v = tap.value(row);
        let reference = nominal
            .and_then(|n| n.is_valid(row).then(|| n.value(row)))
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(1.0);
        if (tap_v - reference).abs() > epsilon {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn count_zip_load_rows(batch: &RecordBatch) -> Result<(usize, usize)> {
    let zip_cols = ["p_i_pu", "q_i_pu", "p_y_pu", "q_y_pu"];
    let mut with_zip = 0usize;
    let total = batch.num_rows();
    for row in 0..total {
        let mut has_zip = false;
        for col in zip_cols {
            if let Some(arr) = column_f64(batch, col)?
                && arr.is_valid(row)
            {
                has_zip = true;
                break;
            }
        }
        if has_zip {
            with_zip += 1;
        }
    }
    Ok((with_zip, total))
}

pub(crate) fn count_gens_at_q_limit(batch: &RecordBatch, epsilon: f64) -> Result<usize> {
    let q_sched =
        column_f64(batch, "q_sched_mvar")?.with_context(|| "column 'q_sched_mvar' missing")?;
    let q_min = column_f64(batch, "q_min_mvar")?.with_context(|| "column 'q_min_mvar' missing")?;
    let q_max = column_f64(batch, "q_max_mvar")?.with_context(|| "column 'q_max_mvar' missing")?;
    let status = column_bool(batch, "status")?;
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if !row_in_service(status, row)
            || !q_sched.is_valid(row)
            || !q_min.is_valid(row)
            || !q_max.is_valid(row)
        {
            continue;
        }
        let q = q_sched.value(row);
        let qmin = q_min.value(row);
        let qmax = q_max.value(row);
        if (q - qmin).abs() <= epsilon || (q - qmax).abs() <= epsilon {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn count_pv_to_pq_solved(batch: &RecordBatch) -> Result<Option<usize>> {
    // Snapshot dialects of generators_solved may omit this column; treat the
    // metric as unavailable rather than failing the whole health inspection.
    let Some(pv) = column_bool(batch, "pv_to_pq")? else {
        return Ok(None);
    };
    let mut count = 0usize;
    for row in 0..batch.num_rows() {
        if pv.is_valid(row) && pv.value(row) {
            count += 1;
        }
    }
    Ok(Some(count))
}

pub(crate) fn initial_voltage_mismatch_rms(
    buses: &RecordBatch,
    buses_solved: &RecordBatch,
) -> Result<Option<f64>> {
    use std::collections::HashMap;

    let bus_ids = column_i32(buses, "bus_id")?.with_context(|| "buses.bus_id missing")?;
    let v_set = column_f64(buses, "v_mag_set")?.with_context(|| "buses.v_mag_set missing")?;
    let ang_set = column_f64(buses, "v_ang_set")?.with_context(|| "buses.v_ang_set missing")?;

    let solved_ids =
        column_i32(buses_solved, "bus_id")?.with_context(|| "buses_solved.bus_id missing")?;
    let v_solved =
        column_f64(buses_solved, "v_mag_pu")?.with_context(|| "buses_solved.v_mag_pu missing")?;
    let ang_solved =
        column_f64(buses_solved, "v_ang_deg")?.with_context(|| "buses_solved.v_ang_deg missing")?;

    let mut solved_map: HashMap<i32, (f64, f64)> = HashMap::new();
    for row in 0..buses_solved.num_rows() {
        if !solved_ids.is_valid(row) || !v_solved.is_valid(row) || !ang_solved.is_valid(row) {
            continue;
        }
        solved_map.insert(
            solved_ids.value(row),
            (v_solved.value(row), ang_solved.value(row)),
        );
    }

    let mut sum_sq = 0.0;
    let mut n = 0usize;
    for row in 0..buses.num_rows() {
        if !bus_ids.is_valid(row) || !v_set.is_valid(row) || !ang_set.is_valid(row) {
            continue;
        }
        let id = bus_ids.value(row);
        let Some((v_sol, ang_sol)) = solved_map.get(&id) else {
            continue;
        };
        let dv = v_set.value(row) - v_sol;
        // `v_ang_set` is radians; `buses_solved.v_ang_deg` is degrees.
        let da_rad = ang_set.value(row) - ang_sol.to_radians();
        sum_sq += dv * dv + da_rad * da_rad;
        n += 1;
    }

    if n == 0 {
        Ok(None)
    } else {
        Ok(Some((sum_sq / n as f64).sqrt()))
    }
}
