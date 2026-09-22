// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders and validation for `dc_converters` (v0.14.4+).
//!
//! `dc_lines_2w.p_setpoint_mw` stays rectifier DC power. `is_meter_end` records
//! which terminal PSS/E `METER` names and does not move that setpoint.
//! Q(V) reads `n_bridges`, `ebas_kv`, `tr`, and `tap`. A null among those four
//! means that end has real power only. `xc_ohm`, `alpha_deg`, and `gamma_deg`
//! are stored and are not equation inputs.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, RecordBatch, StringArray, StringBuilder,
};

use crate::schema::{
    DC_CONVERTER_AC_CONTROLS, DC_CONVERTER_KINDS, DC_CONVERTER_ROLES, dc_converters_schema,
};

/// One AC terminal of a two-terminal DC line.
#[derive(Debug, Clone)]
pub struct DcConverterRow {
    pub dc_line_id: i32,
    pub bus_id: i32,
    pub role: String,
    pub converter_kind: String,
    pub n_bridges: Option<i32>,
    pub ebas_kv: Option<f64>,
    pub tr: Option<f64>,
    pub tap: Option<f64>,
    pub tap_min: Option<f64>,
    pub tap_max: Option<f64>,
    pub xc_ohm: Option<f64>,
    pub alpha_deg: Option<f64>,
    pub gamma_deg: Option<f64>,
    pub is_meter_end: bool,
    pub ac_control: Option<String>,
    pub ac_setpoint: Option<f64>,
    pub q_min_mvar: Option<f64>,
    pub q_max_mvar: Option<f64>,
    pub s_max_mva: Option<f64>,
    pub i_max_ka: Option<f64>,
    pub loss_const_mw: Option<f64>,
    pub loss_i_mw_per_ka: Option<f64>,
}

impl Default for DcConverterRow {
    fn default() -> Self {
        Self {
            dc_line_id: 0,
            bus_id: 0,
            role: "rectifier".to_string(),
            converter_kind: "lcc".to_string(),
            n_bridges: None,
            ebas_kv: None,
            tr: None,
            tap: None,
            tap_min: None,
            tap_max: None,
            xc_ohm: None,
            alpha_deg: None,
            gamma_deg: None,
            is_meter_end: false,
            ac_control: None,
            ac_setpoint: None,
            q_min_mvar: None,
            q_max_mvar: None,
            s_max_mva: None,
            i_max_ka: None,
            loss_const_mw: None,
            loss_i_mw_per_ka: None,
        }
    }
}

fn append_opt_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_opt_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_opt_utf8(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Empty `dc_converters` batch (zero rows).
pub fn empty_dc_converters_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(dc_converters_schema()))
}

/// Build a `dc_converters` batch. Does not invent `1.0` for a null ratio or tap.
pub fn build_dc_converters_batch(rows: &[DcConverterRow]) -> Result<RecordBatch> {
    let schema = Arc::new(dc_converters_schema());
    let n = rows.len();
    let mut dc_line_id = Int32Builder::with_capacity(n);
    let mut bus_id = Int32Builder::with_capacity(n);
    let mut role = StringBuilder::with_capacity(n, n * 12);
    let mut converter_kind = StringBuilder::with_capacity(n, n * 4);
    let mut n_bridges = Int32Builder::with_capacity(n);
    let mut ebas_kv = Float64Builder::with_capacity(n);
    let mut tr = Float64Builder::with_capacity(n);
    let mut tap = Float64Builder::with_capacity(n);
    let mut tap_min = Float64Builder::with_capacity(n);
    let mut tap_max = Float64Builder::with_capacity(n);
    let mut xc_ohm = Float64Builder::with_capacity(n);
    let mut alpha_deg = Float64Builder::with_capacity(n);
    let mut gamma_deg = Float64Builder::with_capacity(n);
    let mut is_meter_end = BooleanBuilder::with_capacity(n);
    let mut ac_control = StringBuilder::with_capacity(n, n * 4);
    let mut ac_setpoint = Float64Builder::with_capacity(n);
    let mut q_min_mvar = Float64Builder::with_capacity(n);
    let mut q_max_mvar = Float64Builder::with_capacity(n);
    let mut s_max_mva = Float64Builder::with_capacity(n);
    let mut i_max_ka = Float64Builder::with_capacity(n);
    let mut loss_const_mw = Float64Builder::with_capacity(n);
    let mut loss_i_mw_per_ka = Float64Builder::with_capacity(n);

    for row in rows {
        dc_line_id.append_value(row.dc_line_id);
        bus_id.append_value(row.bus_id);
        role.append_value(&row.role);
        converter_kind.append_value(&row.converter_kind);
        append_opt_i32(&mut n_bridges, row.n_bridges);
        append_opt_f64(&mut ebas_kv, row.ebas_kv);
        append_opt_f64(&mut tr, row.tr);
        append_opt_f64(&mut tap, row.tap);
        append_opt_f64(&mut tap_min, row.tap_min);
        append_opt_f64(&mut tap_max, row.tap_max);
        append_opt_f64(&mut xc_ohm, row.xc_ohm);
        append_opt_f64(&mut alpha_deg, row.alpha_deg);
        append_opt_f64(&mut gamma_deg, row.gamma_deg);
        is_meter_end.append_value(row.is_meter_end);
        append_opt_utf8(&mut ac_control, row.ac_control.as_deref());
        append_opt_f64(&mut ac_setpoint, row.ac_setpoint);
        append_opt_f64(&mut q_min_mvar, row.q_min_mvar);
        append_opt_f64(&mut q_max_mvar, row.q_max_mvar);
        append_opt_f64(&mut s_max_mva, row.s_max_mva);
        append_opt_f64(&mut i_max_ka, row.i_max_ka);
        append_opt_f64(&mut loss_const_mw, row.loss_const_mw);
        append_opt_f64(&mut loss_i_mw_per_ka, row.loss_i_mw_per_ka);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dc_line_id.finish()) as ArrayRef,
            Arc::new(bus_id.finish()),
            Arc::new(role.finish()),
            Arc::new(converter_kind.finish()),
            Arc::new(n_bridges.finish()),
            Arc::new(ebas_kv.finish()),
            Arc::new(tr.finish()),
            Arc::new(tap.finish()),
            Arc::new(tap_min.finish()),
            Arc::new(tap_max.finish()),
            Arc::new(xc_ohm.finish()),
            Arc::new(alpha_deg.finish()),
            Arc::new(gamma_deg.finish()),
            Arc::new(is_meter_end.finish()),
            Arc::new(ac_control.finish()),
            Arc::new(ac_setpoint.finish()),
            Arc::new(q_min_mvar.finish()),
            Arc::new(q_max_mvar.finish()),
            Arc::new(s_max_mva.finish()),
            Arc::new(i_max_ka.finish()),
            Arc::new(loss_const_mw.finish()),
            Arc::new(loss_i_mw_per_ka.finish()),
        ],
    )
    .context("building dc_converters batch")
}

fn utf8_at(col: &ArrayRef, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_string())
}

fn i32_at(col: &ArrayRef, row: usize) -> Option<i32> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
}

fn f64_at(col: &ArrayRef, row: usize) -> Option<f64> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
}

fn require_finite(value: Option<f64>, row: usize, name: &str) -> Result<()> {
    if let Some(v) = value
        && !v.is_finite()
    {
        bail!("dc_converters row {row}: {name} must be finite");
    }
    Ok(())
}

struct LineEnd {
    from_bus_id: i32,
    to_bus_id: i32,
    converter_type: String,
}

fn line_index(dc_lines: &RecordBatch) -> Result<std::collections::HashMap<i32, LineEnd>> {
    let ids = dc_lines
        .column_by_name("dc_line_id")
        .context("dc_lines_2w.dc_line_id")?
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("dc_lines_2w.dc_line_id must be Int32")?;
    let from = dc_lines
        .column_by_name("from_bus_id")
        .context("dc_lines_2w.from_bus_id")?
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("dc_lines_2w.from_bus_id must be Int32")?;
    let to = dc_lines
        .column_by_name("to_bus_id")
        .context("dc_lines_2w.to_bus_id")?
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("dc_lines_2w.to_bus_id must be Int32")?;
    let kind = dc_lines
        .column_by_name("converter_type")
        .context("dc_lines_2w.converter_type")?;
    let mut out = std::collections::HashMap::new();
    for row in 0..dc_lines.num_rows() {
        if ids.is_null(row) || from.is_null(row) || to.is_null(row) {
            continue;
        }
        out.insert(
            ids.value(row),
            LineEnd {
                from_bus_id: from.value(row),
                to_bus_id: to.value(row),
                converter_type: utf8_at(kind, row).unwrap_or_default(),
            },
        );
    }
    Ok(out)
}

/// Check converter rows against `dc_lines_2w`.
///
/// A line with no converter row is valid (real power only). A null among
/// `n_bridges`, `ebas_kv`, `tr`, and `tap` is valid and means no Q(V) for that end.
pub fn validate_dc_converters(dc_lines: &RecordBatch, converters: &RecordBatch) -> Result<()> {
    let lines = line_index(dc_lines)?;
    let n = converters.num_rows();
    let dc_line_id = converters
        .column_by_name("dc_line_id")
        .context("dc_line_id")?;
    let bus_id = converters.column_by_name("bus_id").context("bus_id")?;
    let role = converters.column_by_name("role").context("role")?;
    let converter_kind = converters
        .column_by_name("converter_kind")
        .context("converter_kind")?;
    let n_bridges = converters
        .column_by_name("n_bridges")
        .context("n_bridges")?;
    let ebas_kv = converters.column_by_name("ebas_kv").context("ebas_kv")?;
    let is_meter = converters
        .column_by_name("is_meter_end")
        .context("is_meter_end")?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("is_meter_end must be Boolean")?;
    let ac_control = converters
        .column_by_name("ac_control")
        .context("ac_control")?;

    let mut seen: HashSet<(i32, String)> = HashSet::new();
    let mut meter_true: HashSet<i32> = HashSet::new();

    for row in 0..n {
        let line_id = i32_at(dc_line_id, row)
            .with_context(|| format!("dc_converters row {row}: dc_line_id is required"))?;
        let line = lines.get(&line_id).with_context(|| {
            format!("dc_converters row {row}: dc_line_id {line_id} is not in dc_lines_2w")
        })?;
        let role_token = utf8_at(role, row).unwrap_or_default();
        if !DC_CONVERTER_ROLES.contains(&role_token.as_str()) {
            bail!("dc_converters row {row}: role '{role_token}' is not rectifier or inverter");
        }
        if !seen.insert((line_id, role_token.clone())) {
            bail!("dc_converters row {row}: duplicate role '{role_token}' on dc_line_id {line_id}");
        }
        let bus = i32_at(bus_id, row).context("dc_converters bus_id")?;
        let expected_bus = if role_token == "rectifier" {
            line.from_bus_id
        } else {
            line.to_bus_id
        };
        if bus != expected_bus {
            bail!(
                "dc_converters row {row}: {role_token} bus_id {bus} does not match dc_lines_2w bus {expected_bus}"
            );
        }
        let kind = utf8_at(converter_kind, row).unwrap_or_default();
        if !DC_CONVERTER_KINDS.contains(&kind.as_str()) {
            bail!("dc_converters row {row}: converter_kind '{kind}' is not lcc or vsc");
        }
        if kind != line.converter_type {
            bail!(
                "dc_converters row {row}: converter_kind '{kind}' does not match dc_lines_2w.converter_type '{}'",
                line.converter_type
            );
        }
        if let Some(bridges) = i32_at(n_bridges, row)
            && !(1..=12).contains(&bridges)
        {
            bail!("dc_converters row {row}: n_bridges must be null or in 1..=12");
        }
        if let Some(ebas) = f64_at(ebas_kv, row)
            && (!ebas.is_finite() || ebas <= 0.0)
        {
            bail!("dc_converters row {row}: ebas_kv must be null or > 0");
        }
        for name in [
            "tr",
            "tap",
            "tap_min",
            "tap_max",
            "xc_ohm",
            "alpha_deg",
            "gamma_deg",
            "ac_setpoint",
            "q_min_mvar",
            "q_max_mvar",
            "s_max_mva",
            "i_max_ka",
            "loss_const_mw",
            "loss_i_mw_per_ka",
        ] {
            let col = converters.column_by_name(name).context(name)?;
            require_finite(f64_at(col, row), row, name)?;
        }
        if let Some(control) = utf8_at(ac_control, row)
            && !DC_CONVERTER_AC_CONTROLS.contains(&control.as_str())
        {
            bail!("dc_converters row {row}: ac_control '{control}' is not v_ac or q");
        }
        if kind == "lcc" {
            for name in [
                "ac_control",
                "ac_setpoint",
                "q_min_mvar",
                "q_max_mvar",
                "s_max_mva",
                "i_max_ka",
                "loss_const_mw",
                "loss_i_mw_per_ka",
            ] {
                let col = converters.column_by_name(name).context(name)?;
                if !col.is_null(row) {
                    bail!(
                        "dc_converters row {row}: {name} must be null when converter_kind is lcc"
                    );
                }
            }
        }
        if is_meter.is_null(row) {
            bail!("dc_converters row {row}: is_meter_end is required");
        }
        if is_meter.value(row) && !meter_true.insert(line_id) {
            bail!("dc_converters dc_line_id {line_id}: more than one is_meter_end");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::dc_lines_2w_schema;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};

    fn worked_line() -> RecordBatch {
        let schema = Arc::new(dc_lines_2w_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![58540])),
            Arc::new(Int32Array::from(vec![55247])),
            Arc::new(StringArray::from(vec!["1"])),
            Arc::new(Float64Array::from(vec![10.0])),
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Arc::new(StringArray::from(vec!["power"])),
            Arc::new(Float64Array::from(vec![Some(550.0)])),
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Arc::new(Float64Array::from(vec![Some(250.0)])),
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![Some("DC Line 1")])),
            Arc::new(StringArray::from(vec!["lcc"])),
        ];
        assert_eq!(columns.len(), 15);
        assert_eq!(schema.fields().len(), 15);
        RecordBatch::try_new(schema, columns).expect("line")
    }

    fn worked_end(bus: i32, role: &str, meter: bool) -> DcConverterRow {
        DcConverterRow {
            dc_line_id: 1,
            bus_id: bus,
            role: role.to_string(),
            converter_kind: "lcc".to_string(),
            n_bridges: Some(2),
            ebas_kv: Some(345.0),
            tr: Some(0.55784),
            tap: Some(1.5),
            tap_min: Some(0.51),
            tap_max: Some(1.5),
            xc_ohm: Some(10.0),
            alpha_deg: Some(15.0),
            gamma_deg: Some(15.0),
            is_meter_end: meter,
            ..DcConverterRow::default()
        }
    }

    #[test]
    fn worked_lcc_example_keeps_fifteen_line_columns() {
        let line = worked_line();
        assert_eq!(
            line.column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "DC Line 1"
        );
        assert!(
            line.column_by_name("q_from_mvar").unwrap().is_null(0)
                && line.column_by_name("q_to_mvar").unwrap().is_null(0)
        );
        let rows = vec![
            worked_end(58540, "rectifier", false),
            worked_end(55247, "inverter", true),
        ];
        let batch = build_dc_converters_batch(&rows).unwrap();
        validate_dc_converters(&line, &batch).unwrap();
        let meter = batch
            .column_by_name("is_meter_end")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let buses = batch
            .column_by_name("bus_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!meter.value(0) && buses.value(0) == 58540);
        assert!(meter.value(1) && buses.value(1) == 55247);
        assert!(batch.column_by_name("ac_control").unwrap().is_null(0));
    }

    #[test]
    fn worked_example_round_trips_in_a_v0144_file() {
        use crate::schema::{TABLE_DC_CONVERTERS, TABLE_DC_LINES_2W, all_table_schemas};
        use crate::{RootWriteOptions, read_rpf_tables, rpf_file_metadata, write_root_rpf};
        use std::collections::HashMap;

        let dir = std::env::temp_dir().join("raptrix_dc_converters_round_trip");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("worked.rpf");
        let mut batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();
        batches.insert(TABLE_DC_LINES_2W, worked_line());
        let converters = build_dc_converters_batch(&[
            worked_end(58540, "rectifier", false),
            worked_end(55247, "inverter", true),
        ])
        .unwrap();
        batches.insert(TABLE_DC_CONVERTERS, converters);
        write_root_rpf(&path, &batches, &RootWriteOptions::default()).unwrap();

        let tables = read_rpf_tables(&path).unwrap();
        let line = tables
            .iter()
            .find(|(name, _)| name == TABLE_DC_LINES_2W)
            .unwrap()
            .1
            .clone();
        assert_eq!(line.num_columns(), 15);
        assert_eq!(
            line.column_by_name("p_setpoint_mw")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            550.0
        );
        let ends = tables
            .iter()
            .find(|(name, _)| name == TABLE_DC_CONVERTERS)
            .unwrap()
            .1
            .clone();
        assert_eq!(ends.num_rows(), 2);
        let meter = ends
            .column_by_name("is_meter_end")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!meter.value(0));
        assert!(meter.value(1));
        let meta = rpf_file_metadata(&path).unwrap();
        assert_eq!(
            meta.get("raptrix.features.dc_converters")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(meta.get("rpf_version").map(String::as_str), Some("v0.14.4"));
    }

    #[test]
    fn null_q_input_is_not_filled() {
        let line = worked_line();
        let mut row = worked_end(58540, "rectifier", false);
        row.tr = None;
        let batch = build_dc_converters_batch(&[row]).unwrap();
        assert!(batch.column_by_name("tr").unwrap().is_null(0));
        validate_dc_converters(&line, &batch).unwrap();
    }
}
