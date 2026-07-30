// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders and semantic validation for optional `computational_load_profiles`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Builder,
    Int32Array, Int32Builder, ListBuilder, MapBuilder, MapFieldNames, RecordBatch, StringArray,
    StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type};

use crate::schema::{
    METADATA_KEY_COMPUTATIONAL_LOAD_MODE, buildout_schedule_element_fields,
    computational_load_profiles_schema, seasonal_envelope_element_fields,
};

/// Closed facility_class vocabulary (v0.13.0).
pub const FACILITY_CLASSES: &[&str] = &["cloud_storage", "ai_hpc", "crypto", "mixed", "other"];

#[derive(Debug, Clone, Default)]
pub struct SeasonalEnvelopeEntry {
    pub season: String,
    pub min_mw: f32,
    pub max_mw: f32,
    pub pf: f32,
}

#[derive(Debug, Clone, Default)]
pub struct BuildoutEntry {
    pub year: i32,
    pub mw: f32,
}

/// Domain row for one `computational_load_profiles` record.
#[derive(Debug, Clone, Default)]
pub struct ComputationalLoadProfileRow {
    pub bus_id: Option<i32>,
    pub load_id: Option<String>,
    pub seasonal_envelope: Option<Vec<SeasonalEnvelopeEntry>>,
    pub buildout_schedule: Option<Vec<BuildoutEntry>>,
    pub ramp_rate_up_mw_per_min: Option<f32>,
    pub ramp_rate_down_mw_per_min: Option<f32>,
    pub it_load_percent: Option<f32>,
    pub non_it_load_percent: Option<f32>,
    pub it_allocation_mode: Option<String>,
    pub ups_config: Option<HashMap<String, f64>>,
    pub pcc_relay_settings: Option<HashMap<String, f64>>,
    pub onsite_gen_bess_mw: Option<f32>,
    pub onsite_gen_parallel: Option<bool>,
    pub bess_ramp_rate_mw_per_min: Option<f32>,
    pub facility_use_case_percent: Option<HashMap<String, f64>>,
    pub mrid: Option<String>,
    pub poi_name: Option<String>,
    pub facility_class: Option<String>,
    pub priority: Option<i32>,
    pub max_step_drop_mw: Option<f32>,
    pub trip_study_percentiles: Option<Vec<f32>>,
    pub common_mode_group: Option<String>,
    pub voltage_sensitivity_hint: Option<f32>,
    pub transfer_to_backup_threshold_pu: Option<f32>,
    pub transfer_delay_ms: Option<f32>,
    pub reconnection_criteria: Option<HashMap<String, f64>>,
    pub ride_through_capability: Option<HashMap<String, f64>>,
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

fn append_optional_f32(builder: &mut Float32Builder, value: Option<f32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_optional_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_optional_bool(builder: &mut BooleanBuilder, value: Option<bool>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_optional_utf8(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_optional_dict(
    builder: &mut StringDictionaryBuilder<Int32Type>,
    value: Option<&str>,
) -> Result<()> {
    match value {
        Some(v) => {
            builder.append(v).context("append dictionary utf8")?;
        }
        None => builder.append_null(),
    }
    Ok(())
}

fn append_string_f64_map(
    builder: &mut MapBuilder<StringBuilder, Float64Builder>,
    map: Option<&HashMap<String, f64>>,
) -> Result<()> {
    match map {
        None => {
            builder.append(false).context("append null map")?;
        }
        Some(entries) if entries.is_empty() => {
            builder.append(true).context("append empty map")?;
        }
        Some(entries) => {
            for (k, v) in entries {
                builder.keys().append_value(k);
                builder.values().append_value(*v);
            }
            builder.append(true).context("append map")?;
        }
    }
    Ok(())
}

/// Build a `computational_load_profiles` RecordBatch matching the locked schema.
pub fn build_computational_load_profiles_batch(
    rows: &[ComputationalLoadProfileRow],
) -> Result<RecordBatch> {
    let schema = Arc::new(computational_load_profiles_schema());
    let n = rows.len();

    let seasonal_item = Field::new(
        "item",
        DataType::Struct(seasonal_envelope_element_fields().into()),
        false,
    );
    let buildout_item = Field::new(
        "item",
        DataType::Struct(buildout_schedule_element_fields().into()),
        false,
    );
    let percentile_item = Field::new("item", DataType::Float32, false);

    let mut bus_id = Int32Builder::with_capacity(n);
    let mut load_id = StringDictionaryBuilder::<Int32Type>::new();
    let mut seasonal = ListBuilder::new(StructBuilder::from_fields(
        seasonal_envelope_element_fields(),
        n,
    ))
    .with_field(Arc::new(seasonal_item));
    let mut buildout = ListBuilder::new(StructBuilder::from_fields(
        buildout_schedule_element_fields(),
        n,
    ))
    .with_field(Arc::new(buildout_item));
    let mut ramp_up = Float32Builder::with_capacity(n);
    let mut ramp_down = Float32Builder::with_capacity(n);
    let mut it_pct = Float32Builder::with_capacity(n);
    let mut non_it_pct = Float32Builder::with_capacity(n);
    let mut it_mode = StringDictionaryBuilder::<Int32Type>::new();
    let mut ups = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));
    let mut pcc = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));
    let mut onsite_mw = Float32Builder::with_capacity(n);
    let mut onsite_parallel = BooleanBuilder::with_capacity(n);
    let mut bess_ramp = Float32Builder::with_capacity(n);
    let mut facility_use = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));
    let mut mrid = StringBuilder::with_capacity(n, n * 16);
    let mut poi_name = StringBuilder::with_capacity(n, n * 24);
    let mut facility_class = StringDictionaryBuilder::<Int32Type>::new();
    let mut priority = Int32Builder::with_capacity(n);
    let mut max_drop = Float32Builder::with_capacity(n);
    let mut percentiles =
        ListBuilder::new(Float32Builder::new()).with_field(Arc::new(percentile_item));
    let mut common_mode = StringBuilder::with_capacity(n, n * 16);
    let mut v_hint = Float32Builder::with_capacity(n);
    let mut transfer_thresh = Float32Builder::with_capacity(n);
    let mut transfer_delay = Float32Builder::with_capacity(n);
    let mut reconnection = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));
    let mut ride_through = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));

    for row in rows {
        append_optional_i32(&mut bus_id, row.bus_id);
        append_optional_dict(&mut load_id, row.load_id.as_deref())?;

        match &row.seasonal_envelope {
            None => {
                seasonal.append(false);
            }
            Some(items) => {
                let values = seasonal.values();
                for item in items {
                    values
                        .field_builder::<StringBuilder>(0)
                        .expect("season")
                        .append_value(&item.season);
                    values
                        .field_builder::<Float32Builder>(1)
                        .expect("min_mw")
                        .append_value(item.min_mw);
                    values
                        .field_builder::<Float32Builder>(2)
                        .expect("max_mw")
                        .append_value(item.max_mw);
                    values
                        .field_builder::<Float32Builder>(3)
                        .expect("pf")
                        .append_value(item.pf);
                    values.append(true);
                }
                seasonal.append(true);
            }
        }

        match &row.buildout_schedule {
            None => {
                buildout.append(false);
            }
            Some(items) => {
                let values = buildout.values();
                for item in items {
                    values
                        .field_builder::<Int32Builder>(0)
                        .expect("year")
                        .append_value(item.year);
                    values
                        .field_builder::<Float32Builder>(1)
                        .expect("mw")
                        .append_value(item.mw);
                    values.append(true);
                }
                buildout.append(true);
            }
        }

        append_optional_f32(&mut ramp_up, row.ramp_rate_up_mw_per_min);
        append_optional_f32(&mut ramp_down, row.ramp_rate_down_mw_per_min);
        append_optional_f32(&mut it_pct, row.it_load_percent);
        append_optional_f32(&mut non_it_pct, row.non_it_load_percent);
        append_optional_dict(&mut it_mode, row.it_allocation_mode.as_deref())?;
        append_string_f64_map(&mut ups, row.ups_config.as_ref())?;
        append_string_f64_map(&mut pcc, row.pcc_relay_settings.as_ref())?;
        append_optional_f32(&mut onsite_mw, row.onsite_gen_bess_mw);
        append_optional_bool(&mut onsite_parallel, row.onsite_gen_parallel);
        append_optional_f32(&mut bess_ramp, row.bess_ramp_rate_mw_per_min);
        append_string_f64_map(&mut facility_use, row.facility_use_case_percent.as_ref())?;
        append_optional_utf8(&mut mrid, row.mrid.as_deref());
        append_optional_utf8(&mut poi_name, row.poi_name.as_deref());
        append_optional_dict(&mut facility_class, row.facility_class.as_deref())?;
        append_optional_i32(&mut priority, row.priority);
        append_optional_f32(&mut max_drop, row.max_step_drop_mw);

        match &row.trip_study_percentiles {
            None => percentiles.append(false),
            Some(pcts) => {
                for p in pcts {
                    percentiles.values().append_value(*p);
                }
                percentiles.append(true);
            }
        }

        append_optional_utf8(&mut common_mode, row.common_mode_group.as_deref());
        append_optional_f32(&mut v_hint, row.voltage_sensitivity_hint);
        append_optional_f32(&mut transfer_thresh, row.transfer_to_backup_threshold_pu);
        append_optional_f32(&mut transfer_delay, row.transfer_delay_ms);
        append_string_f64_map(&mut reconnection, row.reconnection_criteria.as_ref())?;
        append_string_f64_map(&mut ride_through, row.ride_through_capability.as_ref())?;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(load_id.finish()),
            Arc::new(seasonal.finish()),
            Arc::new(buildout.finish()),
            Arc::new(ramp_up.finish()),
            Arc::new(ramp_down.finish()),
            Arc::new(it_pct.finish()),
            Arc::new(non_it_pct.finish()),
            Arc::new(it_mode.finish()),
            Arc::new(ups.finish()),
            Arc::new(pcc.finish()),
            Arc::new(onsite_mw.finish()),
            Arc::new(onsite_parallel.finish()),
            Arc::new(bess_ramp.finish()),
            Arc::new(facility_use.finish()),
            Arc::new(mrid.finish()),
            Arc::new(poi_name.finish()),
            Arc::new(facility_class.finish()),
            Arc::new(priority.finish()),
            Arc::new(max_drop.finish()),
            Arc::new(percentiles.finish()),
            Arc::new(common_mode.finish()),
            Arc::new(v_hint.finish()),
            Arc::new(transfer_thresh.finish()),
            Arc::new(transfer_delay.finish()),
            Arc::new(reconnection.finish()),
            Arc::new(ride_through.finish()),
        ],
    )
    .context("building computational_load_profiles batch")
}

/// Set nullable `metadata.computational_load_mode` without disturbing other columns.
pub fn patch_metadata_computational_load_mode(
    meta: &RecordBatch,
    mode: Option<bool>,
) -> Result<RecordBatch> {
    let schema = meta.schema();
    let idx = schema
        .index_of(METADATA_KEY_COMPUTATIONAL_LOAD_MODE)
        .with_context(|| {
            format!("metadata missing column '{METADATA_KEY_COMPUTATIONAL_LOAD_MODE}'")
        })?;
    let n = meta.num_rows();
    if n == 0 {
        bail!("cannot patch computational_load_mode on empty metadata table");
    }
    let mut columns: Vec<ArrayRef> = meta.columns().to_vec();
    columns[idx] = Arc::new(BooleanArray::from(vec![mode; n]));
    RecordBatch::try_new(schema, columns).context("patch metadata.computational_load_mode")
}

fn dict_value_at(col: &ArrayRef, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Dictionary(_, _) => {
            let dict = col
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()?;
            let values = dict.values().as_any().downcast_ref::<StringArray>()?;
            let key = dict.key(row)?;
            Some(values.value(key).to_string())
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(row).to_string())
        }
        _ => None,
    }
}

fn f32_at(col: &ArrayRef, row: usize) -> Option<f32> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float32Array>()
        .map(|a| a.value(row))
}

fn i32_at(col: &ArrayRef, row: usize) -> Option<i32> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
}

/// Semantic validation for computational-load interchange profiles.
///
/// When `mode == Some(true)`, the table must be non-empty and every row must satisfy
/// the locked contract (exactly one of bus_id/load_id, percentiles 0–100, etc.).
pub fn validate_computational_load_profiles_batch(
    batch: &RecordBatch,
    mode: Option<bool>,
) -> Result<()> {
    let schema = computational_load_profiles_schema();
    if batch.schema().fields().len() != schema.fields().len() {
        bail!(
            "computational_load_profiles: expected {} columns, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }

    let mode_on = mode == Some(true);
    if mode_on && batch.num_rows() == 0 {
        bail!(
            "computational_load_mode=true requires a non-empty computational_load_profiles table"
        );
    }

    let bus_id = batch.column_by_name("bus_id").context("bus_id")?;
    let load_id = batch.column_by_name("load_id").context("load_id")?;
    let priority = batch.column_by_name("priority").context("priority")?;
    let max_drop = batch
        .column_by_name("max_step_drop_mw")
        .context("max_step_drop_mw")?;
    let percentiles = batch
        .column_by_name("trip_study_percentiles")
        .context("trip_study_percentiles")?;
    let facility_class = batch
        .column_by_name("facility_class")
        .context("facility_class")?;

    for row in 0..batch.num_rows() {
        let has_bus = !bus_id.is_null(row);
        let has_load = !load_id.is_null(row);
        if has_bus == has_load {
            bail!(
                "computational_load_profiles row {row}: require exactly one of bus_id or load_id to be set"
            );
        }

        if let Some(p) = i32_at(priority, row) {
            if !(1..=5).contains(&p) {
                bail!("computational_load_profiles row {row}: priority must be 1–5 (got {p})");
            }
        }

        if let Some(mw) = f32_at(max_drop, row) {
            if !mw.is_finite() {
                bail!("computational_load_profiles row {row}: max_step_drop_mw must be finite");
            }
        }

        if let Some(fc) = dict_value_at(facility_class, row) {
            if !FACILITY_CLASSES.iter().any(|c| *c == fc) {
                bail!(
                    "computational_load_profiles row {row}: facility_class '{fc}' is not in closed set"
                );
            }
        }

        if !percentiles.is_null(row) {
            let list = percentiles
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .context("trip_study_percentiles list")?;
            let values = list.value(row);
            let arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("trip_study_percentiles values")?;
            for i in 0..arr.len() {
                let v = arr.value(i);
                if !v.is_finite() || !(0.0..=100.0).contains(&v) {
                    bail!(
                        "computational_load_profiles row {row}: trip_study_percentiles must be 0–100 percentage points (got {v})"
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> ComputationalLoadProfileRow {
        ComputationalLoadProfileRow {
            bus_id: Some(1001),
            load_id: None,
            poi_name: Some("Campus A POI 1".into()),
            facility_class: Some("ai_hpc".into()),
            priority: Some(1),
            max_step_drop_mw: Some(1200.0),
            trip_study_percentiles: Some(vec![60.0, 100.0]),
            common_mode_group: Some("campus_a".into()),
            seasonal_envelope: Some(vec![SeasonalEnvelopeEntry {
                season: "summer".into(),
                min_mw: 800.0,
                max_mw: 1200.0,
                pf: 0.95,
            }]),
            buildout_schedule: Some(vec![BuildoutEntry {
                year: 2027,
                mw: 1500.0,
            }]),
            ups_config: Some(HashMap::from([("autonomy_min".into(), 15.0)])),
            ..Default::default()
        }
    }

    #[test]
    fn build_batch_nested_types() -> Result<()> {
        let batch = build_computational_load_profiles_batch(&[
            sample_row(),
            ComputationalLoadProfileRow {
                bus_id: Some(1002),
                facility_class: Some("mixed".into()),
                priority: Some(2),
                max_step_drop_mw: Some(800.0),
                trip_study_percentiles: Some(vec![100.0]),
                common_mode_group: Some("campus_a".into()),
                ..Default::default()
            },
        ])?;
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch.num_columns(),
            computational_load_profiles_schema().fields().len()
        );
        validate_computational_load_profiles_batch(&batch, Some(true))?;
        Ok(())
    }

    #[test]
    fn validate_rejects_dual_bus_and_load_id() {
        let row = ComputationalLoadProfileRow {
            bus_id: Some(1),
            load_id: Some("1".into()),
            ..Default::default()
        };
        let batch = build_computational_load_profiles_batch(&[row]).unwrap();
        let err = validate_computational_load_profiles_batch(&batch, Some(true)).unwrap_err();
        assert!(err.to_string().contains("exactly one of bus_id or load_id"));
    }

    #[test]
    fn validate_trip_percentiles_range() {
        // Reject >100 (0–100 percentage points on the wire).
        let bad = ComputationalLoadProfileRow {
            bus_id: Some(1),
            trip_study_percentiles: Some(vec![150.0]),
            ..Default::default()
        };
        let batch = build_computational_load_profiles_batch(&[bad]).unwrap();
        let err = validate_computational_load_profiles_batch(&batch, Some(true)).unwrap_err();
        assert!(err.to_string().contains("0–100"));
    }

    #[test]
    fn validate_rejects_empty_when_mode_on() {
        let batch = build_computational_load_profiles_batch(&[]).unwrap();
        let err = validate_computational_load_profiles_batch(&batch, Some(true)).unwrap_err();
        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn patch_metadata_computational_load_mode_sets_bool() -> Result<()> {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::new(vec![
            Field::new("case_id", DataType::Utf8, true),
            Field::new(
                METADATA_KEY_COMPUTATIONAL_LOAD_MODE,
                DataType::Boolean,
                true,
            ),
        ]));
        let meta = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec![Some("c1")])),
                Arc::new(BooleanArray::from(vec![None as Option<bool>])),
            ],
        )?;
        let patched = patch_metadata_computational_load_mode(&meta, Some(true))?;
        let idx = patched
            .schema()
            .index_of(METADATA_KEY_COMPUTATIONAL_LOAD_MODE)?;
        let arr = patched
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.value(0));
        let cleared = patch_metadata_computational_load_mode(&patched, None)?;
        let arr = cleared
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.is_null(0));
        Ok(())
    }
}
