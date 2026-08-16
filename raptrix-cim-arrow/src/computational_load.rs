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
    StringBuilder, StringDictionaryBuilder, StructBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, TimeUnit};

use crate::schema::{
    METADATA_KEY_COMPUTATIONAL_LOAD_MODE, PROTECTION_SETTINGS_SOURCES, VOLTAGE_MEASUREMENT_BASES,
    VOLTAGE_MEASUREMENT_LOCATIONS, VOLTAGE_TRANSFER_ACTIONS, VOLTAGE_TRANSFER_LOAD_CLASSES,
    VOLTAGE_TRANSFER_POLARITIES, buildout_schedule_element_fields,
    computational_load_profiles_schema, disturbance_counter_struct_fields,
    protection_settings_provenance_struct_fields, reconnection_params_struct_fields,
    seasonal_envelope_element_fields, voltage_measurement_struct_fields,
    voltage_transfer_curve_element_fields,
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

/// One stage of a multi-stage voltage-time transfer curve (v0.13.1+).
#[derive(Debug, Clone)]
pub struct VoltageTransferCurveStage {
    pub v_pu: f32,
    pub t_ms: f32,
    pub polarity: String,
    pub action: String,
    pub mw_fraction: Option<f32>,
    pub load_class: Option<String>,
}

/// Optional disturbance / multi-strike counter (v0.13.1+).
#[derive(Debug, Clone, Default)]
pub struct DisturbanceCounter {
    pub strike_limit: Option<i32>,
    pub window_sec: Option<f32>,
    pub qualifying_v_pu: Option<f32>,
    pub qualifying_duration_ms: Option<f32>,
    pub latch_permanent: Option<bool>,
}

/// Typed reconnection parameters (v0.13.1+); opaque `reconnection_criteria` map retained.
#[derive(Debug, Clone, Default)]
pub struct ReconnectionParams {
    pub v_recover_pu: Option<f32>,
    pub delay_ms: Option<f32>,
    pub ramp_mw_per_min: Option<f32>,
    pub manual_reset_required: Option<bool>,
}

/// Voltage measurement / filter configuration (v0.13.1+).
#[derive(Debug, Clone, Default)]
pub struct VoltageMeasurement {
    pub basis: Option<String>,
    pub filter_time_constant_ms: Option<f32>,
    pub location: Option<String>,
    pub reset_hysteresis_pu: Option<f32>,
}

/// Provenance for protection settings (v0.13.1+).
#[derive(Debug, Clone, Default)]
pub struct ProtectionSettingsProvenance {
    pub source: Option<String>,
    pub profile_id: Option<String>,
    /// UTC microseconds since epoch (Arrow Timestamp(us, UTC)).
    pub effective_date_us: Option<i64>,
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
    // v0.13.1 trailing typed protection fields
    pub voltage_transfer_curve: Option<Vec<VoltageTransferCurveStage>>,
    pub disturbance_counter: Option<DisturbanceCounter>,
    pub reconnection_params: Option<ReconnectionParams>,
    pub voltage_measurement: Option<VoltageMeasurement>,
    pub protection_settings_provenance: Option<ProtectionSettingsProvenance>,
}

/// Canonical stage order: under by ascending v_pu, then over by descending v_pu;
/// within equal thresholds, by ascending t_ms then load_class then action.
pub fn canonicalize_voltage_transfer_curve(stages: &mut [VoltageTransferCurveStage]) {
    use std::cmp::Ordering;
    stages.sort_by(|a, b| {
        let pol = a.polarity.cmp(&b.polarity);
        if pol != Ordering::Equal {
            return pol;
        }
        let v_ord = if a.polarity == "over" {
            b.v_pu.partial_cmp(&a.v_pu).unwrap_or(Ordering::Equal)
        } else {
            a.v_pu.partial_cmp(&b.v_pu).unwrap_or(Ordering::Equal)
        };
        if v_ord != Ordering::Equal {
            return v_ord;
        }
        let t = a.t_ms.partial_cmp(&b.t_ms).unwrap_or(Ordering::Equal);
        if t != Ordering::Equal {
            return t;
        }
        a.load_class
            .as_deref()
            .unwrap_or("all")
            .cmp(b.load_class.as_deref().unwrap_or("all"))
            .then_with(|| a.action.cmp(&b.action))
    });
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

fn append_null_to_struct_children(builder: &mut StructBuilder) {
    for i in 0..builder.num_fields() {
        // Dictionary / timestamp / primitive children all implement append_null via Any.
        // Use the generic ArrayBuilder trait through field_builder downcasts we already know.
        let _ = i;
    }
    // Arrow 58: append(false) does not always push child nulls for Dictionary builders.
    // Push an explicit null into every child builder first.
    for i in 0..builder.num_fields() {
        builder
            .field_builder::<Float32Builder>(i)
            .map(|b| b.append_null())
            .or_else(|| {
                builder
                    .field_builder::<Int32Builder>(i)
                    .map(|b| b.append_null())
            })
            .or_else(|| {
                builder
                    .field_builder::<BooleanBuilder>(i)
                    .map(|b| b.append_null())
            })
            .or_else(|| {
                builder
                    .field_builder::<StringBuilder>(i)
                    .map(|b| b.append_null())
            })
            .or_else(|| {
                builder
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(i)
                    .map(|b| b.append_null())
            })
            .or_else(|| {
                builder
                    .field_builder::<TimestampMicrosecondBuilder>(i)
                    .map(|b| b.append_null())
            });
    }
}

fn append_optional_struct_fields(
    builder: &mut StructBuilder,
    present: bool,
    append_children: impl FnOnce(&mut StructBuilder) -> Result<()>,
) -> Result<()> {
    if present {
        append_children(builder)?;
        builder.append(true);
    } else {
        append_null_to_struct_children(builder);
        builder.append_null();
    }
    Ok(())
}

/// Build a `computational_load_profiles` RecordBatch matching the locked schema.
pub fn build_computational_load_profiles_batch(
    rows: &[ComputationalLoadProfileRow],
) -> Result<RecordBatch> {
    // Canonicalize curves at authoring time so wire order is deterministic.
    let mut owned: Vec<ComputationalLoadProfileRow> = rows.to_vec();
    for row in &mut owned {
        if let Some(curve) = row.voltage_transfer_curve.as_mut() {
            if curve.is_empty() {
                row.voltage_transfer_curve = None;
            } else {
                canonicalize_voltage_transfer_curve(curve);
            }
        }
    }
    let rows = &owned;

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
    let curve_item = Field::new(
        "item",
        DataType::Struct(voltage_transfer_curve_element_fields().into()),
        false,
    );
    let utc_tz: Arc<str> = Arc::from("UTC");

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
    let mut voltage_curve = ListBuilder::new(StructBuilder::from_fields(
        voltage_transfer_curve_element_fields(),
        n,
    ))
    .with_field(Arc::new(curve_item));
    let mut disturbance = StructBuilder::from_fields(disturbance_counter_struct_fields(), n);
    let mut reconnection_params =
        StructBuilder::from_fields(reconnection_params_struct_fields(), n);
    let mut voltage_measurement =
        StructBuilder::from_fields(voltage_measurement_struct_fields(), n);
    let mut provenance =
        StructBuilder::from_fields(protection_settings_provenance_struct_fields(), n);

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

        match &row.voltage_transfer_curve {
            None => voltage_curve.append(false),
            Some(stages) => {
                let values = voltage_curve.values();
                for stage in stages {
                    values
                        .field_builder::<Float32Builder>(0)
                        .expect("v_pu")
                        .append_value(stage.v_pu);
                    values
                        .field_builder::<Float32Builder>(1)
                        .expect("t_ms")
                        .append_value(stage.t_ms);
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(2)
                        .expect("polarity")
                        .append_value(&stage.polarity);
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                        .expect("action")
                        .append_value(&stage.action);
                    match stage.mw_fraction {
                        Some(f) => values
                            .field_builder::<Float32Builder>(4)
                            .expect("mw_fraction")
                            .append_value(f),
                        None => values
                            .field_builder::<Float32Builder>(4)
                            .expect("mw_fraction")
                            .append_null(),
                    }
                    match stage.load_class.as_deref() {
                        Some(lc) => values
                            .field_builder::<StringDictionaryBuilder<Int32Type>>(5)
                            .expect("load_class")
                            .append_value(lc),
                        None => values
                            .field_builder::<StringDictionaryBuilder<Int32Type>>(5)
                            .expect("load_class")
                            .append_null(),
                    }
                    values.append(true);
                }
                voltage_curve.append(true);
            }
        }

        append_optional_struct_fields(&mut disturbance, row.disturbance_counter.is_some(), |b| {
            let dc = row.disturbance_counter.as_ref().unwrap();
            append_optional_i32(
                b.field_builder::<Int32Builder>(0).expect("strike_limit"),
                dc.strike_limit,
            );
            append_optional_f32(
                b.field_builder::<Float32Builder>(1).expect("window_sec"),
                dc.window_sec,
            );
            append_optional_f32(
                b.field_builder::<Float32Builder>(2)
                    .expect("qualifying_v_pu"),
                dc.qualifying_v_pu,
            );
            append_optional_f32(
                b.field_builder::<Float32Builder>(3)
                    .expect("qualifying_duration_ms"),
                dc.qualifying_duration_ms,
            );
            append_optional_bool(
                b.field_builder::<BooleanBuilder>(4)
                    .expect("latch_permanent"),
                dc.latch_permanent,
            );
            Ok(())
        })?;

        append_optional_struct_fields(
            &mut reconnection_params,
            row.reconnection_params.is_some(),
            |b| {
                let rp = row.reconnection_params.as_ref().unwrap();
                append_optional_f32(
                    b.field_builder::<Float32Builder>(0).expect("v_recover_pu"),
                    rp.v_recover_pu,
                );
                append_optional_f32(
                    b.field_builder::<Float32Builder>(1).expect("delay_ms"),
                    rp.delay_ms,
                );
                append_optional_f32(
                    b.field_builder::<Float32Builder>(2)
                        .expect("ramp_mw_per_min"),
                    rp.ramp_mw_per_min,
                );
                append_optional_bool(
                    b.field_builder::<BooleanBuilder>(3)
                        .expect("manual_reset_required"),
                    rp.manual_reset_required,
                );
                Ok(())
            },
        )?;

        append_optional_struct_fields(
            &mut voltage_measurement,
            row.voltage_measurement.is_some(),
            |b| {
                let vm = row.voltage_measurement.as_ref().unwrap();
                append_optional_dict(
                    b.field_builder::<StringDictionaryBuilder<Int32Type>>(0)
                        .expect("basis"),
                    vm.basis.as_deref(),
                )?;
                append_optional_f32(
                    b.field_builder::<Float32Builder>(1)
                        .expect("filter_time_constant_ms"),
                    vm.filter_time_constant_ms,
                );
                append_optional_dict(
                    b.field_builder::<StringDictionaryBuilder<Int32Type>>(2)
                        .expect("location"),
                    vm.location.as_deref(),
                )?;
                append_optional_f32(
                    b.field_builder::<Float32Builder>(3)
                        .expect("reset_hysteresis_pu"),
                    vm.reset_hysteresis_pu,
                );
                Ok(())
            },
        )?;

        append_optional_struct_fields(
            &mut provenance,
            row.protection_settings_provenance.is_some(),
            |b| {
                let ps = row.protection_settings_provenance.as_ref().unwrap();
                append_optional_dict(
                    b.field_builder::<StringDictionaryBuilder<Int32Type>>(0)
                        .expect("source"),
                    ps.source.as_deref(),
                )?;
                append_optional_utf8(
                    b.field_builder::<StringBuilder>(1).expect("profile_id"),
                    ps.profile_id.as_deref(),
                );
                {
                    let ts = b
                        .field_builder::<TimestampMicrosecondBuilder>(2)
                        .expect("effective_date");
                    // Ensure timezone metadata matches schema (UTC).
                    let _ = utc_tz.clone();
                    match ps.effective_date_us {
                        Some(us) => ts.append_value(us),
                        None => ts.append_null(),
                    }
                }
                Ok(())
            },
        )?;
    }

    // Rebuild timestamp column with timezone if StructBuilder did not attach it.
    let provenance_array = provenance.finish();
    let provenance_array = ensure_provenance_tz(provenance_array, &utc_tz)?;

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
            Arc::new(voltage_curve.finish()),
            Arc::new(disturbance.finish()),
            Arc::new(reconnection_params.finish()),
            Arc::new(voltage_measurement.finish()),
            provenance_array,
        ],
    )
    .context("building computational_load_profiles batch")
}

fn ensure_provenance_tz(array: arrow::array::StructArray, utc_tz: &Arc<str>) -> Result<ArrayRef> {
    use arrow::array::StructArray;
    use arrow::datatypes::Fields;

    let fields = array.fields().clone();
    let columns = array.columns().to_vec();
    let validity = array.nulls().cloned();

    // If effective_date already has UTC tz, return as-is.
    if let DataType::Struct(fs) = array.data_type() {
        if let Some(f) = fs.iter().find(|f| f.name() == "effective_date") {
            if matches!(
                f.data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == utc_tz.as_ref()
            ) {
                return Ok(Arc::new(array));
            }
        }
    }

    let mut new_fields = Vec::with_capacity(fields.len());
    let mut new_cols = Vec::with_capacity(columns.len());
    for (field, col) in fields.iter().zip(columns.into_iter()) {
        if field.name() == "effective_date" {
            let ts = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .context("effective_date timestamp")?;
            let rebuilt = ts.clone().with_timezone(utc_tz.clone());
            new_fields.push(Arc::new(Field::new(
                "effective_date",
                DataType::Timestamp(TimeUnit::Microsecond, Some(utc_tz.clone())),
                true,
            )));
            new_cols.push(Arc::new(rebuilt) as ArrayRef);
        } else {
            new_fields.push(field.clone());
            new_cols.push(col);
        }
    }
    let out = StructArray::new(Fields::from(new_fields), new_cols, validity);
    Ok(Arc::new(out))
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

        if let (Some(it), Some(non_it)) = (
            f32_at(
                batch
                    .column_by_name("it_load_percent")
                    .context("it_load_percent")?,
                row,
            ),
            f32_at(
                batch
                    .column_by_name("non_it_load_percent")
                    .context("non_it_load_percent")?,
                row,
            ),
        ) {
            if !it.is_finite() || !non_it.is_finite() || (it + non_it - 100.0).abs() > 1e-3 {
                bail!(
                    "computational_load_profiles row {row}: it_load_percent + non_it_load_percent must equal 100 when both set (got {it}+{non_it})"
                );
            }
        }

        validate_voltage_transfer_curve_column(batch, row)?;
        validate_disturbance_counter_column(batch, row)?;
        validate_reconnection_params_column(batch, row)?;
        validate_voltage_measurement_column(batch, row)?;
        validate_provenance_column(batch, row)?;
    }

    Ok(())
}

fn validate_closed(value: &str, allowed: &[&str], row: usize, field: &str) -> Result<()> {
    if !allowed.iter().any(|a| *a == value) {
        bail!("computational_load_profiles row {row}: {field} '{value}' is not in closed set");
    }
    Ok(())
}

fn validate_voltage_transfer_curve_column(batch: &RecordBatch, row: usize) -> Result<()> {
    let col = batch
        .column_by_name("voltage_transfer_curve")
        .context("voltage_transfer_curve")?;
    if col.is_null(row) {
        return Ok(());
    }
    let list = col
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .context("voltage_transfer_curve list")?;
    let values = list.value(row);
    if values.len() == 0 {
        return Ok(());
    }
    let st = values
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("voltage_transfer_curve struct")?;
    let v_pu = st
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let t_ms = st
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let mut under_frac_by_class: HashMap<String, Vec<(f32, f32)>> = HashMap::new();
    let mut over_frac_by_class: HashMap<String, Vec<(f32, f32)>> = HashMap::new();
    let mut seen = std::collections::HashSet::new();

    for i in 0..st.len() {
        let v = v_pu.value(i);
        let t = t_ms.value(i);
        if !v.is_finite() || v <= 0.0 {
            bail!(
                "computational_load_profiles row {row}: voltage_transfer_curve.v_pu must be finite and > 0"
            );
        }
        if !t.is_finite() || t < 0.0 {
            bail!(
                "computational_load_profiles row {row}: voltage_transfer_curve.t_ms must be finite and >= 0"
            );
        }
        let polarity = dict_value_at(st.column(2), i).unwrap_or_default();
        let action = dict_value_at(st.column(3), i).unwrap_or_default();
        validate_closed(&polarity, VOLTAGE_TRANSFER_POLARITIES, row, "polarity")?;
        validate_closed(&action, VOLTAGE_TRANSFER_ACTIONS, row, "action")?;
        let mw_fraction = f32_at(st.column(4), i);
        if let Some(f) = mw_fraction {
            if !f.is_finite() || !(0.0 < f && f <= 1.0) {
                bail!("computational_load_profiles row {row}: mw_fraction must be in (0,1]");
            }
        }
        let load_class = dict_value_at(st.column(5), i).unwrap_or_else(|| "all".into());
        validate_closed(
            &load_class,
            VOLTAGE_TRANSFER_LOAD_CLASSES,
            row,
            "load_class",
        )?;
        let key = (polarity.clone(), load_class.clone(), v.to_bits());
        if !seen.insert(key) {
            bail!(
                "computational_load_profiles row {row}: duplicate voltage_transfer_curve threshold for polarity/load_class"
            );
        }
        let frac = mw_fraction.unwrap_or(1.0);
        if polarity == "under" {
            under_frac_by_class
                .entry(load_class)
                .or_default()
                .push((v, frac));
        } else {
            over_frac_by_class
                .entry(load_class)
                .or_default()
                .push((v, frac));
        }
    }

    for (class, mut stages) in under_frac_by_class {
        stages.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        for w in stages.windows(2) {
            if w[1].1 + 1e-6 < w[0].1 {
                bail!(
                    "computational_load_profiles row {row}: under-voltage mw_fraction must be non-decreasing with severity (class={class})"
                );
            }
        }
    }
    for (class, mut stages) in over_frac_by_class {
        stages.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        for w in stages.windows(2) {
            if w[1].1 + 1e-6 < w[0].1 {
                bail!(
                    "computational_load_profiles row {row}: over-voltage mw_fraction must be non-decreasing with severity (class={class})"
                );
            }
        }
    }
    Ok(())
}

fn validate_disturbance_counter_column(batch: &RecordBatch, row: usize) -> Result<()> {
    let col = batch
        .column_by_name("disturbance_counter")
        .context("disturbance_counter")?;
    if col.is_null(row) {
        return Ok(());
    }
    let st = col
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("disturbance_counter struct")?;
    if let Some(n) = i32_at(st.column(0), row) {
        if n < 1 {
            bail!("computational_load_profiles row {row}: strike_limit must be >= 1");
        }
    }
    for (idx, name) in [
        (1, "window_sec"),
        (2, "qualifying_v_pu"),
        (3, "qualifying_duration_ms"),
    ] {
        if let Some(v) = f32_at(st.column(idx), row) {
            if !v.is_finite() || v <= 0.0 {
                bail!("computational_load_profiles row {row}: {name} must be finite and > 0");
            }
        }
    }
    Ok(())
}

fn validate_reconnection_params_column(batch: &RecordBatch, row: usize) -> Result<()> {
    let col = batch
        .column_by_name("reconnection_params")
        .context("reconnection_params")?;
    if col.is_null(row) {
        return Ok(());
    }
    let st = col
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("reconnection_params struct")?;
    for (idx, name) in [(0, "v_recover_pu"), (1, "delay_ms"), (2, "ramp_mw_per_min")] {
        if let Some(v) = f32_at(st.column(idx), row) {
            if !v.is_finite() || v < 0.0 {
                bail!("computational_load_profiles row {row}: {name} must be finite and >= 0");
            }
        }
    }
    Ok(())
}

fn validate_voltage_measurement_column(batch: &RecordBatch, row: usize) -> Result<()> {
    let col = batch
        .column_by_name("voltage_measurement")
        .context("voltage_measurement")?;
    if col.is_null(row) {
        return Ok(());
    }
    let st = col
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("voltage_measurement struct")?;
    if let Some(b) = dict_value_at(st.column(0), row) {
        validate_closed(
            &b,
            VOLTAGE_MEASUREMENT_BASES,
            row,
            "voltage_measurement.basis",
        )?;
    }
    if let Some(tv) = f32_at(st.column(1), row) {
        if !tv.is_finite() || tv <= 0.0 {
            bail!(
                "computational_load_profiles row {row}: filter_time_constant_ms must be finite and > 0"
            );
        }
    }
    if let Some(loc) = dict_value_at(st.column(2), row) {
        validate_closed(
            &loc,
            VOLTAGE_MEASUREMENT_LOCATIONS,
            row,
            "voltage_measurement.location",
        )?;
    }
    if let Some(h) = f32_at(st.column(3), row) {
        if !h.is_finite() || h < 0.0 {
            bail!(
                "computational_load_profiles row {row}: reset_hysteresis_pu must be finite and >= 0"
            );
        }
    }
    Ok(())
}

fn validate_provenance_column(batch: &RecordBatch, row: usize) -> Result<()> {
    let col = batch
        .column_by_name("protection_settings_provenance")
        .context("protection_settings_provenance")?;
    if col.is_null(row) {
        return Ok(());
    }
    let st = col
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("protection_settings_provenance struct")?;
    if let Some(src) = dict_value_at(st.column(0), row) {
        validate_closed(&src, PROTECTION_SETTINGS_SOURCES, row, "provenance.source")?;
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
    fn build_and_validate_voltage_transfer_curve() -> Result<()> {
        let row = ComputationalLoadProfileRow {
            bus_id: Some(42),
            facility_class: Some("ai_hpc".into()),
            common_mode_group: Some("ashburn_campus_a".into()),
            transfer_to_backup_threshold_pu: Some(0.90),
            voltage_transfer_curve: Some(vec![
                VoltageTransferCurveStage {
                    v_pu: 0.80,
                    t_ms: 80.0,
                    polarity: "under".into(),
                    action: "transfer".into(),
                    mw_fraction: Some(1.0),
                    load_class: Some("it".into()),
                },
                VoltageTransferCurveStage {
                    v_pu: 0.70,
                    t_ms: 30.0,
                    polarity: "under".into(),
                    action: "transfer".into(),
                    mw_fraction: Some(1.0),
                    load_class: Some("it".into()),
                },
                VoltageTransferCurveStage {
                    v_pu: 1.10,
                    t_ms: 100.0,
                    polarity: "over".into(),
                    action: "transfer".into(),
                    mw_fraction: None,
                    load_class: None,
                },
            ]),
            disturbance_counter: Some(DisturbanceCounter {
                strike_limit: Some(3),
                window_sec: Some(60.0),
                qualifying_v_pu: Some(0.90),
                qualifying_duration_ms: Some(50.0),
                latch_permanent: Some(true),
            }),
            reconnection_params: Some(ReconnectionParams {
                v_recover_pu: Some(0.95),
                delay_ms: Some(5000.0),
                ramp_mw_per_min: Some(50.0),
                manual_reset_required: Some(true),
            }),
            voltage_measurement: Some(VoltageMeasurement {
                basis: Some("positive_sequence_rms".into()),
                filter_time_constant_ms: Some(20.0),
                location: Some("poi".into()),
                reset_hysteresis_pu: Some(0.01),
            }),
            protection_settings_provenance: Some(ProtectionSettingsProvenance {
                source: Some("study_assumption".into()),
                profile_id: Some("illustrative_ai_hpc_v1".into()),
                effective_date_us: None,
            }),
            ..Default::default()
        };
        let batch = build_computational_load_profiles_batch(&[row])?;
        validate_computational_load_profiles_batch(&batch, Some(true))?;
        let curve = batch
            .column_by_name("voltage_transfer_curve")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(curve.value(0).len(), 3);
        Ok(())
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
