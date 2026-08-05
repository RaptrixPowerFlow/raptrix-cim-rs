// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders and semantic validation for the required `dynamics_models` table.
//!
//! Distinct from the stub-generation path in `raptrix-cim-rs::rpf_writer`, this module is the
//! full authoring contract: every wire column (`params`, `perc1_params`, `classical_params`) is
//! settable so downstream authoring tools (e.g. Raptrix Studio) can round-trip real per-machine
//! dynamics data, not only PSS/E-converter placeholders.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int32Array, Int32Builder,
    MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Int32Type};

use crate::schema::{
    classical_params_struct_fields, dynamics_models_schema, perc1_params_struct_fields,
};

/// Optional classical first-swing parameters (v0.13.0+ `classical_params` struct).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClassicalParams {
    pub h: Option<f64>,
    pub d: Option<f64>,
    pub xd_prime: Option<f64>,
    pub mbase_mva: Option<f64>,
}

/// Optional PERC1 baseline ride-through parameters (v0.10.0+ `perc1_params` struct).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Perc1Params {
    pub voltage_ride_through_pu: Option<f64>,
    pub frequency_ride_through_hz: Option<f64>,
    pub reactive_power_ceiling_pu: Option<f64>,
    pub active_power_recovery_rate_pu_per_s: Option<f64>,
    pub voltage_support_time_sec: Option<f64>,
    pub frequency_support_time_sec: Option<f64>,
}

/// Domain row for one `dynamics_models` record. Stable identity is `(bus_id, gen_id)`.
#[derive(Debug, Clone, Default)]
pub struct DynamicsModelRow {
    pub bus_id: i32,
    pub gen_id: String,
    pub model_type: String,
    /// Open string->f64 parameter bag; unknown/legacy keys are preserved verbatim.
    pub params: HashMap<String, f64>,
    pub perc1_params: Option<Perc1Params>,
    pub classical_params: Option<ClassicalParams>,
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

fn append_optional_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Build a `dynamics_models` RecordBatch matching the locked schema.
///
/// Unlike the PSS/E-converter stub path, this builder writes real `classical_params` and
/// `perc1_params` structs when present on the row instead of always emitting null.
pub fn build_dynamics_models_batch(rows: &[DynamicsModelRow]) -> Result<RecordBatch> {
    let schema = Arc::new(dynamics_models_schema());
    let n = rows.len();

    let mut bus_id_b = Int32Builder::with_capacity(n);
    let mut gen_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut model_type_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut params_b = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(arrow::datatypes::Field::new(
        "key",
        arrow::datatypes::DataType::Utf8,
        false,
    )))
    .with_values_field(Arc::new(arrow::datatypes::Field::new(
        "value",
        arrow::datatypes::DataType::Float64,
        false,
    )));

    let perc1_fields = perc1_params_struct_fields();
    let classical_fields = classical_params_struct_fields();
    let mut perc1_b = arrow::array::StructBuilder::from_fields(perc1_fields, n);
    let mut classical_b = arrow::array::StructBuilder::from_fields(classical_fields, n);

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        gen_id_b.append(&row.gen_id).context("append gen_id")?;
        model_type_b
            .append(&row.model_type)
            .context("append model_type")?;

        let mut keys: Vec<&String> = row.params.keys().collect();
        keys.sort();
        for key in keys {
            params_b.keys().append_value(key);
            params_b.values().append_value(row.params[key]);
        }
        params_b.append(true).context("append params map row")?;

        match &row.perc1_params {
            Some(p) => {
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(0).expect("perc1[0]"),
                    p.voltage_ride_through_pu,
                );
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(1).expect("perc1[1]"),
                    p.frequency_ride_through_hz,
                );
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(2).expect("perc1[2]"),
                    p.reactive_power_ceiling_pu,
                );
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(3).expect("perc1[3]"),
                    p.active_power_recovery_rate_pu_per_s,
                );
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(4).expect("perc1[4]"),
                    p.voltage_support_time_sec,
                );
                append_optional_f64(
                    perc1_b.field_builder::<Float64Builder>(5).expect("perc1[5]"),
                    p.frequency_support_time_sec,
                );
                perc1_b.append(true);
            }
            None => {
                for i in 0..6 {
                    perc1_b
                        .field_builder::<Float64Builder>(i)
                        .expect("perc1 null child")
                        .append_null();
                }
                perc1_b.append_null();
            }
        }

        match &row.classical_params {
            Some(c) => {
                append_optional_f64(
                    classical_b
                        .field_builder::<Float64Builder>(0)
                        .expect("classical[H]"),
                    c.h,
                );
                append_optional_f64(
                    classical_b
                        .field_builder::<Float64Builder>(1)
                        .expect("classical[D]"),
                    c.d,
                );
                append_optional_f64(
                    classical_b
                        .field_builder::<Float64Builder>(2)
                        .expect("classical[xd_prime]"),
                    c.xd_prime,
                );
                append_optional_f64(
                    classical_b
                        .field_builder::<Float64Builder>(3)
                        .expect("classical[mbase_mva]"),
                    c.mbase_mva,
                );
                classical_b.append(true);
            }
            None => {
                for i in 0..4 {
                    classical_b
                        .field_builder::<Float64Builder>(i)
                        .expect("classical null child")
                        .append_null();
                }
                classical_b.append_null();
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id_b.finish()) as ArrayRef,
            Arc::new(gen_id_b.finish()) as ArrayRef,
            Arc::new(model_type_b.finish()) as ArrayRef,
            Arc::new(params_b.finish()) as ArrayRef,
            Arc::new(perc1_b.finish()) as ArrayRef,
            Arc::new(classical_b.finish()) as ArrayRef,
        ],
    )
    .context("building dynamics_models batch")
}

fn dict_value_at(col: &ArrayRef, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Dictionary(_, _) => {
            let dict = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>()?;
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

fn f64_at(col: &ArrayRef, row: usize) -> Option<f64> {
    if col.is_null(row) {
        return None;
    }
    col.as_any().downcast_ref::<Float64Array>().map(|a| a.value(row))
}

/// Decode a `dynamics_models` RecordBatch back into domain rows.
///
/// This is the inverse of [`build_dynamics_models_batch`] and is used by authoring tools
/// (e.g. Raptrix Studio) to merge keyed edit patches into an existing table without
/// re-serializing every unrelated row.
pub fn read_dynamics_models_batch(batch: &RecordBatch) -> Result<Vec<DynamicsModelRow>> {
    let bus_id = batch.column_by_name("bus_id").context("bus_id")?;
    let gen_id = batch.column_by_name("gen_id").context("gen_id")?;
    let model_type = batch.column_by_name("model_type").context("model_type")?;
    let params = batch.column_by_name("params").context("params")?;
    let perc1 = batch
        .column_by_name("perc1_params")
        .context("perc1_params")?;
    let classical = batch
        .column_by_name("classical_params")
        .context("classical_params")?;

    let bus_arr = bus_id
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("bus_id array")?;
    let params_map = params
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .context("params map")?;
    let perc1_struct = perc1
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("perc1_params struct")?;
    let classical_struct = classical
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .context("classical_params struct")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if bus_id.is_null(row) {
            bail!("dynamics_models row {row}: bus_id must not be null");
        }
        let bus = bus_arr.value(row);
        let gen_value = dict_value_at(gen_id, row).unwrap_or_default();
        let mt = dict_value_at(model_type, row).unwrap_or_default();

        let mut params_out = HashMap::new();
        if !params.is_null(row) {
            let entry = params_map.value(row);
            let keys = entry
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("params key column must be Utf8")?;
            let values = entry
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("params value column must be Float64")?;
            for i in 0..entry.len() {
                if !keys.is_null(i) {
                    params_out.insert(keys.value(i).to_string(), values.value(i));
                }
            }
        }

        let perc1_params = if perc1.is_null(row) {
            None
        } else {
            Some(Perc1Params {
                voltage_ride_through_pu: f64_at(perc1_struct.column(0), row),
                frequency_ride_through_hz: f64_at(perc1_struct.column(1), row),
                reactive_power_ceiling_pu: f64_at(perc1_struct.column(2), row),
                active_power_recovery_rate_pu_per_s: f64_at(perc1_struct.column(3), row),
                voltage_support_time_sec: f64_at(perc1_struct.column(4), row),
                frequency_support_time_sec: f64_at(perc1_struct.column(5), row),
            })
        };

        let classical_params = if classical.is_null(row) {
            None
        } else {
            Some(ClassicalParams {
                h: f64_at(classical_struct.column(0), row),
                d: f64_at(classical_struct.column(1), row),
                xd_prime: f64_at(classical_struct.column(2), row),
                mbase_mva: f64_at(classical_struct.column(3), row),
            })
        };

        out.push(DynamicsModelRow {
            bus_id: bus,
            gen_id: gen_value,
            model_type: mt,
            params: params_out,
            perc1_params,
            classical_params,
        });
    }
    Ok(out)
}

/// Semantic validation for `dynamics_models`.
///
/// `known_generator_keys`, when provided, is the set of `(bus_id, gen_id)` pairs present in the
/// case's `generators` table; unmatched rows are reported but not rejected (a dynamics row for
/// an equipment set not yet present in the network is a warning-level authoring state, not a
/// hard error, since Studio users may stage dynamics ahead of generator edits).
pub fn validate_dynamics_models_batch(
    batch: &RecordBatch,
    known_generator_keys: Option<&std::collections::HashSet<(i32, String)>>,
) -> Result<Vec<String>> {
    let schema = dynamics_models_schema();
    if batch.schema().fields().len() != schema.fields().len() {
        bail!(
            "dynamics_models: expected {} columns, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }

    let bus_id = batch.column_by_name("bus_id").context("bus_id")?;
    let gen_id = batch.column_by_name("gen_id").context("gen_id")?;
    let model_type = batch.column_by_name("model_type").context("model_type")?;
    let classical = batch
        .column_by_name("classical_params")
        .context("classical_params")?;

    let mut warnings = Vec::new();
    let mut seen: std::collections::HashSet<(i32, String)> = std::collections::HashSet::new();

    for row in 0..batch.num_rows() {
        if bus_id.is_null(row) {
            bail!("dynamics_models row {row}: bus_id must not be null");
        }
        let bus = bus_id
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("bus_id array")?
            .value(row);

        if gen_id.is_null(row) {
            bail!("dynamics_models row {row}: gen_id must not be null");
        }
        let gen_value = dict_value_at(gen_id, row)
            .context("dynamics_models: gen_id must be a dictionary-encoded string")?;
        if gen_value.trim().is_empty() {
            bail!("dynamics_models row {row}: gen_id must not be empty");
        }

        if model_type.is_null(row) {
            bail!("dynamics_models row {row}: model_type must not be null");
        }
        let mt = dict_value_at(model_type, row)
            .context("dynamics_models: model_type must be a dictionary-encoded string")?;
        if mt.trim().is_empty() {
            bail!("dynamics_models row {row}: model_type must not be empty");
        }

        let key = (bus, gen_value.clone());
        if !seen.insert(key.clone()) {
            bail!(
                "dynamics_models row {row}: duplicate (bus_id, gen_id) = ({bus}, {gen_value}); \
                 stable identity must be unique"
            );
        }

        if !classical.is_null(row) {
            let st = classical
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .context("classical_params struct")?;
            if let Some(h) = f64_at(st.column(0), row) {
                if !h.is_finite() || h <= 0.0 {
                    bail!("dynamics_models row {row}: classical_params.H must be finite and > 0");
                }
            }
            if let Some(d) = f64_at(st.column(1), row) {
                if !d.is_finite() || d < 0.0 {
                    bail!("dynamics_models row {row}: classical_params.D must be finite and >= 0");
                }
            }
            if let Some(xd) = f64_at(st.column(2), row) {
                if !xd.is_finite() || xd <= 0.0 {
                    bail!(
                        "dynamics_models row {row}: classical_params.xd_prime must be finite and > 0"
                    );
                }
            }
            if let Some(mbase) = f64_at(st.column(3), row) {
                if !mbase.is_finite() || mbase <= 0.0 {
                    bail!(
                        "dynamics_models row {row}: classical_params.mbase_mva must be finite and > 0"
                    );
                }
            }
        }

        if let Some(known) = known_generator_keys {
            if !known.contains(&key) {
                warnings.push(format!(
                    "dynamics_models row {row}: (bus_id={bus}, gen_id={gen_value}) has no matching generators row"
                ));
            }
        }
    }

    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> DynamicsModelRow {
        DynamicsModelRow {
            bus_id: 101,
            gen_id: "1".into(),
            model_type: "GENCLS".into(),
            params: HashMap::new(),
            perc1_params: None,
            classical_params: Some(ClassicalParams {
                h: Some(5.0),
                d: Some(0.0),
                xd_prime: Some(0.25),
                mbase_mva: Some(100.0),
            }),
        }
    }

    #[test]
    fn build_and_validate_round_trip() -> Result<()> {
        let batch = build_dynamics_models_batch(&[sample_row()])?;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), dynamics_models_schema().fields().len());
        let warnings = validate_dynamics_models_batch(&batch, None)?;
        assert!(warnings.is_empty());
        Ok(())
    }

    #[test]
    fn validate_rejects_duplicate_identity() {
        let batch = build_dynamics_models_batch(&[sample_row(), sample_row()]).unwrap();
        let err = validate_dynamics_models_batch(&batch, None).unwrap_err();
        assert!(err.to_string().contains("duplicate (bus_id, gen_id)"));
    }

    #[test]
    fn validate_rejects_nonpositive_inertia() {
        let mut row = sample_row();
        row.classical_params = Some(ClassicalParams {
            h: Some(0.0),
            ..Default::default()
        });
        let batch = build_dynamics_models_batch(&[row]).unwrap();
        let err = validate_dynamics_models_batch(&batch, None).unwrap_err();
        assert!(err.to_string().contains("classical_params.H"));
    }

    #[test]
    fn validate_warns_on_unmatched_generator() -> Result<()> {
        let batch = build_dynamics_models_batch(&[sample_row()])?;
        let known: std::collections::HashSet<(i32, String)> = std::collections::HashSet::new();
        let warnings = validate_dynamics_models_batch(&batch, Some(&known))?;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("no matching generators row"));
        Ok(())
    }

    #[test]
    fn read_dynamics_models_batch_round_trips() -> Result<()> {
        let mut params = HashMap::new();
        params.insert("legacy_key".to_string(), 42.0);
        let row = DynamicsModelRow {
            params,
            perc1_params: Some(Perc1Params {
                voltage_ride_through_pu: Some(0.9),
                ..Default::default()
            }),
            ..sample_row()
        };
        let batch = build_dynamics_models_batch(&[row.clone()])?;
        let decoded = read_dynamics_models_batch(&batch)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].bus_id, row.bus_id);
        assert_eq!(decoded[0].gen_id, row.gen_id);
        assert_eq!(decoded[0].model_type, row.model_type);
        assert_eq!(decoded[0].params.get("legacy_key"), Some(&42.0));
        assert_eq!(
            decoded[0]
                .perc1_params
                .as_ref()
                .and_then(|p| p.voltage_ride_through_pu),
            Some(0.9)
        );
        assert_eq!(decoded[0].classical_params, row.classical_params);
        Ok(())
    }

    #[test]
    fn open_params_map_round_trips() -> Result<()> {
        let mut params = HashMap::new();
        params.insert("legacy_key".to_string(), 42.0);
        let row = DynamicsModelRow {
            params,
            ..sample_row()
        };
        let batch = build_dynamics_models_batch(&[row])?;
        let col = batch.column_by_name("params").unwrap();
        let map_arr = col
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        assert_eq!(map_arr.value_length(0), 1);
        Ok(())
    }
}
