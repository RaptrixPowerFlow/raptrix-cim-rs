// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders and semantic validation for the required `contingencies` table.
//!
//! Distinct from the stub N-1 generation path in `raptrix-cim-rs::rpf_writer` (which only
//! ever emits `branch_outage` elements with null gen/load/amount fields), this module is the
//! full authoring contract used by tools that need to define compound, protection-driven, or
//! generator/load contingencies (e.g. Raptrix Studio's Dynamics editor).

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, DictionaryArray, Float64Array, Float64Builder, Int32Array,
    Int32Builder, ListBuilder, RecordBatch, StringArray, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Int32Type};

use crate::schema::contingencies_schema;

/// Open vocabulary recognized by Studio authoring UI; unknown tokens from other producers must
/// still round-trip (readers tolerate unknown `element_type` values per the schema contract).
pub const KNOWN_ELEMENT_TYPES: &[&str] = &[
    "branch_outage",
    "generator_trip",
    "load_shed",
    "shunt_switch",
    "split_bus",
    "protection_event",
];

/// One compound-contingency element.
#[derive(Debug, Clone, Default)]
pub struct ContingencyElementRow {
    pub element_type: String,
    pub branch_id: Option<i32>,
    pub bus_id: Option<i32>,
    pub gen_id: Option<String>,
    pub load_id: Option<String>,
    pub amount_mw: Option<f64>,
    pub status_change: bool,
    pub equipment_kind: Option<String>,
    pub equipment_id: Option<String>,
}

/// One `contingencies` row. Stable identity is `contingency_id`.
#[derive(Debug, Clone, Default)]
pub struct ContingencyRow {
    pub contingency_id: String,
    pub elements: Vec<ContingencyElementRow>,
    /// Operational-outcome columns (v0.9.0+). Left `None` for planning/authoring rows; Studio
    /// never authors these — they are populated by Sentinel/solver runs only.
    pub risk_score: Option<f64>,
    pub cleared_by_reserves: Option<bool>,
    pub voltage_collapse_flag: Option<bool>,
    pub recovery_possible: Option<bool>,
    pub recovery_time_min: Option<f64>,
    pub greedy_reserve_summary: Option<String>,
}

fn append_optional_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_optional_f64(builder: &mut Float64Builder, value: Option<f64>) {
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

/// Build a `contingencies` RecordBatch matching the locked schema, with full element fidelity
/// (gen_id/load_id/amount_mw populated, unlike the PSS/E-converter N-1 stub path).
pub fn build_contingencies_batch(rows: &[ContingencyRow]) -> Result<RecordBatch> {
    let schema = Arc::new(contingencies_schema());
    let n = rows.len();

    let mut contingency_id_b = StringDictionaryBuilder::<Int32Type>::new();

    let elements_field = match schema.field(1).data_type() {
        DataType::List(field) => field.clone(),
        other => bail!("contingencies.elements field must be List<Struct>, found {other:?}"),
    };
    let element_fields = match elements_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => bail!("contingencies.elements item must be Struct, found {other:?}"),
    };
    let mut elements_b =
        ListBuilder::new(StructBuilder::from_fields(element_fields, n)).with_field(elements_field);

    let mut risk_score_b = Float64Builder::with_capacity(n);
    let mut cleared_by_reserves_b = BooleanBuilder::with_capacity(n);
    let mut voltage_collapse_flag_b = BooleanBuilder::with_capacity(n);
    let mut recovery_possible_b = BooleanBuilder::with_capacity(n);
    let mut recovery_time_min_b = Float64Builder::with_capacity(n);
    let mut greedy_reserve_summary_b = arrow::array::StringBuilder::new();

    for row in rows {
        contingency_id_b
            .append(&row.contingency_id)
            .context("append contingency_id")?;

        let values = elements_b.values();
        for element in &row.elements {
            values
                .field_builder::<StringDictionaryBuilder<Int32Type>>(0)
                .context("element_type builder")?
                .append(&element.element_type)
                .context("append element_type")?;
            append_optional_i32(
                values.field_builder::<Int32Builder>(1).context("branch_id builder")?,
                element.branch_id,
            );
            append_optional_i32(
                values.field_builder::<Int32Builder>(2).context("bus_id builder")?,
                element.bus_id,
            );
            match &element.gen_id {
                Some(v) => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                        .context("gen_id builder")?
                        .append(v)
                        .context("append gen_id")?;
                }
                None => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                        .context("gen_id builder")?
                        .append_null();
                }
            }
            match &element.load_id {
                Some(v) => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(4)
                        .context("load_id builder")?
                        .append(v)
                        .context("append load_id")?;
                }
                None => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(4)
                        .context("load_id builder")?
                        .append_null();
                }
            }
            append_optional_f64(
                values
                    .field_builder::<Float64Builder>(5)
                    .context("amount_mw builder")?,
                element.amount_mw,
            );
            values
                .field_builder::<BooleanBuilder>(6)
                .context("status_change builder")?
                .append_value(element.status_change);
            match &element.equipment_kind {
                Some(v) => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(7)
                        .context("equipment_kind builder")?
                        .append(v)
                        .context("append equipment_kind")?;
                }
                None => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(7)
                        .context("equipment_kind builder")?
                        .append_null();
                }
            }
            match &element.equipment_id {
                Some(v) => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(8)
                        .context("equipment_id builder")?
                        .append(v)
                        .context("append equipment_id")?;
                }
                None => {
                    values
                        .field_builder::<StringDictionaryBuilder<Int32Type>>(8)
                        .context("equipment_id builder")?
                        .append_null();
                }
            }
            values.append(true);
        }
        elements_b.append(true);

        append_optional_f64(&mut risk_score_b, row.risk_score);
        append_optional_bool(&mut cleared_by_reserves_b, row.cleared_by_reserves);
        append_optional_bool(&mut voltage_collapse_flag_b, row.voltage_collapse_flag);
        append_optional_bool(&mut recovery_possible_b, row.recovery_possible);
        append_optional_f64(&mut recovery_time_min_b, row.recovery_time_min);
        match &row.greedy_reserve_summary {
            Some(v) => greedy_reserve_summary_b.append_value(v),
            None => greedy_reserve_summary_b.append_null(),
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(contingency_id_b.finish()) as ArrayRef,
            Arc::new(elements_b.finish()) as ArrayRef,
            Arc::new(risk_score_b.finish()) as ArrayRef,
            Arc::new(cleared_by_reserves_b.finish()) as ArrayRef,
            Arc::new(voltage_collapse_flag_b.finish()) as ArrayRef,
            Arc::new(recovery_possible_b.finish()) as ArrayRef,
            Arc::new(recovery_time_min_b.finish()) as ArrayRef,
            Arc::new(greedy_reserve_summary_b.finish()) as ArrayRef,
        ],
    )
    .context("building contingencies batch")
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

fn i32_at(col: &ArrayRef, row: usize) -> Option<i32> {
    if col.is_null(row) {
        return None;
    }
    col.as_any().downcast_ref::<Int32Array>().map(|a| a.value(row))
}

fn f64_at(col: &ArrayRef, row: usize) -> Option<f64> {
    if col.is_null(row) {
        return None;
    }
    col.as_any().downcast_ref::<Float64Array>().map(|a| a.value(row))
}

fn bool_at(col: &ArrayRef, row: usize) -> Option<bool> {
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .map(|a| a.value(row))
}

/// Decode a `contingencies` RecordBatch back into domain rows.
///
/// This is the inverse of [`build_contingencies_batch`] and is used by authoring tools
/// (e.g. Raptrix Studio) to merge keyed edit patches into an existing table while
/// preserving solver/Sentinel-populated outcome columns Studio never authors.
pub fn read_contingencies_batch(batch: &RecordBatch) -> Result<Vec<ContingencyRow>> {
    let contingency_id = batch
        .column_by_name("contingency_id")
        .context("contingency_id")?;
    let elements = batch.column_by_name("elements").context("elements")?;
    let list = elements
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .context("elements list")?;
    let risk_score = batch.column_by_name("risk_score").context("risk_score")?;
    let cleared_by_reserves = batch
        .column_by_name("cleared_by_reserves")
        .context("cleared_by_reserves")?;
    let voltage_collapse_flag = batch
        .column_by_name("voltage_collapse_flag")
        .context("voltage_collapse_flag")?;
    let recovery_possible = batch
        .column_by_name("recovery_possible")
        .context("recovery_possible")?;
    let recovery_time_min = batch
        .column_by_name("recovery_time_min")
        .context("recovery_time_min")?;
    let greedy_reserve_summary = batch
        .column_by_name("greedy_reserve_summary")
        .context("greedy_reserve_summary")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if contingency_id.is_null(row) {
            bail!("contingencies row {row}: contingency_id must not be null");
        }
        let cid = dict_value_at(contingency_id, row).unwrap_or_default();

        let struct_arr = list.value(row);
        let st = struct_arr
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .context("elements struct")?;

        let mut elements_out = Vec::with_capacity(st.len());
        for i in 0..st.len() {
            elements_out.push(ContingencyElementRow {
                element_type: dict_value_at(st.column(0), i).unwrap_or_default(),
                branch_id: i32_at(st.column(1), i),
                bus_id: i32_at(st.column(2), i),
                gen_id: dict_value_at(st.column(3), i),
                load_id: dict_value_at(st.column(4), i),
                amount_mw: f64_at(st.column(5), i),
                status_change: bool_at(st.column(6), i).unwrap_or(false),
                equipment_kind: dict_value_at(st.column(7), i),
                equipment_id: dict_value_at(st.column(8), i),
            });
        }

        out.push(ContingencyRow {
            contingency_id: cid,
            elements: elements_out,
            risk_score: f64_at(risk_score, row),
            cleared_by_reserves: bool_at(cleared_by_reserves, row),
            voltage_collapse_flag: bool_at(voltage_collapse_flag, row),
            recovery_possible: bool_at(recovery_possible, row),
            recovery_time_min: f64_at(recovery_time_min, row),
            greedy_reserve_summary: dict_value_at(greedy_reserve_summary, row),
        });
    }
    Ok(out)
}

/// Foreign-key sets available in the current case, used to validate compound-contingency
/// element targets. Absence of a set (`None`) skips FK checking for that equipment kind.
#[derive(Debug, Default)]
pub struct ContingencyFkContext<'a> {
    pub branch_ids: Option<&'a std::collections::HashSet<i32>>,
    pub bus_ids: Option<&'a std::collections::HashSet<i32>>,
    pub generator_keys: Option<&'a std::collections::HashSet<(i32, String)>>,
    pub load_keys: Option<&'a std::collections::HashSet<(i32, String)>>,
}

/// Semantic validation for `contingencies`. Returns non-fatal FK warnings; structural violations
/// (missing required fields, unique id collisions, empty element lists) are hard errors.
pub fn validate_contingencies_batch(
    batch: &RecordBatch,
    fk: &ContingencyFkContext<'_>,
) -> Result<Vec<String>> {
    let schema = contingencies_schema();
    if batch.schema().fields().len() != schema.fields().len() {
        bail!(
            "contingencies: expected {} columns, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }

    let contingency_id = batch.column_by_name("contingency_id").context("contingency_id")?;
    let elements = batch.column_by_name("elements").context("elements")?;
    let list = elements
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .context("elements list")?;

    let mut warnings = Vec::new();
    let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    for row in 0..batch.num_rows() {
        if contingency_id.is_null(row) {
            bail!("contingencies row {row}: contingency_id must not be null");
        }
        let cid = dict_value_at(contingency_id, row)
            .context("contingencies: contingency_id must be a dictionary-encoded string")?;
        if cid.trim().is_empty() {
            bail!("contingencies row {row}: contingency_id must not be empty");
        }
        if !seen_ids.insert(cid.clone()) {
            bail!("contingencies row {row}: duplicate contingency_id '{cid}'");
        }

        let struct_arr = list.value(row);
        let st = struct_arr
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .context("elements struct")?;
        if st.len() == 0 {
            bail!("contingencies row {row} ('{cid}'): must have at least one element");
        }

        for i in 0..st.len() {
            let element_type = dict_value_at(st.column(0), i)
                .context("element_type must be a dictionary-encoded string")?;
            if element_type.trim().is_empty() {
                bail!("contingencies row {row} ('{cid}') element {i}: element_type must not be empty");
            }
            if !KNOWN_ELEMENT_TYPES.contains(&element_type.as_str()) {
                warnings.push(format!(
                    "contingencies row {row} ('{cid}') element {i}: element_type '{element_type}' \
                     is outside Studio's known vocabulary; preserved but not editable in-app"
                ));
            }

            let branch_id = i32_at(st.column(1), i);
            let bus_id = i32_at(st.column(2), i);
            let gen_id = dict_value_at(st.column(3), i);
            let load_id = dict_value_at(st.column(4), i);
            let amount_mw = f64_at(st.column(5), i);

            match element_type.as_str() {
                "branch_outage" => {
                    let Some(bid) = branch_id else {
                        bail!(
                            "contingencies row {row} ('{cid}') element {i}: branch_outage requires branch_id"
                        );
                    };
                    if let Some(known) = fk.branch_ids {
                        if !known.contains(&bid) {
                            warnings.push(format!(
                                "contingencies row {row} ('{cid}') element {i}: branch_id {bid} not found in branches"
                            ));
                        }
                    }
                }
                "generator_trip" => {
                    let (Some(bid), Some(gid)) = (bus_id, gen_id.clone()) else {
                        bail!(
                            "contingencies row {row} ('{cid}') element {i}: generator_trip requires bus_id and gen_id"
                        );
                    };
                    if let Some(known) = fk.generator_keys {
                        if !known.contains(&(bid, gid.clone())) {
                            warnings.push(format!(
                                "contingencies row {row} ('{cid}') element {i}: generator (bus_id={bid}, gen_id={gid}) not found in generators"
                            ));
                        }
                    }
                }
                "load_shed" => {
                    let (Some(bid), Some(lid)) = (bus_id, load_id.clone()) else {
                        bail!(
                            "contingencies row {row} ('{cid}') element {i}: load_shed requires bus_id and load_id"
                        );
                    };
                    if amount_mw.is_none_or(|v| !v.is_finite() || v < 0.0) {
                        bail!(
                            "contingencies row {row} ('{cid}') element {i}: load_shed requires a finite, non-negative amount_mw"
                        );
                    }
                    if let Some(known) = fk.load_keys {
                        if !known.contains(&(bid, lid.clone())) {
                            warnings.push(format!(
                                "contingencies row {row} ('{cid}') element {i}: load (bus_id={bid}, load_id={lid}) not found in loads"
                            ));
                        }
                    }
                }
                _ => {
                    // Open vocabulary (shunt_switch, split_bus, protection_event, unknown):
                    // require at least one equipment reference so the row is not vacuous.
                    if branch_id.is_none() && bus_id.is_none() && gen_id.is_none() && load_id.is_none()
                    {
                        warnings.push(format!(
                            "contingencies row {row} ('{cid}') element {i}: '{element_type}' has no equipment reference (branch/bus/gen/load)"
                        ));
                    }
                }
            }

            if let (Some(bid), Some(known)) = (bus_id, fk.bus_ids) {
                if !known.contains(&bid) {
                    warnings.push(format!(
                        "contingencies row {row} ('{cid}') element {i}: bus_id {bid} not found in buses"
                    ));
                }
            }
        }
    }

    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch_row(id: &str, branch_id: i32) -> ContingencyRow {
        ContingencyRow {
            contingency_id: id.into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(branch_id),
                status_change: true,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn build_and_validate_round_trip() -> Result<()> {
        let batch = build_contingencies_batch(&[branch_row("N1_LINE_5", 5)])?;
        assert_eq!(batch.num_rows(), 1);
        let warnings = validate_contingencies_batch(&batch, &ContingencyFkContext::default())?;
        assert!(warnings.is_empty());
        Ok(())
    }

    #[test]
    fn validate_rejects_duplicate_ids() {
        let batch =
            build_contingencies_batch(&[branch_row("DUP", 1), branch_row("DUP", 2)]).unwrap();
        let err =
            validate_contingencies_batch(&batch, &ContingencyFkContext::default()).unwrap_err();
        assert!(err.to_string().contains("duplicate contingency_id"));
    }

    #[test]
    fn validate_rejects_empty_elements() {
        let row = ContingencyRow {
            contingency_id: "EMPTY".into(),
            elements: vec![],
            ..Default::default()
        };
        let batch = build_contingencies_batch(&[row]).unwrap();
        let err =
            validate_contingencies_batch(&batch, &ContingencyFkContext::default()).unwrap_err();
        assert!(err.to_string().contains("at least one element"));
    }

    #[test]
    fn validate_requires_branch_id_for_branch_outage() {
        let row = ContingencyRow {
            contingency_id: "BAD".into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                status_change: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = build_contingencies_batch(&[row]).unwrap();
        let err =
            validate_contingencies_batch(&batch, &ContingencyFkContext::default()).unwrap_err();
        assert!(err.to_string().contains("requires branch_id"));
    }

    #[test]
    fn validate_warns_on_unknown_branch_fk() -> Result<()> {
        let batch = build_contingencies_batch(&[branch_row("N1_LINE_99", 99)])?;
        let known: std::collections::HashSet<i32> = [1, 2, 3].into_iter().collect();
        let fk = ContingencyFkContext {
            branch_ids: Some(&known),
            ..Default::default()
        };
        let warnings = validate_contingencies_batch(&batch, &fk)?;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("not found in branches"));
        Ok(())
    }

    #[test]
    fn read_contingencies_batch_round_trips() -> Result<()> {
        let row = ContingencyRow {
            contingency_id: "N1_LINE_5".into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(5),
                status_change: true,
                ..Default::default()
            }],
            risk_score: Some(0.42),
            cleared_by_reserves: Some(true),
            recovery_time_min: Some(12.5),
            greedy_reserve_summary: Some("reserve-A".into()),
            ..Default::default()
        };
        let batch = build_contingencies_batch(&[row.clone()])?;
        let decoded = read_contingencies_batch(&batch)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].contingency_id, row.contingency_id);
        assert_eq!(decoded[0].elements.len(), 1);
        assert_eq!(decoded[0].elements[0].branch_id, Some(5));
        assert_eq!(decoded[0].risk_score, Some(0.42));
        assert_eq!(decoded[0].cleared_by_reserves, Some(true));
        assert_eq!(decoded[0].recovery_time_min, Some(12.5));
        assert_eq!(
            decoded[0].greedy_reserve_summary.as_deref(),
            Some("reserve-A")
        );
        Ok(())
    }

    #[test]
    fn compound_generator_and_load_elements_round_trip() -> Result<()> {
        let row = ContingencyRow {
            contingency_id: "COMPOUND".into(),
            elements: vec![
                ContingencyElementRow {
                    element_type: "branch_outage".into(),
                    branch_id: Some(5),
                    status_change: true,
                    ..Default::default()
                },
                ContingencyElementRow {
                    element_type: "generator_trip".into(),
                    bus_id: Some(10),
                    gen_id: Some("1".into()),
                    status_change: true,
                    ..Default::default()
                },
                ContingencyElementRow {
                    element_type: "load_shed".into(),
                    bus_id: Some(20),
                    load_id: Some("1".into()),
                    amount_mw: Some(150.0),
                    status_change: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let batch = build_contingencies_batch(&[row])?;
        validate_contingencies_batch(&batch, &ContingencyFkContext::default())?;
        let elements = batch.column_by_name("elements").unwrap();
        let list = elements
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(list.value(0).len(), 3);
        Ok(())
    }
}
