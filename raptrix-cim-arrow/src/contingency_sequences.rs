// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders and semantic validation for the optional `contingency_sequences` table (v0.14.0+).
//!
//! One row is an ordered N-1-1 pair. Endpoints SHOULD be single-element `contingencies`
//! rows; multi-element endpoints are simultaneous physics and are not hard-failed.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int32Builder, RecordBatch, StringArray,
    StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Int32Type};

use crate::schema::{SEQUENCE_PROVENANCES, TPL_CATEGORIES, contingency_sequences_schema};

/// One sequential N-1-1 pair.
#[derive(Debug, Clone, Default)]
pub struct ContingencySequenceRow {
    pub sequence_id: String,
    pub primary_contingency_id: String,
    pub secondary_contingency_id: String,
    pub intervening_window_min: Option<i32>,
    pub tpl_category: Option<String>,
    pub provenance: Option<String>,
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
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
}

/// Build a `contingency_sequences` RecordBatch matching the locked schema.
pub fn build_contingency_sequences_batch(rows: &[ContingencySequenceRow]) -> Result<RecordBatch> {
    let schema = Arc::new(contingency_sequences_schema());
    let n = rows.len();
    let mut sequence_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut primary_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut secondary_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut window_b = Int32Builder::with_capacity(n);
    let mut tpl_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut provenance_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        sequence_id_b
            .append(&row.sequence_id)
            .context("append sequence_id")?;
        primary_b
            .append(&row.primary_contingency_id)
            .context("append primary_contingency_id")?;
        secondary_b
            .append(&row.secondary_contingency_id)
            .context("append secondary_contingency_id")?;
        match row.intervening_window_min {
            Some(v) => window_b.append_value(v),
            None => window_b.append_null(),
        }
        if let Some(v) = &row.tpl_category {
            tpl_b.append(v).context("append tpl_category")?;
        } else {
            tpl_b.append_null();
        }
        if let Some(v) = &row.provenance {
            provenance_b.append(v).context("append provenance")?;
        } else {
            provenance_b.append_null();
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(sequence_id_b.finish()) as ArrayRef,
            Arc::new(primary_b.finish()) as ArrayRef,
            Arc::new(secondary_b.finish()) as ArrayRef,
            Arc::new(window_b.finish()) as ArrayRef,
            Arc::new(tpl_b.finish()) as ArrayRef,
            Arc::new(provenance_b.finish()) as ArrayRef,
        ],
    )
    .context("building contingency_sequences batch")
}

/// Decode a `contingency_sequences` RecordBatch back into domain rows.
pub fn read_contingency_sequences_batch(
    batch: &RecordBatch,
) -> Result<Vec<ContingencySequenceRow>> {
    let sequence_id = batch.column_by_name("sequence_id").context("sequence_id")?;
    let primary = batch
        .column_by_name("primary_contingency_id")
        .context("primary_contingency_id")?;
    let secondary = batch
        .column_by_name("secondary_contingency_id")
        .context("secondary_contingency_id")?;
    let window = batch
        .column_by_name("intervening_window_min")
        .context("intervening_window_min")?;
    let tpl_category = batch
        .column_by_name("tpl_category")
        .context("tpl_category")?;
    let provenance = batch.column_by_name("provenance").context("provenance")?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.push(ContingencySequenceRow {
            sequence_id: dict_value_at(sequence_id, row).unwrap_or_default(),
            primary_contingency_id: dict_value_at(primary, row).unwrap_or_default(),
            secondary_contingency_id: dict_value_at(secondary, row).unwrap_or_default(),
            intervening_window_min: i32_at(window, row),
            tpl_category: dict_value_at(tpl_category, row),
            provenance: dict_value_at(provenance, row),
        });
    }
    Ok(out)
}

/// Known `contingency_id` values and optional element counts (1 = single-element).
#[derive(Debug, Default)]
pub struct SequenceFkContext<'a> {
    pub contingency_ids: Option<&'a HashSet<String>>,
    pub element_counts: Option<&'a std::collections::HashMap<String, usize>>,
}

/// Structural validation. FK misses and `primary == secondary` are hard errors.
/// Multi-element endpoints emit a warning only.
pub fn validate_contingency_sequences_batch(
    batch: &RecordBatch,
    fk: &SequenceFkContext<'_>,
) -> Result<Vec<String>> {
    let schema = contingency_sequences_schema();
    if batch.schema().fields().len() != schema.fields().len() {
        bail!(
            "contingency_sequences: expected {} columns, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }

    let sequence_id = batch.column_by_name("sequence_id").context("sequence_id")?;
    let primary = batch
        .column_by_name("primary_contingency_id")
        .context("primary_contingency_id")?;
    let secondary = batch
        .column_by_name("secondary_contingency_id")
        .context("secondary_contingency_id")?;
    let tpl_category = batch
        .column_by_name("tpl_category")
        .context("tpl_category")?;
    let provenance = batch.column_by_name("provenance").context("provenance")?;

    let mut warnings = Vec::new();
    let mut seen_ids: HashSet<String> = HashSet::new();

    for row in 0..batch.num_rows() {
        let sid = dict_value_at(sequence_id, row).unwrap_or_default();
        if sid.trim().is_empty() {
            bail!("contingency_sequences row {row}: sequence_id must not be empty");
        }
        if !seen_ids.insert(sid.clone()) {
            bail!("contingency_sequences row {row}: duplicate sequence_id '{sid}'");
        }

        let primary_id = dict_value_at(primary, row).unwrap_or_default();
        let secondary_id = dict_value_at(secondary, row).unwrap_or_default();
        if primary_id.trim().is_empty() || secondary_id.trim().is_empty() {
            bail!(
                "contingency_sequences row {row} ('{sid}'): primary and secondary contingency_id are required"
            );
        }
        if primary_id == secondary_id {
            bail!(
                "contingency_sequences row {row} ('{sid}'): primary_contingency_id and \
                 secondary_contingency_id must differ"
            );
        }

        if let Some(known) = fk.contingency_ids {
            if !known.contains(&primary_id) {
                bail!(
                    "contingency_sequences row {row} ('{sid}'): primary_contingency_id \
                     '{primary_id}' not found in contingencies"
                );
            }
            if !known.contains(&secondary_id) {
                bail!(
                    "contingency_sequences row {row} ('{sid}'): secondary_contingency_id \
                     '{secondary_id}' not found in contingencies"
                );
            }
        }

        if let Some(counts) = fk.element_counts {
            for (role, id) in [("primary", &primary_id), ("secondary", &secondary_id)] {
                if let Some(&n) = counts.get(id)
                    && n > 1
                {
                    warnings.push(format!(
                        "contingency_sequences row {row} ('{sid}'): {role} '{id}' has {n} \
                         elements; multi-element sequence endpoints are simultaneous physics \
                         and are rare"
                    ));
                }
            }
        }

        if let Some(token) = dict_value_at(tpl_category, row)
            && !TPL_CATEGORIES.contains(&token.as_str())
        {
            warnings.push(format!(
                "contingency_sequences row {row} ('{sid}'): tpl_category '{token}' is outside \
                 the closed set P1…P7 / unspecified; preserved"
            ));
        }
        if let Some(token) = dict_value_at(provenance, row)
            && !SEQUENCE_PROVENANCES.contains(&token.as_str())
        {
            warnings.push(format!(
                "contingency_sequences row {row} ('{sid}'): provenance '{token}' is outside \
                 the recommended set; preserved"
            ));
        }
    }

    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(id: &str, a: &str, b: &str) -> ContingencySequenceRow {
        ContingencySequenceRow {
            sequence_id: id.into(),
            primary_contingency_id: a.into(),
            secondary_contingency_id: b.into(),
            intervening_window_min: Some(30),
            tpl_category: Some("P3".into()),
            provenance: Some("planner_file".into()),
        }
    }

    #[test]
    fn build_and_round_trip() -> Result<()> {
        let batch = build_contingency_sequences_batch(&[pair("SEQ_P3_1", "GEN_1", "LINE_2")])?;
        let decoded = read_contingency_sequences_batch(&batch)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].sequence_id, "SEQ_P3_1");
        assert_eq!(decoded[0].intervening_window_min, Some(30));
        assert_eq!(decoded[0].tpl_category.as_deref(), Some("P3"));
        Ok(())
    }

    #[test]
    fn rejects_same_primary_and_secondary() {
        let batch = build_contingency_sequences_batch(&[pair("BAD", "LINE_1", "LINE_1")]).unwrap();
        let err = validate_contingency_sequences_batch(&batch, &SequenceFkContext::default())
            .unwrap_err();
        assert!(err.to_string().contains("must differ"));
    }

    #[test]
    fn rejects_missing_fk() {
        let batch = build_contingency_sequences_batch(&[pair("SEQ", "GEN_1", "LINE_2")]).unwrap();
        let known: HashSet<String> = ["GEN_1".into()].into_iter().collect();
        let fk = SequenceFkContext {
            contingency_ids: Some(&known),
            ..Default::default()
        };
        let err = validate_contingency_sequences_batch(&batch, &fk).unwrap_err();
        assert!(err.to_string().contains("LINE_2"));
    }

    #[test]
    fn warns_on_multi_element_endpoint() -> Result<()> {
        let batch = build_contingency_sequences_batch(&[pair("SEQ", "TOWER", "LINE_2")])?;
        let known: HashSet<String> = ["TOWER".into(), "LINE_2".into()].into_iter().collect();
        let mut counts = std::collections::HashMap::new();
        counts.insert("TOWER".into(), 2usize);
        counts.insert("LINE_2".into(), 1usize);
        let fk = SequenceFkContext {
            contingency_ids: Some(&known),
            element_counts: Some(&counts),
        };
        let warnings = validate_contingency_sequences_batch(&batch, &fk)?;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("simultaneous physics"));
        Ok(())
    }
}
