// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! v0.14.1 trailing nullable facility-membership flags.
//!
//! Four independent tri-state booleans (`null` / `true` / `false`) on circuits
//! and transformers. Identity is **never** a 0-based vector index.
//!
//! ## Identity (stable keys, in match order)
//! 1. `mrid` when present and non-empty
//! 2. `branch_id` on `branches`
//! 3. `line_id` on `multi_section_lines`
//! 4. `(from_bus_id, to_bus_id, ckt)` on `branches` / `transformers_2w`
//! 5. `(bus_h_id, bus_m_id, bus_l_id, ckt)` on `transformers_3w`
//!
//! ## Multi-section inheritance
//! For each flag independently: a **non-null section/row value wins**. If the
//! section row is null, inherit the parent `multi_section_lines` value (which
//! may itself be null = unknown). Explicit `false` on a section does **not**
//! inherit `true` from the parent.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Int32Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

const TABLE_BRANCHES: &str = "branches";
const TABLE_MULTI_SECTION_LINES: &str = "multi_section_lines";
const TABLE_TRANSFORMERS_2W: &str = "transformers_2w";
const TABLE_TRANSFORMERS_3W: &str = "transformers_3w";

/// Canonical trailing column names, in on-wire order.
pub const FACILITY_MEMBERSHIP_COLUMNS: &[&str] =
    &["is_secured", "is_bes", "is_bps", "is_bptf"];

/// Trailing nullable Boolean fields appended to membership-bearing tables.
pub fn facility_membership_fields() -> Vec<Field> {
    FACILITY_MEMBERSHIP_COLUMNS
        .iter()
        .map(|name| Field::new(*name, DataType::Boolean, true))
        .collect()
}

/// One flag: explicit row value wins; otherwise parent; otherwise unknown.
#[inline]
pub fn resolve_facility_membership(row: Option<bool>, parent: Option<bool>) -> Option<bool> {
    row.or(parent)
}

/// Authoring stamp. All identity fields optional; at least one stable key required.
#[derive(Debug, Clone, Default)]
pub struct FacilityMembershipStamp {
    pub table: String,
    pub mrid: Option<String>,
    pub branch_id: Option<i32>,
    pub line_id: Option<i32>,
    pub from_bus_id: Option<i32>,
    pub to_bus_id: Option<i32>,
    pub bus_h_id: Option<i32>,
    pub bus_m_id: Option<i32>,
    pub bus_l_id: Option<i32>,
    pub ckt: Option<String>,
    pub is_secured: Option<bool>,
    pub is_bes: Option<bool>,
    pub is_bps: Option<bool>,
    pub is_bptf: Option<bool>,
}

impl FacilityMembershipStamp {
    fn table_name(&self) -> &str {
        if self.table.is_empty() {
            TABLE_BRANCHES
        } else {
            self.table.as_str()
        }
    }

    fn has_stable_identity(&self) -> bool {
        self.mrid.as_deref().is_some_and(|s| !s.is_empty())
            || self.branch_id.is_some()
            || self.line_id.is_some()
            || (self.from_bus_id.is_some() && self.to_bus_id.is_some() && self.ckt.is_some())
            || (self.bus_h_id.is_some()
                && self.bus_m_id.is_some()
                && self.bus_l_id.is_some()
                && self.ckt.is_some())
    }

    fn flag(&self, name: &str) -> Option<bool> {
        match name {
            "is_secured" => self.is_secured,
            "is_bes" => self.is_bes,
            "is_bps" => self.is_bps,
            "is_bptf" => self.is_bptf,
            _ => None,
        }
    }
}

/// Apply stamps to a table batch. Unmatched stamps error. Never matches by row index.
pub fn apply_facility_membership_stamps(
    table_name: &str,
    batch: &RecordBatch,
    stamps: &[FacilityMembershipStamp],
) -> Result<RecordBatch> {
    let relevant: Vec<&FacilityMembershipStamp> = stamps
        .iter()
        .filter(|s| s.table_name() == table_name)
        .collect();
    if relevant.is_empty() {
        return Ok(ensure_membership_columns(batch)?);
    }
    for stamp in &relevant {
        if !stamp.has_stable_identity() {
            bail!(
                "facility_membership stamp for table '{table_name}' must identify equipment by \
                 mrid, branch_id, line_id, from_bus_id+to_bus_id+ckt, or \
                 bus_h_id+bus_m_id+bus_l_id+ckt — never a 0-based vector index"
            );
        }
    }

    let batch = ensure_membership_columns(batch)?;
    let n = batch.num_rows();
    let schema = batch.schema();

    let mut matched = vec![false; relevant.len()];

    let mut builders: Vec<BooleanBuilder> = FACILITY_MEMBERSHIP_COLUMNS
        .iter()
        .map(|_| BooleanBuilder::with_capacity(n))
        .collect();

    for row in 0..n {
        for (col_i, name) in FACILITY_MEMBERSHIP_COLUMNS.iter().enumerate() {
            let existing = bool_at(&batch, name, row)?;
            let mut value = existing;
            for (si, stamp) in relevant.iter().enumerate() {
                if row_matches(table_name, &batch, row, stamp)? {
                    matched[si] = true;
                    if let Some(flag) = stamp.flag(name) {
                        value = Some(flag);
                    }
                }
            }
            match value {
                Some(v) => builders[col_i].append_value(v),
                None => builders[col_i].append_null(),
            }
        }
    }

    if let Some(stamp) = matched
        .iter()
        .zip(relevant.iter())
        .find_map(|(ok, stamp)| if *ok { None } else { Some(*stamp) })
    {
        bail!(
            "facility_membership stamp did not match any row in '{table_name}' \
             (mrid={:?} branch_id={:?} line_id={:?} from={:?} to={:?} ckt={:?})",
            stamp.mrid,
            stamp.branch_id,
            stamp.line_id,
            stamp.from_bus_id,
            stamp.to_bus_id,
            stamp.ckt
        );
    }

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    for name in FACILITY_MEMBERSHIP_COLUMNS {
        let idx = schema.index_of(name)?;
        let col_i = FACILITY_MEMBERSHIP_COLUMNS
            .iter()
            .position(|n| *n == *name)
            .unwrap();
        columns[idx] = Arc::new(builders[col_i].finish()) as ArrayRef;
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Pad a batch with trailing null membership columns when they are absent (dual-read).
pub fn ensure_membership_columns(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let missing: Vec<&str> = FACILITY_MEMBERSHIP_COLUMNS
        .iter()
        .copied()
        .filter(|name| schema.index_of(name).is_err())
        .collect();
    if missing.is_empty() {
        return Ok(batch.clone());
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let n = batch.num_rows();
    for name in missing {
        fields.push(Field::new(name, DataType::Boolean, true));
        columns.push(Arc::new(BooleanArray::from(vec![None; n])) as ArrayRef);
    }
    let new_schema = Schema::new_with_metadata(fields, schema.metadata().clone());
    Ok(RecordBatch::try_new(Arc::new(new_schema), columns)?)
}

/// Parent `line_id` → resolved flags, for inheritance.
pub fn parent_membership_by_line_id(
    msl: &RecordBatch,
) -> Result<HashMap<i32, [Option<bool>; 4]>> {
    let msl = ensure_membership_columns(msl)?;
    let mut map = HashMap::new();
    let id_col = int32_col(&msl, "line_id")?;
    for row in 0..msl.num_rows() {
        if id_col.is_null(row) {
            continue;
        }
        let id = id_col.value(row);
        let mut flags = [None; 4];
        for (i, name) in FACILITY_MEMBERSHIP_COLUMNS.iter().enumerate() {
            flags[i] = bool_at(&msl, name, row)?;
        }
        map.insert(id, flags);
    }
    Ok(map)
}

/// Resolve one branch row against an optional parent line's flags.
pub fn resolve_branch_membership_row(
    branch: &RecordBatch,
    row: usize,
    parent: Option<&[Option<bool>; 4]>,
) -> Result<[Option<bool>; 4]> {
    let mut out = [None; 4];
    for (i, name) in FACILITY_MEMBERSHIP_COLUMNS.iter().enumerate() {
        let section = bool_at(branch, name, row)?;
        let par = parent.and_then(|p| p[i]);
        out[i] = resolve_facility_membership(section, par);
    }
    Ok(out)
}

fn bool_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<bool>> {
    let Ok(idx) = batch.schema().index_of(name) else {
        return Ok(None);
    };
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow::anyhow!("column '{name}' is not Boolean"))?;
    Ok(Some(arr.value(row)))
}

fn int32_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    let idx = batch.schema().index_of(name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("column '{name}' is not Int32"))
}

fn utf8_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let Ok(idx) = batch.schema().index_of(name) else {
        return Ok(None);
    };
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    // Dictionary<Utf8> — take via scalar
    let dtype = col.data_type();
    if matches!(dtype, DataType::Dictionary(_, _)) {
        let v = arrow::array::StringArray::from(vec![""; 0]); // type hint unused
        let _ = v;
        let keyed = arrow::compute::cast(col, &DataType::Utf8)
            .map_err(|e| anyhow::anyhow!("cast {name} to utf8: {e}"))?;
        let arr = keyed
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("cast {name} did not yield Utf8"))?;
        if arr.is_null(row) {
            return Ok(None);
        }
        return Ok(Some(arr.value(row).to_string()));
    }
    Ok(None)
}

fn row_matches(
    table_name: &str,
    batch: &RecordBatch,
    row: usize,
    stamp: &FacilityMembershipStamp,
) -> Result<bool> {
    if let Some(mrid) = stamp.mrid.as_deref().filter(|s| !s.is_empty()) {
        if let Some(got) = utf8_at(batch, "mrid", row)? {
            return Ok(got == mrid);
        }
        return Ok(false);
    }
    match table_name {
        t if t == TABLE_BRANCHES => {
            if let Some(id) = stamp.branch_id {
                let col = int32_col(batch, "branch_id")?;
                if !col.is_null(row) && col.value(row) == id {
                    return Ok(true);
                }
            }
            terminals_match(batch, row, stamp)
        }
        t if t == TABLE_MULTI_SECTION_LINES => {
            if let Some(id) = stamp.line_id {
                let col = int32_col(batch, "line_id")?;
                return Ok(!col.is_null(row) && col.value(row) == id);
            }
            terminals_match(batch, row, stamp)
        }
        t if t == TABLE_TRANSFORMERS_2W => terminals_match(batch, row, stamp),
        t if t == TABLE_TRANSFORMERS_3W => three_winding_match(batch, row, stamp),
        other => bail!("unsupported facility_membership table '{other}'"),
    }
}

fn terminals_match(
    batch: &RecordBatch,
    row: usize,
    stamp: &FacilityMembershipStamp,
) -> Result<bool> {
    let (Some(from), Some(to), Some(ckt)) = (stamp.from_bus_id, stamp.to_bus_id, stamp.ckt.as_deref())
    else {
        return Ok(false);
    };
    let from_col = int32_col(batch, "from_bus_id")?;
    let to_col = int32_col(batch, "to_bus_id")?;
    if from_col.is_null(row) || to_col.is_null(row) {
        return Ok(false);
    }
    if from_col.value(row) != from || to_col.value(row) != to {
        return Ok(false);
    }
    match utf8_at(batch, "ckt", row)? {
        Some(got) => Ok(got == ckt),
        None => Ok(false),
    }
}

fn three_winding_match(
    batch: &RecordBatch,
    row: usize,
    stamp: &FacilityMembershipStamp,
) -> Result<bool> {
    let (Some(h), Some(m), Some(l), Some(ckt)) = (
        stamp.bus_h_id,
        stamp.bus_m_id,
        stamp.bus_l_id,
        stamp.ckt.as_deref(),
    ) else {
        return Ok(false);
    };
    let h_col = int32_col(batch, "bus_h_id")?;
    let m_col = int32_col(batch, "bus_m_id")?;
    let l_col = int32_col(batch, "bus_l_id")?;
    if h_col.is_null(row) || m_col.is_null(row) || l_col.is_null(row) {
        return Ok(false);
    }
    if h_col.value(row) != h || m_col.value(row) != m || l_col.value(row) != l {
        return Ok(false);
    }
    match utf8_at(batch, "ckt", row)? {
        Some(got) => Ok(got == ckt),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches_schema;
    use arrow::array::{Int32Array, StringArray, StringDictionaryBuilder};
    use arrow::datatypes::{DataType, Int32Type, UInt32Type};

    fn dict_i32_ones(n: usize) -> ArrayRef {
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        for _ in 0..n {
            b.append("1").unwrap();
        }
        Arc::new(b.finish())
    }

    fn dict_u32_ones(n: usize) -> ArrayRef {
        let mut b = StringDictionaryBuilder::<UInt32Type>::new();
        for _ in 0..n {
            b.append("L").unwrap();
        }
        Arc::new(b.finish())
    }

    fn tiny_branches(ids: &[i32], mrids: &[Option<&str>]) -> RecordBatch {
        let n = ids.len();
        let schema = branches_schema();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for field in schema.fields() {
            match field.name().as_str() {
                "branch_id" => cols.push(Arc::new(Int32Array::from(ids.to_vec())) as _),
                "from_bus_id" => {
                    cols.push(Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())) as _)
                }
                "to_bus_id" => cols.push(Arc::new(Int32Array::from(
                    (1..=n as i32).collect::<Vec<_>>(),
                )) as _),
                "ckt" => cols.push(dict_i32_ones(n)),
                "name" => cols.push(dict_u32_ones(n)),
                "mrid" => {
                    let v: Vec<Option<&str>> = mrids.to_vec();
                    cols.push(Arc::new(StringArray::from(v)) as _)
                }
                "status" => cols.push(Arc::new(BooleanArray::from(vec![true; n])) as _),
                name if FACILITY_MEMBERSHIP_COLUMNS.contains(&name) => {
                    cols.push(Arc::new(BooleanArray::from(vec![None; n])) as _)
                }
                _ => match field.data_type() {
                    DataType::Float64 => {
                        cols.push(Arc::new(arrow::array::Float64Array::from(vec![1.0; n])) as _)
                    }
                    DataType::Int32 => cols.push(Arc::new(Int32Array::from(vec![0; n])) as _),
                    DataType::Boolean => cols.push(Arc::new(BooleanArray::from(vec![true; n])) as _),
                    DataType::Utf8 => cols.push(Arc::new(StringArray::from(vec![""; n])) as _),
                    _ => cols.push(arrow::array::new_null_array(field.data_type(), n)),
                },
            }
        }
        RecordBatch::try_new(Arc::new(schema), cols).unwrap()
    }

    #[test]
    fn section_non_null_wins_over_parent() {
        assert_eq!(
            resolve_facility_membership(Some(false), Some(true)),
            Some(false)
        );
        assert_eq!(
            resolve_facility_membership(None, Some(true)),
            Some(true)
        );
        assert_eq!(resolve_facility_membership(None, None), None);
        assert_eq!(
            resolve_facility_membership(Some(true), None),
            Some(true)
        );
    }

    #[test]
    fn stamp_by_branch_id_never_by_index() {
        let batch = tiny_branches(&[10, 20, 30], &[None, None, None]);
        let stamps = [FacilityMembershipStamp {
            table: TABLE_BRANCHES.to_string(),
            branch_id: Some(20),
            is_secured: Some(true),
            ..Default::default()
        }];
        let out = apply_facility_membership_stamps(TABLE_BRANCHES, &batch, &stamps).unwrap();
        let idx = out.schema().index_of("is_secured").unwrap();
        let col = out
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.is_null(0));
        assert_eq!(col.value(1), true);
        assert!(col.is_null(2));
    }

    #[test]
    fn stamp_by_mrid_preferred() {
        let batch = tiny_branches(&[10, 20], &[Some("line-a"), Some("line-b")]);
        let stamps = [FacilityMembershipStamp {
            table: TABLE_BRANCHES.to_string(),
            mrid: Some("line-b".into()),
            branch_id: Some(10), // must not win over mrid
            is_bes: Some(true),
            ..Default::default()
        }];
        let out = apply_facility_membership_stamps(TABLE_BRANCHES, &batch, &stamps).unwrap();
        let idx = out.schema().index_of("is_bes").unwrap();
        let col = out
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.is_null(0), "mrid match is row 1, not branch_id 10");
        assert_eq!(col.value(1), true);
    }

    #[test]
    fn stamp_without_stable_identity_errors() {
        let batch = tiny_branches(&[1], &[None]);
        let stamps = [FacilityMembershipStamp {
            table: TABLE_BRANCHES.to_string(),
            is_secured: Some(true),
            ..Default::default()
        }];
        let err = apply_facility_membership_stamps(TABLE_BRANCHES, &batch, &stamps).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("never a 0-based vector index"));
    }

    #[test]
    fn inherit_null_section_from_parent() {
        let mut flags_parent = [None; 4];
        flags_parent[0] = Some(true); // is_secured
        let batch = tiny_branches(&[5], &[None]);
        let resolved = resolve_branch_membership_row(&batch, 0, Some(&flags_parent)).unwrap();
        assert_eq!(resolved[0], Some(true));
        assert_eq!(resolved[1], None);
    }
}
