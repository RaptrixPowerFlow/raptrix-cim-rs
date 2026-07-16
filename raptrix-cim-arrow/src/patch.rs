// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Patch-based RPF re-export: source file + solver patch → lossless output.
//!
//! See `docs/schema-contract.md` § Table Ownership.

use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, BooleanBufferBuilder, StructArray, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use memmap2::MmapOptions;

use crate::io::row_count_metadata_key;
use crate::schema::{
    BRANDING, METADATA_KEY_BRANDING, METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, RPF_VERSION,
    SCHEMA_VERSION, TABLE_METADATA, TableOwnership, is_solver_root_metadata_key, table_ownership,
};

struct RootRpf {
    schema: Arc<Schema>,
    batch: RecordBatch,
}

fn open_root_rpf(path: &Path) -> Result<RootRpf> {
    let file = File::open(path)
        .with_context(|| format!("failed to open .rpf file at {}", path.display()))?;
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("failed to memory-map .rpf file at {}", path.display()))?;

    let mut reader = FileReader::try_new(Cursor::new(&mmap[..]), None).with_context(|| {
        format!(
            "failed to open Arrow IPC file reader for {}",
            path.display()
        )
    })?;

    // Tolerate the documented pad-row encoding used by some exporters.
    let root_batch = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error))
            if format!("{error:#}").contains("Found unmasked nulls for non-nullable") =>
        {
            let mut retry =
                FileReader::try_new(Cursor::new(&mmap[..]), None).with_context(|| {
                    format!(
                        "failed to reopen Arrow IPC file reader for {}",
                        path.display()
                    )
                })?;
            retry = unsafe { retry.with_skip_validation(true) };
            retry
                .next()
                .context("RPF file did not contain a root record batch")?
                .with_context(|| {
                    format!("failed reading root record batch from {}", path.display())
                })?
        }
        Some(Err(error)) => {
            return Err(error).with_context(|| {
                format!("failed reading root record batch from {}", path.display())
            });
        }
        None => bail!("RPF file did not contain a root record batch"),
    };

    Ok(RootRpf {
        schema: reader.schema(),
        batch: root_batch,
    })
}

fn logical_row_count(schema: &Schema, table_name: &str, struct_array: &StructArray) -> usize {
    if let Some(value) = schema.metadata().get(&row_count_metadata_key(table_name))
        && let Ok(rows) = value.parse::<usize>()
    {
        return rows.min(struct_array.len());
    }
    match struct_array.nulls() {
        Some(nulls) => (0..struct_array.len())
            .filter(|&index| nulls.is_valid(index))
            .count(),
        None => struct_array.len(),
    }
}

fn trim_struct_array(struct_array: &StructArray, rows: usize) -> Result<StructArray> {
    let rows = rows.min(struct_array.len());
    let fields = match struct_array.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => bail!("expected StructArray, found {other:?}"),
    };
    let columns: Vec<ArrayRef> = struct_array
        .columns()
        .iter()
        .map(|column| column.slice(0, rows))
        .collect();
    Ok(StructArray::new(fields, columns, None))
}

fn pad_struct_array(struct_array: StructArray, max_rows: usize) -> Result<ArrayRef> {
    let rows = struct_array.len();
    if rows == max_rows {
        return Ok(Arc::new(struct_array) as ArrayRef);
    }
    if rows > max_rows {
        bail!("table struct length {rows} exceeds max_rows {max_rows}");
    }

    let fields = match struct_array.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => bail!("expected StructArray, found {other:?}"),
    };
    let mut padded_columns: Vec<ArrayRef> = Vec::with_capacity(struct_array.num_columns());
    for column in struct_array.columns() {
        let null_tail = new_null_array(column.data_type(), max_rows - rows);
        let concatenated = concat(&[column.as_ref(), null_tail.as_ref()])
            .context("failed to pad struct column for root assembly")?;
        padded_columns.push(concatenated);
    }
    let mut validity = BooleanBufferBuilder::new(max_rows);
    for index in 0..max_rows {
        validity.append(index < rows);
    }
    Ok(Arc::new(StructArray::new(
        fields,
        padded_columns,
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef)
}

fn struct_column_as_batch(struct_array: &StructArray, rows: usize) -> Result<RecordBatch> {
    let trimmed = trim_struct_array(struct_array, rows)?;
    let fields = match trimmed.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => bail!("expected StructArray, found {other:?}"),
    };
    let schema = Arc::new(Schema::new(fields.to_vec()));
    RecordBatch::try_new(schema, trimmed.columns().to_vec())
        .context("failed to rebuild RecordBatch from struct column")
}

fn merge_metadata_batches(source: &RecordBatch, patch: &RecordBatch) -> Result<RecordBatch> {
    // Prefer the wider schema (usually the newer contract); fill from patch
    // when that column is non-null, otherwise from source.
    let row_count = source.num_rows().max(patch.num_rows());
    if row_count == 0 {
        return Ok(source.clone());
    }

    let mut field_names: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for field in patch
        .schema()
        .fields()
        .iter()
        .chain(source.schema().fields())
    {
        if seen.insert(field.name().clone()) {
            field_names.push(field.name().clone());
        }
    }
    let mut fields = Vec::with_capacity(field_names.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(field_names.len());

    for name in &field_names {
        let patch_idx = patch.schema().index_of(name).ok();
        let source_idx = source.schema().index_of(name).ok();
        let (field, chosen) = match (patch_idx, source_idx) {
            (Some(p), Some(s)) => {
                let patch_col = patch.column(p);
                let source_col = source.column(s);
                let use_patch = patch.num_rows() > 0 && !patch_col.is_null(0);
                let field = if use_patch {
                    patch.schema().field(p).clone()
                } else {
                    source.schema().field(s).clone()
                };
                let col = if use_patch {
                    patch_col.clone()
                } else {
                    source_col.clone()
                };
                (field, col)
            }
            (Some(p), None) => (patch.schema().field(p).clone(), patch.column(p).clone()),
            (None, Some(s)) => (source.schema().field(s).clone(), source.column(s).clone()),
            (None, None) => unreachable!("field name collected from one of the schemas"),
        };

        let col = if chosen.len() < row_count {
            let null_tail = new_null_array(chosen.data_type(), row_count - chosen.len());
            concat(&[chosen.as_ref(), null_tail.as_ref()])
                .context("failed to pad metadata column")?
        } else if chosen.len() > row_count {
            chosen.slice(0, row_count)
        } else {
            chosen
        };
        fields.push(field);
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("failed to build merged metadata RecordBatch")
}

fn batch_to_struct_array(batch: &RecordBatch) -> StructArray {
    StructArray::new(
        batch.schema().fields().clone(),
        batch.columns().to_vec(),
        None,
    )
}

fn require_struct_column<'a>(
    root: &'a RootRpf,
    table_name: &str,
) -> Result<Option<&'a StructArray>> {
    let Some(index) = root.schema.index_of(table_name).ok() else {
        return Ok(None);
    };
    let array = root
        .batch
        .column(index)
        .as_any()
        .downcast_ref::<StructArray>()
        .with_context(|| format!("root column '{table_name}' is not a StructArray"))?;
    Ok(Some(array))
}

/// Applies a solver patch onto a source `.rpf`, writing a new file.
///
/// - Converter-owned tables always come from `source_path` (including unknown tables).
/// - Solver-owned tables come from `patch_path` when present.
/// - `metadata` is merged column-wise (patch non-null wins).
/// - Solver file-level metadata keys overlay the source; `rpf.rows.*` is recomputed.
///
/// `patch_path` may be a full core export; converter-owned tables inside it are ignored.
pub fn apply_rpf_patch(
    source_path: impl AsRef<Path>,
    patch_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<()> {
    let source_path = source_path.as_ref();
    let patch_path = patch_path.as_ref();
    let output_path = output_path.as_ref();

    let source = open_root_rpf(source_path)?;
    let patch = open_root_rpf(patch_path)?;

    // Output column order: source order, then any solver-owned tables only in patch.
    let mut out_names: Vec<String> = source
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut present: std::collections::HashSet<String> = out_names.iter().cloned().collect();
    for field in patch.schema.fields() {
        let name = field.name().as_str();
        if table_ownership(name) == TableOwnership::Solver && present.insert(name.to_string()) {
            out_names.push(name.to_string());
        }
    }

    let mut logical_rows_by_table: HashMap<String, usize> = HashMap::new();
    let mut chosen_structs: HashMap<String, StructArray> = HashMap::new();

    for name in &out_names {
        let ownership = table_ownership(name);
        match ownership {
            TableOwnership::Converter => {
                let Some(array) = require_struct_column(&source, name)? else {
                    bail!("source RPF missing converter-owned table '{name}'");
                };
                let rows = logical_row_count(source.schema.as_ref(), name, array);
                logical_rows_by_table.insert(name.clone(), rows);
                chosen_structs.insert(name.clone(), trim_struct_array(array, rows)?);
            }
            TableOwnership::Solver => {
                if let Some(array) = require_struct_column(&patch, name)? {
                    let rows = logical_row_count(patch.schema.as_ref(), name, array);
                    logical_rows_by_table.insert(name.clone(), rows);
                    chosen_structs.insert(name.clone(), trim_struct_array(array, rows)?);
                } else if let Some(array) = require_struct_column(&source, name)? {
                    let rows = logical_row_count(source.schema.as_ref(), name, array);
                    logical_rows_by_table.insert(name.clone(), rows);
                    chosen_structs.insert(name.clone(), trim_struct_array(array, rows)?);
                } else {
                    bail!("solver-owned table '{name}' missing from both source and patch");
                }
            }
            TableOwnership::Shared => {
                let source_array = require_struct_column(&source, name)?
                    .with_context(|| format!("source RPF missing shared table '{name}'"))?;
                let source_rows = logical_row_count(source.schema.as_ref(), name, source_array);
                let source_batch = struct_column_as_batch(source_array, source_rows)?;
                let merged = if name == TABLE_METADATA {
                    if let Some(patch_array) = require_struct_column(&patch, name)? {
                        let patch_rows =
                            logical_row_count(patch.schema.as_ref(), name, patch_array);
                        let patch_batch = struct_column_as_batch(patch_array, patch_rows)?;
                        merge_metadata_batches(&source_batch, &patch_batch)?
                    } else {
                        source_batch
                    }
                } else if let Some(patch_array) = require_struct_column(&patch, name)? {
                    let patch_rows = logical_row_count(patch.schema.as_ref(), name, patch_array);
                    struct_column_as_batch(patch_array, patch_rows)?
                } else {
                    source_batch
                };
                logical_rows_by_table.insert(name.clone(), merged.num_rows());
                chosen_structs.insert(name.clone(), batch_to_struct_array(&merged));
            }
        }
    }

    let max_rows = logical_rows_by_table.values().copied().max().unwrap_or(0);

    let mut out_fields = Vec::with_capacity(out_names.len());
    let mut out_columns = Vec::with_capacity(out_names.len());
    for name in &out_names {
        let struct_array = chosen_structs
            .remove(name)
            .with_context(|| format!("internal error: missing assembled table '{name}'"))?;
        let field = if let Ok(index) = source.schema.index_of(name) {
            let base = source.schema.field(index).clone();
            Field::new(
                base.name(),
                DataType::Struct(match struct_array.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => unreachable!(),
                }),
                base.is_nullable(),
            )
        } else if let Ok(index) = patch.schema.index_of(name) {
            let base = patch.schema.field(index).clone();
            Field::new(
                base.name(),
                DataType::Struct(match struct_array.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => unreachable!(),
                }),
                base.is_nullable(),
            )
        } else {
            Field::new(
                name,
                DataType::Struct(match struct_array.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => unreachable!(),
                }),
                true,
            )
        };
        out_fields.push(field);
        out_columns.push(pad_struct_array(struct_array, max_rows)?);
    }

    let mut metadata = source.schema.metadata().clone();
    for (key, value) in patch.schema.metadata() {
        if key.starts_with("rpf.rows.") {
            continue;
        }
        if is_solver_root_metadata_key(key) || !metadata.contains_key(key) {
            metadata.insert(key.clone(), value.clone());
        }
    }
    metadata.insert(METADATA_KEY_BRANDING.to_string(), BRANDING.to_string());
    metadata.insert(METADATA_KEY_VERSION.to_string(), SCHEMA_VERSION.to_string());
    metadata.insert(
        METADATA_KEY_RPF_VERSION.to_string(),
        RPF_VERSION.to_string(),
    );
    for (table_name, rows) in &logical_rows_by_table {
        metadata.insert(row_count_metadata_key(table_name), rows.to_string());
    }

    let out_schema = Arc::new(Schema::new_with_metadata(out_fields, metadata));
    let out_batch = RecordBatch::try_new(out_schema.clone(), out_columns)
        .context("failed to build patched root RecordBatch")?;

    let mut output = File::create(output_path).with_context(|| {
        format!(
            "failed to create output .rpf file at {}",
            output_path.display()
        )
    })?;
    let mut writer = FileWriter::try_new(&mut output, &out_schema)
        .context("failed to initialize patched Arrow IPC FileWriter")?;
    writer.write_metadata(METADATA_KEY_BRANDING, BRANDING);
    writer.write_metadata(METADATA_KEY_VERSION, SCHEMA_VERSION);
    writer.write_metadata(METADATA_KEY_RPF_VERSION, RPF_VERSION);
    writer
        .write(&out_batch)
        .context("failed writing patched root RPF record batch")?;
    writer
        .finish()
        .context("failed finishing patched Arrow IPC file")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TABLE_BUSES, TABLE_BUSES_SOLVED, TableOwnership, table_ownership};

    #[test]
    fn ownership_defaults_unknown_to_converter() {
        assert_eq!(
            table_ownership("future_enrichment_v99"),
            TableOwnership::Converter
        );
        assert_eq!(table_ownership(TABLE_BUSES), TableOwnership::Converter);
        assert_eq!(table_ownership(TABLE_BUSES_SOLVED), TableOwnership::Solver);
        assert_eq!(table_ownership(TABLE_METADATA), TableOwnership::Shared);
    }
}
