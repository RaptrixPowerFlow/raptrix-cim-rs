// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic Arrow IPC read/write helpers for Raptrix PowerFlow Interchange files.
//!
//! These APIs are intentionally source-format-agnostic. Callers are expected to
//! prepare canonical table batches before invoking the writer.

use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, StructArray, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use memmap2::MmapOptions;

use crate::schema::{
    BRANDING, METADATA_KEY_BRANDING, METADATA_KEY_FEATURE_CONTINGENCIES_STUB,
    METADATA_KEY_FEATURE_DYNAMICS_STUB, METADATA_KEY_FEATURE_NODE_BREAKER,
    METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, SCHEMA_VERSION, TABLE_BRANCHES, TABLE_BUSES,
    TABLE_GENERATORS, TABLE_LOADS, TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, all_table_schemas,
    node_breaker_table_schemas, schema_metadata, table_schema,
};

/// Summary stats for a single logical table found in an `.rpf` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSummary {
    /// Canonical table name.
    pub table_name: String,
    /// Number of root record batches that contributed rows to this table.
    pub batches: usize,
    /// Total logical row count across contributing batches.
    pub rows: usize,
}

/// Aggregate summary stats for an `.rpf` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpfSummary {
    /// Per-table row and batch counts.
    pub tables: Vec<TableSummary>,
    /// Total count of logical table batches encountered.
    pub total_batches: usize,
    /// Total logical row count across all tables.
    pub total_rows: usize,
    /// Number of canonical required tables for this schema version.
    pub canonical_table_count: usize,
    /// Whether every canonical required table was present.
    pub has_all_canonical_tables: bool,
}

impl RpfSummary {
    /// Returns the logical row count for a named table if it was present.
    pub fn table_rows(&self, table_name: &str) -> Option<usize> {
        self.tables
            .iter()
            .find(|table| table.table_name == table_name)
            .map(|table| table.rows)
    }
}

/// Options controlling root `.rpf` file assembly.
#[derive(Debug, Clone, Copy, Default)]
pub struct RootWriteOptions {
    /// When true, append optional node-breaker detail tables after the 15
    /// canonical required root columns.
    pub include_node_breaker_detail: bool,
    /// When true, mark contingencies payload as stub-derived.
    pub contingencies_are_stub: bool,
    /// When true, mark dynamics payload as stub-derived.
    pub dynamics_are_stub: bool,
}

/// Returns the metadata key used to store the logical row count for a table.
pub fn row_count_metadata_key(table_name: &str) -> String {
    format!("rpf.rows.{table_name}")
}

/// Builds the canonical root schema for an RPF Arrow IPC file.
pub fn root_rpf_schema(include_node_breaker_detail: bool) -> Schema {
    let mut table_schemas = all_table_schemas();
    if include_node_breaker_detail {
        table_schemas.extend(node_breaker_table_schemas());
    }

    let fields = table_schemas
        .into_iter()
        .map(|(table_name, schema)| {
            Field::new(table_name, DataType::Struct(schema.fields().clone()), true)
        })
        .collect::<Vec<_>>();

    Schema::new_with_metadata(fields, schema_metadata())
}

fn require_non_null_count_equals_len(
    table_name: &str,
    batch: &RecordBatch,
    column_name: &str,
) -> Result<()> {
    let index = batch.schema().index_of(column_name).with_context(|| {
        format!("missing required column '{column_name}' in table '{table_name}'")
    })?;
    let column = batch.column(index);
    let non_null_count = batch.num_rows().saturating_sub(column.null_count());
    if non_null_count != batch.num_rows() {
        bail!(
            "post-write contract violation: table '{table_name}' column '{column_name}' has non-null count {non_null_count} but table length is {}",
            batch.num_rows()
        );
    }
    Ok(())
}

/// Reads all known tables from an RPF v0.7.0 root Arrow IPC file.
pub fn read_rpf_tables(path: impl AsRef<Path>) -> Result<Vec<(String, RecordBatch)>> {
    let path = path.as_ref();
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

    let reader_schema = reader.schema();
    let canonical_count = all_table_schemas().len();
    if reader_schema.fields().len() < canonical_count {
        bail!(
            "invalid RPF root schema: expected at least {} columns, found {}",
            canonical_count,
            reader_schema.fields().len()
        );
    }
    for (idx, (expected_name, _)) in all_table_schemas().iter().enumerate() {
        let actual_name = reader_schema.field(idx).name();
        if actual_name != *expected_name {
            bail!(
                "invalid RPF root schema at column {idx}: expected '{expected_name}', found '{actual_name}'"
            );
        }
    }

    let mut out = Vec::new();
    for root_batch_result in &mut reader {
        let root_batch = root_batch_result
            .with_context(|| format!("failed reading root record batch from {}", path.display()))?;

        for column_idx in 0..reader_schema.fields().len() {
            let table_name = reader_schema.field(column_idx).name().as_str();
            let Some(expected_schema) = table_schema(table_name) else {
                continue;
            };
            let struct_array = root_batch
                .column(column_idx)
                .as_any()
                .downcast_ref::<StructArray>()
                .with_context(|| {
                    format!(
                        "invalid root column '{table_name}': expected StructArray at index {column_idx}"
                    )
                })?;

            if struct_array.columns().len() != expected_schema.fields().len() {
                bail!(
                    "invalid struct column '{table_name}': expected {} fields, found {}",
                    expected_schema.fields().len(),
                    struct_array.columns().len()
                );
            }

            let expected_rows = reader_schema
                .metadata()
                .get(&row_count_metadata_key(table_name))
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(struct_array.len());

            if expected_rows > struct_array.len() {
                bail!(
                    "invalid row count metadata for table '{table_name}': expected_rows={expected_rows} exceeds struct length {}",
                    struct_array.len()
                );
            }

            let trimmed_columns: Vec<ArrayRef> = struct_array
                .columns()
                .iter()
                .map(|column| column.slice(0, expected_rows))
                .collect();

            let table_batch =
                RecordBatch::try_new(Arc::new(expected_schema.clone()), trimmed_columns)
                    .with_context(|| {
                        format!("failed reconstructing table '{table_name}' from root record batch")
                    })?;
            out.push((table_name.to_string(), table_batch));
        }
    }

    if out.is_empty() {
        bail!("RPF file did not contain any root record batches")
    }

    Ok(out)
}

/// Reads an `.rpf` file and returns table-level row and batch counts.
pub fn summarize_rpf(path: impl AsRef<Path>) -> Result<RpfSummary> {
    let tables = read_rpf_tables(path)?;
    let canonical_table_count = all_table_schemas().len();

    let mut summaries: Vec<TableSummary> = Vec::new();
    let mut by_name_index: HashMap<String, usize> = HashMap::new();

    for (table_name, batch) in tables {
        let idx = if let Some(existing_idx) = by_name_index.get(&table_name) {
            *existing_idx
        } else {
            let next_idx = summaries.len();
            summaries.push(TableSummary {
                table_name: table_name.clone(),
                batches: 0,
                rows: 0,
            });
            by_name_index.insert(table_name, next_idx);
            next_idx
        };

        summaries[idx].batches += 1;
        summaries[idx].rows += batch.num_rows();
    }

    let total_batches = summaries.iter().map(|table| table.batches).sum();
    let total_rows = summaries.iter().map(|table| table.rows).sum();

    Ok(RpfSummary {
        has_all_canonical_tables: summaries.len() >= canonical_table_count,
        tables: summaries,
        total_batches,
        total_rows,
        canonical_table_count,
    })
}

/// Reads file-level root metadata from an `.rpf` Arrow IPC file.
pub fn rpf_file_metadata(path: impl AsRef<Path>) -> Result<HashMap<String, String>> {
    let path = path.as_ref();
    let file = File::open(path)
        .with_context(|| format!("failed to open .rpf file at {}", path.display()))?;
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("failed to memory-map .rpf file at {}", path.display()))?;

    let reader = FileReader::try_new(Cursor::new(&mmap[..]), None).with_context(|| {
        format!(
            "failed to open Arrow IPC file reader for {}",
            path.display()
        )
    })?;

    Ok(reader.schema().metadata().clone())
}

/// Writes a canonical root `.rpf` Arrow IPC file from prepared table batches.
pub fn write_root_rpf(
    output_path: impl AsRef<Path>,
    table_batches: &HashMap<&'static str, RecordBatch>,
    options: &RootWriteOptions,
) -> Result<()> {
    let output_path = output_path.as_ref();

    let mut table_specs = all_table_schemas();
    if options.include_node_breaker_detail {
        table_specs.extend(node_breaker_table_schemas());
    }

    let max_rows = table_specs
        .iter()
        .map(|(name, _)| {
            table_batches
                .get(name)
                .map(RecordBatch::num_rows)
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0);

    let mut root_schema = root_rpf_schema(options.include_node_breaker_detail);
    let mut root_metadata = root_schema.metadata().clone();
    for (table_name, _) in &table_specs {
        let row_count = table_batches
            .get(*table_name)
            .map(RecordBatch::num_rows)
            .unwrap_or(0);
        root_metadata.insert(row_count_metadata_key(table_name), row_count.to_string());
    }
    if options.include_node_breaker_detail {
        root_metadata.insert(
            METADATA_KEY_FEATURE_NODE_BREAKER.to_string(),
            "true".to_string(),
        );
    }
    if options.contingencies_are_stub {
        root_metadata.insert(
            METADATA_KEY_FEATURE_CONTINGENCIES_STUB.to_string(),
            "true".to_string(),
        );
    }
    if options.dynamics_are_stub {
        root_metadata.insert(
            METADATA_KEY_FEATURE_DYNAMICS_STUB.to_string(),
            "true".to_string(),
        );
    }
    root_schema = root_schema.with_metadata(root_metadata);
    let root_schema = Arc::new(root_schema);

    let mut root_columns: Vec<ArrayRef> = Vec::with_capacity(table_specs.len());

    for (table_name, expected_schema) in table_specs {
        let table_batch = table_batches
            .get(table_name)
            .with_context(|| format!("missing required table batch '{table_name}'"))?;

        if table_batch.schema().fields() != expected_schema.fields() {
            bail!("schema drift in table '{table_name}' while assembling root IPC file");
        }

        let mut padded_columns: Vec<ArrayRef> = Vec::with_capacity(table_batch.num_columns());
        for column in table_batch.columns() {
            if table_batch.num_rows() < max_rows {
                let null_tail =
                    new_null_array(column.data_type(), max_rows - table_batch.num_rows());
                let concatenated =
                    concat(&[column.as_ref(), null_tail.as_ref()]).with_context(|| {
                        format!("failed to pad table '{table_name}' to root row length")
                    })?;
                padded_columns.push(concatenated);
            } else {
                padded_columns.push(column.clone());
            }
        }

        let struct_validity = if table_batch.num_rows() < max_rows {
            Some(NullBuffer::from(
                (0..max_rows)
                    .map(|index| index < table_batch.num_rows())
                    .collect::<Vec<_>>(),
            ))
        } else {
            None
        };

        let struct_array = StructArray::new(
            expected_schema.fields().clone(),
            padded_columns,
            struct_validity,
        );
        root_columns.push(Arc::new(struct_array) as ArrayRef);
    }

    let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)
        .context("failed to build root RPF record batch")?;

    let mut output = File::create(output_path).with_context(|| {
        format!(
            "failed to create output .rpf file at {}",
            output_path.display()
        )
    })?;
    let mut writer = FileWriter::try_new(&mut output, &root_schema)
        .context("failed to initialize root Arrow IPC FileWriter")?;
    writer.write_metadata(METADATA_KEY_BRANDING, BRANDING);
    writer.write_metadata(METADATA_KEY_VERSION, SCHEMA_VERSION);
    writer.write_metadata(METADATA_KEY_RPF_VERSION, SCHEMA_VERSION);
    writer
        .write(&root_batch)
        .context("failed writing root RPF record batch")?;
    writer
        .finish()
        .context("failed finishing root Arrow IPC file")?;

    validate_rpf_file(output_path, options)?;
    Ok(())
}

/// Validates a just-written `.rpf` file against the locked root contract.
pub fn validate_rpf_file(path: impl AsRef<Path>, options: &RootWriteOptions) -> Result<()> {
    let path = path.as_ref();

    let file = File::open(path)
        .with_context(|| format!("failed to reopen emitted .rpf at {}", path.display()))?;
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("failed to memory-map emitted .rpf at {}", path.display()))?;
    let mut reader = FileReader::try_new(Cursor::new(&mmap[..]), None)
        .with_context(|| format!("failed to open Arrow IPC FileReader for {}", path.display()))?;

    let mut canonical = all_table_schemas();
    if options.include_node_breaker_detail {
        canonical.extend(node_breaker_table_schemas());
    }
    let reader_schema = reader.schema();
    if reader_schema.fields().len() != canonical.len() {
        bail!(
            "post-write contract violation: expected {} canonical root columns, found {}",
            canonical.len(),
            reader_schema.fields().len()
        );
    }
    for (index, (expected_name, _)) in canonical.iter().enumerate() {
        let found = reader_schema.field(index).name();
        if found != *expected_name {
            bail!(
                "post-write contract violation: root column {index} expected '{expected_name}', found '{found}'"
            );
        }
    }

    let metadata = reader_schema.metadata();
    let version = metadata.get(METADATA_KEY_VERSION).with_context(|| {
        format!(
            "post-write contract violation: missing metadata key '{}'",
            METADATA_KEY_VERSION
        )
    })?;
    if version != SCHEMA_VERSION {
        bail!(
            "post-write contract violation: raptrix.version expected '{}', found '{}'",
            SCHEMA_VERSION,
            version
        );
    }
    let branding = metadata
        .get("raptrix.branding")
        .context("post-write contract violation: missing metadata key 'raptrix.branding'")?;
    if !branding.contains("Musto Technologies") {
        bail!(
            "post-write contract violation: raptrix.branding does not contain 'Musto Technologies'"
        );
    }

    if reader.next().is_none() {
        bail!("post-write contract violation: file contains zero root record batches");
    }

    let tables = read_rpf_tables(path)?;
    let by_name: HashMap<String, RecordBatch> = tables.into_iter().collect();

    let buses = by_name
        .get(TABLE_BUSES)
        .context("post-write contract violation: missing buses table")?;
    require_non_null_count_equals_len(TABLE_BUSES, buses, "bus_id")?;

    let branches = by_name
        .get(TABLE_BRANCHES)
        .context("post-write contract violation: missing branches table")?;
    require_non_null_count_equals_len(TABLE_BRANCHES, branches, "branch_id")?;
    require_non_null_count_equals_len(TABLE_BRANCHES, branches, "from_bus_id")?;
    require_non_null_count_equals_len(TABLE_BRANCHES, branches, "to_bus_id")?;

    let generators = by_name
        .get(TABLE_GENERATORS)
        .context("post-write contract violation: missing generators table")?;
    require_non_null_count_equals_len(TABLE_GENERATORS, generators, "bus_id")?;
    require_non_null_count_equals_len(TABLE_GENERATORS, generators, "id")?;

    let loads = by_name
        .get(TABLE_LOADS)
        .context("post-write contract violation: missing loads table")?;
    require_non_null_count_equals_len(TABLE_LOADS, loads, "bus_id")?;
    require_non_null_count_equals_len(TABLE_LOADS, loads, "id")?;

    let t2w = by_name
        .get(TABLE_TRANSFORMERS_2W)
        .context("post-write contract violation: missing transformers_2w table")?;
    require_non_null_count_equals_len(TABLE_TRANSFORMERS_2W, t2w, "from_bus_id")?;
    require_non_null_count_equals_len(TABLE_TRANSFORMERS_2W, t2w, "to_bus_id")?;

    let t3w = by_name
        .get(TABLE_TRANSFORMERS_3W)
        .context("post-write contract violation: missing transformers_3w table")?;
    require_non_null_count_equals_len(TABLE_TRANSFORMERS_3W, t3w, "bus_h_id")?;
    require_non_null_count_equals_len(TABLE_TRANSFORMERS_3W, t3w, "bus_m_id")?;
    require_non_null_count_equals_len(TABLE_TRANSFORMERS_3W, t3w, "bus_l_id")?;

    Ok(())
}
