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
    METADATA_KEY_FEATURE_DIAGRAM_LAYOUT, METADATA_KEY_FEATURE_DYNAMICS_STUB,
    METADATA_KEY_FEATURE_FACTS, METADATA_KEY_FEATURE_FACTS_SOLVED,
    METADATA_KEY_FACTS_SOLVED_STATE_PRESENCE, METADATA_KEY_FEATURE_NODE_BREAKER,
    METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION,
    SCHEMA_VERSION, SUPPORTED_RPF_VERSIONS, TABLE_BRANCHES, TABLE_BUSES, TABLE_BUSES_SOLVED,
    TABLE_DIAGRAM_OBJECTS, TABLE_DIAGRAM_POINTS, TABLE_FACTS_DEVICES, TABLE_FACTS_SOLVED,
    TABLE_GENERATORS, TABLE_GENERATORS_SOLVED, TABLE_LOADS, TABLE_TRANSFORMERS_2W,
    TABLE_TRANSFORMERS_3W, all_table_schemas, diagram_layout_table_schemas,
    facts_table_schemas, node_breaker_table_schemas, schema_metadata, solved_state_table_schemas,
    table_schema,
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
    /// When true, append optional diagram layout tables after other enabled
    /// optional root columns.
    pub include_diagram_layout: bool,
    /// When true, mark contingencies payload as stub-derived.
    pub contingencies_are_stub: bool,
    /// When true, mark dynamics payload as stub-derived.
    pub dynamics_are_stub: bool,
    /// When true, append optional solved-state tables (`buses_solved`,
    /// `generators_solved`) after all other root columns (v0.8.4+).
    pub include_solved_state: bool,
    /// When true, append optional FACTS metadata table (`facts_devices`).
    pub include_facts_devices: bool,
    /// When true, append optional solved FACTS replay table (`facts_solved`).
    /// Requires `include_facts_devices=true`.
    pub include_facts_solved: bool,
}

/// Returns the metadata key used to store the logical row count for a table.
pub fn row_count_metadata_key(table_name: &str) -> String {
    format!("rpf.rows.{table_name}")
}

fn enabled_optional_table_schemas(options: &RootWriteOptions) -> Vec<(&'static str, Schema)> {
    let mut optional = Vec::new();
    if options.include_node_breaker_detail {
        optional.extend(node_breaker_table_schemas());
    }
    if options.include_diagram_layout {
        optional.extend(diagram_layout_table_schemas());
    }
    if options.include_solved_state {
        optional.extend(solved_state_table_schemas());
    }
    if options.include_facts_devices {
        optional.extend(facts_table_schemas(options.include_facts_solved));
    }
    optional
}

fn validate_supported_rpf_version(metadata: &HashMap<String, String>) -> Result<()> {
    let version = metadata
        .get(METADATA_KEY_RPF_VERSION)
        .or_else(|| metadata.get(METADATA_KEY_VERSION))
        .context("invalid RPF file metadata: missing version tag")?;

    if !SUPPORTED_RPF_VERSIONS.contains(&version.as_str()) {
        bail!(
            "unsupported RPF version '{version}'; supported versions are {}",
            SUPPORTED_RPF_VERSIONS.join(", ")
        );
    }

    if let Some(alias) = metadata.get(METADATA_KEY_VERSION) {
        if alias != version {
            bail!(
                "invalid RPF file metadata: '{}'='{}' does not match '{}'='{}'",
                METADATA_KEY_VERSION,
                alias,
                METADATA_KEY_RPF_VERSION,
                version
            );
        }
    }

    Ok(())
}

fn validate_diagram_layout_pair(root_schema: &Schema) -> Result<()> {
    let has_objects = root_schema
        .fields()
        .iter()
        .any(|field| field.name() == TABLE_DIAGRAM_OBJECTS);
    let has_points = root_schema
        .fields()
        .iter()
        .any(|field| field.name() == TABLE_DIAGRAM_POINTS);

    if has_objects != has_points {
        bail!(
            "malformed RPF root schema: '{}' and '{}' must be present together",
            TABLE_DIAGRAM_OBJECTS,
            TABLE_DIAGRAM_POINTS
        );
    }

    Ok(())
}

/// Builds the canonical root schema for an RPF Arrow IPC file.
pub fn root_rpf_schema(include_node_breaker_detail: bool, include_diagram_layout: bool) -> Schema {
    let options = RootWriteOptions {
        include_node_breaker_detail,
        include_diagram_layout,
        ..Default::default()
    };
    root_rpf_schema_with_options(&options)
}

/// Builds the canonical root schema for an RPF Arrow IPC file from full options.
pub fn root_rpf_schema_with_options(options: &RootWriteOptions) -> Schema {
    let mut table_schemas = all_table_schemas();
    if options.include_node_breaker_detail {
        table_schemas.extend(node_breaker_table_schemas());
    }
    if options.include_diagram_layout {
        table_schemas.extend(diagram_layout_table_schemas());
    }
    if options.include_solved_state {
        table_schemas.extend(solved_state_table_schemas());
    }
    if options.include_facts_devices {
        table_schemas.extend(facts_table_schemas(options.include_facts_solved));
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

/// Reads all known tables from an RPF v0.7.x root Arrow IPC file.
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
    validate_supported_rpf_version(reader_schema.metadata())?;
    validate_diagram_layout_pair(reader_schema.as_ref())?;
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

            let actual_fields = match reader_schema.field(column_idx).data_type() {
                DataType::Struct(fields) => fields,
                other => {
                    bail!(
                        "invalid root column '{table_name}': expected Struct field type, found {other:?}"
                    )
                }
            };

            if struct_array.columns().len() > expected_schema.fields().len() {
                bail!(
                    "invalid struct column '{table_name}': expected at most {} fields, found {}",
                    expected_schema.fields().len(),
                    struct_array.columns().len()
                );
            }

            for index in 0..struct_array.columns().len() {
                let expected_field = expected_schema.field(index);
                let actual_field = &actual_fields[index];
                if actual_field.name() != expected_field.name()
                    || actual_field.data_type() != expected_field.data_type()
                {
                    bail!(
                        "invalid struct field in '{table_name}' at index {index}: expected '{}'/{:?}, found '{}'/{:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.name(),
                        actual_field.data_type()
                    );
                }
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

            let mut trimmed_columns: Vec<ArrayRef> = struct_array
                .columns()
                .iter()
                .map(|column| column.slice(0, expected_rows))
                .collect();

            for index in struct_array.columns().len()..expected_schema.fields().len() {
                let expected_field = expected_schema.field(index);
                if !expected_field.is_nullable() {
                    bail!(
                        "invalid struct column '{table_name}': missing non-nullable field '{}'",
                        expected_field.name()
                    );
                }
                trimmed_columns.push(new_null_array(expected_field.data_type(), expected_rows));
            }

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
    write_root_rpf_with_metadata(output_path, table_batches, options, &HashMap::new())
}

/// Writes a canonical root `.rpf` Arrow IPC file from prepared table batches,
/// merging caller-provided metadata keys into root schema metadata.
pub fn write_root_rpf_with_metadata(
    output_path: impl AsRef<Path>,
    table_batches: &HashMap<&'static str, RecordBatch>,
    options: &RootWriteOptions,
    additional_root_metadata: &HashMap<String, String>,
) -> Result<()> {
    let output_path = output_path.as_ref();

    if options.include_facts_solved && !options.include_facts_devices {
        bail!(
            "invalid RootWriteOptions: include_facts_solved=true requires include_facts_devices=true"
        );
    }

    let mut table_specs = all_table_schemas();
    table_specs.extend(enabled_optional_table_schemas(options));

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

    let mut root_schema = root_rpf_schema_with_options(options);
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
    if options.include_diagram_layout {
        root_metadata.insert(
            METADATA_KEY_FEATURE_DIAGRAM_LAYOUT.to_string(),
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
    if options.include_facts_devices {
        root_metadata.insert(METADATA_KEY_FEATURE_FACTS.to_string(), "true".to_string());
        let presence = if options.include_facts_solved {
            "actual_solved"
        } else {
            "not_available"
        };
        root_metadata.insert(
            METADATA_KEY_FACTS_SOLVED_STATE_PRESENCE.to_string(),
            presence.to_string(),
        );
    }
    if options.include_facts_solved {
        root_metadata.insert(
            METADATA_KEY_FEATURE_FACTS_SOLVED.to_string(),
            "true".to_string(),
        );
    }
    for (key, value) in additional_root_metadata {
        root_metadata.insert(key.clone(), value.clone());
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
    canonical.extend(enabled_optional_table_schemas(options));
    let reader_schema = reader.schema();
    validate_supported_rpf_version(reader_schema.metadata())?;
    validate_diagram_layout_pair(reader_schema.as_ref())?;
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
    let rpf_version = metadata.get(METADATA_KEY_RPF_VERSION).with_context(|| {
        format!(
            "post-write contract violation: missing metadata key '{}'",
            METADATA_KEY_RPF_VERSION
        )
    })?;
    if rpf_version != SCHEMA_VERSION {
        bail!(
            "post-write contract violation: rpf_version expected '{}', found '{}'",
            SCHEMA_VERSION,
            rpf_version
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

    if options.include_diagram_layout {
        let feature = metadata
            .get(METADATA_KEY_FEATURE_DIAGRAM_LAYOUT)
            .with_context(|| {
                format!(
                    "post-write contract violation: missing metadata key '{}'",
                    METADATA_KEY_FEATURE_DIAGRAM_LAYOUT
                )
            })?;
        if feature != "true" {
            bail!(
                "post-write contract violation: '{}' expected 'true', found '{}'",
                METADATA_KEY_FEATURE_DIAGRAM_LAYOUT,
                feature
            );
        }

        let diagram_objects = by_name
            .get(TABLE_DIAGRAM_OBJECTS)
            .context("post-write contract violation: missing diagram_objects table")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_OBJECTS, diagram_objects, "element_id")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_OBJECTS, diagram_objects, "element_type")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_OBJECTS, diagram_objects, "diagram_id")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_OBJECTS, diagram_objects, "visible")?;

        let diagram_points = by_name
            .get(TABLE_DIAGRAM_POINTS)
            .context("post-write contract violation: missing diagram_points table")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_POINTS, diagram_points, "element_id")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_POINTS, diagram_points, "diagram_id")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_POINTS, diagram_points, "seq")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_POINTS, diagram_points, "x")?;
        require_non_null_count_equals_len(TABLE_DIAGRAM_POINTS, diagram_points, "y")?;
    }

    // v0.8.4: validate solved-state tables when included.
    if options.include_solved_state {
        let buses_solved = by_name
            .get(TABLE_BUSES_SOLVED)
            .context("post-write contract violation: missing buses_solved table")?;
        require_non_null_count_equals_len(TABLE_BUSES_SOLVED, buses_solved, "bus_id")?;

        let generators_solved = by_name
            .get(TABLE_GENERATORS_SOLVED)
            .context("post-write contract violation: missing generators_solved table")?;
        require_non_null_count_equals_len(TABLE_GENERATORS_SOLVED, generators_solved, "bus_id")?;
        require_non_null_count_equals_len(TABLE_GENERATORS_SOLVED, generators_solved, "id")?;
    }

    if options.include_facts_devices {
        let facts_devices = by_name
            .get(TABLE_FACTS_DEVICES)
            .context("post-write contract violation: missing facts_devices table")?;
        require_non_null_count_equals_len(TABLE_FACTS_DEVICES, facts_devices, "device_id")?;
        require_non_null_count_equals_len(TABLE_FACTS_DEVICES, facts_devices, "device_type")?;
        require_non_null_count_equals_len(TABLE_FACTS_DEVICES, facts_devices, "status")?;
    }

    if options.include_facts_solved {
        let facts_solved = by_name
            .get(TABLE_FACTS_SOLVED)
            .context("post-write contract violation: missing facts_solved table")?;
        require_non_null_count_equals_len(TABLE_FACTS_SOLVED, facts_solved, "device_id")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use crate::schema::{
        METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, SCHEMA_VERSION, all_table_schemas,
        branches_schema, diagram_objects_schema, diagram_points_schema, facts_devices_schema,
        facts_solved_schema, schema_metadata, TABLE_BRANCHES, TABLE_DIAGRAM_OBJECTS,
        TABLE_DIAGRAM_POINTS, TABLE_FACTS_DEVICES, TABLE_FACTS_SOLVED,
    };

    use super::{RootWriteOptions, read_rpf_tables, row_count_metadata_key, rpf_file_metadata, write_root_rpf};

    #[test]
    fn round_trip_preserves_diagram_layout_optional_tables() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_diagram_round_trip");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("diagram_round_trip.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let objects = RecordBatch::try_new(
            Arc::new(diagram_objects_schema()),
            vec![
                Arc::new(StringArray::from(vec!["bus:1"])) as _,
                Arc::new(StringArray::from(vec!["bus"])) as _,
                Arc::new(StringArray::from(vec!["overview"])) as _,
                Arc::new(Float32Array::from(vec![Some(15.0)])) as _,
                Arc::new(arrow::array::BooleanArray::from(vec![true])) as _,
                Arc::new(Int32Array::from(vec![Some(2)])) as _,
            ],
        )?;
        let points = RecordBatch::try_new(
            Arc::new(diagram_points_schema()),
            vec![
                Arc::new(StringArray::from(vec!["bus:1", "bus:1"])) as _,
                Arc::new(StringArray::from(vec!["overview", "overview"])) as _,
                Arc::new(Int32Array::from(vec![0, 1])) as _,
                Arc::new(Float64Array::from(vec![10.0, 25.0])) as _,
                Arc::new(Float64Array::from(vec![30.0, 30.0])) as _,
            ],
        )?;

        table_batches.insert(TABLE_DIAGRAM_OBJECTS, objects);
        table_batches.insert(TABLE_DIAGRAM_POINTS, points);

        write_root_rpf(
            &output_path,
            &table_batches,
            &RootWriteOptions {
                include_node_breaker_detail: false,
                include_diagram_layout: true,
                contingencies_are_stub: false,
                dynamics_are_stub: false,
                include_solved_state: false,
                include_facts_devices: false,
                include_facts_solved: false,
            },
        )?;

        let tables = read_rpf_tables(&output_path)?;
        let diagram_objects = tables
            .iter()
            .find(|(name, _)| name == TABLE_DIAGRAM_OBJECTS)
            .map(|(_, batch)| batch)
            .context("expected diagram_objects table")?;
        let diagram_points = tables
            .iter()
            .find(|(name, _)| name == TABLE_DIAGRAM_POINTS)
            .map(|(_, batch)| batch)
            .context("expected diagram_points table")?;

        assert_eq!(diagram_objects.num_rows(), 1);
        assert_eq!(diagram_points.num_rows(), 2);

        let object_ids = diagram_objects
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("diagram_objects.element_id must be Utf8")?;
        let rotations = diagram_objects
            .column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("diagram_objects.rotation must be Float32")?;
        let point_x = diagram_points
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("diagram_points.x must be Float64")?;
        let point_seq = diagram_points
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("diagram_points.seq must be Int32")?;

        assert_eq!(object_ids.value(0), "bus:1");
        assert!((rotations.value(0) - 15.0).abs() < f32::EPSILON);
        assert_eq!(point_seq.value(1), 1);
        assert!((point_x.value(1) - 25.0).abs() < f64::EPSILON);

        Ok(())
    }

    #[test]
    fn facts_optional_tables_are_absent_when_not_enabled() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_facts_absent");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("facts_absent.rpf");

        let table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        write_root_rpf(&output_path, &table_batches, &RootWriteOptions::default())?;
        let tables = read_rpf_tables(&output_path)?;
        assert!(!tables.iter().any(|(name, _)| name == TABLE_FACTS_DEVICES));
        assert!(!tables.iter().any(|(name, _)| name == TABLE_FACTS_SOLVED));
        Ok(())
    }

    #[test]
    fn facts_optional_tables_round_trip_when_enabled() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_facts_present");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("facts_present.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();
        table_batches.insert(
            TABLE_FACTS_DEVICES,
            RecordBatch::new_empty(Arc::new(facts_devices_schema())),
        );
        table_batches.insert(
            TABLE_FACTS_SOLVED,
            RecordBatch::new_empty(Arc::new(facts_solved_schema())),
        );

        write_root_rpf(
            &output_path,
            &table_batches,
            &RootWriteOptions {
                include_facts_devices: true,
                include_facts_solved: true,
                ..Default::default()
            },
        )?;

        let tables = read_rpf_tables(&output_path)?;
        assert!(tables.iter().any(|(name, _)| name == TABLE_FACTS_DEVICES));
        assert!(tables.iter().any(|(name, _)| name == TABLE_FACTS_SOLVED));

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get("rpf.facts_solved_state_presence"),
            Some(&"actual_solved".to_string())
        );
        Ok(())
    }

    #[test]
    fn read_supports_older_branches_schema_with_missing_additive_columns() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_backward_read");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("v085_like_branches.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let old_branch_fields: Vec<Field> = branches_schema().fields()[0..16]
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let old_branches_schema = Schema::new_with_metadata(old_branch_fields, schema_metadata());
        table_batches.insert(
            TABLE_BRANCHES,
            RecordBatch::new_empty(Arc::new(old_branches_schema.clone())),
        );

        let mut root_fields = Vec::new();
        let mut root_columns: Vec<ArrayRef> = Vec::new();
        for (name, _) in all_table_schemas() {
            let table_batch = table_batches
                .get(name)
                .expect("table batch should exist for each required table");
            let table_schema = table_batch.schema();
            root_fields.push(Field::new(
                name,
                DataType::Struct(table_schema.fields().clone()),
                true,
            ));
            root_columns.push(Arc::new(StructArray::new(
                table_schema.fields().clone(),
                table_batch.columns().to_vec(),
                None,
            )) as ArrayRef);
        }

        let mut root_meta = schema_metadata();
        root_meta.insert(METADATA_KEY_VERSION.to_string(), "0.8.5".to_string());
        root_meta.insert(METADATA_KEY_RPF_VERSION.to_string(), "0.8.5".to_string());
        for (name, _) in all_table_schemas() {
            root_meta.insert(row_count_metadata_key(name), "0".to_string());
        }
        let root_schema = Arc::new(Schema::new_with_metadata(root_fields, root_meta));
        let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)?;

        let mut out = File::create(&output_path)?;
        let mut writer = FileWriter::try_new(&mut out, &root_schema)?;
        writer.write_metadata(METADATA_KEY_VERSION, "0.8.5");
        writer.write_metadata(METADATA_KEY_RPF_VERSION, "0.8.5");
        writer.write(&root_batch)?;
        writer.finish()?;

        let tables = read_rpf_tables(&output_path)?;
        let (_, branches) = tables
            .iter()
            .find(|(name, _)| name == TABLE_BRANCHES)
            .context("missing branches table")?;
        assert_eq!(branches.schema().fields().len(), branches_schema().fields().len());
        assert_eq!(SCHEMA_VERSION, "0.8.6");
        Ok(())
    }
}
