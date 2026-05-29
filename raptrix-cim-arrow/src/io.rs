/*
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
*/

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic Arrow IPC read/write helpers for Raptrix Power Interchange files.
//!
//! These APIs are intentionally source-format-agnostic. Callers are expected to
//! prepare canonical table batches before invoking the writer.

use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Int32Array, StructArray, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use memmap2::MmapOptions;

use crate::schema::{
    BRANDING, METADATA_KEY_BRANDING, METADATA_KEY_FACTS_SOLVED_STATE_PRESENCE,
    METADATA_KEY_FEATURE_COMPUTATIONAL_LOAD_PROFILES, METADATA_KEY_FEATURE_CONTINGENCIES_STUB,
    METADATA_KEY_FEATURE_DIAGRAM_LAYOUT, METADATA_KEY_FEATURE_DYNAMICS_STUB,
    METADATA_KEY_FEATURE_FACTS, METADATA_KEY_FEATURE_FACTS_SOLVED,
    METADATA_KEY_FEATURE_NODE_BREAKER, METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES,
    METADATA_KEY_FEATURE_TOPOLOGY_CHANGES, METADATA_KEY_PROTECTION_FIDELITY,
    METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, SCHEMA_VERSION, SUPPORTED_RPF_VERSIONS,
    TABLE_BRANCHES, TABLE_BUSES, TABLE_BUSES_SOLVED, TABLE_COMPUTATIONAL_LOAD_PROFILES,
    TABLE_DC_LINES_2W, TABLE_DIAGRAM_OBJECTS, TABLE_DIAGRAM_POINTS, TABLE_FACTS_DEVICES,
    TABLE_FACTS_SOLVED, TABLE_GENERATORS, TABLE_GENERATORS_SOLVED, TABLE_LOADS,
    TABLE_MULTI_SECTION_LINES, TABLE_PROTECTION_CONTINGENCIES, TABLE_SWITCHED_SHUNT_BANKS,
    TABLE_TOPOLOGY_CHANGES, TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, all_table_schemas,
    computational_load_table_schemas, diagram_layout_table_schemas, facts_table_schemas,
    node_breaker_table_schemas, protection_table_schemas, schema_metadata,
    solved_state_table_schemas, table_schema,
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
    /// `generators_solved`, `switched_shunts_solved`) after all other root
    /// columns (v0.8.4+).
    ///
    /// Used for both `case_mode = solved_snapshot` (full post-solve payload)
    /// and `case_mode = warm_start_planning` with
    /// `solved_state_presence = "seed_only"` (v0.9.6+) — in the latter case
    /// only `buses_solved` carries data and the other two tables are emitted
    /// as zero-row, structurally valid placeholders.
    pub include_solved_state: bool,
    /// When true, append optional FACTS metadata table (`facts_devices`).
    pub include_facts_devices: bool,
    /// When true, append optional solved FACTS replay table (`facts_solved`).
    /// Requires `include_facts_devices=true`.
    pub include_facts_solved: bool,
    /// When true, append optional `computational_load_profiles` table (v0.10.0+).
    pub include_computational_load_profiles: bool,
    /// When true, append optional `protection_contingencies` table (v0.11.0+).
    pub include_protection_contingencies: bool,
    /// When true, append optional `topology_changes` table (v0.11.0+).
    /// Requires `include_protection_contingencies = true`.
    pub include_topology_changes: bool,
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
    if options.include_protection_contingencies {
        optional.extend(protection_table_schemas(options.include_topology_changes));
    }
    if options.include_computational_load_profiles {
        optional.extend(computational_load_table_schemas());
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

    if let Some(alias) = metadata.get(METADATA_KEY_VERSION)
        && alias != version
    {
        bail!(
            "invalid RPF file metadata: '{}'='{}' does not match '{}'='{}'",
            METADATA_KEY_VERSION,
            alias,
            METADATA_KEY_RPF_VERSION,
            version
        );
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
    if options.include_protection_contingencies {
        table_schemas.extend(protection_table_schemas(options.include_topology_changes));
    }
    if options.include_computational_load_profiles {
        table_schemas.extend(computational_load_table_schemas());
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
                    // v0.9.5: 24-column `generators` files omit trailing `controlled_bus_id`; synthesize
                    // local regulation as `0` (same semantics as optional_if_short_struct in C++ readers).
                    if table_name == TABLE_GENERATORS
                        && expected_field.name() == "controlled_bus_id"
                        && expected_field.data_type() == &DataType::Int32
                    {
                        trimmed_columns
                            .push(Arc::new(Int32Array::from_value(0, expected_rows)) as ArrayRef);
                        continue;
                    }
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

    if options.include_topology_changes && !options.include_protection_contingencies {
        bail!(
            "invalid RootWriteOptions: include_topology_changes=true requires include_protection_contingencies=true"
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
    if options.include_computational_load_profiles {
        root_metadata.insert(
            METADATA_KEY_FEATURE_COMPUTATIONAL_LOAD_PROFILES.to_string(),
            "true".to_string(),
        );
    }
    if options.include_protection_contingencies {
        root_metadata.insert(
            METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES.to_string(),
            "true".to_string(),
        );
        // Default declared fidelity; callers may override via additional_root_metadata.
        root_metadata
            .entry(METADATA_KEY_PROTECTION_FIDELITY.to_string())
            .or_insert_with(|| "logical".to_string());
    }
    if options.include_topology_changes {
        root_metadata.insert(
            METADATA_KEY_FEATURE_TOPOLOGY_CHANGES.to_string(),
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
    if branding.contains("Raptrix PowerFlow")
        || !branding.contains("Copyright (c) 2026 Raptrix Power")
    {
        bail!(
            "post-write contract violation: raptrix.branding must identify Raptrix Power (not legacy PowerFlow branding)"
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

    let multi_section = by_name
        .get(TABLE_MULTI_SECTION_LINES)
        .context("post-write contract violation: missing multi_section_lines table")?;
    require_non_null_count_equals_len(TABLE_MULTI_SECTION_LINES, multi_section, "line_id")?;
    require_non_null_count_equals_len(TABLE_MULTI_SECTION_LINES, multi_section, "from_bus_id")?;
    require_non_null_count_equals_len(TABLE_MULTI_SECTION_LINES, multi_section, "to_bus_id")?;

    let dc_lines = by_name
        .get(TABLE_DC_LINES_2W)
        .context("post-write contract violation: missing dc_lines_2w table")?;
    require_non_null_count_equals_len(TABLE_DC_LINES_2W, dc_lines, "dc_line_id")?;
    require_non_null_count_equals_len(TABLE_DC_LINES_2W, dc_lines, "from_bus_id")?;
    require_non_null_count_equals_len(TABLE_DC_LINES_2W, dc_lines, "to_bus_id")?;

    let generators = by_name
        .get(TABLE_GENERATORS)
        .context("post-write contract violation: missing generators table")?;
    require_non_null_count_equals_len(TABLE_GENERATORS, generators, "generator_id")?;
    require_non_null_count_equals_len(TABLE_GENERATORS, generators, "bus_id")?;
    require_non_null_count_equals_len(TABLE_GENERATORS, generators, "controlled_bus_id")?;

    let loads = by_name
        .get(TABLE_LOADS)
        .context("post-write contract violation: missing loads table")?;
    require_non_null_count_equals_len(TABLE_LOADS, loads, "bus_id")?;
    require_non_null_count_equals_len(TABLE_LOADS, loads, "id")?;

    let switched_shunt_banks = by_name
        .get(TABLE_SWITCHED_SHUNT_BANKS)
        .context("post-write contract violation: missing switched_shunt_banks table")?;
    require_non_null_count_equals_len(
        TABLE_SWITCHED_SHUNT_BANKS,
        switched_shunt_banks,
        "shunt_id",
    )?;

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

    if options.include_computational_load_profiles {
        by_name
            .get(TABLE_COMPUTATIONAL_LOAD_PROFILES)
            .context("post-write contract violation: missing computational_load_profiles table")?;
    }

    if options.include_protection_contingencies {
        let feature = metadata
            .get(METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES)
            .with_context(|| {
                format!(
                    "post-write contract violation: missing metadata key '{}'",
                    METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES
                )
            })?;
        if feature != "true" {
            bail!(
                "post-write contract violation: '{}' expected 'true', found '{}'",
                METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES,
                feature
            );
        }

        let protection = by_name
            .get(TABLE_PROTECTION_CONTINGENCIES)
            .context("post-write contract violation: missing protection_contingencies table")?;
        require_non_null_count_equals_len(
            TABLE_PROTECTION_CONTINGENCIES,
            protection,
            "contingency_id",
        )?;
        require_non_null_count_equals_len(
            TABLE_PROTECTION_CONTINGENCIES,
            protection,
            "protection_group_id",
        )?;
        require_non_null_count_equals_len(TABLE_PROTECTION_CONTINGENCIES, protection, "scheme_type")?;
        require_non_null_count_equals_len(
            TABLE_PROTECTION_CONTINGENCIES,
            protection,
            "tripped_elements",
        )?;
        require_non_null_count_equals_len(
            TABLE_PROTECTION_CONTINGENCIES,
            protection,
            "data_confidence",
        )?;

        if options.include_topology_changes {
            let topology = by_name
                .get(TABLE_TOPOLOGY_CHANGES)
                .context("post-write contract violation: missing topology_changes table")?;
            require_non_null_count_equals_len(
                TABLE_TOPOLOGY_CHANGES,
                topology,
                "topology_change_id",
            )?;
            require_non_null_count_equals_len(TABLE_TOPOLOGY_CHANGES, topology, "change_type")?;
            require_non_null_count_equals_len(
                TABLE_TOPOLOGY_CHANGES,
                topology,
                "affected_bus_ids",
            )?;

            validate_topology_change_fk(protection, topology)?;
        }
    }

    Ok(())
}

/// Verifies every non-null `protection_contingencies.topology_change_id` resolves to a
/// `topology_changes.topology_change_id` (referential integrity for the v0.11.0 join).
fn validate_topology_change_fk(protection: &RecordBatch, topology: &RecordBatch) -> Result<()> {
    use std::collections::HashSet;

    let topo_ids = topology
        .column(
            topology
                .schema()
                .index_of("topology_change_id")
                .context("topology_changes missing topology_change_id")?,
        )
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("topology_changes.topology_change_id must be Int32")?;
    let known: HashSet<i32> = (0..topo_ids.len()).map(|i| topo_ids.value(i)).collect();

    let fk = protection
        .column(
            protection
                .schema()
                .index_of("topology_change_id")
                .context("protection_contingencies missing topology_change_id")?,
        )
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("protection_contingencies.topology_change_id must be Int32")?;

    for row in 0..fk.len() {
        if fk.is_null(row) {
            continue;
        }
        let id = fk.value(row);
        if !known.contains(&id) {
            bail!(
                "post-write contract violation: protection_contingencies.topology_change_id={id} \
                 has no matching topology_changes.topology_change_id"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use arrow::array::StringDictionaryBuilder;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, StringArray,
        StructArray, new_null_array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::datatypes::{Int32Type, UInt32Type};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use crate::schema::{
        METADATA_KEY_FEATURE_COMPUTATIONAL_LOAD_PROFILES,
        METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES, METADATA_KEY_FEATURE_TOPOLOGY_CHANGES,
        METADATA_KEY_PROTECTION_FIDELITY, METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION,
        SCHEMA_VERSION, TABLE_BRANCHES, TABLE_COMPUTATIONAL_LOAD_PROFILES, TABLE_DIAGRAM_OBJECTS,
        TABLE_DIAGRAM_POINTS, TABLE_FACTS_DEVICES, TABLE_FACTS_SOLVED, TABLE_GENERATORS,
        TABLE_LOADS, TABLE_PROTECTION_CONTINGENCIES, TABLE_TOPOLOGY_CHANGES, all_table_schemas,
        branches_schema, computational_load_profiles_schema, diagram_objects_schema,
        diagram_points_schema, facts_devices_schema, facts_solved_schema, generators_schema,
        loads_schema, protection_contingencies_schema, schema_metadata, topology_changes_schema,
    };

    use super::{
        RootWriteOptions, read_rpf_tables, row_count_metadata_key, rpf_file_metadata,
        write_root_rpf,
    };

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
                include_computational_load_profiles: false,
                include_protection_contingencies: false,
                include_topology_changes: false,
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
    fn computational_load_profiles_optional_round_trip() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_clp_roundtrip");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("clp.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();
        table_batches.insert(
            TABLE_COMPUTATIONAL_LOAD_PROFILES,
            RecordBatch::new_empty(Arc::new(computational_load_profiles_schema())),
        );

        write_root_rpf(
            &output_path,
            &table_batches,
            &RootWriteOptions {
                include_computational_load_profiles: true,
                ..Default::default()
            },
        )?;

        let tables = read_rpf_tables(&output_path)?;
        assert!(
            tables
                .iter()
                .any(|(name, _)| name == TABLE_COMPUTATIONAL_LOAD_PROFILES)
        );
        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get(METADATA_KEY_FEATURE_COMPUTATIONAL_LOAD_PROFILES),
            Some(&"true".to_string())
        );
        Ok(())
    }

    #[test]
    fn read_rejects_branches_schema_missing_required_nominal_kv_columns() -> Result<()> {
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
        root_meta.insert(METADATA_KEY_VERSION.to_string(), SCHEMA_VERSION.to_string());
        root_meta.insert(
            METADATA_KEY_RPF_VERSION.to_string(),
            SCHEMA_VERSION.to_string(),
        );
        for (name, _) in all_table_schemas() {
            root_meta.insert(row_count_metadata_key(name), "0".to_string());
        }
        let root_schema = Arc::new(Schema::new_with_metadata(root_fields, root_meta));
        let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)?;

        let mut out = File::create(&output_path)?;
        let mut writer = FileWriter::try_new(&mut out, &root_schema)?;
        writer.write_metadata(METADATA_KEY_VERSION, SCHEMA_VERSION);
        writer.write_metadata(METADATA_KEY_RPF_VERSION, SCHEMA_VERSION);
        writer.write(&root_batch)?;
        writer.finish()?;

        let err = read_rpf_tables(&output_path)
            .expect_err("v0.9.3 reader should reject missing required nominal_kv fields");
        let message = format!("{err:#}");
        assert!(message.contains("missing non-nullable field 'to_nominal_kv'"));
        assert_eq!(SCHEMA_VERSION, "v0.11.0");
        Ok(())
    }

    #[test]
    fn read_supports_older_loads_schema_with_missing_zip_columns() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_backward_loads_read");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("v090_like_loads.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let old_load_fields: Vec<Field> = loads_schema().fields()[0..6]
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let old_loads_schema = Schema::new_with_metadata(old_load_fields, schema_metadata());
        table_batches.insert(
            TABLE_LOADS,
            RecordBatch::new_empty(Arc::new(old_loads_schema.clone())),
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
        root_meta.insert(METADATA_KEY_VERSION.to_string(), SCHEMA_VERSION.to_string());
        root_meta.insert(
            METADATA_KEY_RPF_VERSION.to_string(),
            SCHEMA_VERSION.to_string(),
        );
        for (name, _) in all_table_schemas() {
            root_meta.insert(row_count_metadata_key(name), "0".to_string());
        }
        let root_schema = Arc::new(Schema::new_with_metadata(root_fields, root_meta));
        let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)?;

        let mut out = File::create(&output_path)?;
        let mut writer = FileWriter::try_new(&mut out, &root_schema)?;
        writer.write_metadata(METADATA_KEY_VERSION, SCHEMA_VERSION);
        writer.write_metadata(METADATA_KEY_RPF_VERSION, SCHEMA_VERSION);
        writer.write(&root_batch)?;
        writer.finish()?;

        let tables = read_rpf_tables(&output_path)?;
        let (_, loads) = tables
            .iter()
            .find(|(name, _)| name == TABLE_LOADS)
            .context("missing loads table")?;
        assert_eq!(loads.schema().fields().len(), loads_schema().fields().len());
        assert_eq!(loads.column(5).null_count(), 0);
        assert_eq!(loads.column(6).null_count(), 0);
        assert_eq!(loads.column(7).null_count(), 0);
        assert_eq!(loads.column(8).null_count(), 0);
        Ok(())
    }

    #[test]
    fn read_supports_v094_generators_missing_controlled_bus_id() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_backward_generators_read");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("v094_like_generators.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let full = generators_schema();
        let old_gen_fields: Vec<Field> = full.fields()[0..24]
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let old_generators_schema = Schema::new_with_metadata(old_gen_fields, schema_metadata());
        table_batches.insert(
            TABLE_GENERATORS,
            RecordBatch::new_empty(Arc::new(old_generators_schema)),
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
        root_meta.insert(METADATA_KEY_VERSION.to_string(), SCHEMA_VERSION.to_string());
        root_meta.insert(
            METADATA_KEY_RPF_VERSION.to_string(),
            SCHEMA_VERSION.to_string(),
        );
        for (name, _) in all_table_schemas() {
            root_meta.insert(row_count_metadata_key(name), "0".to_string());
        }
        let root_schema = Arc::new(Schema::new_with_metadata(root_fields, root_meta));
        let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)?;

        let mut out = File::create(&output_path)?;
        let mut writer = FileWriter::try_new(&mut out, &root_schema)?;
        writer.write_metadata(METADATA_KEY_VERSION, SCHEMA_VERSION);
        writer.write_metadata(METADATA_KEY_RPF_VERSION, SCHEMA_VERSION);
        writer.write(&root_batch)?;
        writer.finish()?;

        let tables = read_rpf_tables(&output_path)?;
        let (_, generators) = tables
            .iter()
            .find(|(name, _)| name == TABLE_GENERATORS)
            .context("missing generators table")?;
        assert_eq!(generators.schema().fields().len(), 25);
        let controlled = generators
            .column(24)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("controlled_bus_id must be Int32")?;
        assert_eq!(controlled.len(), 0);
        Ok(())
    }

    #[test]
    fn loads_zip_columns_round_trip_when_populated() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_loads_zip_round_trip");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("loads_zip_round_trip.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
        id_b.append("L1")?;
        id_b.append("L2")?;
        let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
        name_b.append("ZIP A")?;
        name_b.append("ZIP B")?;

        let loads_batch = RecordBatch::try_new(
            Arc::new(loads_schema()),
            vec![
                Arc::new(Int32Array::from(vec![101, 102])) as ArrayRef,
                Arc::new(id_b.finish()) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.2, 2.4])) as ArrayRef,
                Arc::new(Float64Array::from(vec![0.2, -0.1])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(0.5), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(0.1), Some(0.0)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(0.03), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(-0.02), Some(0.04)])) as ArrayRef,
                Arc::new(name_b.finish()) as ArrayRef,
            ],
        )?;
        table_batches.insert(TABLE_LOADS, loads_batch);

        write_root_rpf(&output_path, &table_batches, &RootWriteOptions::default())?;

        let tables = read_rpf_tables(&output_path)?;
        let loads = tables
            .iter()
            .find(|(name, _)| name == TABLE_LOADS)
            .map(|(_, batch)| batch)
            .context("expected loads table")?;
        assert_eq!(loads.num_rows(), 2);

        let p_i = loads
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("loads.p_i_pu must be Float64")?;
        let q_i = loads
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("loads.q_i_pu must be Float64")?;
        let p_y = loads
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("loads.p_y_pu must be Float64")?;
        let q_y = loads
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("loads.q_y_pu must be Float64")?;
        assert_eq!(p_i.value(0), 0.5);
        assert!(p_i.is_null(1));
        assert_eq!(q_i.value(0), 0.1);
        assert_eq!(p_y.value(0), 0.03);
        assert_eq!(q_y.value(1), 0.04);
        Ok(())
    }

    /// Builds a length-1 non-null array holding a single empty list, for a `List<...>` type.
    fn one_empty_list(list_type: &DataType) -> ArrayRef {
        use arrow::array::{ListArray, new_empty_array};
        use arrow::buffer::OffsetBuffer;
        let DataType::List(field) = list_type else {
            panic!("expected a List data type");
        };
        let child = new_empty_array(field.data_type());
        Arc::new(ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([0usize]),
            child,
            None,
        )) as ArrayRef
    }

    /// Builds a length-1 `Dictionary<Int32, Utf8>` array holding one value.
    fn one_dict(value: &str) -> ArrayRef {
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        b.append(value).expect("append dict value");
        Arc::new(b.finish()) as ArrayRef
    }

    #[test]
    fn protection_tables_absent_when_not_enabled() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_protection_absent");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("protection_absent.rpf");

        let table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        // Default options produce a v0.10.0-shaped file with neither optional table; it must
        // still read cleanly under the v0.11.0 reader (backward-read of additive contract).
        write_root_rpf(&output_path, &table_batches, &RootWriteOptions::default())?;
        let tables = read_rpf_tables(&output_path)?;
        assert!(
            !tables
                .iter()
                .any(|(name, _)| name == TABLE_PROTECTION_CONTINGENCIES)
        );
        assert!(!tables.iter().any(|(name, _)| name == TABLE_TOPOLOGY_CHANGES));
        Ok(())
    }

    #[test]
    fn topology_changes_requires_protection_contingencies() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_topo_requires_protection");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("topo_requires_protection.rpf");

        let table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        let err = write_root_rpf(
            &output_path,
            &table_batches,
            &RootWriteOptions {
                include_topology_changes: true,
                ..Default::default()
            },
        )
        .expect_err("topology_changes without protection_contingencies must error");
        assert!(
            format!("{err:#}").contains("requires include_protection_contingencies=true"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn protection_tables_round_trip_when_enabled() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_protection_present");
        std::fs::create_dir_all(&tmp_dir)?;
        let output_path = tmp_dir.join("protection_present.rpf");

        let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
            .into_iter()
            .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
            .collect();

        // One protection row referencing topology_change_id = 7.
        let pc_schema = protection_contingencies_schema();
        let protection = RecordBatch::try_new(
            Arc::new(pc_schema.clone()),
            vec![
                one_dict("BF_BUS47"),                                            // contingency_id
                one_dict("BUS47_BF_ZONE"),                                       // protection_group_id
                Arc::new(StringArray::from(vec![Some("Bus 47 BF backup")])) as _, // name
                one_dict("breaker_failure"),                                     // scheme_type
                one_dict("branch"),                                              // initiating_equipment_kind
                one_dict("1023"),                                                // initiating_equipment_id
                one_empty_list(pc_schema.field(6).data_type()),                  // tripped_elements
                new_null_array(pc_schema.field(7).data_type(), 1),               // sequence
                Arc::new(Int32Array::from(vec![Some(7)])) as _,                  // topology_change_id
                one_dict("inferred"),                                            // data_confidence
                new_null_array(pc_schema.field(10).data_type(), 1),              // breaker_ids
                new_null_array(pc_schema.field(11).data_type(), 1),              // params
            ],
        )?;
        table_batches.insert(TABLE_PROTECTION_CONTINGENCIES, protection);

        // One matching topology_changes row, id = 7.
        let tc_schema = topology_changes_schema();
        let topology = RecordBatch::try_new(
            Arc::new(tc_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![7])) as _,            // topology_change_id
                one_dict("BF_BUS47"),                                // contingency_id
                one_dict("bus_split"),                               // change_type
                one_empty_list(tc_schema.field(3).data_type()),      // affected_bus_ids
                new_null_array(tc_schema.field(4).data_type(), 1),   // resulting_islands
                Arc::new(Int32Array::from(vec![Some(1)])) as _,      // isolated_element_count
                Arc::new(StringArray::from(vec![Some("section cleared")])) as _, // summary
                one_dict("declared"),                                // provenance
                new_null_array(tc_schema.field(8).data_type(), 1),   // params
            ],
        )?;
        table_batches.insert(TABLE_TOPOLOGY_CHANGES, topology);

        write_root_rpf(
            &output_path,
            &table_batches,
            &RootWriteOptions {
                include_protection_contingencies: true,
                include_topology_changes: true,
                ..Default::default()
            },
        )?;

        let tables = read_rpf_tables(&output_path)?;
        let protection = tables
            .iter()
            .find(|(name, _)| name == TABLE_PROTECTION_CONTINGENCIES)
            .map(|(_, batch)| batch)
            .context("expected protection_contingencies table")?;
        let topology = tables
            .iter()
            .find(|(name, _)| name == TABLE_TOPOLOGY_CHANGES)
            .map(|(_, batch)| batch)
            .context("expected topology_changes table")?;
        assert_eq!(protection.num_rows(), 1);
        assert_eq!(topology.num_rows(), 1);

        let fk = protection
            .column(8)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("topology_change_id must be Int32")?;
        assert_eq!(fk.value(0), 7);

        let metadata = rpf_file_metadata(&output_path)?;
        assert_eq!(
            metadata.get(METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES),
            Some(&"true".to_string())
        );
        assert_eq!(
            metadata.get(METADATA_KEY_FEATURE_TOPOLOGY_CHANGES),
            Some(&"true".to_string())
        );
        assert_eq!(
            metadata.get(METADATA_KEY_PROTECTION_FIDELITY),
            Some(&"logical".to_string())
        );
        Ok(())
    }
}
