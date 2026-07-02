/*
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
*/

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! v0.12.4 solved-snapshot read-compatibility tests.
//!
//! Current PowerFlow solved-snapshot exports differ from the canonical writer
//! layout in documented, read-tolerated ways:
//!
//! - root tables are ordered differently (extension tables trail the
//!   solved-state tables) — readers match root columns by name;
//! - nested list items carry different field names / nullability;
//! - the `metadata` table ends at `computational_load_mode` (35 columns);
//! - `multi_section_lines`, `dc_lines_2w`, and `switched_shunt_banks` use the
//!   solved-snapshot dialect layouts;
//! - optional `q_limits_solved` and `feasibility_certificate_buses` tables are
//!   appended.
//!
//! The fixture built here is fully synthetic (no real network data).

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, Int32Array, StructArray, new_null_array};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use raptrix_cim_arrow::{
    BRANDING, METADATA_KEY_BRANDING, METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION,
    buses_solved_schema, dc_lines_2w_snapshot_dialect_schema,
    feasibility_certificate_buses_schema, generators_solved_snapshot_dialect_schema,
    metadata_schema,
    multi_section_lines_snapshot_dialect_schema, q_limits_solved_schema, read_rpf_tables,
    row_count_metadata_key, schema_metadata, switched_shunt_banks_snapshot_dialect_schema,
    switched_shunts_solved_schema, table_schema,
};

/// Rewrites a canonical field the way current solved-snapshot exporters emit it:
/// everything nullable, list items named `item` with nullable contents.
fn snapshotize_field(field: &Field) -> Field {
    let data_type = snapshotize_type(field.data_type());
    Field::new(field.name(), data_type, true)
}

fn snapshotize_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(item) => DataType::List(Arc::new(Field::new(
            "item",
            snapshotize_type(item.data_type()),
            true,
        ))),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(snapshotize_field(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn snapshotize_schema(schema: &Schema) -> Vec<Field> {
    schema.fields().iter().map(|f| snapshotize_field(f)).collect()
}

/// Builds an all-null struct column of `pad_len` rows for the given fields.
fn null_struct(fields: Vec<Field>, pad_len: usize) -> (Field, ArrayRef, Fields) {
    let arrow_fields: Fields = fields.into_iter().map(Arc::new).collect();
    let children: Vec<ArrayRef> = arrow_fields
        .iter()
        .map(|field| new_null_array(field.data_type(), pad_len))
        .collect();
    let array = Arc::new(StructArray::new(arrow_fields.clone(), children, None)) as ArrayRef;
    (
        Field::new("placeholder", DataType::Struct(arrow_fields.clone()), true),
        array,
        arrow_fields,
    )
}

struct FixtureTable {
    name: &'static str,
    fields: Vec<Field>,
    /// Real (non-pad) row count recorded in `rpf.rows.*`.
    rows: usize,
    /// Optional overrides: (column index, values for the real rows).
    overrides: Vec<(usize, ArrayRef)>,
}

fn write_synthetic_solved_snapshot(path: &std::path::Path, version: &str) -> Result<()> {
    let pad_len = 6usize;

    let canonical = |name: &str| -> Vec<Field> {
        snapshotize_schema(&table_schema(name).unwrap_or_else(|| panic!("schema for {name}")))
    };
    // metadata ends at computational_load_mode (35 columns) in snapshot exports.
    let metadata_35: Vec<Field> = snapshotize_schema(&metadata_schema())
        .into_iter()
        .take(35)
        .collect();
    let solved_prefix = |schema: &Schema, count: usize| -> Vec<Field> {
        snapshotize_schema(schema).into_iter().take(count).collect()
    };

    // Root order used by current solved-snapshot exporters: 15 legacy tables,
    // solved-state tables, then extension tables.
    let tables = vec![
        FixtureTable {
            name: "metadata",
            fields: metadata_35,
            rows: 1,
            overrides: vec![],
        },
        FixtureTable {
            name: "buses",
            fields: canonical("buses"),
            rows: 3,
            overrides: vec![(
                0,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            )],
        },
        FixtureTable {
            name: "branches",
            fields: canonical("branches"),
            rows: 2,
            overrides: vec![(
                0,
                Arc::new(Int32Array::from(vec![Some(10), Some(11)])) as ArrayRef,
            )],
        },
        FixtureTable {
            name: "generators",
            fields: canonical("generators"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "loads",
            fields: canonical("loads"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "fixed_shunts",
            fields: canonical("fixed_shunts"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "switched_shunts",
            fields: canonical("switched_shunts"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "transformers_2w",
            fields: canonical("transformers_2w"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "transformers_3w",
            fields: canonical("transformers_3w"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "areas",
            fields: canonical("areas"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "zones",
            fields: canonical("zones"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "owners",
            fields: canonical("owners"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "contingencies",
            // Snapshot exports emit only the 2 base columns with `item`-named
            // nullable list contents.
            fields: canonical("contingencies").into_iter().take(2).collect(),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "interfaces",
            fields: canonical("interfaces"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "dynamics_models",
            fields: canonical("dynamics_models"),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "buses_solved",
            fields: solved_prefix(&buses_solved_schema(), 3),
            rows: 3,
            overrides: vec![(
                0,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            )],
        },
        FixtureTable {
            name: "generators_solved",
            fields: snapshotize_schema(&generators_solved_snapshot_dialect_schema()),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "q_limits_solved",
            fields: snapshotize_schema(&q_limits_solved_schema()),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "switched_shunts_solved",
            fields: solved_prefix(&switched_shunts_solved_schema(), 4),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "multi_section_lines",
            fields: snapshotize_schema(&multi_section_lines_snapshot_dialect_schema()),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "dc_lines_2w",
            fields: snapshotize_schema(&dc_lines_2w_snapshot_dialect_schema()),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "switched_shunt_banks",
            fields: snapshotize_schema(&switched_shunt_banks_snapshot_dialect_schema()),
            rows: 0,
            overrides: vec![],
        },
        FixtureTable {
            name: "feasibility_certificate_buses",
            fields: snapshotize_schema(&feasibility_certificate_buses_schema()),
            rows: 0,
            overrides: vec![],
        },
    ];

    let mut root_fields: Vec<Field> = Vec::new();
    let mut root_columns: Vec<ArrayRef> = Vec::new();
    let mut root_meta: HashMap<String, String> = schema_metadata();
    root_meta.insert(METADATA_KEY_VERSION.to_string(), version.to_string());
    root_meta.insert(METADATA_KEY_RPF_VERSION.to_string(), version.to_string());
    root_meta.insert(METADATA_KEY_BRANDING.to_string(), BRANDING.to_string());
    root_meta.insert("rpf.case_mode".to_string(), "solved_snapshot".to_string());
    root_meta.insert(
        "rpf.solved_state_presence".to_string(),
        "actual_solved".to_string(),
    );

    for table in &tables {
        let (_, _, arrow_fields) = null_struct(table.fields.clone(), pad_len);
        let mut children: Vec<ArrayRef> = arrow_fields
            .iter()
            .map(|field| new_null_array(field.data_type(), pad_len))
            .collect();
        for (column_index, values) in &table.overrides {
            let tail = new_null_array(values.data_type(), pad_len - values.len());
            children[*column_index] = concat(&[values.as_ref(), tail.as_ref()])?;
        }
        let array = Arc::new(StructArray::new(arrow_fields.clone(), children, None)) as ArrayRef;
        root_fields.push(Field::new(
            table.name,
            DataType::Struct(arrow_fields),
            true,
        ));
        root_columns.push(array);
        root_meta.insert(row_count_metadata_key(table.name), table.rows.to_string());
    }

    let root_schema = Arc::new(Schema::new_with_metadata(root_fields, root_meta));
    let root_batch = RecordBatch::try_new(root_schema.clone(), root_columns)?;

    let mut out = File::create(path)?;
    let mut writer = FileWriter::try_new(&mut out, &root_schema)?;
    writer.write_metadata(METADATA_KEY_VERSION, version);
    writer.write_metadata(METADATA_KEY_RPF_VERSION, version);
    writer.write_metadata(METADATA_KEY_BRANDING, BRANDING);
    writer.write(&root_batch)?;
    writer.finish()?;
    Ok(())
}

#[test]
fn reads_synthetic_v0124_solved_snapshot_layout() -> Result<()> {
    let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_v0124_snapshot_read");
    std::fs::create_dir_all(&tmp_dir)?;
    let path = tmp_dir.join("synthetic_v0124_solved_snapshot.rpf");
    write_synthetic_solved_snapshot(&path, "v0.12.4")?;

    let tables = read_rpf_tables(&path)?;
    let by_name: HashMap<&str, &RecordBatch> = tables
        .iter()
        .map(|(name, batch)| (name.as_str(), batch))
        .collect();

    // Row trimming honors rpf.rows.* despite pad rows.
    let buses = by_name.get("buses").context("buses missing")?;
    assert_eq!(buses.num_rows(), 3);
    let bus_id = buses
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("bus_id must be Int32")?;
    assert_eq!(bus_id.values(), &[1, 2, 3]);

    let branches = by_name.get("branches").context("branches missing")?;
    assert_eq!(branches.num_rows(), 2);

    // 35-column snapshot metadata reconstructs to the 45-column canonical shape
    // with null-padded trailing provenance columns.
    let metadata = by_name.get("metadata").context("metadata missing")?;
    assert_eq!(metadata.num_rows(), 1);
    assert_eq!(metadata.schema().fields().len(), 45);
    for index in 35..45 {
        assert_eq!(
            metadata.column(index).null_count(),
            metadata.num_rows(),
            "trailing metadata column {} should be null-padded",
            metadata.schema().field(index).name()
        );
    }

    // Extension tables match the snapshot dialect layouts.
    let msl = by_name
        .get("multi_section_lines")
        .context("multi_section_lines missing")?;
    assert_eq!(msl.schema().fields().len(), 8);
    assert_eq!(msl.schema().field(1).name(), "parent_line_id");

    let dc = by_name.get("dc_lines_2w").context("dc_lines_2w missing")?;
    assert_eq!(dc.schema().fields().len(), 11);
    assert_eq!(dc.schema().field(4).name(), "is_vsc");

    let banks = by_name
        .get("switched_shunt_banks")
        .context("switched_shunt_banks missing")?;
    assert_eq!(banks.schema().fields().len(), 8);
    assert_eq!(banks.schema().field(0).name(), "bank_id");

    // v0.12.4 optional tables are surfaced.
    assert!(by_name.contains_key("q_limits_solved"));
    assert!(by_name.contains_key("feasibility_certificate_buses"));

    // Solved-state tables with short field prefixes null-pad to canonical widths.
    let buses_solved = by_name.get("buses_solved").context("buses_solved missing")?;
    assert_eq!(
        buses_solved.schema().fields().len(),
        buses_solved_schema().fields().len()
    );

    // transformers_2w is structurally present (zero rows in this fixture).
    let t2w = by_name
        .get("transformers_2w")
        .context("transformers_2w missing")?;
    assert_eq!(t2w.num_rows(), 0);

    Ok(())
}

#[test]
fn rejects_unknown_future_version() -> Result<()> {
    let tmp_dir = std::env::temp_dir().join("raptrix_cim_arrow_v0124_snapshot_gate");
    std::fs::create_dir_all(&tmp_dir)?;
    let path = tmp_dir.join("synthetic_future_version.rpf");
    write_synthetic_solved_snapshot(&path, "v0.12.5")?;

    let err = read_rpf_tables(&path).expect_err("future versions must be rejected");
    let message = format!("{err:#}");
    assert!(
        message.contains("unsupported RPF version 'v0.12.5'"),
        "unexpected error: {message}"
    );
    Ok(())
}

/// Optional local check against a real solved-snapshot export. Skipped when no
/// external fixture is available (e.g. in CI); never references case names.
#[test]
fn reads_external_solved_snapshot_when_available() -> Result<()> {
    let Ok(path) = std::env::var("RAPTRIX_SOLVED_SNAPSHOT_RPF") else {
        eprintln!(
            "[test] skipping external solved-snapshot read: RAPTRIX_SOLVED_SNAPSHOT_RPF not set"
        );
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    if !path.exists() {
        eprintln!("[test] skipping external solved-snapshot read: file not found");
        return Ok(());
    }

    let tables = read_rpf_tables(&path)?;
    let names: Vec<&str> = tables.iter().map(|(name, _)| name.as_str()).collect();
    assert!(names.contains(&"buses"));
    assert!(names.contains(&"branches"));
    let metadata = tables
        .iter()
        .find(|(name, _)| name == "metadata")
        .map(|(_, batch)| batch)
        .context("metadata missing")?;
    assert_eq!(metadata.schema().fields().len(), 45);
    Ok(())
}
