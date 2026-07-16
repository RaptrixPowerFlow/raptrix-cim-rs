/*
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
*/

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Lossless patch re-export: converter-owned tables survive solve patches.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int32Array, StringArray,
    StringDictionaryBuilder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;

use raptrix_cim_arrow::{
    RootWriteOptions, TABLE_BUSES, TABLE_BUSES_SOLVED, TABLE_DIAGRAM_OBJECTS, TABLE_DIAGRAM_POINTS,
    TABLE_GENERATORS_SOLVED, TABLE_SWITCHED_SHUNTS_SOLVED, all_table_schemas, apply_rpf_patch,
    buses_schema, buses_solved_schema, diagram_objects_schema, diagram_points_schema,
    generators_solved_schema, read_rpf_tables, switched_shunts_solved_schema, write_root_rpf,
};

fn empty_canonical_batches() -> HashMap<&'static str, RecordBatch> {
    all_table_schemas()
        .into_iter()
        .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
        .collect()
}

fn one_bus_batch(lat: Option<f64>, lon: Option<f64>) -> Result<RecordBatch> {
    let mut name = StringDictionaryBuilder::<Int32Type>::new();
    name.append("NORTH")?;
    let mut uuid = StringDictionaryBuilder::<Int32Type>::new();
    uuid.append("bus-uuid-1")?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(name.finish()),
        Arc::new(Int8Array::from(vec![1])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![1.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![-9999.0])),
        Arc::new(Float64Array::from(vec![9999.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![Some(1)])),
        Arc::new(Float64Array::from(vec![0.9])),
        Arc::new(Float64Array::from(vec![1.1])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![230.0])),
        Arc::new(uuid.finish()),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![0.0])),
        Arc::new(Float64Array::from(vec![lat])),
        Arc::new(Float64Array::from(vec![lon])),
    ];
    Ok(RecordBatch::try_new(Arc::new(buses_schema()), columns)?)
}

fn diagram_batches() -> Result<(RecordBatch, RecordBatch)> {
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
            Arc::new(StringArray::from(vec!["bus:1"])) as _,
            Arc::new(StringArray::from(vec!["overview"])) as _,
            Arc::new(Int32Array::from(vec![0])) as _,
            Arc::new(Float64Array::from(vec![10.0])) as _,
            Arc::new(Float64Array::from(vec![30.0])) as _,
        ],
    )?;
    Ok((objects, points))
}

#[test]
fn patch_preserves_converter_gis_and_diagram_takes_solved_from_patch() -> Result<()> {
    let tmp = std::env::temp_dir().join("raptrix_cim_arrow_apply_patch");
    std::fs::create_dir_all(&tmp)?;
    let source_path = tmp.join("source_rich.rpf");
    let patch_path = tmp.join("patch_lossy.rpf");
    let output_path = tmp.join("merged.rpf");

    let (objects, points) = diagram_batches()?;
    let mut source_batches = empty_canonical_batches();
    source_batches.insert(TABLE_BUSES, one_bus_batch(Some(32.85), Some(-97.75))?);
    source_batches.insert(TABLE_DIAGRAM_OBJECTS, objects);
    source_batches.insert(TABLE_DIAGRAM_POINTS, points);
    write_root_rpf(
        &source_path,
        &source_batches,
        &RootWriteOptions {
            include_diagram_layout: true,
            ..Default::default()
        },
    )?;

    // Simulate a core full rebuild: GIS wiped, no diagram, but solved table present.
    let mut patch_batches = empty_canonical_batches();
    patch_batches.insert(TABLE_BUSES, one_bus_batch(None, None)?);
    patch_batches.insert(
        TABLE_BUSES_SOLVED,
        RecordBatch::try_new(
            Arc::new(buses_solved_schema()),
            {
                let schema = buses_solved_schema();
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if field.name() == "bus_id" {
                            Arc::new(Int32Array::from(vec![1])) as ArrayRef
                        } else if field.data_type() == &arrow::datatypes::DataType::Float64 {
                            Arc::new(Float64Array::from(vec![Some(1.02)])) as ArrayRef
                        } else {
                            arrow::array::new_null_array(field.data_type(), 1)
                        }
                    })
                    .collect()
            },
        )?,
    );
    patch_batches.insert(
        TABLE_GENERATORS_SOLVED,
        RecordBatch::new_empty(Arc::new(generators_solved_schema())),
    );
    patch_batches.insert(
        TABLE_SWITCHED_SHUNTS_SOLVED,
        RecordBatch::new_empty(Arc::new(switched_shunts_solved_schema())),
    );
    write_root_rpf(
        &patch_path,
        &patch_batches,
        &RootWriteOptions {
            include_solved_state: true,
            ..Default::default()
        },
    )?;

    apply_rpf_patch(&source_path, &patch_path, &output_path)?;

    let tables = read_rpf_tables(&output_path)?;
    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .context("buses missing")?;
    let lat = buses
        .column_by_name("latitude")
        .context("latitude missing")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("latitude type")?;
    let lon = buses
        .column_by_name("longitude")
        .context("longitude missing")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("longitude type")?;
    assert!((lat.value(0) - 32.85).abs() < 1e-9, "GIS latitude must come from source");
    assert!((lon.value(0) - (-97.75)).abs() < 1e-9, "GIS longitude must come from source");

    let diagram_objects = tables
        .iter()
        .find(|(name, _)| name == TABLE_DIAGRAM_OBJECTS)
        .map(|(_, batch)| batch)
        .context("diagram_objects must passthrough from source")?;
    assert_eq!(diagram_objects.num_rows(), 1);

    let buses_solved = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES_SOLVED)
        .map(|(_, batch)| batch)
        .context("buses_solved must come from patch")?;
    assert_eq!(buses_solved.num_rows(), 1);

    Ok(())
}

#[test]
fn empty_solver_patch_is_lossless_for_converter_tables() -> Result<()> {
    let tmp = std::env::temp_dir().join("raptrix_cim_arrow_apply_patch_empty");
    std::fs::create_dir_all(&tmp)?;
    let source_path = tmp.join("source.rpf");
    let patch_path = tmp.join("patch_same_structural.rpf");
    let output_path = tmp.join("out.rpf");

    let (objects, points) = diagram_batches()?;
    let mut source_batches = empty_canonical_batches();
    source_batches.insert(TABLE_BUSES, one_bus_batch(Some(53.23), Some(-4.35))?);
    source_batches.insert(TABLE_DIAGRAM_OBJECTS, objects.clone());
    source_batches.insert(TABLE_DIAGRAM_POINTS, points.clone());
    write_root_rpf(
        &source_path,
        &source_batches,
        &RootWriteOptions {
            include_diagram_layout: true,
            ..Default::default()
        },
    )?;

    // Patch is a structural rebuild with GIS wiped — still must not win for converter tables.
    let mut patch_batches = empty_canonical_batches();
    patch_batches.insert(TABLE_BUSES, one_bus_batch(None, None)?);
    write_root_rpf(&patch_path, &patch_batches, &RootWriteOptions::default())?;

    apply_rpf_patch(&source_path, &patch_path, &output_path)?;

    let source_tables: HashMap<_, _> = read_rpf_tables(&source_path)?.into_iter().collect();
    let out_tables: HashMap<_, _> = read_rpf_tables(&output_path)?.into_iter().collect();

    for name in [
        TABLE_BUSES,
        TABLE_DIAGRAM_OBJECTS,
        TABLE_DIAGRAM_POINTS,
    ] {
        let source_batch = source_tables.get(name).context(name)?;
        let out_batch = out_tables.get(name).context(name)?;
        assert_eq!(
            source_batch.num_rows(),
            out_batch.num_rows(),
            "{name} row count changed"
        );
        assert_eq!(
            source_batch.num_columns(),
            out_batch.num_columns(),
            "{name} column count changed"
        );
        for col in 0..source_batch.num_columns() {
            assert_eq!(
                source_batch.column(col).as_ref(),
                out_batch.column(col).as_ref(),
                "{name} column {col} diverged after empty-ish patch"
            );
        }
    }

    Ok(())
}
