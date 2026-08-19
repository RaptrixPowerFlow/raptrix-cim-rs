// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Synthetic v0.14 funnel demo `.rpf` for downstream reader tests.
//! IDs are synthetic only (no CEII).
//!
//! Populates every v0.14 column, every `tpl_category` token (`P1`…`P7` /
//! `unspecified` plus one untagged null), every sequence provenance, every
//! known `element_type`, and the optional protection / topology columns
//! (including `params` and `resulting_islands`). A tiny joinable network
//! (3 buses / 3 branches / 1 gen / 1 load / 1 shunt) is included so readers
//! can resolve FKs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Float64Builder, Int32Array, ListArray, MapBuilder,
    MapFieldNames, StringArray, StringBuilder, StringDictionaryBuilder, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Int32Type, UInt32Type};
use arrow::record_batch::RecordBatch;
use raptrix_cim_arrow::{
    ContingencyElementRow, ContingencyRow, ContingencySequenceRow, HIERARCHY_LEVEL_UNIT,
    RootWriteOptions, SEQUENCE_PROVENANCES, TABLE_BRANCHES, TABLE_BUSES, TABLE_CONTINGENCIES,
    TABLE_CONTINGENCY_SEQUENCES, TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_LOADS, TABLE_METADATA,
    TABLE_PROTECTION_CONTINGENCIES, TABLE_TOPOLOGY_CHANGES, TPL_CATEGORIES, all_table_schemas,
    branches_schema, build_contingencies_batch_full, build_contingency_sequences_batch,
    buses_schema, fixed_shunts_schema, generators_schema, loads_schema, metadata_schema,
    protection_contingencies_schema, read_rpf_tables, rpf_file_metadata, topology_changes_schema,
    write_root_rpf,
};

fn one_dict(value: &str) -> ArrayRef {
    let mut b = StringDictionaryBuilder::<Int32Type>::new();
    b.append(value).expect("append");
    Arc::new(b.finish()) as ArrayRef
}

fn n_dict(values: &[&str]) -> ArrayRef {
    let mut b = StringDictionaryBuilder::<Int32Type>::new();
    for v in values {
        b.append(*v).expect("append");
    }
    Arc::new(b.finish()) as ArrayRef
}

fn n_dict_u32(values: &[&str]) -> ArrayRef {
    let mut b = StringDictionaryBuilder::<UInt32Type>::new();
    for v in values {
        b.append(*v).expect("append");
    }
    Arc::new(b.finish()) as ArrayRef
}

fn one_params_map(key: &str, value: f64) -> Result<ArrayRef> {
    let mut b = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(arrow::datatypes::Field::new(
        "key",
        DataType::Utf8,
        false,
    )))
    .with_values_field(Arc::new(arrow::datatypes::Field::new(
        "value",
        DataType::Float64,
        false,
    )));
    b.keys().append_value(key);
    b.values().append_value(value);
    b.append(true)?;
    Ok(Arc::new(b.finish()) as ArrayRef)
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/data/fixtures")
}

fn protection_batch() -> Result<RecordBatch> {
    let schema = protection_contingencies_schema();
    let tripped = build_contingencies_batch_full(&[ContingencyRow {
        contingency_id: "unused".into(),
        elements: vec![
            ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(1),
                status_change: true,
                ..Default::default()
            },
            ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(3),
                status_change: true,
                ..Default::default()
            },
        ],
        ..Default::default()
    }])?;
    let elements_col = tripped.column_by_name("elements").context("elements")?;

    let seq_type = schema.field(7).data_type().clone();
    let DataType::List(item_field) = &seq_type else {
        anyhow::bail!("sequence must be list");
    };
    let DataType::Struct(step_fields) = item_field.data_type() else {
        anyhow::bail!("sequence item must be struct");
    };
    let mut step_b = StringDictionaryBuilder::<Int32Type>::new();
    step_b.append("branch")?;
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    id_b.append("1")?;
    let step_struct = arrow::array::StructArray::try_new(
        step_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(200.0)])) as ArrayRef,
            Arc::new(step_b.finish()) as ArrayRef,
            Arc::new(id_b.finish()) as ArrayRef,
        ],
        None,
    )?;
    let sequence = ListArray::new(
        item_field.clone(),
        OffsetBuffer::from_lengths([1usize]),
        Arc::new(step_struct),
        None,
    );

    let breaker_type = schema.field(10).data_type().clone();
    let DataType::List(breaker_field) = &breaker_type else {
        anyhow::bail!("breaker_ids must be list");
    };
    let breaker_ids = ListArray::new(
        breaker_field.clone(),
        OffsetBuffer::from_lengths([1usize]),
        Arc::new(StringArray::from(vec!["SW_SYNTH_1"])),
        None,
    );

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            one_dict("BF_BUS47"),
            one_dict("BUS47_BF_ZONE"),
            Arc::new(StringArray::from(vec![Some(
                "Synthetic stuck-breaker demo",
            )])) as _,
            one_dict("stuck_breaker"),
            one_dict("branch"),
            one_dict("1"),
            elements_col.clone(),
            Arc::new(sequence) as _,
            Arc::new(Int32Array::from(vec![Some(7)])) as _,
            one_dict("modeled"),
            Arc::new(breaker_ids) as _,
            one_params_map("backup_delay_ms", 200.0)?,
        ],
    )
    .context("protection batch")
}

fn topology_batch() -> Result<RecordBatch> {
    let schema = topology_changes_schema();
    let affected_type = schema.field(3).data_type().clone();
    let DataType::List(item) = &affected_type else {
        anyhow::bail!("affected_bus_ids must be list");
    };
    let affected = ListArray::new(
        item.clone(),
        OffsetBuffer::from_lengths([1usize]),
        Arc::new(Int32Array::from(vec![47])),
        None,
    );

    let islands_type = schema.field(4).data_type().clone();
    let DataType::List(island_field) = &islands_type else {
        anyhow::bail!("resulting_islands must be list");
    };
    let DataType::Struct(island_fields) = island_field.data_type() else {
        anyhow::bail!("resulting_islands item must be struct");
    };
    let DataType::List(bus_item) = island_fields[1].data_type() else {
        anyhow::bail!("island.bus_ids must be list");
    };
    let island_bus_ids = ListArray::new(
        bus_item.clone(),
        OffsetBuffer::from_lengths([2usize, 1usize]),
        Arc::new(Int32Array::from(vec![47, 20, 10])),
        None,
    );
    let island_struct = arrow::array::StructArray::try_new(
        island_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
            Arc::new(island_bus_ids) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
        ],
        None,
    )?;
    let resulting_islands = ListArray::new(
        island_field.clone(),
        OffsetBuffer::from_lengths([2usize]),
        Arc::new(island_struct),
        None,
    );

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![7])) as _,
            one_dict("BF_BUS47"),
            one_dict("bus_split"),
            Arc::new(affected) as _,
            Arc::new(resulting_islands) as _,
            Arc::new(Int32Array::from(vec![Some(1)])) as _,
            Arc::new(StringArray::from(vec![Some(
                "synthetic BF backup; no CEII",
            )])) as _,
            one_dict("declared"),
            one_params_map("isolated_mw", 12.5)?,
            one_dict("Model_Alignment"),
            one_dict("Planning_Study_Prep"),
        ],
    )
    .context("topology batch")
}

fn demo_contingencies() -> Result<RecordBatch> {
    build_contingencies_batch_full(&[
        ContingencyRow {
            contingency_id: "LINE_1".into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(1),
                status_change: true,
                ..Default::default()
            }],
            tpl_category: Some("P1".into()),
            reserved: Some(false),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "LINE_2".into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(2),
                status_change: true,
                ..Default::default()
            }],
            tpl_category: Some("P1".into()),
            reserved: None,
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "LINE_3".into(),
            elements: vec![ContingencyElementRow {
                element_type: "branch_outage".into(),
                branch_id: Some(3),
                status_change: true,
                ..Default::default()
            }],
            tpl_category: Some("P6".into()),
            reserved: Some(false),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "GEN_1".into(),
            elements: vec![ContingencyElementRow {
                element_type: "generator_trip".into(),
                bus_id: Some(10),
                gen_id: Some("SM_SYNTH_1".into()),
                status_change: true,
                equipment_kind: Some("generator".into()),
                equipment_id: Some("1".into()),
                ..Default::default()
            }],
            tpl_category: Some("P3".into()),
            reserved: Some(false),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "TOWER_L1_L2".into(),
            elements: vec![
                ContingencyElementRow {
                    element_type: "branch_outage".into(),
                    branch_id: Some(1),
                    status_change: true,
                    ..Default::default()
                },
                ContingencyElementRow {
                    element_type: "branch_outage".into(),
                    branch_id: Some(2),
                    status_change: true,
                    ..Default::default()
                },
            ],
            tpl_category: Some("P7".into()),
            reserved: Some(true),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "BF_BUS47".into(),
            elements: vec![ContingencyElementRow {
                element_type: "protection_event".into(),
                bus_id: Some(47),
                status_change: true,
                ..Default::default()
            }],
            risk_score: Some(0.91),
            cleared_by_reserves: Some(false),
            voltage_collapse_flag: Some(false),
            recovery_possible: Some(true),
            recovery_time_min: Some(12.0),
            greedy_reserve_summary: Some("synthetic analysis stamp; not a planning value".into()),
            tpl_category: Some("P4".into()),
            reserved: Some(true),
        },
        ContingencyRow {
            contingency_id: "LOAD_SHED_20".into(),
            elements: vec![ContingencyElementRow {
                element_type: "load_shed".into(),
                bus_id: Some(20),
                load_id: Some("L1".into()),
                amount_mw: Some(25.0),
                status_change: true,
                ..Default::default()
            }],
            tpl_category: Some("unspecified".into()),
            reserved: None,
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "BUS_SEC_47".into(),
            elements: vec![ContingencyElementRow {
                element_type: "split_bus".into(),
                bus_id: Some(47),
                status_change: true,
                equipment_kind: Some("bus".into()),
                equipment_id: Some("47".into()),
                ..Default::default()
            }],
            tpl_category: Some("P2".into()),
            reserved: Some(true),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "DELAYED_L1_L3".into(),
            elements: vec![
                ContingencyElementRow {
                    element_type: "branch_outage".into(),
                    branch_id: Some(1),
                    status_change: true,
                    ..Default::default()
                },
                ContingencyElementRow {
                    element_type: "branch_outage".into(),
                    branch_id: Some(3),
                    status_change: true,
                    ..Default::default()
                },
            ],
            tpl_category: Some("P5".into()),
            reserved: Some(true),
            ..Default::default()
        },
        ContingencyRow {
            contingency_id: "SHUNT_47".into(),
            elements: vec![ContingencyElementRow {
                element_type: "shunt_switch".into(),
                bus_id: Some(47),
                status_change: true,
                equipment_kind: Some("shunt".into()),
                equipment_id: Some("SH1".into()),
                ..Default::default()
            }],
            tpl_category: None,
            reserved: Some(false),
            ..Default::default()
        },
    ])
}

fn demo_sequences() -> Result<RecordBatch> {
    build_contingency_sequences_batch(&[
        ContingencySequenceRow {
            sequence_id: "SEQ_P3_GEN_THEN_LINE".into(),
            primary_contingency_id: "GEN_1".into(),
            secondary_contingency_id: "LINE_2".into(),
            intervening_window_min: Some(30),
            tpl_category: Some("P3".into()),
            provenance: Some("planner_file".into()),
        },
        ContingencySequenceRow {
            sequence_id: "SEQ_P6_LINE_THEN_LINE".into(),
            primary_contingency_id: "LINE_1".into(),
            secondary_contingency_id: "LINE_2".into(),
            intervening_window_min: Some(20),
            tpl_category: Some("P6".into()),
            provenance: Some("autonomous".into()),
        },
        ContingencySequenceRow {
            sequence_id: "SEQ_P6_EMS".into(),
            primary_contingency_id: "LINE_1".into(),
            secondary_contingency_id: "LINE_3".into(),
            intervening_window_min: Some(15),
            tpl_category: Some("P6".into()),
            provenance: Some("ems_export".into()),
        },
        ContingencySequenceRow {
            sequence_id: "SEQ_P3_RPF".into(),
            primary_contingency_id: "GEN_1".into(),
            secondary_contingency_id: "LINE_3".into(),
            intervening_window_min: None,
            tpl_category: Some("P3".into()),
            provenance: Some("rpf".into()),
        },
    ])
}

fn utc_ts() -> ArrayRef {
    Arc::new(
        arrow::array::TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000i64)])
            .with_timezone(std::sync::Arc::<str>::from("UTC")),
    ) as ArrayRef
}

fn metadata_batch() -> Result<RecordBatch> {
    let schema = metadata_schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col: ArrayRef = match field.name().as_str() {
            "base_mva" => Arc::new(Float64Array::from(vec![100.0])) as _,
            "frequency_hz" => Arc::new(Float64Array::from(vec![60.0])) as _,
            "study_name" => one_dict("v014_funnel_demo"),
            "timestamp_utc" | "snapshot_timestamp_utc" => utc_ts(),
            "raptrix_version" => {
                Arc::new(StringArray::from(vec![raptrix_cim_arrow::RPF_VERSION])) as _
            }
            "is_planning_case" => Arc::new(BooleanArray::from(vec![true])) as _,
            "source_case_id" => one_dict("synthetic"),
            "case_fingerprint" => Arc::new(StringArray::from(vec!["v014_funnel_demo"])) as _,
            "validation_mode" => one_dict("converter_export"),
            "case_mode" => one_dict("flat_start_planning"),
            "modern_grid_profile" | "has_ibr" | "has_smart_valve" | "has_multi_terminal_dc" => {
                Arc::new(BooleanArray::from(vec![false])) as _
            }
            _ => new_null_array(field.data_type(), 1),
        };
        cols.push(col);
    }
    RecordBatch::try_new(Arc::new(schema), cols).context("metadata batch")
}

fn buses_batch() -> Result<RecordBatch> {
    let schema = buses_schema();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 47])) as _,
            n_dict(&["BUS_10", "BUS_20", "BUS_47"]),
            n_dict(&["Slack", "PQ", "PQ"]),
            Arc::new(Float64Array::from(vec![0.5, -0.25, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, -0.05, 0.0])) as _,
            Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![-0.3, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.3, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.05])) as _,
            Arc::new(Int32Array::from(vec![1, 1, 1])) as _,
            Arc::new(Int32Array::from(vec![1, 1, 1])) as _,
            new_null_array(&DataType::Int32, 3),
            Arc::new(Float64Array::from(vec![0.95, 0.95, 0.95])) as _,
            Arc::new(Float64Array::from(vec![1.05, 1.05, 1.05])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![138.0, 138.0, 138.0])) as _,
            n_dict(&["uuid-bus-10", "uuid-bus-20", "uuid-bus-47"]),
            Arc::new(Float64Array::from(vec![0.0, 0.05, 0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            new_null_array(&DataType::Float64, 3),
            new_null_array(&DataType::Float64, 3),
        ],
    )
    .context("buses batch")
}

fn branches_batch() -> Result<RecordBatch> {
    let schema = branches_schema();
    let n = 3;
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            Arc::new(Int32Array::from(vec![10, 20, 47])) as _,
            Arc::new(Int32Array::from(vec![20, 47, 10])) as _,
            n_dict(&["1", "1", "1"]),
            Arc::new(Float64Array::from(vec![0.01, 0.012, 0.011])) as _,
            Arc::new(Float64Array::from(vec![0.1, 0.11, 0.105])) as _,
            Arc::new(Float64Array::from(vec![0.02, 0.02, 0.02])) as _,
            Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0])) as _,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as _,
            Arc::new(Float64Array::from(vec![100.0, 100.0, 100.0])) as _,
            Arc::new(Float64Array::from(vec![110.0, 110.0, 110.0])) as _,
            Arc::new(Float64Array::from(vec![120.0, 120.0, 120.0])) as _,
            Arc::new(BooleanArray::from(vec![true, true, true])) as _,
            new_null_array(&DataType::Int32, n),
            n_dict_u32(&["L1", "L2", "L3"]),
            Arc::new(Float64Array::from(vec![138.0, 138.0, 138.0])) as _,
            Arc::new(Float64Array::from(vec![138.0, 138.0, 138.0])) as _,
            new_null_array(schema.field(17).data_type(), n),
            new_null_array(schema.field(18).data_type(), n),
            new_null_array(&DataType::Float64, n),
            new_null_array(&DataType::Float64, n),
            new_null_array(&DataType::Float64, n),
            new_null_array(&DataType::Float64, n),
            new_null_array(&DataType::Float64, n),
            new_null_array(schema.field(24).data_type(), n),
            new_null_array(&DataType::Int32, n),
            new_null_array(&DataType::Int32, n),
            new_null_array(&DataType::Utf8, n),
            new_null_array(&DataType::Boolean, n),
            new_null_array(&DataType::Boolean, n),
            new_null_array(&DataType::Boolean, n),
            new_null_array(&DataType::Boolean, n),
        ],
    )
    .context("branches batch")
}

fn generators_batch() -> Result<RecordBatch> {
    let schema = generators_schema();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1])) as _,
            Arc::new(Int32Array::from(vec![10])) as _,
            Arc::new(StringArray::from(vec![Some("Synth Unit 1")])) as _,
            Arc::new(StringArray::from(vec!["SYNC"])) as _,
            Arc::new(StringArray::from(vec![HIERARCHY_LEVEL_UNIT])) as _,
            new_null_array(&DataType::Int32, 1),
            new_null_array(&DataType::Int32, 1),
            Arc::new(BooleanArray::from(vec![true])) as _,
            Arc::new(BooleanArray::from(vec![false])) as _,
            new_null_array(&DataType::Utf8, 1),
            Arc::new(Float64Array::from(vec![50.0])) as _,
            Arc::new(Float64Array::from(vec![0.0])) as _,
            Arc::new(Float64Array::from(vec![0.0])) as _,
            Arc::new(Float64Array::from(vec![80.0])) as _,
            Arc::new(Float64Array::from(vec![-30.0])) as _,
            Arc::new(Float64Array::from(vec![30.0])) as _,
            Arc::new(Float64Array::from(vec![100.0])) as _,
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Int32, 1),
            new_null_array(&DataType::Utf8, 1),
            new_null_array(schema.field(23).data_type(), 1),
            new_null_array(&DataType::Int32, 1),
            Arc::new(StringArray::from(vec![Some("SM_SYNTH_1")])) as _,
        ],
    )
    .context("generators batch")
}

fn loads_batch() -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(loads_schema()),
        vec![
            Arc::new(Int32Array::from(vec![20])) as _,
            one_dict("L1"),
            Arc::new(BooleanArray::from(vec![true])) as _,
            Arc::new(Float64Array::from(vec![0.25])) as _,
            Arc::new(Float64Array::from(vec![0.05])) as _,
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            new_null_array(&DataType::Float64, 1),
            n_dict_u32(&["Load 20"]),
            new_null_array(&DataType::Utf8, 1),
        ],
    )
    .context("loads batch")
}

fn shunts_batch() -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(fixed_shunts_schema()),
        vec![
            Arc::new(Int32Array::from(vec![47])) as _,
            one_dict("SH1"),
            Arc::new(BooleanArray::from(vec![true])) as _,
            Arc::new(Float64Array::from(vec![0.0])) as _,
            Arc::new(Float64Array::from(vec![0.05])) as _,
            new_null_array(&DataType::Utf8, 1),
        ],
    )
    .context("shunts batch")
}

fn write_demo(path: &std::path::Path) -> Result<()> {
    let mut table_batches: HashMap<&'static str, RecordBatch> = all_table_schemas()
        .into_iter()
        .map(|(name, schema)| (name, RecordBatch::new_empty(Arc::new(schema))))
        .collect();
    table_batches.insert(TABLE_METADATA, metadata_batch()?);
    table_batches.insert(TABLE_BUSES, buses_batch()?);
    table_batches.insert(TABLE_BRANCHES, branches_batch()?);
    table_batches.insert(TABLE_GENERATORS, generators_batch()?);
    table_batches.insert(TABLE_LOADS, loads_batch()?);
    table_batches.insert(TABLE_FIXED_SHUNTS, shunts_batch()?);
    table_batches.insert(TABLE_CONTINGENCIES, demo_contingencies()?);
    table_batches.insert(TABLE_PROTECTION_CONTINGENCIES, protection_batch()?);
    table_batches.insert(TABLE_TOPOLOGY_CHANGES, topology_batch()?);
    table_batches.insert(TABLE_CONTINGENCY_SEQUENCES, demo_sequences()?);

    write_root_rpf(
        path,
        &table_batches,
        &RootWriteOptions {
            include_protection_contingencies: true,
            include_topology_changes: true,
            include_contingency_sequences: true,
            ..Default::default()
        },
    )
}

fn assert_every_column_has_a_value(batch: &RecordBatch, table: &str) {
    for field in batch.schema().fields() {
        let col = batch
            .column_by_name(field.name())
            .unwrap_or_else(|| panic!("{table}.{} missing", field.name()));
        assert!(
            col.null_count() < col.len(),
            "{table}.{} is all-null in the dummy file",
            field.name()
        );
    }
}

#[test]
fn v014_funnel_demo_round_trips_all_new_columns() -> Result<()> {
    let tmp = std::env::temp_dir().join("raptrix_v014_funnel_demo");
    std::fs::create_dir_all(&tmp)?;
    let tmp_path = tmp.join("v014_funnel_demo.rpf");
    write_demo(&tmp_path)?;

    let tables = read_rpf_tables(&tmp_path)?;
    let conting = tables
        .iter()
        .find(|(n, _)| n == TABLE_CONTINGENCIES)
        .map(|(_, b)| b)
        .context("contingencies")?;
    assert_eq!(conting.num_rows(), 10);
    assert_eq!(conting.schema().fields().len(), 10);
    assert_every_column_has_a_value(conting, TABLE_CONTINGENCIES);

    let decoded = raptrix_cim_arrow::read_contingencies_batch(conting)?;
    let gen_row = decoded
        .iter()
        .find(|r| r.contingency_id == "GEN_1")
        .context("GEN_1")?;
    assert_eq!(gen_row.elements[0].element_type, "gen_trip");
    let tower = decoded
        .iter()
        .find(|r| r.contingency_id == "TOWER_L1_L2")
        .context("tower")?;
    assert_eq!(tower.elements.len(), 2);
    assert_eq!(tower.tpl_category.as_deref(), Some("P7"));
    assert_eq!(tower.reserved, Some(true));
    let analysis = decoded
        .iter()
        .find(|r| r.contingency_id == "BF_BUS47")
        .context("BF")?;
    assert_eq!(analysis.risk_score, Some(0.91));
    assert_eq!(analysis.recovery_time_min, Some(12.0));
    let untagged = decoded
        .iter()
        .find(|r| r.contingency_id == "SHUNT_47")
        .context("untagged")?;
    assert!(untagged.tpl_category.is_none());

    let seen_tpl: Vec<_> = decoded
        .iter()
        .filter_map(|r| r.tpl_category.as_deref())
        .collect();
    for token in TPL_CATEGORIES {
        assert!(
            seen_tpl.contains(token),
            "dummy missing tpl_category token {token}"
        );
    }
    let seen_types: Vec<_> = decoded
        .iter()
        .flat_map(|r| r.elements.iter().map(|e| e.element_type.as_str()))
        .collect();
    for token in [
        "branch_outage",
        "gen_trip",
        "load_shed",
        "shunt_switch",
        "split_bus",
        "protection_event",
    ] {
        assert!(
            seen_types.contains(&token),
            "dummy missing element_type {token}"
        );
    }

    let seq = tables
        .iter()
        .find(|(n, _)| n == TABLE_CONTINGENCY_SEQUENCES)
        .map(|(_, b)| b)
        .context("sequences")?;
    assert_eq!(seq.num_rows(), 4);
    assert_every_column_has_a_value(seq, TABLE_CONTINGENCY_SEQUENCES);
    let seq_rows = raptrix_cim_arrow::read_contingency_sequences_batch(seq)?;
    let seen_prov: Vec<_> = seq_rows
        .iter()
        .filter_map(|r| r.provenance.as_deref())
        .collect();
    for token in SEQUENCE_PROVENANCES {
        assert!(
            seen_prov.contains(token),
            "dummy missing sequence provenance {token}"
        );
    }

    let prot = tables
        .iter()
        .find(|(n, _)| n == TABLE_PROTECTION_CONTINGENCIES)
        .map(|(_, b)| b)
        .context("protection")?;
    assert_eq!(prot.num_rows(), 1);
    assert_eq!(prot.schema().fields().len(), 12);
    assert_every_column_has_a_value(prot, TABLE_PROTECTION_CONTINGENCIES);

    let topo = tables
        .iter()
        .find(|(n, _)| n == TABLE_TOPOLOGY_CHANGES)
        .map(|(_, b)| b)
        .context("topology")?;
    assert_every_column_has_a_value(topo, TABLE_TOPOLOGY_CHANGES);

    let buses = tables
        .iter()
        .find(|(n, _)| n == TABLE_BUSES)
        .map(|(_, b)| b)
        .context("buses")?;
    assert_eq!(buses.num_rows(), 3);
    let gens = tables
        .iter()
        .find(|(n, _)| n == TABLE_GENERATORS)
        .map(|(_, b)| b)
        .context("generators")?;
    assert_eq!(gens.num_rows(), 1);

    let meta = rpf_file_metadata(&tmp_path)?;
    assert_eq!(
        meta.get("raptrix.features.contingency_sequences"),
        Some(&"true".to_string())
    );
    assert_eq!(
        meta.get("raptrix.features.protection_contingencies"),
        Some(&"true".to_string())
    );
    assert_eq!(
        meta.get(raptrix_cim_arrow::METADATA_KEY_VERSION)
            .map(String::as_str),
        Some(raptrix_cim_arrow::RPF_VERSION)
    );

    let dest = fixture_dir().join("v014_funnel_demo.rpf");
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(&tmp_path, &dest).with_context(|| format!("copy demo to {}", dest.display()))?;
    Ok(())
}
