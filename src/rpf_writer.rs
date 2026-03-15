// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! End-to-end Raptrix PowerFlow Interchange (.rpf) writer.
//!
//! Tenet references:
//! - Tenet 1 (zero-copy first): parser helpers and dictionary/string builders
//!   are used in a way that avoids unnecessary string cloning in hot paths.
//! - Tenet 2 (locked schema): table schemas are sourced only from
//!   `arrow_schema::all_table_schemas()` and written in canonical order.
//! - Tenet 3 (separation of concerns): this module performs parsing/mapping/
//!   serialization only; no solver math is implemented here.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{
    new_null_array, ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int32Builder,
    Int8Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use memmap2::MmapOptions;

use crate::arrow_schema::{
    all_table_schemas, buses_schema, branches_schema, contingencies_schema, dynamics_models_schema,
    metadata_schema, BRANDING, SCHEMA_VERSION, TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES,
    TABLE_CONTINGENCIES, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS,
    TABLE_INTERFACES, TABLE_LOADS, TABLE_METADATA, TABLE_OWNERS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES,
};
use crate::parser;

#[derive(Debug, Clone)]
struct MetadataRow<'a> {
    base_mva: f64,
    frequency_hz: f64,
    psse_version: i32,
    study_name: Cow<'a, str>,
    timestamp_utc: Cow<'a, str>,
    raptrix_version: Cow<'a, str>,
    is_planning_case: bool,
}

#[derive(Debug, Clone)]
struct BusRow<'a> {
    bus_id: i32,
    name: Cow<'a, str>,
    bus_type: i8,
    p_sched: f64,
    q_sched: f64,
    v_mag_set: f64,
    v_ang_set: f64,
    q_min: f64,
    q_max: f64,
    g_shunt: f64,
    b_shunt: f64,
    area: i32,
    zone: i32,
    owner: i32,
    v_min: f64,
    v_max: f64,
    p_min_agg: f64,
    p_max_agg: f64,
}

#[derive(Debug, Clone)]
struct BranchRow<'a> {
    branch_id: i32,
    from_bus_id: i32,
    to_bus_id: i32,
    ckt: Cow<'a, str>,
    r: f64,
    x: f64,
    b_shunt: f64,
    tap: f64,
    phase: f64,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
}

#[derive(Debug, Clone)]
struct ContingencyRow<'a> {
    contingency_id: Cow<'a, str>,
    elements: Vec<ContingencyElement<'a>>,
}

#[derive(Debug, Clone)]
struct ContingencyElement<'a> {
    element_type: Cow<'a, str>,
    branch_id: Option<i32>,
    bus_id: Option<i32>,
    id: Option<Cow<'a, str>>,
    status_change: bool,
}

#[derive(Debug, Clone)]
struct DynamicsModelRow<'a> {
    bus_id: i32,
    gen_id: Cow<'a, str>,
    model_type: Cow<'a, str>,
    params: Vec<(Cow<'a, str>, f64)>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Endpoints {
    from_terminal_idx: Option<usize>,
    to_terminal_idx: Option<usize>,
}

fn schema_with_table_name(table_name: &str, schema: &Schema) -> Schema {
    let mut metadata = schema.metadata().clone();
    metadata.insert("table_name".to_string(), table_name.to_string());
    schema.clone().with_metadata(metadata)
}

fn split_concatenated_ipc_segments(bytes: &[u8]) -> Vec<&[u8]> {
    const ARROW_MAGIC: &[u8] = b"ARROW1";
    const DOUBLE_MAGIC: &[u8] = b"ARROW1ARROW1";

    let mut starts = vec![0usize];
    let mut cursor = 0usize;

    while cursor + DOUBLE_MAGIC.len() <= bytes.len() {
        if &bytes[cursor..cursor + DOUBLE_MAGIC.len()] == DOUBLE_MAGIC {
            starts.push(cursor + ARROW_MAGIC.len());
            cursor += DOUBLE_MAGIC.len();
            continue;
        }
        cursor += 1;
    }

    let mut segments = Vec::new();
    for (idx, start) in starts.iter().enumerate() {
        let end = starts.get(idx + 1).copied().unwrap_or(bytes.len());
        segments.push(&bytes[*start..end]);
    }
    segments
}

/// Validates that the active writer schema matches the expected v0.5 schema.
///
/// Tenet 2: catches schema drift at write-time by enforcing exact
/// field-by-field schema equality.
pub fn validate_rpf_segments<W: Write>(
    writer: &mut FileWriter<W>,
    expected_schemas: &[(String, Schema)],
) -> Result<()> {
    let writer_schema = writer.schema().as_ref();
    let table_name = writer_schema
        .metadata()
        .get("table_name")
        .cloned()
        .context("writer schema missing required table_name metadata key")?;

    let (_, expected_schema) = expected_schemas
        .first()
        .context("validate_rpf_segments requires at least one expected schema")?;

    if writer_schema != expected_schema {
        bail!("Schema drift in table {table_name}")
    }

    Ok(())
}

/// Reads all tables from a concatenated `.rpf` file in write order.
///
/// Uses memory-mapped reads to avoid copying file bytes while iterating
/// individual IPC file segments.
pub fn read_rpf_tables(path: impl AsRef<Path>) -> Result<Vec<(String, RecordBatch)>> {
    let path = path.as_ref();
    let file = File::open(path)
        .with_context(|| format!("failed to open .rpf file at {}", path.display()))?;
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("failed to memory-map .rpf file at {}", path.display()))?;

    let expected_table_names: Vec<String> = all_table_schemas()
        .into_iter()
        .map(|(name, _)| name.to_string())
        .collect();
    let segments = split_concatenated_ipc_segments(&mmap);

    let mut out = Vec::new();
    for (segment_idx, segment) in segments.into_iter().enumerate() {
        let mut reader = FileReader::try_new(Cursor::new(segment), None)
            .with_context(|| format!("Failed reading segment {segment_idx}"))?;

        let fallback_name = expected_table_names
            .get(segment_idx)
            .cloned()
            .unwrap_or_else(|| format!("segment_{segment_idx}"));
        let table_name = reader
            .schema()
            .metadata()
            .get("table_name")
            .cloned()
            .unwrap_or(fallback_name);

        let expected_batches = reader.num_batches();
        for _ in 0..expected_batches {
            let batch = reader
                .next()
                .transpose()
                .with_context(|| format!("Failed reading segment {segment_idx}"))?
                .with_context(|| {
                    format!(
                        "segment {segment_idx} ended before expected record batch count"
                    )
                })?;
            out.push((table_name.clone(), batch));
        }
    }

    Ok(out)
}

/// Writes a complete Raptrix v0.5 `.rpf` IPC artifact.
///
/// The writer currently supports EQ-driven topology ingestion and materializes
/// all required v0.5 tables (empty tables allowed) in canonical order.
///
/// Notes:
/// - Multi-profile merges (TP/SV/SSH/DY) are intentionally incremental and
///   will be added in future revisions.
/// - Output path must end in `.rpf`.
pub fn write_complete_rpf(cgmes_paths: &[&str], output_path: &str) -> Result<()> {
    if cgmes_paths.is_empty() {
        bail!("cgmes_paths is empty; provide at least one CGMES XML file path");
    }
    if !output_path.ends_with(".rpf") {
        bail!(
            "output_path must end with .rpf for Arrow IPC interchange output; got {output_path}"
        );
    }

    let (bus_rows, branch_rows) = parse_eq_topology_rows(cgmes_paths).with_context(|| {
        format!(
            "failed while parsing EQ profile content from {} input path(s)",
            cgmes_paths.len()
        )
    })?;

    let metadata_row = MetadataRow {
        base_mva: 100.0,
        frequency_hz: 60.0,
        psse_version: 35,
        study_name: Cow::Borrowed("cgmes_eq_import"),
        timestamp_utc: Cow::Borrowed("1970-01-01T00:00:00Z"),
        raptrix_version: Cow::Borrowed(SCHEMA_VERSION),
        is_planning_case: true,
    };

    let metadata_batch = build_metadata_batch(&metadata_row)?;
    let buses_batch = build_buses_batch(&bus_rows)?;
    let branches_batch = build_branches_batch(&branch_rows)?;
    let contingencies_rows = stub_contingency_rows();
    let dynamics_models_rows = stub_dynamics_model_rows();
    let contingencies_batch = build_contingencies_batch(&contingencies_rows)?;
    let dynamics_models_batch = build_dynamics_models_batch(&dynamics_models_rows)?;

    let expected_schemas: Vec<(String, Schema)> = all_table_schemas()
        .into_iter()
        .map(|(name, schema)| (name.to_string(), schema_with_table_name(name, &schema)))
        .collect();

    let mut ordered_batches: Vec<(String, Schema, RecordBatch)> = Vec::new();
    for (table_name, schema) in all_table_schemas() {
        let schema = schema_with_table_name(table_name, &schema);
        let batch = match table_name {
            TABLE_METADATA => metadata_batch.clone(),
            TABLE_BUSES => buses_batch.clone(),
            TABLE_BRANCHES => branches_batch.clone(),
            TABLE_CONTINGENCIES => contingencies_batch.clone(),
            TABLE_DYNAMICS_MODELS => dynamics_models_batch.clone(),
            TABLE_GENERATORS
            | TABLE_LOADS
            | TABLE_FIXED_SHUNTS
            | TABLE_SWITCHED_SHUNTS
            | TABLE_TRANSFORMERS_2W
            | TABLE_TRANSFORMERS_3W
            | TABLE_AREAS
            | TABLE_ZONES
            | TABLE_OWNERS
            | TABLE_INTERFACES
            => RecordBatch::new_empty(Arc::new(schema.clone())),
            _ => bail!("unrecognized table in canonical registry: {table_name}"),
        };
        ordered_batches.push((table_name.to_string(), schema, batch));
    }

    let mut output = File::create(output_path)
        .with_context(|| format!("failed to create output .rpf file at {output_path}"))?;

    // Arrow IPC FileWriter has a single-schema header per file segment.
    // We emit canonical table segments in deterministic order to preserve each
    // table's exact v0.5 schema while still using FileWriter for every segment.
    for (table_index, (table_name, schema, batch)) in ordered_batches.iter().enumerate() {
        let mut writer = FileWriter::try_new(&mut output, schema).with_context(|| {
            format!("failed to initialize IPC FileWriter for table '{table_name}'")
        })?;
        writer.write_metadata("raptrix.branding", BRANDING);
        writer.write_metadata("raptrix.version", SCHEMA_VERSION);
        writer.write_metadata("raptrix.table", table_name);
        writer.write(batch).with_context(|| {
            format!("failed writing IPC record batch for table '{table_name}'")
        })?;

        #[cfg(any(debug_assertions, feature = "strict"))]
        validate_rpf_segments(
            &mut writer,
            &expected_schemas[table_index..=table_index],
        )
        .with_context(|| format!("Schema drift in table {table_name}"))?;

        writer.finish().with_context(|| {
            format!("failed finishing IPC segment for table '{table_name}'")
        })?;
    }

    Ok(())
}

fn stub_contingency_rows() -> Vec<ContingencyRow<'static>> {
    vec![
        ContingencyRow {
            contingency_id: Cow::Borrowed("N-1 Line1"),
            elements: vec![ContingencyElement {
                element_type: Cow::Borrowed("branch_outage"),
                branch_id: Some(1),
                bus_id: None,
                id: Some(Cow::Borrowed("Line1")),
                status_change: true,
            }],
        },
        ContingencyRow {
            contingency_id: Cow::Borrowed("N-1 Bus2"),
            elements: vec![
                ContingencyElement {
                    element_type: Cow::Borrowed("branch_outage"),
                    branch_id: Some(1),
                    bus_id: None,
                    id: Some(Cow::Borrowed("Line1")),
                    status_change: true,
                },
                ContingencyElement {
                    element_type: Cow::Borrowed("shunt_switch"),
                    branch_id: None,
                    bus_id: Some(2),
                    id: Some(Cow::Borrowed("ShuntA")),
                    status_change: true,
                },
            ],
        },
    ]
}

fn stub_dynamics_model_rows() -> Vec<DynamicsModelRow<'static>> {
    vec![DynamicsModelRow {
        bus_id: 1,
        gen_id: Cow::Borrowed("G1"),
        model_type: Cow::Borrowed("GENROU"),
        params: vec![
            (Cow::Borrowed("H"), 5.0),
            (Cow::Borrowed("xd_prime"), 0.3),
        ],
    }]
}

fn parse_eq_components_for_path(
    path: &str,
) -> Result<(Vec<crate::models::ACLineSegment<'static>>, Vec<parser::TerminalLink>)> {
    let file = File::open(path)
        .with_context(|| format!("failed to open CGMES input file at {path}"))?;

    parser::eq_lines_and_terminals_from_reader(BufReader::new(file)).with_context(|| {
        format!(
            "failed to extract ACLineSegment/Terminal elements from CGMES input file at {path}"
        )
    })
}

/// Parses EQ data and deterministically maps terminal connectivity into
/// dense bus IDs and concrete branch rows.
///
/// Tenet 1: uses parsed row data directly and avoids string reallocation in
/// hot loops where possible.
/// Tenet 2: prepares rows that map exactly to locked `buses` and `branches`
/// schemas without mutation.
fn parse_eq_topology_rows(cgmes_paths: &[&str]) -> Result<(Vec<BusRow<'static>>, Vec<BranchRow<'static>>)> {
    let mut lines = Vec::new();
    let mut terminals = Vec::new();

    for path in cgmes_paths {
        let (mut parsed_lines, mut parsed_terminals) = parse_eq_components_for_path(path)?;
        if !parsed_lines.is_empty() {
            lines.append(&mut parsed_lines);
        }
        if !parsed_terminals.is_empty() {
            terminals.append(&mut parsed_terminals);
        }
    }

    if lines.is_empty() {
        bail!("no ACLineSegment elements found across supplied CGMES paths")
    }
    if terminals.is_empty() {
        bail!("no Terminal elements found across supplied CGMES paths")
    }

    let mut sorted_node_mrids: Vec<&str> = terminals
        .iter()
        .map(|terminal| terminal.connectivity_node_mrid.as_str())
        .collect();
    sorted_node_mrids.sort_unstable();
    sorted_node_mrids.dedup();

    let mut node_to_bus_id: HashMap<&str, i32> = HashMap::with_capacity(sorted_node_mrids.len());
    for (idx, node_mrid) in sorted_node_mrids.iter().enumerate() {
        node_to_bus_id.insert(*node_mrid, (idx as i32) + 1);
    }

    let bus_rows: Vec<BusRow<'static>> = sorted_node_mrids
        .iter()
        .enumerate()
        .map(|(idx, node_mrid)| BusRow {
            bus_id: (idx as i32) + 1,
            name: Cow::Owned((*node_mrid).to_owned()),
            bus_type: 1,
            p_sched: 0.0,
            q_sched: 0.0,
            v_mag_set: 1.0,
            v_ang_set: 0.0,
            q_min: -9999.0,
            q_max: 9999.0,
            g_shunt: 0.0,
            b_shunt: 0.0,
            area: 1,
            zone: 1,
            owner: 1,
            v_min: 0.9,
            v_max: 1.1,
            p_min_agg: 0.0,
            p_max_agg: 0.0,
        })
        .collect();

    let mut endpoints_by_line: HashMap<&str, Endpoints> = HashMap::new();
    for (terminal_idx, terminal) in terminals.iter().enumerate() {
        let entry = endpoints_by_line
            .entry(terminal.line_mrid.as_str())
            .or_default();
        match terminal.sequence_number {
            1 => entry.from_terminal_idx = Some(terminal_idx),
            2 => entry.to_terminal_idx = Some(terminal_idx),
            _ => {}
        }
    }

    lines.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    let mut branch_rows = Vec::with_capacity(lines.len());
    for (idx, line) in lines.iter().enumerate() {
        let line_mrid = line.base.m_rid.as_ref();
        let endpoints = endpoints_by_line.get(line_mrid).copied().with_context(|| {
            format!(
                "missing Terminal linkage for ACLineSegment mRID '{line_mrid}'"
            )
        })?;

        let from_idx = endpoints.from_terminal_idx.with_context(|| {
            format!(
                "missing Terminal sequenceNumber=1 for ACLineSegment mRID '{line_mrid}'"
            )
        })?;
        let to_idx = endpoints.to_terminal_idx.with_context(|| {
            format!(
                "missing Terminal sequenceNumber=2 for ACLineSegment mRID '{line_mrid}'"
            )
        })?;

        let from_node = terminals[from_idx].connectivity_node_mrid.as_str();
        let to_node = terminals[to_idx].connectivity_node_mrid.as_str();

        let from_bus_id = node_to_bus_id.get(from_node).copied().with_context(|| {
            format!(
                "failed to resolve ConnectivityNode '{from_node}' to dense bus_id"
            )
        })?;
        let to_bus_id = node_to_bus_id.get(to_node).copied().with_context(|| {
            format!(
                "failed to resolve ConnectivityNode '{to_node}' to dense bus_id"
            )
        })?;

        branch_rows.push(BranchRow {
            branch_id: (idx as i32) + 1,
            from_bus_id,
            to_bus_id,
            ckt: Cow::Borrowed("1"),
            r: line.r.unwrap_or(0.0),
            x: line.x.unwrap_or(0.0),
            b_shunt: line.bch.unwrap_or(0.0),
            tap: 1.0,
            phase: 0.0,
            rate_a: 9999.0,
            rate_b: 9999.0,
            rate_c: 9999.0,
            status: true,
        });
    }

    Ok((bus_rows, branch_rows))
}

/// Builds the one-row `metadata` table batch using the locked v0.5 schema.
fn build_metadata_batch(row: &MetadataRow<'_>) -> Result<RecordBatch> {
    let schema = Arc::new(metadata_schema());

    let mut base_mva_b = Float64Builder::new();
    let mut frequency_b = Float64Builder::new();
    let mut psse_b = Int32Builder::new();
    let mut study_name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut timestamp_b = StringBuilder::new();
    let mut raptrix_version_b = StringBuilder::new();
    let mut planning_b = BooleanBuilder::new();

    base_mva_b.append_value(row.base_mva);
    frequency_b.append_value(row.frequency_hz);
    psse_b.append_value(row.psse_version);
    // Zero-copy-friendly append path for borrowed Cow data.
    study_name_b.append(row.study_name.as_ref())?;
    timestamp_b.append_value(row.timestamp_utc.as_ref());
    raptrix_version_b.append_value(row.raptrix_version.as_ref());
    planning_b.append_value(row.is_planning_case);

    let custom_metadata_type = schema
        .field(7)
        .data_type()
        .clone();
    let custom_metadata_array = new_null_array(&custom_metadata_type, 1);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(base_mva_b.finish()) as ArrayRef,
        Arc::new(frequency_b.finish()) as ArrayRef,
        Arc::new(psse_b.finish()) as ArrayRef,
        Arc::new(study_name_b.finish()) as ArrayRef,
        Arc::new(timestamp_b.finish()) as ArrayRef,
        Arc::new(raptrix_version_b.finish()) as ArrayRef,
        Arc::new(planning_b.finish()) as ArrayRef,
        custom_metadata_array,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build metadata record batch")
}

/// Builds the `buses` table batch with dictionary encoding on low-cardinality
/// string fields (`name`) and fixed, schema-ordered columns.
fn build_buses_batch(rows: &[BusRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(buses_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut type_b = Int8Builder::new();
    let mut p_sched_b = Float64Builder::new();
    let mut q_sched_b = Float64Builder::new();
    let mut v_mag_set_b = Float64Builder::new();
    let mut v_ang_set_b = Float64Builder::new();
    let mut q_min_b = Float64Builder::new();
    let mut q_max_b = Float64Builder::new();
    let mut g_shunt_b = Float64Builder::new();
    let mut b_shunt_b = Float64Builder::new();
    let mut area_b = Int32Builder::new();
    let mut zone_b = Int32Builder::new();
    let mut owner_b = Int32Builder::new();
    let mut v_min_b = Float64Builder::new();
    let mut v_max_b = Float64Builder::new();
    let mut p_min_agg_b = Float64Builder::new();
    let mut p_max_agg_b = Float64Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        name_b.append(row.name.as_ref())?;
        type_b.append_value(row.bus_type);
        p_sched_b.append_value(row.p_sched);
        q_sched_b.append_value(row.q_sched);
        v_mag_set_b.append_value(row.v_mag_set);
        v_ang_set_b.append_value(row.v_ang_set);
        q_min_b.append_value(row.q_min);
        q_max_b.append_value(row.q_max);
        g_shunt_b.append_value(row.g_shunt);
        b_shunt_b.append_value(row.b_shunt);
        area_b.append_value(row.area);
        zone_b.append_value(row.zone);
        owner_b.append_value(row.owner);
        v_min_b.append_value(row.v_min);
        v_max_b.append_value(row.v_max);
        p_min_agg_b.append_value(row.p_min_agg);
        p_max_agg_b.append_value(row.p_max_agg);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(type_b.finish()) as ArrayRef,
        Arc::new(p_sched_b.finish()) as ArrayRef,
        Arc::new(q_sched_b.finish()) as ArrayRef,
        Arc::new(v_mag_set_b.finish()) as ArrayRef,
        Arc::new(v_ang_set_b.finish()) as ArrayRef,
        Arc::new(q_min_b.finish()) as ArrayRef,
        Arc::new(q_max_b.finish()) as ArrayRef,
        Arc::new(g_shunt_b.finish()) as ArrayRef,
        Arc::new(b_shunt_b.finish()) as ArrayRef,
        Arc::new(area_b.finish()) as ArrayRef,
        Arc::new(zone_b.finish()) as ArrayRef,
        Arc::new(owner_b.finish()) as ArrayRef,
        Arc::new(v_min_b.finish()) as ArrayRef,
        Arc::new(v_max_b.finish()) as ArrayRef,
        Arc::new(p_min_agg_b.finish()) as ArrayRef,
        Arc::new(p_max_agg_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build buses record batch")
}

fn build_branches_batch(rows: &[BranchRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(branches_schema());

    let mut branch_id_b = Int32Builder::new();
    let mut from_bus_id_b = Int32Builder::new();
    let mut to_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_b = Float64Builder::new();
    let mut x_b = Float64Builder::new();
    let mut b_shunt_b = Float64Builder::new();
    let mut tap_b = Float64Builder::new();
    let mut phase_b = Float64Builder::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();

    for row in rows {
        branch_id_b.append_value(row.branch_id);
        from_bus_id_b.append_value(row.from_bus_id);
        to_bus_id_b.append_value(row.to_bus_id);
        ckt_b.append(row.ckt.as_ref())?;
        r_b.append_value(row.r);
        x_b.append_value(row.x);
        b_shunt_b.append_value(row.b_shunt);
        tap_b.append_value(row.tap);
        phase_b.append_value(row.phase);
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(branch_id_b.finish()) as ArrayRef,
        Arc::new(from_bus_id_b.finish()) as ArrayRef,
        Arc::new(to_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_b.finish()) as ArrayRef,
        Arc::new(x_b.finish()) as ArrayRef,
        Arc::new(b_shunt_b.finish()) as ArrayRef,
        Arc::new(tap_b.finish()) as ArrayRef,
        Arc::new(phase_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build branches record batch")
}

/// Builds the `contingencies` table batch with list-of-struct payloads.
///
/// Tenet 2: this uses the exact locked `contingencies_schema()` ordering and
/// nested field layout.
fn build_contingencies_batch(rows: &[ContingencyRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(contingencies_schema());

    let mut contingency_id_b = StringDictionaryBuilder::<Int32Type>::new();

    let element_fields = vec![
        Field::new(
            "element_type",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("branch_id", DataType::Int32, true),
        Field::new("bus_id", DataType::Int32, true),
        Field::new(
            "gen_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "load_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("amount_mw", DataType::Float64, true),
        Field::new("status_change", DataType::Boolean, false),
    ];
    let element_field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(Int32Builder::new()),
        Box::new(Int32Builder::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(StringDictionaryBuilder::<Int32Type>::new()),
        Box::new(Float64Builder::new()),
        Box::new(BooleanBuilder::new()),
    ];
    let element_struct_b = StructBuilder::new(element_fields, element_field_builders);
    let elements_field = match schema.field(1).data_type() {
        DataType::List(field) => field.clone(),
        other => {
            bail!(
                "contingencies.elements field must be List<Struct>, found {other:?}"
            )
        }
    };
    let mut elements_b = ListBuilder::new(element_struct_b).with_field(elements_field);

    for row in rows {
        contingency_id_b.append(row.contingency_id.as_ref())?;

        for element in &row.elements {
            let struct_b = elements_b.values();

            struct_b
                .field_builder::<StringDictionaryBuilder<Int32Type>>(0)
                .context("missing element_type builder")?
                .append(element.element_type.as_ref())?;

            if let Some(branch_id) = element.branch_id {
                struct_b
                    .field_builder::<Int32Builder>(1)
                    .context("missing branch_id builder")?
                    .append_value(branch_id);
            } else {
                struct_b
                    .field_builder::<Int32Builder>(1)
                    .context("missing branch_id builder")?
                    .append_null();
            }

            if let Some(bus_id) = element.bus_id {
                struct_b
                    .field_builder::<Int32Builder>(2)
                    .context("missing bus_id builder")?
                    .append_value(bus_id);
            } else {
                struct_b
                    .field_builder::<Int32Builder>(2)
                    .context("missing bus_id builder")?
                    .append_null();
            }

            if let Some(id) = &element.id {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                    .context("missing gen_id builder")?
                    .append(id.as_ref())?;
            } else {
                struct_b
                    .field_builder::<StringDictionaryBuilder<Int32Type>>(3)
                    .context("missing gen_id builder")?
                    .append_null();
            }

            struct_b
                .field_builder::<StringDictionaryBuilder<Int32Type>>(4)
                .context("missing load_id builder")?
                .append_null();
            struct_b
                .field_builder::<Float64Builder>(5)
                .context("missing amount_mw builder")?
                .append_null();
            struct_b
                .field_builder::<BooleanBuilder>(6)
                .context("missing status_change builder")?
                .append_value(element.status_change);
            struct_b.append(true);
        }

        elements_b.append(true);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(contingency_id_b.finish()) as ArrayRef,
        Arc::new(elements_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build contingencies record batch")
}

/// Builds the `dynamics_models` table batch with map-typed parameter payloads.
///
/// Tenet 3: this is currently stubbed ingestion data only; no dynamic model
/// solving behavior exists here.
fn build_dynamics_models_batch(rows: &[DynamicsModelRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(dynamics_models_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut gen_id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut model_type_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut params_b = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        Float64Builder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Float64, false)));

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        gen_id_b.append(row.gen_id.as_ref())?;
        model_type_b.append(row.model_type.as_ref())?;

        for (key, value) in &row.params {
            params_b.keys().append_value(key.as_ref());
            params_b.values().append_value(*value);
        }
        params_b.append(true).context("failed to append params map row")?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(gen_id_b.finish()) as ArrayRef,
        Arc::new(model_type_b.finish()) as ArrayRef,
        Arc::new(params_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build dynamics_models record batch")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;

    use anyhow::{Context, Result};
    use arrow::array::{
        Array, Float64Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::datatypes::Schema;

    use crate::arrow_schema::{
        all_table_schemas, branches_schema, buses_schema, contingencies_schema,
        dynamics_models_schema, metadata_schema, BRANDING, SCHEMA_VERSION,
    };

    use super::{read_rpf_tables, schema_with_table_name, write_complete_rpf};

    fn assert_schema_shape_matches(actual: &Schema, expected: &Schema) {
        assert_eq!(actual.fields().len(), expected.fields().len());
        for idx in 0..actual.fields().len() {
            let actual_field = actual.field(idx);
            let expected_field = expected.field(idx);
            assert_eq!(actual_field.name(), expected_field.name());
            assert_eq!(actual_field.data_type(), expected_field.data_type());
            assert_eq!(actual_field.is_nullable(), expected_field.is_nullable());
        }
    }

    fn generate_eq_fixture_branch_count(n_branches: usize) -> String {
        let mut xml = String::from(
            r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">"#,
        );

        for idx in 0..n_branches {
            let line_id = format!("Line{idx}");
            let from_node = format!("Node{}", idx * 2 + 1);
            let to_node = format!("Node{}", idx * 2 + 2);
            xml.push_str(&format!(
                r##"<cim:ACLineSegment rdf:ID="{line_id}"><IdentifiedObject.name>Line {idx}</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x><ACLineSegment.bch>0.001</ACLineSegment.bch></cim:ACLineSegment><cim:Terminal rdf:ID="T{idx}_1"><Terminal.ConductingEquipment rdf:resource="#{line_id}"/><Terminal.ConnectivityNode rdf:resource="#{from_node}"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal><cim:Terminal rdf:ID="T{idx}_2"><Terminal.ConductingEquipment rdf:resource="#{line_id}"/><Terminal.ConnectivityNode rdf:resource="#{to_node}"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>"##
            ));
        }

        xml.push_str("</rdf:RDF>");
        xml
    }

    #[test]
    #[ignore = "skeleton test for incremental .rpf IPC validation"]
    fn write_complete_rpf_skeleton() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_writer_tests");
        fs::create_dir_all(&tmp_dir)?;

        let eq_path = tmp_dir.join("minimal_eq.xml");
        fs::write(
            &eq_path,
                r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
        <cim:ACLineSegment rdf:ID="L1"><IdentifiedObject.name>Line 1</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x></cim:ACLineSegment>
        <cim:Terminal rdf:ID="T1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
        <cim:Terminal rdf:ID="T2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
    </rdf:RDF>"##,
        )?;

        let output_path = tmp_dir.join("case.rpf");
        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        write_complete_rpf(&[&eq_path_str], &output_path_str)?;

        let tables = read_rpf_tables(&output_path)?;
        assert_eq!(tables.len(), all_table_schemas().len());

        let expected_table_names: Vec<String> = all_table_schemas()
            .into_iter()
            .map(|(name, _)| name.to_string())
            .collect();
        let actual_table_names: Vec<String> = tables.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(actual_table_names, expected_table_names);

        // Spot-check metadata segment and required file metadata keys.
        assert_eq!(
            tables[0].1.schema().as_ref(),
            &schema_with_table_name("metadata", &metadata_schema())
        );
        let schema_ref = tables[0].1.schema();
        let meta = schema_ref.metadata();
        assert_eq!(meta.get("raptrix.branding"), Some(&BRANDING.to_string()));
        assert_eq!(meta.get("raptrix.version"), Some(&SCHEMA_VERSION.to_string()));
        assert_eq!(meta.get("table_name"), Some(&"metadata".to_string()));

        assert_schema_shape_matches(tables[1].1.schema().as_ref(), &buses_schema());
        assert_schema_shape_matches(tables[2].1.schema().as_ref(), &branches_schema());
        assert_schema_shape_matches(tables[12].1.schema().as_ref(), &contingencies_schema());
        assert_schema_shape_matches(tables[14].1.schema().as_ref(), &dynamics_models_schema());
        assert_eq!(
            tables[14].1.schema().metadata().get("table_name"),
            Some(&"dynamics_models".to_string())
        );

        let branches_batch = &tables[2].1;
        assert_eq!(branches_batch.num_rows(), 1, "fixture should produce one branch");

        let branch_id = branches_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("branch_id must be Int32");
        let from_bus_id = branches_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("from_bus_id must be Int32");
        let to_bus_id = branches_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("to_bus_id must be Int32");
        let r = branches_batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("r must be Float64");
        let x = branches_batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("x must be Float64");

        assert_eq!(branch_id.value(0), 1);
        assert_eq!(from_bus_id.value(0), 1);
        assert_eq!(to_bus_id.value(0), 2);
        assert!((r.value(0) - 0.01).abs() < 1e-12);
        assert!((x.value(0) - 0.05).abs() < 1e-12);

        let contingencies_batch = &tables[12].1;
        assert!(
            contingencies_batch.num_rows() >= 1,
            "expected at least one contingency row"
        );
        let elements = contingencies_batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("contingencies.elements must be ListArray");
        assert!(
            elements.value_length(0) > 0,
            "expected first contingency to contain at least one element"
        );

        let dynamics_models_batch = &tables[14].1;
        assert_schema_shape_matches(
            dynamics_models_batch.schema().as_ref(),
            &dynamics_models_schema(),
        );
        assert!(
            dynamics_models_batch.num_rows() >= 1,
            "expected at least one dynamics model row"
        );

        let params_map = dynamics_models_batch
            .column(3)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("dynamics_models.params must be MapArray");
        assert!(
            params_map.value_length(0) > 0,
            "expected at least one params key/value pair"
        );
        let entries: &StructArray = params_map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("params key column must be Utf8");
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("params value column must be Float64");

        let mut saw_h = false;
        for index in 0..keys.len() {
            if keys.value(index) == "H" {
                assert!((values.value(index) - 5.0).abs() < 1e-12);
                saw_h = true;
                break;
            }
        }
        saw_h
            .then_some(())
            .context("expected dynamics params to contain key 'H' with value 5.0")?;

        Ok(())
    }

    #[test]
    #[ignore = "performance smoke test for RPF write/read throughput"]
    fn benchmark_write_complete_rpf_round_trip() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_perf_tests");
        fs::create_dir_all(&tmp_dir)?;

        let branch_count = 1_000usize;
        let eq_path = tmp_dir.join("perf_eq.xml");
        let output_path = tmp_dir.join("perf_case.rpf");
        fs::write(&eq_path, generate_eq_fixture_branch_count(branch_count))?;

        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        let write_start = Instant::now();
        write_complete_rpf(&[&eq_path_str], &output_path_str)?;
        let write_elapsed = write_start.elapsed();
        let write_ms = write_elapsed.as_millis();
        let file_size_bytes = fs::metadata(&output_path)?.len();
        let file_size_mib = file_size_bytes as f64 / (1024.0 * 1024.0);

        let read_start = Instant::now();
        let tables = read_rpf_tables(&output_path)?;
        let read_elapsed = read_start.elapsed();
        let read_ms = read_elapsed.as_millis();

        let branches_batch = tables
            .iter()
            .find(|(table_name, _)| table_name == "branches")
            .map(|(_, batch)| batch)
            .context("expected branches batch in perf round-trip output")?;

        assert_eq!(tables.len(), all_table_schemas().len());
        assert_eq!(branches_batch.num_rows(), branch_count);

        println!(
            "RPF write: {} branches in {} ms",
            branch_count,
            write_ms,
        );
        println!(
            "Wrote {} bytes ({:.2} MiB)",
            file_size_bytes,
            file_size_mib,
        );
        println!(
            "Write throughput: {:.0} branches/s",
            branch_count as f64 / write_elapsed.as_secs_f64()
        );
        println!(
            "RPF read: {} tables in {} ms",
            tables.len(),
            read_ms,
        );
        println!(
            "Read throughput: {:.0} tables/s",
            tables.len() as f64 / read_elapsed.as_secs_f64()
        );

        Ok(())
    }

    #[test]
    #[ignore = "performance scaling test for 10k-branch RPF write/read throughput"]
    fn benchmark_write_complete_rpf_round_trip_10k() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join("raptrix_rpf_perf_tests");
        fs::create_dir_all(&tmp_dir)?;

        let branch_count = 10_000usize;
        let eq_path = tmp_dir.join("perf_eq_10k.xml");
        let output_path = tmp_dir.join("perf_case_10k.rpf");
        fs::write(&eq_path, generate_eq_fixture_branch_count(branch_count))?;

        let eq_path_str = eq_path.to_string_lossy().into_owned();
        let output_path_str = output_path.to_string_lossy().into_owned();

        let write_start = Instant::now();
        write_complete_rpf(&[&eq_path_str], &output_path_str)?;
        let write_elapsed = write_start.elapsed();
        let write_ms = write_elapsed.as_millis();
        let file_size_bytes = fs::metadata(&output_path)?.len();
        let file_size_mib = file_size_bytes as f64 / (1024.0 * 1024.0);

        let read_start = Instant::now();
        let tables = read_rpf_tables(&output_path)?;
        let read_elapsed = read_start.elapsed();
        let read_ms = read_elapsed.as_millis();

        let branches_batch = tables
            .iter()
            .find(|(table_name, _)| table_name == "branches")
            .map(|(_, batch)| batch)
            .context("expected branches batch in 10k perf round-trip output")?;

        assert_eq!(tables.len(), all_table_schemas().len());
        assert_eq!(branches_batch.num_rows(), branch_count);

        println!(
            "RPF 10k write: {} branches in {} ms",
            branch_count,
            write_ms,
        );
        println!(
            "Wrote {} bytes ({:.2} MiB)",
            file_size_bytes,
            file_size_mib,
        );
        println!(
            "Write throughput: {:.0} branches/s",
            branch_count as f64 / write_elapsed.as_secs_f64()
        );
        println!(
            "RPF 10k read: {} tables in {} ms",
            tables.len(),
            read_ms,
        );
        println!(
            "Read throughput: {:.0} tables/s",
            tables.len() as f64 / read_elapsed.as_secs_f64()
        );

        Ok(())
    }
}
