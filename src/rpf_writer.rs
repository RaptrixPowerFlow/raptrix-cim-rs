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
use std::io::{Cursor, Write};
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
    all_table_schemas, areas_schema, buses_schema, branches_schema, contingencies_schema,
    dynamics_models_schema, connectivity_groups_schema, fixed_shunts_schema, generators_schema,
    loads_schema, metadata_schema, owners_schema, switched_shunts_schema, transformers_2w_schema,
    transformers_3w_schema, zones_schema, BRANDING,
    SCHEMA_VERSION, TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES, TABLE_CONNECTIVITY_GROUPS,
    TABLE_CONTINGENCIES, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS,
    TABLE_INTERFACES, TABLE_LOADS, TABLE_METADATA, TABLE_OWNERS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES,
};
#[cfg(any(debug_assertions, feature = "strict"))]
use crate::arrow_schema::table_schema;
use crate::parser;

/// Bus resolution mode for EQ+TP merges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusResolutionMode {
    /// Default interoperability mode: collapse to TP `TopologicalNode`.
    Topological,
    /// Detailed mode: keep EQ `ConnectivityNode` granularity.
    ConnectivityDetail,
}

/// Writer options for profile merge behavior.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub bus_resolution_mode: BusResolutionMode,
    pub emit_connectivity_groups: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            bus_resolution_mode: BusResolutionMode::Topological,
            emit_connectivity_groups: false,
        }
    }
}

/// Summary emitted by the writer for CLI/reporting.
#[derive(Debug, Clone, Default)]
pub struct WriteSummary {
    pub connectivity_bus_count: usize,
    pub final_bus_count: usize,
    pub tp_merged: bool,
    pub connectivity_groups_rows: usize,
}

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
struct GenRow<'a> {
    bus_id: i32,
    id: Cow<'a, str>,
    p_sched_mw: f64,
    p_min_mw: f64,
    p_max_mw: f64,
    q_min_mvar: f64,
    q_max_mvar: f64,
    status: bool,
    mbase_mva: f64,
    h: f64,
    xd_prime: f64,
    d: f64,
}

#[derive(Debug, Clone)]
struct LoadRow<'a> {
    bus_id: i32,
    id: Cow<'a, str>,
    status: bool,
    p_mw: f64,
    q_mvar: f64,
}

#[derive(Debug, Clone)]
struct FixedShuntRow<'a> {
    bus_id: i32,
    id: Cow<'a, str>,
    status: bool,
    g_mw: f64,
    b_mvar: f64,
}

#[derive(Debug, Clone)]
struct SwitchedShuntRow {
    bus_id: i32,
    status: bool,
    v_low: f64,
    v_high: f64,
    b_steps: Vec<f64>,
    current_step: i32,
}

#[derive(Debug, Clone)]
struct Transformer2WRow<'a> {
    from_bus_id: i32,
    to_bus_id: i32,
    ckt: Cow<'a, str>,
    r: f64,
    x: f64,
    winding1_r: f64,
    winding1_x: f64,
    winding2_r: f64,
    winding2_x: f64,
    g: f64,
    b: f64,
    tap_ratio: f64,
    nominal_tap_ratio: f64,
    phase_shift: f64,
    vector_group: Cow<'a, str>,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
}

#[derive(Debug, Clone)]
struct Transformer3WRow<'a> {
    bus_h_id: i32,
    bus_m_id: i32,
    bus_l_id: i32,
    ckt: Cow<'a, str>,
    r_hm: f64,
    x_hm: f64,
    r_hl: f64,
    x_hl: f64,
    r_ml: f64,
    x_ml: f64,
    tap_h: f64,
    tap_m: f64,
    tap_l: f64,
    phase_shift: f64,
    vector_group: Cow<'a, str>,
    rate_a: f64,
    rate_b: f64,
    rate_c: f64,
    status: bool,
}

#[derive(Debug, Clone)]
struct AreaRow<'a> {
    area_id: i32,
    name: Cow<'a, str>,
    interchange_mw: Option<f64>,
}

#[derive(Debug, Clone)]
struct ZoneRow<'a> {
    zone_id: i32,
    name: Cow<'a, str>,
}

#[derive(Debug, Clone)]
struct OwnerRow<'a> {
    owner_id: i32,
    name: Cow<'a, str>,
}

#[derive(Debug, Clone)]
struct ConnectivityGroupRow<'a> {
    topological_bus_id: i32,
    topological_node_mrid: Cow<'a, str>,
    connectivity_node_mrids: Vec<Cow<'a, str>>,
    connectivity_count: i32,
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
    write_complete_rpf_with_options(cgmes_paths, output_path, &WriteOptions::default())?;
    Ok(())
}

/// Writes a complete Raptrix v0.5 `.rpf` IPC artifact with merge options.
pub fn write_complete_rpf_with_options(
    cgmes_paths: &[&str],
    output_path: &str,
    options: &WriteOptions,
) -> Result<WriteSummary> {
    if cgmes_paths.is_empty() {
        bail!("cgmes_paths is empty; provide at least one CGMES XML file path");
    }
    if !output_path.ends_with(".rpf") {
        bail!(
            "output_path must end with .rpf for Arrow IPC interchange output; got {output_path}"
        );
    }

    let topology = parse_eq_topology_rows(cgmes_paths, options.bus_resolution_mode)
        .with_context(|| {
            format!(
                "failed while parsing EQ/TP profile content from {} input path(s)",
                cgmes_paths.len()
            )
        })?;
    let (
        bus_rows,
        branch_rows,
        gen_rows,
        load_rows,
        transformer_2w_rows,
        transformer_3w_rows,
        area_rows,
        zone_rows,
        owner_rows,
        fixed_shunt_rows,
        switched_shunt_rows,
        connectivity_group_rows,
        split_bus_stub_elements,
    ) = topology;

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
    let generators_batch = build_generators_batch(&gen_rows)?;
    let loads_batch = build_loads_batch(&load_rows)?;
    let transformers_2w_batch = build_transformers_2w_batch(&transformer_2w_rows)?;
    let transformers_3w_batch = build_transformers_3w_batch(&transformer_3w_rows)?;
    let areas_batch = build_areas_batch(&area_rows)?;
    let zones_batch = build_zones_batch(&zone_rows)?;
    let owners_batch = build_owners_batch(&owner_rows)?;
    let fixed_shunts_batch = build_fixed_shunts_batch(&fixed_shunt_rows)?;
    let switched_shunts_batch = build_switched_shunts_batch(&switched_shunt_rows)?;
    let contingencies_rows = stub_contingency_rows(split_bus_stub_elements);
    let dynamics_models_rows = stub_dynamics_model_rows();
    let contingencies_batch = build_contingencies_batch(&contingencies_rows)?;
    let dynamics_models_batch = build_dynamics_models_batch(&dynamics_models_rows)?;
    let connectivity_groups_batch = build_connectivity_groups_batch(&connectivity_group_rows)?;

    let mut ordered_batches: Vec<(String, Schema, RecordBatch)> = Vec::new();
    for (table_name, schema) in all_table_schemas() {
        let schema = schema_with_table_name(table_name, &schema);
        let batch = match table_name {
            TABLE_METADATA => metadata_batch.clone(),
            TABLE_BUSES => buses_batch.clone(),
            TABLE_BRANCHES => branches_batch.clone(),
            TABLE_GENERATORS => generators_batch.clone(),
            TABLE_LOADS => loads_batch.clone(),
            TABLE_FIXED_SHUNTS => fixed_shunts_batch.clone(),
            TABLE_SWITCHED_SHUNTS => switched_shunts_batch.clone(),
            TABLE_TRANSFORMERS_2W => transformers_2w_batch.clone(),
            TABLE_TRANSFORMERS_3W => transformers_3w_batch.clone(),
            TABLE_AREAS => areas_batch.clone(),
            TABLE_ZONES => zones_batch.clone(),
            TABLE_OWNERS => owners_batch.clone(),
            TABLE_CONTINGENCIES => contingencies_batch.clone(),
            TABLE_DYNAMICS_MODELS => dynamics_models_batch.clone(),
            TABLE_INTERFACES
            => RecordBatch::new_empty(Arc::new(schema.clone())),
            _ => bail!("unrecognized table in canonical registry: {table_name}"),
        };
        ordered_batches.push((table_name.to_string(), schema, batch));
    }

    if options.emit_connectivity_groups {
        let schema = schema_with_table_name(TABLE_CONNECTIVITY_GROUPS, &connectivity_groups_schema());
        ordered_batches.push((
            TABLE_CONNECTIVITY_GROUPS.to_string(),
            schema,
            connectivity_groups_batch,
        ));
    }

    let mut output = File::create(output_path)
        .with_context(|| format!("failed to create output .rpf file at {output_path}"))?;

    // Arrow IPC FileWriter has a single-schema header per file segment.
    // We emit canonical table segments in deterministic order to preserve each
    // table's exact v0.5 schema while still using FileWriter for every segment.
    for (table_name, schema, batch) in &ordered_batches {
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
        {
            let expected = table_schema(table_name)
                .with_context(|| format!("missing expected schema for table {table_name}"))?;
            let expected_with_table = schema_with_table_name(table_name, &expected);
            validate_rpf_segments(
                &mut writer,
                &[(table_name.clone(), expected_with_table)],
            )
            .with_context(|| format!("Schema drift in table {table_name}"))?;
        }

        writer.finish().with_context(|| {
            format!("failed finishing IPC segment for table '{table_name}'")
        })?;
    }

    let tp_merged = options.bus_resolution_mode == BusResolutionMode::Topological
        && !connectivity_group_rows.is_empty();
    let connectivity_bus_count = if tp_merged {
        connectivity_group_rows
            .iter()
            .flat_map(|row| row.connectivity_node_mrids.iter())
            .count()
    } else {
        bus_rows.len()
    };

    Ok(WriteSummary {
        connectivity_bus_count,
        final_bus_count: bus_rows.len(),
        tp_merged,
        connectivity_groups_rows: connectivity_group_rows.len(),
    })
}

fn stub_contingency_rows(split_bus_stub_elements: Vec<ContingencyElement<'static>>) -> Vec<ContingencyRow<'static>> {
    let mut rows = vec![
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
    ];

    if !split_bus_stub_elements.is_empty() {
        rows.push(ContingencyRow {
            contingency_id: Cow::Borrowed("split-bus-stub"),
            elements: split_bus_stub_elements,
        });
    }

    rows
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
) -> Result<(
    Vec<crate::models::ACLineSegment<'static>>,
    Vec<crate::models::SynchronousMachine<'static>>,
    Vec<crate::models::EnergyConsumer<'static>>,
    Vec<parser::PowerTransformer<'static>>,
    Vec<crate::models::Area<'static>>,
    Vec<crate::models::Zone<'static>>,
    Vec<crate::models::Owner<'static>>,
    Vec<parser::FixedShuntSpec>,
    Vec<crate::models::SvShuntCompensator<'static>>,
    Vec<parser::TerminalLink>,
    Vec<crate::models::TopologicalNode<'static>>,
    Vec<crate::models::ConnectivityNodeGroup<'static>>,
)> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to open CGMES input file at {path}"))?;

    let (lines, machines, terminals) = parser::eq_lines_machines_and_terminals_from_reader(Cursor::new(&bytes))
        .with_context(|| {
            format!(
                "failed to extract ACLineSegment/SynchronousMachine/Terminal elements from CGMES input file at {path}"
            )
        })?;

    let loads = parser::energy_consumers_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract EnergyConsumer elements from CGMES input file at {path}")
    })?;

    let transformers = parser::power_transformers_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract PowerTransformer elements from CGMES input file at {path}")
    })?;

    let areas = parser::areas_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract ControlArea elements from CGMES input file at {path}")
    })?;

    let zones = parser::zones_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract SubGeographicalRegion elements from CGMES input file at {path}")
    })?;

    let owners = parser::owners_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract Organisation elements from CGMES input file at {path}")
    })?;

    let fixed_shunts = parser::fixed_shunts_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract LinearShuntCompensator elements from CGMES input file at {path}")
    })?;

    let switched_shunts = parser::sv_shunt_compensators_from_reader(Cursor::new(&bytes)).with_context(|| {
        format!("failed to extract SvShuntCompensator elements from CGMES input file at {path}")
    })?;

    let topological_nodes = parser::topological_nodes_from_reader(Cursor::new(&bytes))
        .with_context(|| format!("failed to extract TopologicalNode elements from CGMES input file at {path}"))?;

    let connectivity_groups = parser::connectivity_node_groups_from_reader(Cursor::new(&bytes))
        .with_context(|| {
            format!(
                "failed to extract TopologicalNode/ConnectivityNode group links from CGMES input file at {path}"
            )
        })?;

    Ok((
        lines,
        machines,
        loads,
        transformers,
        areas,
        zones,
        owners,
        fixed_shunts,
        switched_shunts,
        terminals,
        topological_nodes,
        connectivity_groups,
    ))
}

/// Parses EQ data and deterministically maps terminal connectivity into
/// dense bus IDs and concrete branch rows.
///
/// Tenet 1: uses parsed row data directly and avoids string reallocation in
/// hot loops where possible.
/// Tenet 2: prepares rows that map exactly to locked `buses` and `branches`
/// schemas without mutation.
/// Tenet 3: EQ ingestion ownership remains in Rust by joining `EnergyConsumer`
/// and `SvShuntCompensator` through `Terminal` references in this function.
fn parse_eq_topology_rows(
    cgmes_paths: &[&str],
    bus_resolution_mode: BusResolutionMode,
) -> Result<(
    Vec<BusRow<'static>>,
    Vec<BranchRow<'static>>,
    Vec<GenRow<'static>>,
    Vec<LoadRow<'static>>,
    Vec<Transformer2WRow<'static>>,
    Vec<Transformer3WRow<'static>>,
    Vec<AreaRow<'static>>,
    Vec<ZoneRow<'static>>,
    Vec<OwnerRow<'static>>,
    Vec<FixedShuntRow<'static>>,
    Vec<SwitchedShuntRow>,
    Vec<ConnectivityGroupRow<'static>>,
    Vec<ContingencyElement<'static>>,
)> {
    let mut lines = Vec::new();
    let mut machines = Vec::new();
    let mut loads = Vec::new();
    let mut transformers = Vec::new();
    let mut areas = Vec::new();
    let mut zones = Vec::new();
    let mut owners = Vec::new();
    let mut fixed_shunts = Vec::new();
    let mut switched_shunts = Vec::new();
    let mut terminals = Vec::new();
    let mut topological_nodes = Vec::new();
    let mut connectivity_groups = Vec::new();

    for path in cgmes_paths {
        let (
            mut parsed_lines,
            mut parsed_machines,
            mut parsed_loads,
            mut parsed_transformers,
            mut parsed_areas,
            mut parsed_zones,
            mut parsed_owners,
            mut parsed_fixed_shunts,
            mut parsed_switched_shunts,
            mut parsed_terminals,
            mut parsed_topological_nodes,
            mut parsed_connectivity_groups,
        ) =
            parse_eq_components_for_path(path)?;
        if !parsed_lines.is_empty() {
            lines.append(&mut parsed_lines);
        }
        if !parsed_machines.is_empty() {
            machines.append(&mut parsed_machines);
        }
        if !parsed_loads.is_empty() {
            loads.append(&mut parsed_loads);
        }
        if !parsed_transformers.is_empty() {
            transformers.append(&mut parsed_transformers);
        }
        if !parsed_areas.is_empty() {
            areas.append(&mut parsed_areas);
        }
        if !parsed_zones.is_empty() {
            zones.append(&mut parsed_zones);
        }
        if !parsed_owners.is_empty() {
            owners.append(&mut parsed_owners);
        }
        if !parsed_fixed_shunts.is_empty() {
            fixed_shunts.append(&mut parsed_fixed_shunts);
        }
        if !parsed_switched_shunts.is_empty() {
            switched_shunts.append(&mut parsed_switched_shunts);
        }
        if !parsed_terminals.is_empty() {
            terminals.append(&mut parsed_terminals);
        }
        if !parsed_topological_nodes.is_empty() {
            topological_nodes.append(&mut parsed_topological_nodes);
        }
        if !parsed_connectivity_groups.is_empty() {
            connectivity_groups.append(&mut parsed_connectivity_groups);
        }
    }

    if lines.is_empty() {
        bail!("no ACLineSegment elements found across supplied CGMES paths")
    }
    if terminals.is_empty() {
        bail!("no Terminal elements found across supplied CGMES paths")
    }

    let mut conn_to_topo: HashMap<&str, &str> = HashMap::new();
    for group in &connectivity_groups {
        let topological_mrid = group.topological_node_mrid.as_ref();
        for connectivity_mrid in &group.connectivity_node_mrids {
            conn_to_topo.insert(connectivity_mrid.as_ref(), topological_mrid);
        }
    }

    let use_topological = !conn_to_topo.is_empty() && bus_resolution_mode == BusResolutionMode::Topological;

    let mut sorted_bus_keys: Vec<&str> = terminals
        .iter()
        .map(|terminal| {
            if use_topological {
                conn_to_topo
                    .get(terminal.connectivity_node_mrid.as_str())
                    .copied()
                    .with_context(|| {
                        format!(
                            "missing TP TopologicalNode mapping for ConnectivityNode '{}'",
                            terminal.connectivity_node_mrid
                        )
                    })
            } else {
                Ok(terminal.connectivity_node_mrid.as_str())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    sorted_bus_keys.sort_unstable();
    sorted_bus_keys.dedup();

    let mut bus_key_to_bus_id: HashMap<&str, i32> = HashMap::with_capacity(sorted_bus_keys.len());
    for (idx, bus_key) in sorted_bus_keys.iter().enumerate() {
        bus_key_to_bus_id.insert(*bus_key, (idx as i32) + 1);
    }

    let bus_rows: Vec<BusRow<'static>> = sorted_bus_keys
        .iter()
        .map(|bus_key| BusRow {
            bus_id: bus_key_to_bus_id[bus_key],
            name: Cow::Owned((*bus_key).to_owned()),
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

        let from_bus_key = if use_topological {
            conn_to_topo.get(from_node).copied().with_context(|| {
                format!(
                    "failed to resolve ConnectivityNode '{from_node}' to TopologicalNode"
                )
            })?
        } else {
            from_node
        };
        let to_bus_key = if use_topological {
            conn_to_topo.get(to_node).copied().with_context(|| {
                format!("failed to resolve ConnectivityNode '{to_node}' to TopologicalNode")
            })?
        } else {
            to_node
        };

        let from_bus_id = bus_key_to_bus_id.get(from_bus_key).copied().with_context(|| {
            format!(
                "failed to resolve bus key '{from_bus_key}' to dense bus_id"
            )
        })?;
        let to_bus_id = bus_key_to_bus_id.get(to_bus_key).copied().with_context(|| {
            format!(
                "failed to resolve bus key '{to_bus_key}' to dense bus_id"
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

    machines.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    let mut terminals_by_equipment: HashMap<&str, Vec<&parser::TerminalLink>> = HashMap::new();
    for terminal in &terminals {
        terminals_by_equipment
            .entry(terminal.line_mrid.as_str())
            .or_default()
            .push(terminal);
    }

    let mut gen_rows = Vec::with_capacity(machines.len());
    for machine in machines {
        let machine_mrid = machine.base.m_rid.as_ref();
        let machine_terminals = terminals_by_equipment.get(machine_mrid).with_context(|| {
            format!("missing Terminal linkage for SynchronousMachine mRID '{machine_mrid}'")
        })?;

        let selected_terminal = machine_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!(
                    "missing Terminal linkage for SynchronousMachine mRID '{machine_mrid}'"
                )
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for SynchronousMachine mRID '{machine_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for SynchronousMachine mRID '{machine_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for SynchronousMachine mRID '{machine_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        gen_rows.push(GenRow {
            bus_id,
            id: machine.base.m_rid,
            p_sched_mw: machine.p_sched_mw.unwrap_or(0.0),
            p_min_mw: machine.p_min_mw.unwrap_or(0.0),
            p_max_mw: machine.p_max_mw.unwrap_or(0.0),
            q_min_mvar: machine.q_min_mvar.unwrap_or(0.0),
            q_max_mvar: machine.q_max_mvar.unwrap_or(0.0),
            status: true,
            mbase_mva: machine.mbase_mva.unwrap_or(100.0),
            h: machine.h.unwrap_or(0.0),
            xd_prime: machine.xd_prime.unwrap_or(0.0),
            d: machine.d.unwrap_or(0.0),
        });
    }

    loads.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));

    let mut load_rows = Vec::with_capacity(loads.len());
    for load in loads {
        let load_mrid = load.base.m_rid.as_ref();
        let load_terminals = terminals_by_equipment.get(load_mrid).with_context(|| {
            format!("missing Terminal linkage for EnergyConsumer mRID '{load_mrid}'")
        })?;

        let selected_terminal = load_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for EnergyConsumer mRID '{load_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for EnergyConsumer mRID '{load_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for EnergyConsumer mRID '{load_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for EnergyConsumer mRID '{load_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        load_rows.push(LoadRow {
            bus_id,
            id: load.base.m_rid,
            status: load.status.unwrap_or(true),
            p_mw: load.p_mw.unwrap_or(0.0),
            q_mvar: load.q_mvar.unwrap_or(0.0),
        });
    }

    transformers.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let mut transformer_2w_rows = Vec::new();
    let mut transformer_3w_rows = Vec::new();
    for transformer in transformers {
        let transformer_mrid = transformer.base.m_rid.as_ref();
        let transformer_terminals = terminals_by_equipment
            .get(transformer_mrid)
            .with_context(|| {
                format!("missing Terminal linkage for PowerTransformer mRID '{transformer_mrid}'")
            })?;

        let mut unique_terminals: Vec<&parser::TerminalLink> = Vec::new();
        for terminal in transformer_terminals {
            if unique_terminals
                .iter()
                .all(|existing| existing.connectivity_node_mrid != terminal.connectivity_node_mrid)
            {
                unique_terminals.push(*terminal);
            }
        }
        unique_terminals.sort_by_key(|terminal| {
            (
                terminal.sequence_number,
                terminal.connectivity_node_mrid.as_str(),
            )
        });

        let winding_count = transformer.ends.len().max(unique_terminals.len());
        if winding_count < 2 {
            bail!(
                "PowerTransformer mRID '{transformer_mrid}' has fewer than 2 winding/terminal endpoints"
            );
        }

        let resolve_terminal_bus_id = |terminal: &parser::TerminalLink| -> Result<i32> {
            if use_topological {
                let topological_bus_key = conn_to_topo
                    .get(terminal.connectivity_node_mrid.as_str())
                    .copied()
                    .with_context(|| {
                        format!(
                            "failed to resolve ConnectivityNode '{}' to TopologicalNode for PowerTransformer mRID '{transformer_mrid}'",
                            terminal.connectivity_node_mrid
                        )
                    })?;
                bus_key_to_bus_id
                    .get(topological_bus_key)
                    .copied()
                    .with_context(|| {
                        format!(
                            "failed to resolve TopologicalNode '{}' to dense bus_id for PowerTransformer mRID '{transformer_mrid}'",
                            topological_bus_key
                        )
                    })
            } else {
                bus_key_to_bus_id
                    .get(terminal.connectivity_node_mrid.as_str())
                    .copied()
                    .with_context(|| {
                        format!(
                            "failed to resolve ConnectivityNode '{}' to dense bus_id for PowerTransformer mRID '{transformer_mrid}'",
                            terminal.connectivity_node_mrid
                        )
                    })
            }
        };

        let vector_group = transformer
            .vector_group
            .map(|value| Cow::Owned(value.into_owned()))
            .unwrap_or_else(|| Cow::Borrowed(""));
        let rate_a = transformer
            .rate_a
            .or_else(|| transformer.ends.iter().find_map(|end| end.rate))
            .unwrap_or(0.0);
        let rate_b = transformer.rate_b.unwrap_or(rate_a);
        let rate_c = transformer.rate_c.unwrap_or(rate_b);
        let status = transformer.status.unwrap_or(true);

        if winding_count == 2 {
            let from_terminal = unique_terminals.first().copied().with_context(|| {
                format!("missing Terminal endpoint #1 for PowerTransformer mRID '{transformer_mrid}'")
            })?;
            let to_terminal = unique_terminals.get(1).copied().with_context(|| {
                format!("missing Terminal endpoint #2 for PowerTransformer mRID '{transformer_mrid}'")
            })?;

            let from_bus_id = resolve_terminal_bus_id(from_terminal)?;
            let to_bus_id = resolve_terminal_bus_id(to_terminal)?;

            let winding1 = transformer.ends.first();
            let winding2 = transformer.ends.get(1);
            let winding1_r = winding1.and_then(|end| end.r).unwrap_or(0.0);
            let winding1_x = winding1.and_then(|end| end.x).unwrap_or(0.0);
            let winding2_r = winding2.and_then(|end| end.r).unwrap_or(0.0);
            let winding2_x = winding2.and_then(|end| end.x).unwrap_or(0.0);

            transformer_2w_rows.push(Transformer2WRow {
                from_bus_id,
                to_bus_id,
                ckt: Cow::Borrowed("1"),
                r: winding1_r + winding2_r,
                x: winding1_x + winding2_x,
                winding1_r,
                winding1_x,
                winding2_r,
                winding2_x,
                g: transformer.ends.iter().filter_map(|end| end.g).sum(),
                b: transformer.ends.iter().filter_map(|end| end.b).sum(),
                tap_ratio: winding1.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                nominal_tap_ratio: 1.0,
                phase_shift: winding1.and_then(|end| end.phase_shift).unwrap_or(0.0),
                vector_group,
                rate_a,
                rate_b,
                rate_c,
                status,
            });
        } else {
            let terminal_h = unique_terminals.first().copied().with_context(|| {
                format!("missing Terminal endpoint #1 for PowerTransformer mRID '{transformer_mrid}'")
            })?;
            let terminal_m = unique_terminals.get(1).copied().with_context(|| {
                format!("missing Terminal endpoint #2 for PowerTransformer mRID '{transformer_mrid}'")
            })?;
            let terminal_l = unique_terminals.get(2).copied().with_context(|| {
                format!("missing Terminal endpoint #3 for PowerTransformer mRID '{transformer_mrid}'")
            })?;

            let bus_h_id = resolve_terminal_bus_id(terminal_h)?;
            let bus_m_id = resolve_terminal_bus_id(terminal_m)?;
            let bus_l_id = resolve_terminal_bus_id(terminal_l)?;

            let end_h = transformer.ends.first();
            let end_m = transformer.ends.get(1);
            let end_l = transformer.ends.get(2);

            transformer_3w_rows.push(Transformer3WRow {
                bus_h_id,
                bus_m_id,
                bus_l_id,
                ckt: Cow::Borrowed("1"),
                r_hm: end_h.and_then(|end| end.r).unwrap_or(0.0),
                x_hm: end_h.and_then(|end| end.x).unwrap_or(0.0),
                r_hl: end_m.and_then(|end| end.r).unwrap_or(0.0),
                x_hl: end_m.and_then(|end| end.x).unwrap_or(0.0),
                r_ml: end_l.and_then(|end| end.r).unwrap_or(0.0),
                x_ml: end_l.and_then(|end| end.x).unwrap_or(0.0),
                tap_h: end_h.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                tap_m: end_m.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                tap_l: end_l.and_then(|end| end.tap_ratio).unwrap_or(1.0),
                phase_shift: end_h.and_then(|end| end.phase_shift).unwrap_or(0.0),
                vector_group,
                rate_a,
                rate_b,
                rate_c,
                status,
            });
        }
    }

    areas.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let area_rows: Vec<AreaRow<'static>> = areas
        .into_iter()
        .enumerate()
        .map(|(idx, area)| AreaRow {
            area_id: (idx as i32) + 1,
            name: area
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(area.base.m_rid.as_ref().to_owned())),
            interchange_mw: area.interchange_mw,
        })
        .collect();

    zones.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let zone_rows: Vec<ZoneRow<'static>> = zones
        .into_iter()
        .enumerate()
        .map(|(idx, zone)| ZoneRow {
            zone_id: (idx as i32) + 1,
            name: zone
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(zone.base.m_rid.as_ref().to_owned())),
        })
        .collect();

    owners.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let owner_rows: Vec<OwnerRow<'static>> = owners
        .into_iter()
        .enumerate()
        .map(|(idx, owner)| OwnerRow {
            owner_id: (idx as i32) + 1,
            name: owner
                .base
                .name
                .unwrap_or_else(|| Cow::Owned(owner.base.m_rid.as_ref().to_owned())),
        })
        .collect();

    fixed_shunts.sort_unstable_by(|left, right| left.equipment_mrid.cmp(&right.equipment_mrid));
    let mut fixed_shunt_rows = Vec::with_capacity(fixed_shunts.len());
    for shunt in fixed_shunts {
        let shunt_mrid = shunt.equipment_mrid.as_str();
        let shunt_terminals = terminals_by_equipment.get(shunt_mrid).with_context(|| {
            format!("missing Terminal linkage for LinearShuntCompensator mRID '{shunt_mrid}'")
        })?;

        let selected_terminal = shunt_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for LinearShuntCompensator mRID '{shunt_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for LinearShuntCompensator mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for LinearShuntCompensator mRID '{shunt_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for LinearShuntCompensator mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        fixed_shunt_rows.push(FixedShuntRow {
            bus_id,
            id: Cow::Owned(shunt.equipment_mrid),
            status: shunt.status.unwrap_or(true),
            g_mw: shunt.g_mw.unwrap_or(0.0),
            b_mvar: shunt.b_mvar.unwrap_or(0.0),
        });
    }

    switched_shunts.sort_unstable_by(|left, right| left.base.m_rid.cmp(&right.base.m_rid));
    let mut switched_shunt_rows = Vec::with_capacity(switched_shunts.len());
    for shunt in switched_shunts {
        let shunt_mrid = shunt.base.m_rid.as_ref();
        let shunt_terminals = terminals_by_equipment.get(shunt_mrid).with_context(|| {
            format!("missing Terminal linkage for SvShuntCompensator mRID '{shunt_mrid}'")
        })?;

        let selected_terminal = shunt_terminals
            .iter()
            .copied()
            .min_by_key(|terminal| {
                (
                    terminal.sequence_number,
                    terminal.connectivity_node_mrid.as_str(),
                )
            })
            .with_context(|| {
                format!("missing Terminal linkage for SvShuntCompensator mRID '{shunt_mrid}'")
            })?;

        let bus_id = if use_topological {
            let topological_bus_key = conn_to_topo
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to TopologicalNode for switched shunt mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?;
            bus_key_to_bus_id
                .get(topological_bus_key)
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve TopologicalNode '{}' to dense bus_id for switched shunt mRID '{shunt_mrid}'",
                        topological_bus_key
                    )
                })?
        } else {
            bus_key_to_bus_id
                .get(selected_terminal.connectivity_node_mrid.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "failed to resolve ConnectivityNode '{}' to dense bus_id for switched shunt mRID '{shunt_mrid}'",
                        selected_terminal.connectivity_node_mrid
                    )
                })?
        };

        let mut b_steps = shunt.b_steps.unwrap_or_default();
        if b_steps.is_empty() {
            b_steps.push(0.0);
        }

        switched_shunt_rows.push(SwitchedShuntRow {
            bus_id,
            status: true,
            v_low: shunt.v_low.unwrap_or(0.95),
            v_high: shunt.v_high.unwrap_or(1.05),
            b_steps,
            current_step: shunt.current_step.unwrap_or(0),
        });
    }

    let mut topology_name_by_mrid: HashMap<&str, &str> = HashMap::new();
    for node in &topological_nodes {
        if let Some(name) = node.base.name.as_deref() {
            topology_name_by_mrid.insert(node.base.m_rid.as_ref(), name);
        }
    }

    let mut connectivity_group_rows: Vec<ConnectivityGroupRow<'static>> = Vec::new();
    let mut split_bus_stub_elements: Vec<ContingencyElement<'static>> = Vec::new();
    for group in &connectivity_groups {
        let topological_mrid = group.topological_node_mrid.as_ref().to_owned();
        let bus_key = if use_topological {
            topological_mrid.as_str()
        } else {
            continue;
        };
        let Some(topological_bus_id) = bus_key_to_bus_id.get(bus_key).copied() else {
            continue;
        };

        let mut connectivity_node_mrids: Vec<Cow<'static, str>> = group
            .connectivity_node_mrids
            .iter()
            .map(|value| Cow::Owned(value.as_ref().to_owned()))
            .collect();
        connectivity_node_mrids.sort_unstable();
        connectivity_node_mrids.dedup();

        let display_topological = topology_name_by_mrid
            .get(topological_mrid.as_str())
            .copied()
            .unwrap_or(topological_mrid.as_str())
            .to_owned();

        if connectivity_node_mrids.len() > 1 {
            let split_id = format!(
                "topological_node_id={topological_bus_id};connectivity_node_a={};connectivity_node_b={};breaker_mrid=stub",
                connectivity_node_mrids[0],
                connectivity_node_mrids[1]
            );
            split_bus_stub_elements.push(ContingencyElement {
                element_type: Cow::Borrowed("split_bus"),
                branch_id: None,
                bus_id: Some(topological_bus_id),
                id: Some(Cow::Owned(split_id)),
                status_change: true,
            });
        }

        connectivity_group_rows.push(ConnectivityGroupRow {
            topological_bus_id,
            topological_node_mrid: Cow::Owned(display_topological),
            connectivity_count: connectivity_node_mrids.len() as i32,
            connectivity_node_mrids,
        });
    }

    Ok((
        bus_rows,
        branch_rows,
        gen_rows,
        load_rows,
        transformer_2w_rows,
        transformer_3w_rows,
        area_rows,
        zone_rows,
        owner_rows,
        fixed_shunt_rows,
        switched_shunt_rows,
        connectivity_group_rows,
        split_bus_stub_elements,
    ))
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

fn build_generators_batch(rows: &[GenRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(generators_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut p_sched_mw_b = Float64Builder::new();
    let mut p_min_mw_b = Float64Builder::new();
    let mut p_max_mw_b = Float64Builder::new();
    let mut q_min_mvar_b = Float64Builder::new();
    let mut q_max_mvar_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut mbase_mva_b = Float64Builder::new();
    let mut h_b = Float64Builder::new();
    let mut xd_prime_b = Float64Builder::new();
    let mut d_b = Float64Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_ref())?;
        p_sched_mw_b.append_value(row.p_sched_mw);
        p_min_mw_b.append_value(row.p_min_mw);
        p_max_mw_b.append_value(row.p_max_mw);
        q_min_mvar_b.append_value(row.q_min_mvar);
        q_max_mvar_b.append_value(row.q_max_mvar);
        status_b.append_value(row.status);
        mbase_mva_b.append_value(row.mbase_mva);
        h_b.append_value(row.h);
        xd_prime_b.append_value(row.xd_prime);
        d_b.append_value(row.d);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(id_b.finish()) as ArrayRef,
        Arc::new(p_sched_mw_b.finish()) as ArrayRef,
        Arc::new(p_min_mw_b.finish()) as ArrayRef,
        Arc::new(p_max_mw_b.finish()) as ArrayRef,
        Arc::new(q_min_mvar_b.finish()) as ArrayRef,
        Arc::new(q_max_mvar_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(mbase_mva_b.finish()) as ArrayRef,
        Arc::new(h_b.finish()) as ArrayRef,
        Arc::new(xd_prime_b.finish()) as ArrayRef,
        Arc::new(d_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build generators record batch")
}

/// Builds the `loads` table batch from EQ `EnergyConsumer` joins.
///
/// Tenet 1: preserves borrowed IDs until Arrow append points.
/// Tenet 2: writes exact locked v0.5 `loads` schema ordering.
fn build_loads_batch(rows: &[LoadRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(loads_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut status_b = BooleanBuilder::new();
    let mut p_mw_b = Float64Builder::new();
    let mut q_mvar_b = Float64Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_ref())?;
        status_b.append_value(row.status);
        p_mw_b.append_value(row.p_mw);
        q_mvar_b.append_value(row.q_mvar);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(p_mw_b.finish()) as ArrayRef,
        Arc::new(q_mvar_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build loads record batch")
}

/// Builds the `transformers_2w` table batch from resolved EQ transformer rows.
///
/// Tenet 2: preserves exact v0.5 schema ordering and primitive types.
fn build_transformers_2w_batch(rows: &[Transformer2WRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(transformers_2w_schema());

    let mut from_bus_id_b = Int32Builder::new();
    let mut to_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_b = Float64Builder::new();
    let mut x_b = Float64Builder::new();
    let mut winding1_r_b = Float64Builder::new();
    let mut winding1_x_b = Float64Builder::new();
    let mut winding2_r_b = Float64Builder::new();
    let mut winding2_x_b = Float64Builder::new();
    let mut g_b = Float64Builder::new();
    let mut b_b = Float64Builder::new();
    let mut tap_ratio_b = Float64Builder::new();
    let mut nominal_tap_ratio_b = Float64Builder::new();
    let mut phase_shift_b = Float64Builder::new();
    let mut vector_group_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();

    for row in rows {
        from_bus_id_b.append_value(row.from_bus_id);
        to_bus_id_b.append_value(row.to_bus_id);
        ckt_b.append(row.ckt.as_ref())?;
        r_b.append_value(row.r);
        x_b.append_value(row.x);
        winding1_r_b.append_value(row.winding1_r);
        winding1_x_b.append_value(row.winding1_x);
        winding2_r_b.append_value(row.winding2_r);
        winding2_x_b.append_value(row.winding2_x);
        g_b.append_value(row.g);
        b_b.append_value(row.b);
        tap_ratio_b.append_value(row.tap_ratio);
        nominal_tap_ratio_b.append_value(row.nominal_tap_ratio);
        phase_shift_b.append_value(row.phase_shift);
        vector_group_b.append(row.vector_group.as_ref())?;
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(from_bus_id_b.finish()) as ArrayRef,
        Arc::new(to_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_b.finish()) as ArrayRef,
        Arc::new(x_b.finish()) as ArrayRef,
        Arc::new(winding1_r_b.finish()) as ArrayRef,
        Arc::new(winding1_x_b.finish()) as ArrayRef,
        Arc::new(winding2_r_b.finish()) as ArrayRef,
        Arc::new(winding2_x_b.finish()) as ArrayRef,
        Arc::new(g_b.finish()) as ArrayRef,
        Arc::new(b_b.finish()) as ArrayRef,
        Arc::new(tap_ratio_b.finish()) as ArrayRef,
        Arc::new(nominal_tap_ratio_b.finish()) as ArrayRef,
        Arc::new(phase_shift_b.finish()) as ArrayRef,
        Arc::new(vector_group_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build transformers_2w record batch")
}

/// Builds the `transformers_3w` table batch from resolved EQ transformer rows.
///
/// Tenet 2: preserves exact v0.5 schema ordering and primitive types.
fn build_transformers_3w_batch(rows: &[Transformer3WRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(transformers_3w_schema());

    let mut bus_h_id_b = Int32Builder::new();
    let mut bus_m_id_b = Int32Builder::new();
    let mut bus_l_id_b = Int32Builder::new();
    let mut star_bus_id_b = Int32Builder::new();
    let mut ckt_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_hm_b = Float64Builder::new();
    let mut x_hm_b = Float64Builder::new();
    let mut r_hl_b = Float64Builder::new();
    let mut x_hl_b = Float64Builder::new();
    let mut r_ml_b = Float64Builder::new();
    let mut x_ml_b = Float64Builder::new();
    let mut tap_h_b = Float64Builder::new();
    let mut tap_m_b = Float64Builder::new();
    let mut tap_l_b = Float64Builder::new();
    let mut phase_shift_b = Float64Builder::new();
    let mut vector_group_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a_b = Float64Builder::new();
    let mut rate_b_b = Float64Builder::new();
    let mut rate_c_b = Float64Builder::new();
    let mut status_b = BooleanBuilder::new();

    for row in rows {
        bus_h_id_b.append_value(row.bus_h_id);
        bus_m_id_b.append_value(row.bus_m_id);
        bus_l_id_b.append_value(row.bus_l_id);
        star_bus_id_b.append_null();
        ckt_b.append(row.ckt.as_ref())?;
        r_hm_b.append_value(row.r_hm);
        x_hm_b.append_value(row.x_hm);
        r_hl_b.append_value(row.r_hl);
        x_hl_b.append_value(row.x_hl);
        r_ml_b.append_value(row.r_ml);
        x_ml_b.append_value(row.x_ml);
        tap_h_b.append_value(row.tap_h);
        tap_m_b.append_value(row.tap_m);
        tap_l_b.append_value(row.tap_l);
        phase_shift_b.append_value(row.phase_shift);
        vector_group_b.append(row.vector_group.as_ref())?;
        rate_a_b.append_value(row.rate_a);
        rate_b_b.append_value(row.rate_b);
        rate_c_b.append_value(row.rate_c);
        status_b.append_value(row.status);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_h_id_b.finish()) as ArrayRef,
        Arc::new(bus_m_id_b.finish()) as ArrayRef,
        Arc::new(bus_l_id_b.finish()) as ArrayRef,
        Arc::new(star_bus_id_b.finish()) as ArrayRef,
        Arc::new(ckt_b.finish()) as ArrayRef,
        Arc::new(r_hm_b.finish()) as ArrayRef,
        Arc::new(x_hm_b.finish()) as ArrayRef,
        Arc::new(r_hl_b.finish()) as ArrayRef,
        Arc::new(x_hl_b.finish()) as ArrayRef,
        Arc::new(r_ml_b.finish()) as ArrayRef,
        Arc::new(x_ml_b.finish()) as ArrayRef,
        Arc::new(tap_h_b.finish()) as ArrayRef,
        Arc::new(tap_m_b.finish()) as ArrayRef,
        Arc::new(tap_l_b.finish()) as ArrayRef,
        Arc::new(phase_shift_b.finish()) as ArrayRef,
        Arc::new(vector_group_b.finish()) as ArrayRef,
        Arc::new(rate_a_b.finish()) as ArrayRef,
        Arc::new(rate_b_b.finish()) as ArrayRef,
        Arc::new(rate_c_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build transformers_3w record batch")
}

fn build_areas_batch(rows: &[AreaRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(areas_schema());

    let mut area_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut interchange_mw_b = Float64Builder::new();

    for row in rows {
        area_id_b.append_value(row.area_id);
        name_b.append(row.name.as_ref())?;
        if let Some(interchange_mw) = row.interchange_mw {
            interchange_mw_b.append_value(interchange_mw);
        } else {
            interchange_mw_b.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(area_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(interchange_mw_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build areas record batch")
}

fn build_zones_batch(rows: &[ZoneRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(zones_schema());

    let mut zone_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        zone_id_b.append_value(row.zone_id);
        name_b.append(row.name.as_ref())?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(zone_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build zones record batch")
}

fn build_owners_batch(rows: &[OwnerRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(owners_schema());

    let mut owner_id_b = Int32Builder::new();
    let mut name_b = StringDictionaryBuilder::<Int32Type>::new();

    for row in rows {
        owner_id_b.append_value(row.owner_id);
        name_b.append(row.name.as_ref())?;
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(owner_id_b.finish()) as ArrayRef,
        Arc::new(name_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build owners record batch")
}

fn build_fixed_shunts_batch(rows: &[FixedShuntRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(fixed_shunts_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut id_b = StringDictionaryBuilder::<Int32Type>::new();
    let mut status_b = BooleanBuilder::new();
    let mut g_mw_b = Float64Builder::new();
    let mut b_mvar_b = Float64Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        id_b.append(row.id.as_ref())?;
        status_b.append_value(row.status);
        g_mw_b.append_value(row.g_mw);
        b_mvar_b.append_value(row.b_mvar);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(g_mw_b.finish()) as ArrayRef,
        Arc::new(b_mvar_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build fixed_shunts record batch")
}

/// Builds the `switched_shunts` table batch from `SvShuntCompensator` joins.
///
/// Tenet 1: appends list values directly from parsed row vectors.
/// Tenet 2: preserves locked v0.5 switched_shunts schema ordering and types.
fn build_switched_shunts_batch(rows: &[SwitchedShuntRow]) -> Result<RecordBatch> {
    let schema = Arc::new(switched_shunts_schema());

    let mut bus_id_b = Int32Builder::new();
    let mut status_b = BooleanBuilder::new();
    let mut v_low_b = Float64Builder::new();
    let mut v_high_b = Float64Builder::new();
    let list_field = schema
        .field(4)
        .data_type()
        .clone();
    let mut b_steps_b = ListBuilder::new(Float64Builder::new()).with_field(match list_field {
        DataType::List(field) => field,
        _ => Arc::new(Field::new("item", DataType::Float64, false)),
    });
    let mut current_step_b = Int32Builder::new();

    for row in rows {
        bus_id_b.append_value(row.bus_id);
        status_b.append_value(row.status);
        v_low_b.append_value(row.v_low);
        v_high_b.append_value(row.v_high);

        for value in &row.b_steps {
            b_steps_b.values().append_value(*value);
        }
        b_steps_b.append(true);

        current_step_b.append_value(row.current_step.max(0));
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bus_id_b.finish()) as ArrayRef,
        Arc::new(status_b.finish()) as ArrayRef,
        Arc::new(v_low_b.finish()) as ArrayRef,
        Arc::new(v_high_b.finish()) as ArrayRef,
        Arc::new(b_steps_b.finish()) as ArrayRef,
        Arc::new(current_step_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build switched_shunts record batch")
}

fn build_connectivity_groups_batch(rows: &[ConnectivityGroupRow<'_>]) -> Result<RecordBatch> {
    let schema = Arc::new(connectivity_groups_schema());

    let mut topological_bus_id_b = Int32Builder::new();
    let mut topological_node_mrid_b = StringDictionaryBuilder::<Int32Type>::new();
    let list_field = schema
        .field(2)
        .data_type()
        .clone();
    let mut connectivity_node_mrids_b =
        ListBuilder::new(StringBuilder::new()).with_field(match list_field {
            DataType::List(field) => field,
            _ => Arc::new(Field::new("item", DataType::Utf8, false)),
        });
    let mut connectivity_count_b = Int32Builder::new();

    for row in rows {
        topological_bus_id_b.append_value(row.topological_bus_id);
        topological_node_mrid_b.append(row.topological_node_mrid.as_ref())?;

        for connectivity_node_mrid in &row.connectivity_node_mrids {
            connectivity_node_mrids_b
                .values()
                .append_value(connectivity_node_mrid.as_ref());
        }
        connectivity_node_mrids_b.append(true);

        connectivity_count_b.append_value(row.connectivity_count);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(topological_bus_id_b.finish()) as ArrayRef,
        Arc::new(topological_node_mrid_b.finish()) as ArrayRef,
        Arc::new(connectivity_node_mrids_b.finish()) as ArrayRef,
        Arc::new(connectivity_count_b.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays).context("failed to build connectivity_groups record batch")
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
