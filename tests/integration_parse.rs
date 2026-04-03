use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringDictionaryBuilder};
use arrow::datatypes::{Int32Type, UInt32Type};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use raptrix_cim_rs::arrow_schema::{BRANDING, SCHEMA_VERSION, branch_schema};
use raptrix_cim_rs::models::base::IdentifiedObject;
use raptrix_cim_rs::parser;
use raptrix_cim_rs::rpf_writer::{
    BusResolutionMode, WriteOptions, read_rpf_tables, write_complete_rpf_with_options,
};
use raptrix_cim_rs::test_utils::get_external_cgmes_path;

#[test]
#[ignore = "requires RAPTRIX_TEST_DATA_ROOT pointing at the CGMES v3.0 data root"]
fn parse_smallgrid_eq_aclinesegment() -> Result<()> {
    // Now produces real Arrow output from live CGMES data.
    let Some(path) = get_external_cgmes_path("SmallGrid", "EQ") else {
        println!("Skipping: RAPTRIX_TEST_DATA_ROOT is not set.");
        return Ok(());
    };

    let file = File::open(&path)
        .with_context(|| format!("failed to open external CGMES file at {}", path.display()))?;
    let lines = parser::ac_line_segments_from_reader(BufReader::new(file)).with_context(|| {
        format!(
            "failed to parse ACLineSegment elements from {}",
            path.display()
        )
    })?;

    let file = File::open(&path)
        .with_context(|| format!("failed to reopen external CGMES file at {}", path.display()))?;
    let branch_rows =
        parser::branch_rows_from_eq_reader(BufReader::new(file)).with_context(|| {
            format!(
                "failed to map ACLineSegment + Terminal endpoint data from {}",
                path.display()
            )
        })?;

    println!(
        "Parsed {} ACLineSegment elements from {}",
        lines.len(),
        path.display()
    );
    if let Some(first) = lines.first() {
        println!(
            "First ACLineSegment: mRID={} r={:?} x={:?}",
            first.mrid(),
            first.r,
            first.x
        );
    }

    assert!(
        !lines.is_empty(),
        "expected at least one ACLineSegment in SmallGrid EQ"
    );
    assert!(
        !branch_rows.is_empty(),
        "expected at least one ACLineSegment with terminal endpoint data"
    );

    // Convert mapped branch rows into the Arrow branch schema columns.
    let from_bus: Vec<i32> = branch_rows.iter().map(|row| row.from_bus_id).collect();
    let to_bus: Vec<i32> = branch_rows.iter().map(|row| row.to_bus_id).collect();
    let branch_id: Vec<i32> = (1..=branch_rows.len() as i32).collect();
    let r: Vec<f64> = branch_rows.iter().map(|row| row.r).collect();
    let x: Vec<f64> = branch_rows.iter().map(|row| row.x).collect();
    let b_shunt: Vec<f64> = branch_rows.iter().map(|row| row.b_shunt).collect();
    let tap: Vec<f64> = vec![1.0; branch_rows.len()];
    let phase: Vec<f64> = vec![0.0; branch_rows.len()];
    let rate_a: Vec<f64> = vec![250.0; branch_rows.len()];
    let rate_b: Vec<f64> = vec![250.0; branch_rows.len()];
    let rate_c: Vec<f64> = vec![250.0; branch_rows.len()];
    let status: Vec<bool> = vec![true; branch_rows.len()];

    let mut ckt = StringDictionaryBuilder::<Int32Type>::new();
    let mut name = StringDictionaryBuilder::<UInt32Type>::new();
    for row in &branch_rows {
        ckt.append("1")?;
        name.append(row.line_mrid.as_str())?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(branch_id)) as ArrayRef,
        Arc::new(Int32Array::from(from_bus)) as ArrayRef,
        Arc::new(Int32Array::from(to_bus)) as ArrayRef,
        Arc::new(ckt.finish()) as ArrayRef,
        Arc::new(Float64Array::from(r)) as ArrayRef,
        Arc::new(Float64Array::from(x)) as ArrayRef,
        Arc::new(Float64Array::from(b_shunt)) as ArrayRef,
        Arc::new(Float64Array::from(tap)) as ArrayRef,
        Arc::new(Float64Array::from(phase)) as ArrayRef,
        Arc::new(Float64Array::from(rate_a)) as ArrayRef,
        Arc::new(Float64Array::from(rate_b)) as ArrayRef,
        Arc::new(Float64Array::from(rate_c)) as ArrayRef,
        Arc::new(BooleanArray::from(status)) as ArrayRef,
        Arc::new(name.finish()) as ArrayRef,
    ];

    let schema = Arc::new(branch_schema());
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    // Reuse the Raptrix file-level metadata so downstream readers can see the
    // branding and schema version in the Parquet footer.
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![
            KeyValue {
                key: "raptrix.branding".to_string(),
                value: Some(BRANDING.to_string()),
            },
            KeyValue {
                key: "raptrix.version".to_string(),
                value: Some(SCHEMA_VERSION.to_string()),
            },
        ]))
        .build();

    let output_path = "smallgrid_branches.parquet";
    let output = File::create(output_path)?;
    let mut writer = ArrowWriter::try_new(output, Arc::clone(&schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    println!(
        "✅ Wrote {} branches to smallgrid_branches.parquet",
        branch_rows.len()
    );
    println!(
        "   File size: {} bytes",
        std::fs::metadata(output_path)?.len()
    );

    Ok(())
}

#[test]
#[ignore = "requires RAPTRIX_TEST_DATA_ROOT or local CGMES SmallGrid data"]
fn write_smallgrid_rpf_with_optional_node_breaker_tables() -> Result<()> {
    let Some(eq_path) = get_external_cgmes_path("SmallGrid", "EQ") else {
        println!("Skipping: RAPTRIX_TEST_DATA_ROOT is not set.");
        return Ok(());
    };

    let tp_path = get_external_cgmes_path("SmallGrid", "TP");
    let output_path = std::env::temp_dir().join("smallgrid_node_breaker_optional.rpf");

    let eq_string = eq_path.to_string_lossy().into_owned();
    let mut owned_paths = vec![eq_string];
    if let Some(tp) = tp_path {
        owned_paths.push(tp.to_string_lossy().into_owned());
    }
    let path_refs: Vec<&str> = owned_paths.iter().map(String::as_str).collect();

    let summary = write_complete_rpf_with_options(
        &path_refs,
        output_path.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::Topological,
            emit_connectivity_groups: false,
            emit_node_breaker_detail: true,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;

    let tables = read_rpf_tables(&output_path)?;
    let table_names: Vec<&str> = tables.iter().map(|(name, _)| name.as_str()).collect();
    assert!(table_names.contains(&"node_breaker_detail"));
    assert!(table_names.contains(&"switch_detail"));
    assert!(table_names.contains(&"connectivity_nodes"));
    assert_eq!(summary.node_breaker_rows, summary.switch_detail_rows);

    Ok(())
}

#[test]
#[ignore = "requires local FullGrid external dataset"]
fn write_fullgrid_rpf_with_optional_node_breaker_tables() -> Result<()> {
    let fullgrid_merged = PathBuf::from(
        r"C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0\FullGrid\FullGrid-Merged",
    );
    let eq_path = fullgrid_merged.join("FullGrid_EQ.xml");
    let tp_path = fullgrid_merged.join("FullGrid_TP.xml");

    if !eq_path.is_file() {
        println!(
            "Skipping: FullGrid_EQ.xml not found at {}",
            eq_path.display()
        );
        return Ok(());
    }

    let output_path = std::env::temp_dir().join("fullgrid_node_breaker_optional.rpf");
    let mut owned_paths = vec![eq_path.to_string_lossy().into_owned()];
    if tp_path.is_file() {
        owned_paths.push(tp_path.to_string_lossy().into_owned());
    }
    let path_refs: Vec<&str> = owned_paths.iter().map(String::as_str).collect();

    let summary = write_complete_rpf_with_options(
        &path_refs,
        output_path.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::Topological,
            emit_connectivity_groups: false,
            emit_node_breaker_detail: true,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;

    let tables = read_rpf_tables(&output_path)?;
    let node_breaker_table = tables
        .iter()
        .find(|(name, _)| name == "node_breaker_detail")
        .map(|(_, batch)| batch)
        .context("expected node_breaker_detail table in FullGrid output")?;

    assert_eq!(node_breaker_table.num_rows(), summary.node_breaker_rows);
    assert!(summary.connectivity_node_rows > 0);

    Ok(())
}
