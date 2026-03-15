use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use raptrix_cim_rs::models::base::IdentifiedObject;
use raptrix_cim_rs::test_utils::get_external_cgmes_path;
use raptrix_cim_rs::arrow_schema::{branch_schema, BRANDING, SCHEMA_VERSION};
use raptrix_cim_rs::parser;

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
    let lines = parser::ac_line_segments_from_reader(BufReader::new(file))
        .with_context(|| format!("failed to parse ACLineSegment elements from {}", path.display()))?;

    println!("Parsed {} ACLineSegment elements from {}", lines.len(), path.display());
    if let Some(first) = lines.first() {
        println!("First ACLineSegment: mRID={} r={:?} x={:?}", first.mrid(), first.r, first.x);
    }

    assert!(!lines.is_empty(), "expected at least one ACLineSegment in SmallGrid EQ");

    // Convert the parsed CGMES lines into the branch table expected by the
    // Arrow schema. For now we synthesize from/to bus identifiers from the
    // row index so the live dataset can flow through the full writer path.
    let from_bus: Vec<i32> = (0..lines.len()).map(|index| 1001 + index as i32).collect();
    let to_bus: Vec<i32> = (0..lines.len()).map(|index| 2001 + index as i32).collect();
    let r: Vec<f64> = lines.iter().map(|line| line.r.unwrap_or(0.0)).collect();
    let x: Vec<f64> = lines.iter().map(|line| line.x.unwrap_or(0.0)).collect();
    let b_shunt: Vec<f64> = lines.iter().map(|line| line.bch.unwrap_or(0.0)).collect();
    let tap: Vec<f64> = vec![1.0; lines.len()];
    let phase: Vec<f64> = vec![0.0; lines.len()];
    let rate_a: Vec<f64> = vec![250.0; lines.len()];
    let status: Vec<bool> = vec![true; lines.len()];

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(from_bus)) as ArrayRef,
        Arc::new(Int32Array::from(to_bus)) as ArrayRef,
        Arc::new(Float64Array::from(r)) as ArrayRef,
        Arc::new(Float64Array::from(x)) as ArrayRef,
        Arc::new(Float64Array::from(b_shunt)) as ArrayRef,
        Arc::new(Float64Array::from(tap)) as ArrayRef,
        Arc::new(Float64Array::from(phase)) as ArrayRef,
        Arc::new(Float64Array::from(rate_a)) as ArrayRef,
        Arc::new(BooleanArray::from(status)) as ArrayRef,
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

    println!("✅ Wrote {} branches to smallgrid_branches.parquet", lines.len());
    println!(
        "   File size: {} bytes",
        std::fs::metadata(output_path)?.len()
    );

    Ok(())
}