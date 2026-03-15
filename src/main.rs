// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Demo binary: build a power-flow RecordBatch and write it to Parquet.
//!
//! Run with:
//!   cargo run
//! Output: example_powerflow.parquet in the current working directory.

use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int8Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

// Re-use the schema definitions from our library crate.
use raptrix_cim_rs::arrow_schema::{branch_schema, powerflow_schema, BRANDING, SCHEMA_VERSION};

fn main() -> Result<()> {
    // ── Step 1: Build bus arrays (3 buses) ───────────────────────────────────
    //
    // Bus layout (Matpower / CIM convention):
    //   bus 1 = slack reference  (type 3)
    //   bus 2 = PV generator bus (type 2)
    //   bus 3 = PQ load bus      (type 1)
    //
    // Column order MUST match powerflow_schema() field order exactly.
    let bus_columns: Vec<ArrayRef> = vec![
        // bus_id
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        // type: 3 = slack, 2 = PV, 1 = PQ
        Arc::new(Int8Array::from(vec![3i8, 2, 1])) as ArrayRef,
        // p_sched (MW)
        Arc::new(Float64Array::from(vec![0.0, 100.0, -80.0])) as ArrayRef,
        // q_sched (MVAr)
        Arc::new(Float64Array::from(vec![0.0, 50.0, -20.0])) as ArrayRef,
        // v_mag_set (pu)
        Arc::new(Float64Array::from(vec![1.05, 1.02, 1.0])) as ArrayRef,
        // v_ang_set (radians) — all zero as starting guess
        Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0])) as ArrayRef,
        // q_min (MVAr)
        Arc::new(Float64Array::from(vec![-999.0, -50.0, 0.0])) as ArrayRef,
        // q_max (MVAr)
        Arc::new(Float64Array::from(vec![999.0, 50.0, 0.0])) as ArrayRef,
        // g_shunt (pu)
        Arc::new(Float64Array::from(vec![0.0, 0.0, 0.01])) as ArrayRef,
        // b_shunt (pu)
        Arc::new(Float64Array::from(vec![0.0, 0.0, 0.05])) as ArrayRef,
        // v_min (pu)
        Arc::new(Float64Array::from(vec![0.95, 0.95, 0.95])) as ArrayRef,
        // v_max (pu)
        Arc::new(Float64Array::from(vec![1.05, 1.05, 1.05])) as ArrayRef,
        // area
        Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef,
        // zone
        Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef,
        // name — nullable: bus 3 has no label in this dataset
        Arc::new(StringArray::from(vec![
            Some("Slack"),
            Some("GenBus"),
            None::<&str>,
        ])) as ArrayRef,
    ];

    // Wrap the schema in an Arc so it can be shared with the writer.
    let bus_schema = Arc::new(powerflow_schema());
    // RecordBatch validates lengths and types against the schema.
    let bus_batch = RecordBatch::try_new(Arc::clone(&bus_schema), bus_columns)?;

    // ── Step 2: Build branch arrays (2 branches) ─────────────────────────────
    //
    //   branch 1: bus 1 → bus 2  (line)
    //   branch 2: bus 2 → bus 3  (line)
    //
    // Column order MUST match branch_schema() field order exactly.
    let branch_columns: Vec<ArrayRef> = vec![
        // from
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        // to
        Arc::new(Int32Array::from(vec![2, 3])) as ArrayRef,
        // r — series resistance (pu)
        Arc::new(Float64Array::from(vec![0.01, 0.02])) as ArrayRef,
        // x — series reactance (pu)
        Arc::new(Float64Array::from(vec![0.10, 0.15])) as ArrayRef,
        // b_shunt — line charging susceptance (pu)
        Arc::new(Float64Array::from(vec![0.02, 0.03])) as ArrayRef,
        // tap — turns ratio (1.0 = nominal)
        Arc::new(Float64Array::from(vec![1.0, 1.0])) as ArrayRef,
        // phase — phase shift (radians)
        Arc::new(Float64Array::from(vec![0.0, 0.0])) as ArrayRef,
        // rate_a — MVA thermal limit
        Arc::new(Float64Array::from(vec![250.0, 200.0])) as ArrayRef,
        // status
        Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef,
    ];

    let br_schema = Arc::new(branch_schema());
    // Validate branch data against its schema (in a full implementation this
    // batch would be written to a separate table or row-group).
    let _branch_batch = RecordBatch::try_new(Arc::clone(&br_schema), branch_columns)?;

    // ── Step 3: Configure the Parquet writer with Raptrix branding metadata ──
    //
    // Key-value metadata is embedded in the Parquet file footer and is
    // readable by any Arrow/Parquet consumer without parsing the column data.
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![
            KeyValue {
                key:   "raptrix.branding".to_string(),
                value: Some(BRANDING.to_string()),
            },
            KeyValue {
                key:   "raptrix.version".to_string(),
                value: Some(SCHEMA_VERSION.to_string()),
            },
        ]))
        .build();

    // ── Step 4: Write the buses RecordBatch to a Parquet file ─────────────────
    let file = File::create("example_powerflow.parquet")?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&bus_schema), Some(props))?;
    writer.write(&bus_batch)?;
    // close() flushes all buffers and writes the Parquet footer.
    writer.close()?;

    println!("Wrote example_powerflow.parquet");
    Ok(())
}
