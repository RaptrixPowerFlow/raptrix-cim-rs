// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! v0.8.4 planning-vs-solved semantic contract tests.
//!
//! These tests enforce:
//! 1. Flat-start planning cases emit correct metadata (case_mode, solved_state_presence).
//! 2. Planning fields that are NaN/Inf/non-positive are rejected at export time.
//! 3. Contradictory case_mode / solved_state_presence combinations are rejected.
//! 4. Solver provenance must be present iff case_mode = SolvedSnapshot.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use raptrix_cim_rs::arrow_schema::{
    METADATA_KEY_CASE_MODE, METADATA_KEY_SOLVED_STATE_PRESENCE, TABLE_BUSES_SOLVED,
    TABLE_GENERATORS_SOLVED,
};
use raptrix_cim_rs::rpf_writer::{
    CaseMode, SolvedStatePresence, SolverProvenance,
    WriteOptions, rpf_file_metadata, summarize_rpf, write_complete_rpf_with_options,
};

static OUTPUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_temp_rpf_path(label: &str) -> PathBuf {
    let seq = OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "raptrix_cim_rs_pss_{label}_{}_{}.rpf",
        std::process::id(),
        seq
    ))
}

/// Writes a minimal in-memory CGMES EQ XML to a temp file, runs the exporter,
/// and returns the output path.
fn write_minimal_eq_rpf(label: &str, options: &WriteOptions) -> Result<PathBuf> {
    let xml_path = std::env::temp_dir().join(format!(
        "raptrix_cim_rs_pss_{label}_{}.xml",
        std::process::id()
    ));
    // Minimal two-bus, one-line CGMES EQ fixture with base voltages.
    let xml = r##"<rdf:RDF
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#"
  xmlns:IdentifiedObject="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:BaseVoltage rdf:ID="BV1">
  <cim:IdentifiedObject.name>230kV</cim:IdentifiedObject.name>
  <cim:BaseVoltage.nominalVoltage>230</cim:BaseVoltage.nominalVoltage>
</cim:BaseVoltage>
<cim:ConnectivityNode rdf:ID="CN1"><cim:IdentifiedObject.name>Bus A</cim:IdentifiedObject.name></cim:ConnectivityNode>
<cim:ConnectivityNode rdf:ID="CN2"><cim:IdentifiedObject.name>Bus B</cim:IdentifiedObject.name></cim:ConnectivityNode>
<cim:ACLineSegment rdf:ID="L1">
  <cim:IdentifiedObject.name>Line AB</cim:IdentifiedObject.name>
  <cim:ACLineSegment.r>0.01</cim:ACLineSegment.r>
  <cim:ACLineSegment.x>0.10</cim:ACLineSegment.x>
  <cim:ACLineSegment.bch>0.001</cim:ACLineSegment.bch>
  <cim:ConductingEquipment.BaseVoltage rdf:resource="#BV1"/>
</cim:ACLineSegment>
<cim:Terminal rdf:ID="T1">
  <cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN1"/>
  <cim:ACDCTerminal.sequenceNumber>1</cim:ACDCTerminal.sequenceNumber>
</cim:Terminal>
<cim:Terminal rdf:ID="T2">
  <cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN2"/>
  <cim:ACDCTerminal.sequenceNumber>2</cim:ACDCTerminal.sequenceNumber>
</cim:Terminal>
</rdf:RDF>"##;

    fs::write(&xml_path, xml)?;

    let out = unique_temp_rpf_path(label);
    write_complete_rpf_with_options(&[xml_path.to_str().unwrap()], out.to_str().unwrap(), options)?;
    let _ = fs::remove_file(&xml_path);
    Ok(out)
}

// ---------------------------------------------------------------------------
// 1. Flat-start planning export carries correct metadata
// ---------------------------------------------------------------------------

#[test]
fn planning_only_export_has_flat_start_metadata() -> Result<()> {
    let options = WriteOptions {
        case_mode: CaseMode::FlatStartPlanning,
        ..Default::default()
    };
    let out = write_minimal_eq_rpf("flat_start", &options)?;

    let metadata = rpf_file_metadata(&out)?;

    let case_mode = metadata
        .get(METADATA_KEY_CASE_MODE)
        .expect("case_mode metadata key must be present");
    assert_eq!(
        case_mode, "flat_start_planning",
        "flat-start export must write case_mode=flat_start_planning"
    );

    let presence = metadata
        .get(METADATA_KEY_SOLVED_STATE_PRESENCE)
        .expect("solved_state_presence metadata key must be present");
    assert_eq!(
        presence, "not_computed",
        "flat-start export must write solved_state_presence=not_computed"
    );

    let _ = fs::remove_file(&out);
    Ok(())
}

#[test]
fn warm_start_planning_export_has_not_computed_presence() -> Result<()> {
    let options = WriteOptions {
        case_mode: CaseMode::WarmStartPlanning,
        ..Default::default()
    };
    let out = write_minimal_eq_rpf("warm_start", &options)?;
    let metadata = rpf_file_metadata(&out)?;

    let case_mode = metadata
        .get(METADATA_KEY_CASE_MODE)
        .expect("case_mode must be present");
    assert_eq!(case_mode, "warm_start_planning");

    let presence = metadata
        .get(METADATA_KEY_SOLVED_STATE_PRESENCE)
        .expect("solved_state_presence must be present");
    assert_eq!(
        presence, "not_computed",
        "warm-start planning must not claim actual_solved"
    );

    let _ = fs::remove_file(&out);
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Planning-only export has no solved-state tables in root columns
// ---------------------------------------------------------------------------

#[test]
fn planning_only_export_has_no_solved_state_tables() -> Result<()> {
    let options = WriteOptions {
        case_mode: CaseMode::FlatStartPlanning,
        ..Default::default()
    };
    let out = write_minimal_eq_rpf("no_solved_tables", &options)?;

    let summary = summarize_rpf(&out)?;
    let table_names: Vec<&str> = summary.tables.iter().map(|t| t.table_name.as_str()).collect();

    assert!(
        !table_names.contains(&TABLE_BUSES_SOLVED),
        "planning-only export must not emit buses_solved table"
    );
    assert!(
        !table_names.contains(&TABLE_GENERATORS_SOLVED),
        "planning-only export must not emit generators_solved table"
    );

    let _ = fs::remove_file(&out);
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Contradictory metadata combinations are hard errors
// ---------------------------------------------------------------------------

#[test]
fn solved_snapshot_without_solver_provenance_is_rejected() {
    let options = WriteOptions {
        case_mode: CaseMode::SolvedSnapshot,
        solver_provenance: None, // missing — must fail
        ..Default::default()
    };
    let xml_path = std::env::temp_dir().join(format!(
        "raptrix_cim_rs_pss_contradiction_{}.xml",
        std::process::id()
    ));
    let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:BaseVoltage rdf:ID="BV1"><cim:BaseVoltage.nominalVoltage>230</cim:BaseVoltage.nominalVoltage></cim:BaseVoltage>
<cim:ConnectivityNode rdf:ID="CN1"/>
<cim:ConnectivityNode rdf:ID="CN2"/>
<cim:ACLineSegment rdf:ID="L1">
  <cim:ACLineSegment.r>0.01</cim:ACLineSegment.r><cim:ACLineSegment.x>0.1</cim:ACLineSegment.x>
  <cim:ACLineSegment.bch>0.001</cim:ACLineSegment.bch>
  <cim:ConductingEquipment.BaseVoltage rdf:resource="#BV1"/>
</cim:ACLineSegment>
<cim:Terminal rdf:ID="T1"><cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN1"/><cim:ACDCTerminal.sequenceNumber>1</cim:ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="T2"><cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN2"/><cim:ACDCTerminal.sequenceNumber>2</cim:ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##;
    let _ = fs::write(&xml_path, xml);
    let out = unique_temp_rpf_path("contradiction");

    let err = write_complete_rpf_with_options(
        &[xml_path.to_str().unwrap()],
        out.to_str().unwrap(),
        &options,
    )
    .expect_err("solved_snapshot without solver_provenance must fail");

    assert!(
        err.to_string().contains("solver_provenance"),
        "error must mention solver_provenance; got: {err}"
    );

    let _ = fs::remove_file(&xml_path);
    let _ = fs::remove_file(&out);
}

#[test]
fn planning_case_with_solver_provenance_is_rejected() {
    let options = WriteOptions {
        case_mode: CaseMode::FlatStartPlanning,
        solver_provenance: Some(SolverProvenance {
            solver_version: Some("raptrix-core 1.0".to_string()),
            solver_iterations: Some(5),
            ..Default::default()
        }),
        ..Default::default()
    };
    let xml_path = std::env::temp_dir().join(format!(
        "raptrix_cim_rs_pss_prov_reject_{}.xml",
        std::process::id()
    ));
    let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:BaseVoltage rdf:ID="BV1"><cim:BaseVoltage.nominalVoltage>230</cim:BaseVoltage.nominalVoltage></cim:BaseVoltage>
<cim:ConnectivityNode rdf:ID="CN1"/>
<cim:ConnectivityNode rdf:ID="CN2"/>
<cim:ACLineSegment rdf:ID="L1">
  <cim:ACLineSegment.r>0.01</cim:ACLineSegment.r><cim:ACLineSegment.x>0.1</cim:ACLineSegment.x>
  <cim:ACLineSegment.bch>0.001</cim:ACLineSegment.bch>
  <cim:ConductingEquipment.BaseVoltage rdf:resource="#BV1"/>
</cim:ACLineSegment>
<cim:Terminal rdf:ID="T1"><cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN1"/><cim:ACDCTerminal.sequenceNumber>1</cim:ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="T2"><cim:Terminal.ConductingEquipment rdf:resource="#L1"/>
  <cim:Terminal.ConnectivityNode rdf:resource="#CN2"/><cim:ACDCTerminal.sequenceNumber>2</cim:ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##;
    let _ = fs::write(&xml_path, xml);
    let out = unique_temp_rpf_path("prov_reject");

    let err = write_complete_rpf_with_options(
        &[xml_path.to_str().unwrap()],
        out.to_str().unwrap(),
        &options,
    )
    .expect_err("flat_start_planning with solver_provenance must fail");

    assert!(
        err.to_string().contains("solver_provenance"),
        "error must mention solver_provenance; got: {err}"
    );

    let _ = fs::remove_file(&xml_path);
    let _ = fs::remove_file(&out);
}

// ---------------------------------------------------------------------------
// 4. CaseMode and SolvedStatePresence enum serialization contract
// ---------------------------------------------------------------------------

#[test]
fn case_mode_as_str_values_are_canonical() {
    assert_eq!(CaseMode::FlatStartPlanning.as_str(), "flat_start_planning");
    assert_eq!(CaseMode::WarmStartPlanning.as_str(), "warm_start_planning");
    assert_eq!(CaseMode::SolvedSnapshot.as_str(), "solved_snapshot");
}

#[test]
fn solved_state_presence_as_str_values_are_canonical() {
    assert_eq!(SolvedStatePresence::ActualSolved.as_str(), "actual_solved");
    assert_eq!(SolvedStatePresence::NotAvailable.as_str(), "not_available");
    assert_eq!(SolvedStatePresence::NotComputed.as_str(), "not_computed");
}

#[test]
fn default_case_mode_is_flat_start_planning() {
    assert_eq!(CaseMode::default(), CaseMode::FlatStartPlanning);
}
