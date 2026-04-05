use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use arrow::array::{Array, DictionaryArray, MapArray, StringArray};
use arrow::datatypes::Int32Type;
use raptrix_cim_rs::arrow_schema::{
    SCHEMA_VERSION, SUPPORTED_RPF_VERSIONS, TABLE_CONNECTIVITY_GROUPS, TABLE_CONNECTIVITY_NODES,
    TABLE_DYNAMICS_MODELS,
    TABLE_DIAGRAM_OBJECTS, TABLE_DIAGRAM_POINTS, TABLE_NODE_BREAKER_DETAIL, TABLE_SWITCH_DETAIL,
};
use raptrix_cim_rs::rpf_writer::{
    BusResolutionMode, DetachedIslandPolicy, WriteOptions, read_rpf_tables, rpf_file_metadata,
    summarize_rpf,
    write_complete_rpf_with_options,
};

static OUTPUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixture_path(relative: &str) -> PathBuf {
    repo_root().join(relative)
}

fn unique_temp_rpf_path(label: &str) -> PathBuf {
    let seq = OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("raptrix_cim_rs_{label}_{}_{}.rpf", std::process::id(), seq))
}

fn write_eq_fixture_with_breaker() -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "raptrix_cim_rs_detail_fixture_{}_{}.xml",
        std::process::id(),
        OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
<cim:ConnectivityNode rdf:ID="N1" />
<cim:ConnectivityNode rdf:ID="N2" />
<cim:ACLineSegment rdf:ID="L1"><IdentifiedObject.name>Line 1</IdentifiedObject.name><ACLineSegment.r>0.01</ACLineSegment.r><ACLineSegment.x>0.05</ACLineSegment.x><ACLineSegment.bch>0.001</ACLineSegment.bch></cim:ACLineSegment>
<cim:Terminal rdf:ID="LT1"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="LT2"><Terminal.ConductingEquipment rdf:resource="#L1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Breaker rdf:ID="BR1"><IdentifiedObject.name>Breaker 1</IdentifiedObject.name><Switch.open>false</Switch.open><Switch.normalOpen>false</Switch.normalOpen><Switch.retained>true</Switch.retained></cim:Breaker>
<cim:Terminal rdf:ID="BT1"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N1"/><ACDCTerminal.sequenceNumber>1</ACDCTerminal.sequenceNumber></cim:Terminal>
<cim:Terminal rdf:ID="BT2"><Terminal.ConductingEquipment rdf:resource="#BR1"/><Terminal.ConnectivityNode rdf:resource="#N2"/><ACDCTerminal.sequenceNumber>2</ACDCTerminal.sequenceNumber></cim:Terminal>
</rdf:RDF>"##;
    fs::write(&path, xml)?;
    Ok(path)
}

fn table_names(path: &Path) -> Result<Vec<String>> {
    Ok(summarize_rpf(path)?.tables.into_iter().map(|t| t.table_name).collect())
}

fn dictionary_value_at_int32(array: &DictionaryArray<Int32Type>, row: usize) -> String {
    let value_index = array.keys().value(row) as usize;
    array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dictionary values should be StringArray")
        .value(value_index)
        .to_string()
}

#[test]
fn legacy_v060_fixtures_are_rejected_with_clear_error() {
    let mut observed_any_fixture = false;
    for file_name in ["realgrid_rpf_v0.6.0.rpf", "realgrid_rpf_v0.6.0_node_breaker.rpf"] {
        let path = fixture_path(&format!("tests/data/external/{file_name}"));
        if !path.is_file() {
            continue;
        }
        observed_any_fixture = true;

        let err_text = summarize_rpf(&path)
            .expect_err("v0.6.0 fixture should be rejected")
            .to_string();

        assert!(
            err_text.contains("unsupported RPF version '0.6.0'"),
            "error should mention unsupported v0.6.0 for {} but was: {}",
            file_name,
            err_text
        );
    }
    if !observed_any_fixture {
        eprintln!("Skipping legacy fixture assertions: no v0.6.0 fixtures found in tests/data/external");
    }
}

#[test]
fn supported_external_v3_fixtures_match_expected_optional_tables() -> Result<()> {
    let cases = [
        ("smallgrid_v3.0_eq.rpf", false),
        ("smallgrid_v3.0_eq_tp.rpf", false),
        ("smallgrid_v3.0_eq_tp_dl.rpf", true),
        ("realgrid_v3.0_eq.rpf", false),
        ("realgrid_v3.0_eq_tp.rpf", false),
        ("fullgrid_v3.0_eq.rpf", false),
        ("smallgrid_v3.0_full.rpf", false),
    ];

    for (file_name, expects_diagram_tables) in cases {
        let path = fixture_path(&format!("tests/data/external/{file_name}"));
        if !path.is_file() {
            continue;
        }
        let summary = summarize_rpf(&path)
            .with_context(|| format!("failed to summarize fixture {}", path.display()))?;
        let metadata = rpf_file_metadata(&path)
            .with_context(|| format!("failed to read metadata from {}", path.display()))?;

        let version = metadata
            .get("rpf_version")
            .or_else(|| metadata.get("raptrix.version"))
            .context("missing rpf_version metadata")?;
        assert!(
            SUPPORTED_RPF_VERSIONS.contains(&version.as_str()),
            "fixture {} should stay on a supported version, got {}",
            file_name,
            version
        );

        let names: Vec<&str> = summary.tables.iter().map(|table| table.table_name.as_str()).collect();
        assert_eq!(
            names.contains(&TABLE_CONNECTIVITY_GROUPS),
            false,
            "fixture {} should not include connectivity_groups",
            file_name
        );
        assert_eq!(
            names.contains(&TABLE_NODE_BREAKER_DETAIL),
            false,
            "fixture {} should not include node_breaker_detail",
            file_name
        );
        assert_eq!(
            names.contains(&TABLE_SWITCH_DETAIL),
            false,
            "fixture {} should not include switch_detail",
            file_name
        );
        assert_eq!(
            names.contains(&TABLE_CONNECTIVITY_NODES),
            false,
            "fixture {} should not include connectivity_nodes",
            file_name
        );

        assert_eq!(
            names.contains(&TABLE_DIAGRAM_OBJECTS),
            expects_diagram_tables,
            "fixture {} diagram_objects presence mismatch",
            file_name
        );
        assert_eq!(
            names.contains(&TABLE_DIAGRAM_POINTS),
            expects_diagram_tables,
            "fixture {} diagram_points presence mismatch",
            file_name
        );
    }

    // Treat external fixture assertions as optional in repositories where
    // those binary fixture files are not checked in.
    if cases
        .iter()
        .all(|(file_name, _)| !fixture_path(&format!("tests/data/external/{file_name}")).is_file())
    {
        eprintln!("Skipping external v3 fixture assertions: no matching .rpf fixtures found");
    }

    Ok(())
}

#[test]
fn generated_v080_outputs_make_detail_levels_explicit() -> Result<()> {
    let eq_path = write_eq_fixture_with_breaker()?;
    let eq_owned = eq_path.to_string_lossy().into_owned();
    let inputs = vec![eq_owned.as_str()];

    let topological_output = unique_temp_rpf_path("detail_topological");
    let topological_summary = write_complete_rpf_with_options(
        &inputs,
        topological_output.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::Topological,
            detached_island_policy: DetachedIslandPolicy::Permissive,
            emit_connectivity_groups: false,
            emit_node_breaker_detail: false,
            emit_diagram_layout: true,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;
    let topological_tables = table_names(&topological_output)?;
    assert_eq!(topological_tables.len(), 15);
    assert!(!topological_tables.iter().any(|name| name == TABLE_CONNECTIVITY_GROUPS));
    assert!(!topological_tables.iter().any(|name| name == TABLE_NODE_BREAKER_DETAIL));
    assert!(!topological_tables.iter().any(|name| name == TABLE_SWITCH_DETAIL));
    assert!(!topological_tables.iter().any(|name| name == TABLE_CONNECTIVITY_NODES));
    assert!(!topological_tables.iter().any(|name| name == TABLE_DIAGRAM_OBJECTS));
    assert!(!topological_tables.iter().any(|name| name == TABLE_DIAGRAM_POINTS));
    assert_eq!(topological_summary.connectivity_groups_rows, 0);
    assert_eq!(topological_summary.node_breaker_rows, 1);

    let connectivity_output = unique_temp_rpf_path("detail_connectivity");
    let connectivity_summary = write_complete_rpf_with_options(
        &inputs,
        connectivity_output.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::ConnectivityDetail,
            detached_island_policy: DetachedIslandPolicy::Permissive,
            emit_connectivity_groups: true,
            emit_node_breaker_detail: false,
            emit_diagram_layout: true,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;
    let connectivity_tables = table_names(&connectivity_output)?;
    assert_eq!(connectivity_tables.len(), 15);
    assert!(!connectivity_tables.iter().any(|name| name == TABLE_CONNECTIVITY_GROUPS));
    assert!(!connectivity_tables.iter().any(|name| name == TABLE_NODE_BREAKER_DETAIL));
    assert!(!connectivity_tables.iter().any(|name| name == TABLE_SWITCH_DETAIL));
    assert!(!connectivity_tables.iter().any(|name| name == TABLE_CONNECTIVITY_NODES));
    assert!(
        connectivity_summary.final_bus_count >= topological_summary.final_bus_count,
        "connectivity detail should not reduce bus count"
    );

    let node_breaker_output = unique_temp_rpf_path("detail_node_breaker");
    let node_breaker_summary = write_complete_rpf_with_options(
        &inputs,
        node_breaker_output.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::ConnectivityDetail,
            detached_island_policy: DetachedIslandPolicy::Permissive,
            emit_connectivity_groups: true,
            emit_node_breaker_detail: true,
            emit_diagram_layout: true,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;
    let node_breaker_tables = table_names(&node_breaker_output)?;
    assert_eq!(node_breaker_tables.len(), 18);
    assert!(!node_breaker_tables.iter().any(|name| name == TABLE_CONNECTIVITY_GROUPS));
    assert!(node_breaker_tables.iter().any(|name| name == TABLE_NODE_BREAKER_DETAIL));
    assert!(node_breaker_tables.iter().any(|name| name == TABLE_SWITCH_DETAIL));
    assert!(node_breaker_tables.iter().any(|name| name == TABLE_CONNECTIVITY_NODES));
    assert_eq!(
        node_breaker_summary.node_breaker_rows,
        node_breaker_summary.switch_detail_rows,
        "node breaker and switch detail rows should stay aligned"
    );
    assert_eq!(
        node_breaker_summary.connectivity_node_rows,
        node_breaker_summary.final_bus_count,
        "connectivity-detail mode should expose one connectivity_nodes row per final bus in this fixture"
    );

    for output in [topological_output, connectivity_output, node_breaker_output] {
        let metadata = rpf_file_metadata(&output)?;
        assert_eq!(metadata.get("rpf_version"), Some(&SCHEMA_VERSION.to_string()));
        assert_eq!(metadata.get("raptrix.version"), Some(&SCHEMA_VERSION.to_string()));
        let _ = fs::remove_file(output);
    }
    let _ = fs::remove_file(eq_path);

    Ok(())
}

#[test]
fn smart_valve_custom_dy_model_round_trips_into_dynamics_models() -> Result<()> {
    let eq_path = fixture_path("tests/data/fixtures/smart_valve_demo_EQ.xml");
    let dy_path = fixture_path("tests/data/fixtures/smart_valve_demo_DY.xml");
    let output_path = unique_temp_rpf_path("smart_valve_custom_dy");

    let eq_owned = eq_path.to_string_lossy().into_owned();
    let dy_owned = dy_path.to_string_lossy().into_owned();
    let inputs = vec![eq_owned.as_str(), dy_owned.as_str()];

    let summary = write_complete_rpf_with_options(
        &inputs,
        output_path.to_string_lossy().as_ref(),
        &WriteOptions {
            bus_resolution_mode: BusResolutionMode::Topological,
            detached_island_policy: DetachedIslandPolicy::Permissive,
            emit_connectivity_groups: false,
            emit_node_breaker_detail: false,
            emit_diagram_layout: false,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: 100.0,
            frequency_hz: 60.0,
            study_name: None,
            timestamp_utc: None,
        },
    )?;

    assert!(
        summary.dynamics_rows_dy_linked > 0,
        "expected at least one DY-linked dynamics row"
    );

    let tables = read_rpf_tables(&output_path)?;
    let (_, dynamics_batch) = tables
        .iter()
        .find(|(name, _)| name == TABLE_DYNAMICS_MODELS)
        .context("missing dynamics_models table")?;

    let model_type_col = dynamics_batch
        .column(2)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .context("dynamics_models.model_type should be Dictionary<Int32, Utf8>")?;
    let params_col = dynamics_batch
        .column(3)
        .as_any()
        .downcast_ref::<MapArray>()
        .context("dynamics_models.params should be Map<String, Float64>")?;

    let mut found_custom_type = false;
    let mut found_source_dy = false;
    for row in 0..dynamics_batch.num_rows() {
        let model_type = dictionary_value_at_int32(model_type_col, row);
        if model_type == "raptrix.smart_valve.v1" {
            found_custom_type = true;
            if params_col.value_length(row) > 0 {
                let entries = params_col.value(row);
                let entry_keys = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("map key column should be StringArray")?;
                for idx in 0..entry_keys.len() {
                    if entry_keys.value(idx) == "source_dy" {
                        found_source_dy = true;
                        break;
                    }
                }
            }
        }
    }

    assert!(
        found_custom_type,
        "expected raptrix.smart_valve.v1 in dynamics_models.model_type"
    );
    assert!(
        found_source_dy,
        "expected source_dy provenance key for custom DY model row"
    );

    let _ = fs::remove_file(output_path);
    Ok(())
}
