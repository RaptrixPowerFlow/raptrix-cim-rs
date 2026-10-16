// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validate one `.rpf` with [`raptrix_cim_arrow::validate_rpf_file`].
//! Optional tables are inferred from the root column names so a patched
//! export is checked against the contract it actually carries.

use std::env;
use std::process::ExitCode;

use raptrix_cim_arrow::{
    RootWriteOptions, TABLE_BUSES_SOLVED, TABLE_COMPUTATIONAL_LOAD_PROFILES,
    TABLE_CONTINGENCY_ISLAND_ANALYSIS, TABLE_CONTINGENCY_SEQUENCES, TABLE_DC_CONVERTERS,
    TABLE_DIAGRAM_OBJECTS, TABLE_FACTS_DEVICES, TABLE_FACTS_SOLVED, TABLE_NODE_BREAKER_DETAIL,
    TABLE_PROTECTION_CONTINGENCIES, TABLE_REMEDIAL_ACTION_SCHEMES, TABLE_TOPOLOGY_CHANGES,
    rpf_file_metadata, validate_rpf_file,
};

fn main() -> ExitCode {
    let Some(path) = env::args().nth(1) else {
        eprintln!("usage: validate_rpf <file.rpf>");
        return ExitCode::from(2);
    };
    let meta = match rpf_file_metadata(&path) {
        Ok(meta) => meta,
        Err(err) => {
            eprintln!("{err:#}");
            return ExitCode::from(1);
        }
    };
    let has = |name: &str| meta.contains_key(&format!("rpf.rows.{name}"));
    let feature = |key: &str| meta.get(key).map(String::as_str) == Some("true");
    let options = RootWriteOptions {
        include_node_breaker_detail: has(TABLE_NODE_BREAKER_DETAIL),
        include_diagram_layout: has(TABLE_DIAGRAM_OBJECTS),
        contingencies_are_stub: feature("raptrix.features.contingencies_stub"),
        dynamics_are_stub: feature("raptrix.features.dynamics_stub"),
        include_solved_state: has(TABLE_BUSES_SOLVED),
        include_facts_devices: has(TABLE_FACTS_DEVICES),
        include_facts_solved: has(TABLE_FACTS_SOLVED),
        include_computational_load_profiles: has(TABLE_COMPUTATIONAL_LOAD_PROFILES),
        include_protection_contingencies: has(TABLE_PROTECTION_CONTINGENCIES),
        include_topology_changes: has(TABLE_TOPOLOGY_CHANGES),
        include_remedial_action_schemes: has(TABLE_REMEDIAL_ACTION_SCHEMES),
        include_contingency_island_analysis: has(TABLE_CONTINGENCY_ISLAND_ANALYSIS),
        include_contingency_sequences: has(TABLE_CONTINGENCY_SEQUENCES),
        include_dc_converters: has(TABLE_DC_CONVERTERS),
    };
    if let Err(err) = validate_rpf_file(&path, &options) {
        eprintln!("{err:#}");
        return ExitCode::from(1);
    }
    println!("validated {path}");
    ExitCode::SUCCESS
}
