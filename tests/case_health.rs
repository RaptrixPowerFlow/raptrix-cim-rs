// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Case-health inspector on CGMES fixture exports.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use raptrix_cim_rs::inspect_rpf_file;
use raptrix_cim_rs::rpf_writer::{WriteOptions, write_complete_rpf_with_options};
use raptrix_cim_rs::{RpfHealthGrade, rpf_file_metadata};

static OUTPUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_temp_rpf(label: &str) -> PathBuf {
    let seq = OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "raptrix_health_{}_{}_{}.rpf",
        label,
        std::process::id(),
        seq
    ))
}

fn first_convertible_fixture_eq() -> Option<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&base)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("xml"))
        .filter(|p| {
            let text = std::fs::read_to_string(p).unwrap_or_default();
            (text.contains("Terminal.ConductingEquipment") || text.contains("<cim:Terminal"))
                && text.contains("BaseVoltage.nominalVoltage")
                && text.contains("ConductingEquipment.BaseVoltage")
        })
        .collect();
    paths.sort();
    paths.into_iter().next()
}

#[test]
fn health_minimal_fixture_export() -> Result<()> {
    let Some(eq) = first_convertible_fixture_eq() else {
        eprintln!("skip health_minimal_fixture_export: no convertible fixture");
        return Ok(());
    };

    let out = unique_temp_rpf("fixture");
    write_complete_rpf_with_options(
        &[eq.to_string_lossy().as_ref()],
        out.to_str().unwrap(),
        &WriteOptions::default(),
    )?;

    let health = inspect_rpf_file(&out)?;
    assert!(health.counts.buses > 0);
    assert!(!health.reasons.is_empty());
    assert!(
        !matches!(health.grade, RpfHealthGrade::Pathological),
        "grade={:?} reasons={:?}",
        health.grade,
        health.reasons
    );
    assert!(rpf_file_metadata(&out)?.contains_key("rpf.case_mode"));
    let _ = std::fs::remove_file(out);
    Ok(())
}
