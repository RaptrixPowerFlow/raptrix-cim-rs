// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Case-health inspector tests (fixtures + optional PSS/E golden RPFs).

use std::path::{Path, PathBuf};

use raptrix_cim_arrow::{RpfHealthGrade, inspect_rpf_file};

fn psse_golden_dir() -> PathBuf {
    std::env::var("RAPTRIX_PSSE_GOLDEN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..")
                .join("raptrix-psse-rs")
                .join("tests")
                .join("golden")
        })
}

fn golden_rpf(name: &str) -> Option<PathBuf> {
    let path = psse_golden_dir().join(name);
    path.is_file().then_some(path)
}

fn assert_health_sane(path: &Path, expect_large: bool) {
    let health = inspect_rpf_file(path).unwrap_or_else(|e| {
        panic!("inspect_rpf_file({}) failed: {e:#}", path.display());
    });
    assert!(health.counts.buses > 0, "expected buses in {}", path.display());
    assert!(
        health.aggregates.base_mva > 0.0,
        "expected positive base_mva in {}",
        path.display()
    );
    assert!(
        !health.reasons.is_empty(),
        "expected at least one reason in {}",
        path.display()
    );
    if expect_large {
        assert!(
            health.counts.buses >= 1_000,
            "expected large case buses in {}",
            path.display()
        );
    }
}

#[test]
fn health_golden_ieee14() {
    let Some(path) = golden_rpf("ieee14.rpf").or_else(|| {
        std::fs::read_dir(psse_golden_dir())
            .ok()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.extension().map(|x| x == "rpf").unwrap_or(false)
                    && p.file_stem()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_ascii_lowercase().contains("ieee14"))
                        .unwrap_or(false)
            })
    }) else {
        eprintln!("skip health_golden_ieee14: no ieee14 RPF under psse golden dir");
        return;
    };
    assert_health_sane(&path, false);
    let health = inspect_rpf_file(&path).unwrap();
    assert!(health.counts.buses < 500);
}

#[test]
fn health_golden_texas2k() {
    let Some(path) = golden_rpf("Texas2k_series25_case1_summerpeak.rpf") else {
        eprintln!("skip health_golden_texas2k: golden RPF not present");
        return;
    };
    assert_health_sane(&path, true);
    let health = inspect_rpf_file(&path).unwrap();
    assert!(
        matches!(
            health.grade,
            RpfHealthGrade::Caution | RpfHealthGrade::Stressed | RpfHealthGrade::Healthy
        ),
        "grade={:?} reasons={:?}",
        health.grade,
        health.reasons
    );
}

#[test]
fn health_golden_nyiso() {
    let Some(path) = golden_rpf("NYISO_offpeak2019_v23.rpf") else {
        eprintln!("skip health_golden_nyiso: golden RPF not present");
        return;
    };
    assert_health_sane(&path, true);
    let health = inspect_rpf_file(&path).unwrap();
    assert!(health.topology.island_count >= 1);
}
