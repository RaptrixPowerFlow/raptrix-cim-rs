// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Case-health inspector tests (fixtures + optional PSS/E golden RPFs).

use std::path::{Path, PathBuf};

use raptrix_cim_arrow::{
    METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, RpfHealthGrade, SUPPORTED_RPF_VERSIONS,
    inspect_rpf_file, rpf_file_metadata,
};

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

fn skip_unsupported_contract(path: &Path) -> bool {
    let metadata = rpf_file_metadata(path).unwrap_or_else(|e| {
        panic!("rpf_file_metadata({}) failed: {e:#}", path.display());
    });
    let version = metadata
        .get(METADATA_KEY_RPF_VERSION)
        .or_else(|| metadata.get(METADATA_KEY_VERSION));
    if let Some(version) = version
        && !SUPPORTED_RPF_VERSIONS.contains(&version.as_str())
    {
        eprintln!(
            "skip {}: contract version {} not supported (re-export as v0.12.3)",
            path.display(),
            version
        );
        return true;
    }
    false
}

fn assert_health_sane(path: &Path, expect_large: bool) {
    if skip_unsupported_contract(path) {
        return;
    }
    let health = inspect_rpf_file(path).unwrap_or_else(|e| {
        panic!("inspect_rpf_file({}) failed: {e:#}", path.display());
    });
    assert!(
        health.counts.buses > 0,
        "expected buses in {}",
        path.display()
    );
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

/// Optional gate against any locally available large converted case.
///
/// Scans the golden directory (or `RAPTRIX_PSSE_GOLDEN_DIR`) for the first
/// readable `.rpf` with at least 1,000 buses; skips silently when none exist,
/// so CI without local fixtures stays green.
#[test]
fn health_golden_large_external() {
    let Ok(entries) = std::fs::read_dir(psse_golden_dir()) else {
        eprintln!("skip health_golden_large_external: golden dir not present");
        return;
    };
    let mut candidates: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "rpf").unwrap_or(false))
        .collect();
    candidates.sort();
    let large = candidates.into_iter().find(|path| {
        if skip_unsupported_contract(path) {
            return false;
        }
        inspect_rpf_file(path)
            .map(|h| h.counts.buses >= 1_000)
            .unwrap_or(false)
    });
    let Some(path) = large else {
        eprintln!("skip health_golden_large_external: no large supported RPF present");
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
    assert!(health.topology.island_count >= 1);
}
