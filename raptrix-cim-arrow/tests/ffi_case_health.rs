// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! C ABI round-trip tests for case health inspection.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::{Path, PathBuf};

use raptrix_cim_arrow::ffi::{
    RPF_HEALTH_HEALTHY, RPF_HEALTH_PATHOLOGICAL, RPF_HEALTH_STRESSED, RpfCaseHealthFFI,
};
use raptrix_cim_arrow::{
    METADATA_KEY_RPF_VERSION, METADATA_KEY_VERSION, SUPPORTED_RPF_VERSIONS, rpf_file_metadata,
};

unsafe extern "C" {
    fn inspect_rpf_file_c(path: *const c_char) -> *mut RpfCaseHealthFFI;
    fn free_rpf_case_health_ffi(ptr: *mut RpfCaseHealthFFI);
    fn inspect_rpf_file_c_json(path: *const c_char) -> *mut c_char;
    fn free_rpf_case_health_json(json: *mut c_char);
    fn rpf_case_health_last_error() -> *const c_char;
}

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

fn contract_supported(path: &Path) -> bool {
    let metadata = rpf_file_metadata(path).unwrap_or_else(|e| {
        panic!("rpf_file_metadata({}) failed: {e:#}", path.display());
    });
    let version = metadata
        .get(METADATA_KEY_RPF_VERSION)
        .or_else(|| metadata.get(METADATA_KEY_VERSION));
    version.is_some_and(|version| SUPPORTED_RPF_VERSIONS.contains(&version.as_str()))
}

fn first_golden_rpf() -> Option<PathBuf> {
    let mut candidates = golden_rpf("ieee14.rpf").into_iter().chain(
        std::fs::read_dir(psse_golden_dir())
            .ok()
            .into_iter()
            .flat_map(|entries| entries.filter_map(|e| e.ok()).map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "rpf").unwrap_or(false)),
    );
    candidates.find(|path| contract_supported(path))
}

unsafe fn last_error_message() -> String {
    unsafe {
        CStr::from_ptr(rpf_case_health_last_error())
            .to_string_lossy()
            .into_owned()
    }
}

#[test]
fn ffi_inspect_roundtrip() {
    let Some(path) = first_golden_rpf() else {
        eprintln!("skip ffi_inspect_roundtrip: no v0.12.1 golden RPF present");
        return;
    };
    run_ffi_roundtrip(&path);
}

#[test]
fn ffi_inspect_invalid_path() {
    let bad = CString::new("definitely-not-an-rpf-file.rpf").unwrap();
    unsafe {
        let health = inspect_rpf_file_c(bad.as_ptr());
        assert!(health.is_null());
        let err = last_error_message();
        assert!(!err.is_empty(), "expected last_error message");
    }
}

fn run_ffi_roundtrip(path: &Path) {
    let c_path = CString::new(path.to_string_lossy().as_bytes()).unwrap();
    unsafe {
        let health = inspect_rpf_file_c(c_path.as_ptr());
        assert!(
            !health.is_null(),
            "inspect_rpf_file_c failed: {}",
            last_error_message()
        );

        let health = Box::from_raw(health);
        assert!(health.buses > 0);
        assert!(health.base_mva > 0.0);
        assert!(
            (RPF_HEALTH_HEALTHY..=RPF_HEALTH_PATHOLOGICAL).contains(&health.grade),
            "unexpected grade {}",
            health.grade
        );

        let json = inspect_rpf_file_c_json(c_path.as_ptr());
        assert!(
            !json.is_null(),
            "inspect_rpf_file_c_json failed: {}",
            last_error_message()
        );
        let json_text = CStr::from_ptr(json).to_string_lossy();
        assert!(!json_text.contains('\n'), "JSON must be single-line");
        assert!(json_text.starts_with('{') && json_text.ends_with('}'));
        assert!(json_text.contains("\"grade\":"));
        assert!(json_text.contains("\"reasons\":["));
        assert!(json_text.contains("\"grade_code\":"));
        assert!(json_text.contains(&format!("\"grade_code\":{}", health.grade)));
        free_rpf_case_health_json(json);

        free_rpf_case_health_ffi(Box::into_raw(health));
    }
}

#[test]
fn ffi_grade_constants_match_rust() {
    use raptrix_cim_arrow::RpfHealthGrade;

    assert_eq!(RPF_HEALTH_HEALTHY, 0);
    assert_eq!(raptrix_cim_arrow::ffi::RPF_HEALTH_CAUTION, 1);
    assert_eq!(RPF_HEALTH_STRESSED, 2);
    assert_eq!(RPF_HEALTH_PATHOLOGICAL, 3);

    let _ = [
        RpfHealthGrade::Healthy,
        RpfHealthGrade::Caution,
        RpfHealthGrade::Stressed,
        RpfHealthGrade::Pathological,
    ];
}
