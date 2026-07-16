// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stable C ABI for RPF case health inspection and patch-based re-export.
//!
//! See `include/raptrix_cim_arrow_health.h` for the consumer-facing contract.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::ptr;

use crate::health::{RpfCaseHealth, RpfHealthGrade, inspect_rpf_file};
use crate::patch::apply_rpf_patch;

/// Overall case health grade (matches `RpfHealthGrade` and C header constants).
pub const RPF_HEALTH_HEALTHY: i32 = 0;
pub const RPF_HEALTH_CAUTION: i32 = 1;
pub const RPF_HEALTH_STRESSED: i32 = 2;
pub const RPF_HEALTH_PATHOLOGICAL: i32 = 3;

const OPTION_I32_ABSENT: i32 = -1;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// C-friendly scalar snapshot of [`RpfCaseHealth`].
#[repr(C)]
pub struct RpfCaseHealthFFI {
    pub grade: i32,
    pub reactive_support_headroom_mvar: f64,
    pub q_violation_count: i32,
    pub solver_q_limit_infeasible_count: i32,
    pub island_count: u32,
    pub buses: u32,
    pub total_load_p_mw: f64,
    pub total_gen_p_mw: f64,
    pub base_mva: f64,
    pub detached_islands_present: u8,
    pub solve_data_present: u8,
    pub buses_out_of_voltage_band: u32,
    pub pv_to_pq_switch_count: i32,
}

fn set_last_error(message: impl Into<String>) {
    let cstr = CString::new(message.into()).unwrap_or_else(|_| {
        CString::new("failed to encode error message").expect("static error string")
    });
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(cstr);
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn grade_to_i32(grade: RpfHealthGrade) -> i32 {
    match grade {
        RpfHealthGrade::Healthy => RPF_HEALTH_HEALTHY,
        RpfHealthGrade::Caution => RPF_HEALTH_CAUTION,
        RpfHealthGrade::Stressed => RPF_HEALTH_STRESSED,
        RpfHealthGrade::Pathological => RPF_HEALTH_PATHOLOGICAL,
    }
}

fn grade_name(grade: RpfHealthGrade) -> &'static str {
    match grade {
        RpfHealthGrade::Healthy => "Healthy",
        RpfHealthGrade::Caution => "Caution",
        RpfHealthGrade::Stressed => "Stressed",
        RpfHealthGrade::Pathological => "Pathological",
    }
}

fn health_to_ffi(health: &RpfCaseHealth) -> RpfCaseHealthFFI {
    RpfCaseHealthFFI {
        grade: grade_to_i32(health.grade),
        reactive_support_headroom_mvar: health.aggregates.reactive_support_headroom_mvar,
        q_violation_count: health
            .convergence
            .q_violation_count
            .map(|v| v as i32)
            .unwrap_or(OPTION_I32_ABSENT),
        solver_q_limit_infeasible_count: health
            .convergence
            .solver_q_limit_infeasible_count
            .unwrap_or(OPTION_I32_ABSENT),
        island_count: health.topology.island_count as u32,
        buses: health.counts.buses as u32,
        total_load_p_mw: health.aggregates.total_load_p_mw,
        total_gen_p_mw: health.aggregates.total_gen_p_mw,
        base_mva: health.aggregates.base_mva,
        detached_islands_present: u8::from(health.topology.detached_islands_present),
        solve_data_present: u8::from(health.convergence.solve_data_present),
        buses_out_of_voltage_band: health.topology.buses_out_of_voltage_band as u32,
        pv_to_pq_switch_count: health
            .convergence
            .pv_to_pq_switch_count
            .unwrap_or(OPTION_I32_ABSENT),
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn health_to_json_line(health: &RpfCaseHealth) -> String {
    let ffi = health_to_ffi(health);
    let mut reasons = String::from("[");
    for (idx, reason) in health.reasons.iter().enumerate() {
        if idx > 0 {
            reasons.push(',');
        }
        reasons.push('"');
        reasons.push_str(&json_escape(reason));
        reasons.push('"');
    }
    reasons.push(']');

    format!(
        "{{\"grade\":\"{}\",\"grade_code\":{},\"reactive_support_headroom_mvar\":{},\
\"q_violation_count\":{},\"solver_q_limit_infeasible_count\":{},\"island_count\":{},\
\"buses\":{},\"total_load_p_mw\":{},\"total_gen_p_mw\":{},\"base_mva\":{},\
\"detached_islands_present\":{},\"solve_data_present\":{},\"buses_out_of_voltage_band\":{},\
\"pv_to_pq_switch_count\":{},\"reasons\":{}}}",
        grade_name(health.grade),
        ffi.grade,
        ffi.reactive_support_headroom_mvar,
        ffi.q_violation_count,
        ffi.solver_q_limit_infeasible_count,
        ffi.island_count,
        ffi.buses,
        ffi.total_load_p_mw,
        ffi.total_gen_p_mw,
        ffi.base_mva,
        ffi.detached_islands_present,
        ffi.solve_data_present,
        ffi.buses_out_of_voltage_band,
        ffi.pv_to_pq_switch_count,
        reasons,
    )
}

fn inspect_path(path: *const c_char) -> Result<RpfCaseHealth, ()> {
    if path.is_null() {
        set_last_error("inspect_rpf_file_c: path is null");
        return Err(());
    }
    let path = unsafe {
        CStr::from_ptr(path)
            .to_str()
            .map_err(|_| {
                set_last_error("inspect_rpf_file_c: path is not valid UTF-8");
            })
            .map(std::path::Path::new)?
    };
    inspect_rpf_file(path).map_err(|err| {
        set_last_error(format!("inspect_rpf_file_c: {err:#}"));
    })
}

/// Reads an `.rpf` file and returns a heap-allocated health snapshot, or `NULL` on error.
///
/// `path` must be a valid, NUL-terminated UTF-8 string (including on Windows).
///
/// # Safety
///
/// The returned pointer must be released with [`free_rpf_case_health_ffi`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inspect_rpf_file_c(path: *const c_char) -> *mut RpfCaseHealthFFI {
    clear_last_error();
    match inspect_path(path) {
        Ok(health) => Box::into_raw(Box::new(health_to_ffi(&health))),
        Err(()) => ptr::null_mut(),
    }
}

/// Frees a pointer returned by [`inspect_rpf_file_c`].
///
/// # Safety
///
/// `ptr` must be null or a pointer previously returned by [`inspect_rpf_file_c`]
/// and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_rpf_case_health_ffi(ptr: *mut RpfCaseHealthFFI) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

/// Returns a single-line JSON object describing case health, or `NULL` on error.
///
/// `path` must be a valid, NUL-terminated UTF-8 string (including on Windows).
///
/// # Safety
///
/// The returned pointer must be released with [`free_rpf_case_health_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inspect_rpf_file_c_json(path: *const c_char) -> *mut c_char {
    clear_last_error();
    match inspect_path(path) {
        Ok(health) => match CString::new(health_to_json_line(&health)) {
            Ok(json) => json.into_raw(),
            Err(_) => {
                set_last_error("inspect_rpf_file_c_json: failed to encode JSON");
                ptr::null_mut()
            }
        },
        Err(()) => ptr::null_mut(),
    }
}

/// Frees a pointer returned by [`inspect_rpf_file_c_json`].
///
/// # Safety
///
/// `json` must be null or a pointer previously returned by [`inspect_rpf_file_c_json`]
/// and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_rpf_case_health_json(json: *mut c_char) {
    if !json.is_null() {
        unsafe {
            drop(CString::from_raw(json));
        }
    }
}

/// Returns the last error message for the calling thread, or an empty string.
///
/// Valid until the next FFI call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn rpf_case_health_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => c"".as_ptr(),
    })
}

fn path_from_c<'a>(ptr: *const c_char, label: &str) -> Result<&'a Path, ()> {
    if ptr.is_null() {
        set_last_error(format!("apply_rpf_patch_c: {label} is null"));
        return Err(());
    }
    let path = unsafe {
        CStr::from_ptr(ptr).to_str().map_err(|_| {
            set_last_error(format!("apply_rpf_patch_c: {label} is not valid UTF-8"));
        })?
    };
    Ok(Path::new(path))
}

/// Merges solver-owned tables from `patch_rpf` into `source_rpf`, writing `output_rpf`.
///
/// Converter-owned tables (GIS, contingencies, RAS, diagrams, unknown enrichment, …)
/// always come from `source_rpf`. Returns `0` on success, `-1` on error.
///
/// # Safety
///
/// All path arguments must be valid, NUL-terminated UTF-8 strings (including on Windows).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn apply_rpf_patch_c(
    source_rpf: *const c_char,
    patch_rpf: *const c_char,
    output_rpf: *const c_char,
) -> i32 {
    clear_last_error();
    let source = match path_from_c(source_rpf, "source_rpf") {
        Ok(path) => path,
        Err(()) => return -1,
    };
    let patch = match path_from_c(patch_rpf, "patch_rpf") {
        Ok(path) => path,
        Err(()) => return -1,
    };
    let output = match path_from_c(output_rpf, "output_rpf") {
        Ok(path) => path,
        Err(()) => return -1,
    };
    match apply_rpf_patch(source, patch, output) {
        Ok(()) => 0,
        Err(err) => {
            set_last_error(format!("apply_rpf_patch_c: {err:#}"));
            -1
        }
    }
}
