// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # raptrix-cim-arrow
//!
//! Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
//!
//! Copyright (c) 2026 Raptrix Power
//!
//! This crate is the shared home for the locked Raptrix Power Interchange
//! schema contract and generic Arrow IPC infrastructure.
//!
//! Ownership boundaries:
//! - This crate owns canonical table schemas, metadata keys, deterministic
//!   table ordering, and reusable `.rpf` Arrow IPC file assembly and readback.
//! - Upstream converter crates such as `raptrix-cim-rs` and future
//!   `raptrix-psse-rs` own source-format parsing and mapping into canonical
//!   `RecordBatch` values.
//! - Solver crates and viewers should treat this crate as the executable source
//!   of truth for the RPF contract.
//!
//! Downstream usage model:
//! 1. Build canonical table `RecordBatch` values using the schema helpers.
//! 2. Pass those batches to [`write_root_rpf`] to emit a standards-compliant
//!    Arrow IPC `.rpf` file.
//! 3. After a solve, call [`apply_rpf_patch`] (FFI: `apply_rpf_patch_c`) with the
//!    source `.rpf` plus a solver patch so converter-owned tables are preserved.
//! 4. Use [`read_rpf_tables`], [`summarize_rpf`], or [`rpf_file_metadata`] for
//!    validation, inspection, and regression tests.

pub mod computational_load;
pub mod contingencies;
pub mod dynamics;
pub mod ffi;
mod health;
mod io;
mod patch;
mod schema;

pub use computational_load::{
    BuildoutEntry, ComputationalLoadProfileRow, DisturbanceCounter, FACILITY_CLASSES,
    ProtectionSettingsProvenance, ReconnectionParams, SeasonalEnvelopeEntry,
    VoltageMeasurement, VoltageTransferCurveStage, build_computational_load_profiles_batch,
    canonicalize_voltage_transfer_curve, patch_metadata_computational_load_mode,
    validate_computational_load_profiles_batch,
};
pub use contingencies::{
    ContingencyElementRow, ContingencyFkContext, ContingencyRow, KNOWN_ELEMENT_TYPES,
    build_contingencies_batch as build_contingencies_batch_full,
    read_contingencies_batch, validate_contingencies_batch as validate_contingencies_batch_full,
};
pub use dynamics::{
    ClassicalParams, DynamicsModelRow as DynamicsModelRowFull, Perc1Params,
    build_dynamics_models_batch as build_dynamics_models_batch_full, read_dynamics_models_batch,
    validate_dynamics_models_batch,
};
pub use health::{
    RpfCaseAggregates, RpfCaseCounts, RpfCaseHealth, RpfConvergenceHints, RpfHealthGrade,
    RpfTables, RpfTopologySignals, TopologySource, format_health_report, inspect_rpf_case,
    inspect_rpf_file,
};
pub use io::{
    RootWriteOptions, RpfSummary, TableSummary, read_rpf_tables, root_rpf_schema,
    row_count_metadata_key, rpf_file_metadata, summarize_rpf, validate_rpf_file, write_root_rpf,
    write_root_rpf_with_metadata,
};
pub use patch::apply_rpf_patch;
pub use schema::*;
