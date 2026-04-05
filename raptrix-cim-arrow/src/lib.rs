// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # raptrix-cim-arrow
//!
//! Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
//!
//! Copyright (c) 2026 Musto Technologies LLC
//!
//! This crate is the shared home for the locked Raptrix PowerFlow Interchange
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
//! 3. Use [`read_rpf_tables`], [`summarize_rpf`], or [`rpf_file_metadata`] for
//!    validation, inspection, and regression tests.

mod io;
mod schema;

pub use io::{
    RootWriteOptions, RpfSummary, TableSummary, read_rpf_tables, root_rpf_schema,
    row_count_metadata_key, rpf_file_metadata, summarize_rpf, validate_rpf_file, write_root_rpf,
    write_root_rpf_with_metadata,
};
pub use schema::*;
