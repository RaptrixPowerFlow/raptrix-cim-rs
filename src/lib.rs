// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # raptrix-cim-rs
//!
//! High-performance IEC 61970 Common Information Model (CIM) parser in Rust,
//! optimised for zero-copy RDF/XML parsing for real-time power flow and
//! SCED applications.
//!
//! ## Crate layout
//!
//! | Module | Purpose |
//! |--------|---------|
//! [`models`] | CIM data-model types and base traits |
//! [`parser`] | RDF/XML parsing helpers built on `quick-xml` |

pub mod models;
pub mod parser;
pub mod rpf_writer;
pub mod test_utils;

pub use raptrix_cim_arrow as arrow_schema;
pub use raptrix_cim_arrow::{
    RpfSummary, TableSummary, read_rpf_tables, rpf_file_metadata, summarize_rpf,
};

pub use rpf_writer::{
    BusResolutionMode, LoadZipComponentsPu, TransformerRepresentationMode, WriteOptions,
    WriteSummary, map_psse_zip_terms_to_rpf_pu, write_complete_rpf,
    write_complete_rpf_with_options,
};
