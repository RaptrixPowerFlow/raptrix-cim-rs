// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow schema definitions for the Raptrix CIM power-flow profile.
//!
//! Schemas match the column layout used by the companion C++ CIM adapter so
//! that Parquet files produced here can be consumed by any Arrow-compatible
//! solver without a translation layer.

use arrow::datatypes::{DataType, Field, Schema};

/// Human-readable branding string embedded as Parquet file-level metadata.
pub const BRANDING: &str =
    "Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC";

/// Schema version tag embedded as Parquet file-level metadata.
pub const SCHEMA_VERSION: &str = "v0.1";

/// Returns the Arrow [`Schema`] for the **buses** table of a power-flow problem.
///
/// Column semantics follow IEC 61970 CIM / Matpower bus-type conventions:
/// - `type`      : 1 = PQ (load bus), 2 = PV (generator bus), 3 = slack (reference)
/// - `v_ang_set` : voltage angle setpoint in **radians**
/// - `name`      : nullable — not all CIM exports carry a human-readable label
pub fn powerflow_schema() -> Schema {
    Schema::new(vec![
        // ── Bus identity & type ───────────────────────────────────────────────
        Field::new("bus_id",    DataType::Int32,   false),
        Field::new("type",      DataType::Int8,    false), // 1=PQ  2=PV  3=Slack

        // ── Scheduled power injections (MW / MVAr, system base) ──────────────
        Field::new("p_sched",   DataType::Float64, false),
        Field::new("q_sched",   DataType::Float64, false),

        // ── Voltage setpoints ────────────────────────────────────────────────
        Field::new("v_mag_set", DataType::Float64, false),
        Field::new("v_ang_set", DataType::Float64, false), // radians

        // ── Reactive-power limits (MVAr) ─────────────────────────────────────
        Field::new("q_min",     DataType::Float64, false),
        Field::new("q_max",     DataType::Float64, false),

        // ── Shunt admittance (pu) ─────────────────────────────────────────────
        Field::new("g_shunt",   DataType::Float64, false),
        Field::new("b_shunt",   DataType::Float64, false),

        // ── Per-unit voltage operating limits ────────────────────────────────
        Field::new("v_min",     DataType::Float64, false),
        Field::new("v_max",     DataType::Float64, false),

        // ── Area / zone grouping ─────────────────────────────────────────────
        Field::new("area",      DataType::Int32,   false),
        Field::new("zone",      DataType::Int32,   false),

        // ── Human-readable label (optional in CIM exports) ───────────────────
        Field::new("name",      DataType::Utf8,    true), // nullable
    ])
}

/// Returns the Arrow [`Schema`] for the **branches** table of a power-flow problem.
///
/// A branch represents a transmission line or transformer connecting two buses.
/// - `b_shunt` : total line-charging susceptance (pu)
/// - `tap`     : off-nominal turns ratio (1.0 = nominal)
/// - `phase`   : fixed phase-shift angle in radians
pub fn branch_schema() -> Schema {
    Schema::new(vec![
        Field::new("from",    DataType::Int32,   false),
        Field::new("to",      DataType::Int32,   false),
        Field::new("r",       DataType::Float64, false), // series resistance (pu)
        Field::new("x",       DataType::Float64, false), // series reactance  (pu)
        Field::new("b_shunt", DataType::Float64, false), // line charging     (pu)
        Field::new("tap",     DataType::Float64, false),
        Field::new("phase",   DataType::Float64, false), // radians
        Field::new("rate_a",  DataType::Float64, false), // MVA thermal limit
        Field::new("status",  DataType::Boolean, false),
    ])
}
