// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! v0.14.3 trailing nullable switched-shunt control columns.
//!
//! Per-device bank mode and remote regulation bus. These are **not**
//! `metadata.default_shunt_control_mode` (file-level SVD loop knob) and
//! **not** `branches.control_mode` (FACTS Utf8) or `tap_control_mode`
//! (RAW COD Int32). Dual-read of v0.14.2 pads them null.
//!
//! CIM and PSLF writers leave both columns null unless an honest join
//! exists. PSS/E maps MODSW 0/1/2 and SWREG/SWREM.

use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field};

/// Canonical trailing column names, in on-wire order after `mrid`.
pub const SHUNT_CONTROL_COLUMNS: &[&str] = &["shunt_control_mode", "regulated_bus_id"];

/// Bank does not switch. PSS/E `MODSW` = 0.
pub const SHUNT_CONTROL_MODE_LOCKED: &str = "locked";
/// Stepped voltage control. PSS/E `MODSW` = 1.
pub const SHUNT_CONTROL_MODE_DISCRETE_VOLTAGE: &str = "discrete_voltage";
/// Continuous voltage control. PSS/E `MODSW` = 2.
pub const SHUNT_CONTROL_MODE_CONTINUOUS_VOLTAGE: &str = "continuous_voltage";

/// Allowed non-null `shunt_control_mode` tokens (voltage modes only).
pub const SHUNT_CONTROL_MODES: &[&str] = &[
    SHUNT_CONTROL_MODE_LOCKED,
    SHUNT_CONTROL_MODE_DISCRETE_VOLTAGE,
    SHUNT_CONTROL_MODE_CONTINUOUS_VOLTAGE,
];

/// Writer warning code when MODSW is not 0/1/2. Include bus I and the integer.
pub const UNKNOWN_MODSW_WARNING: &str = "unknown_modsw";

fn shunt_control_mode_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Trailing nullable fields appended to `switched_shunts`.
pub fn shunt_control_fields() -> Vec<Field> {
    vec![
        Field::new("shunt_control_mode", shunt_control_mode_type(), true),
        Field::new("regulated_bus_id", DataType::Int32, true),
    ]
}

/// Null-padded shunt-control columns for writers that do not invent MODSW.
pub fn null_shunt_control_arrays(n: usize) -> Vec<ArrayRef> {
    shunt_control_fields()
        .into_iter()
        .map(|field| new_null_array(field.data_type(), n))
        .collect()
}

/// Map PSS/E `MODSW` to a voltage-mode token. Any other code → `None`.
#[inline]
pub fn shunt_control_mode_from_modsw(modsw: i32) -> Option<&'static str> {
    match modsw {
        0 => Some(SHUNT_CONTROL_MODE_LOCKED),
        1 => Some(SHUNT_CONTROL_MODE_DISCRETE_VOLTAGE),
        2 => Some(SHUNT_CONTROL_MODE_CONTINUOUS_VOLTAGE),
        _ => None,
    }
}

/// RAW `SWREG` / `SWREM` → dense `bus_id`. `None` means local (never write 0).
#[inline]
pub fn regulated_bus_id_from_swreg(swreg: i32, bank_bus_id: i32) -> Option<i32> {
    if swreg == 0 || swreg == bank_bus_id {
        None
    } else {
        Some(swreg)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    #[test]
    fn columns_match_fields() {
        let fields = shunt_control_fields();
        assert_eq!(fields.len(), SHUNT_CONTROL_COLUMNS.len());
        for (field, name) in fields.iter().zip(SHUNT_CONTROL_COLUMNS) {
            assert_eq!(field.name(), *name);
            assert!(field.is_nullable());
        }
        assert_eq!(
            fields[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(fields[1].data_type(), &DataType::Int32);
    }

    #[test]
    fn modsw_voltage_modes_only() {
        assert_eq!(
            shunt_control_mode_from_modsw(0),
            Some(SHUNT_CONTROL_MODE_LOCKED)
        );
        assert_eq!(
            shunt_control_mode_from_modsw(1),
            Some(SHUNT_CONTROL_MODE_DISCRETE_VOLTAGE)
        );
        assert_eq!(
            shunt_control_mode_from_modsw(2),
            Some(SHUNT_CONTROL_MODE_CONTINUOUS_VOLTAGE)
        );
        assert_eq!(shunt_control_mode_from_modsw(3), None);
        assert_eq!(shunt_control_mode_from_modsw(-1), None);
    }

    #[test]
    fn swreg_zero_or_local_is_null() {
        assert_eq!(regulated_bus_id_from_swreg(0, 76), None);
        assert_eq!(regulated_bus_id_from_swreg(76, 76), None);
        assert_eq!(regulated_bus_id_from_swreg(88, 575), Some(88));
    }

    #[test]
    fn null_pad_width() {
        let cols = null_shunt_control_arrays(4);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].len(), 4);
        assert_eq!(cols[0].null_count(), 4);
        assert_eq!(cols[1].null_count(), 4);
    }
}
