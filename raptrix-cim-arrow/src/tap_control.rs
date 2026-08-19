// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! v0.14.2 trailing nullable transformer tap / PST control columns.
//!
//! These are the movable range and discrete grid. They are **not** inferred
//! from a static `tap_ratio` / `phase_shift`. Dual-read of older files pads
//! them null. Converters emit values from PSS/E COD/CONT/RMA/RMI/NTP when
//! present; `operation_time_min` is never invented from RAW.
//!
//! `transformers_3w` carries a single COD1-style control block on the H
//! winding; M/L tap control is out of scope for this revision.
//!
//! `tap_control_mode` is RAW COD (Int32). It is **not** `branches.control_mode`
//! (Utf8 dict / FACTS). Never share identity across those columns.

use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field};

/// Canonical trailing column names, in on-wire order after membership flags.
pub const TAP_CONTROL_COLUMNS: &[&str] = &[
    "tap_min",
    "tap_max",
    "tap_limit_unit",
    "n_positions",
    "tap_step",
    "tap_control_mode",
    "regulated_bus_id",
    "operation_time_min",
];

/// `tap_min` / `tap_max` are per-unit tap ratios.
pub const TAP_LIMIT_UNIT_RATIO: &str = "ratio";
/// `tap_min` / `tap_max` are phase-shift limits in degrees.
pub const TAP_LIMIT_UNIT_DEGREES: &str = "degrees";

fn tap_limit_unit_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Trailing nullable fields appended to `transformers_2w` / `transformers_3w`.
pub fn tap_control_fields() -> Vec<Field> {
    vec![
        Field::new("tap_min", DataType::Float64, true),
        Field::new("tap_max", DataType::Float64, true),
        Field::new("tap_limit_unit", tap_limit_unit_type(), true),
        Field::new("n_positions", DataType::Int32, true),
        Field::new("tap_step", DataType::Float64, true),
        Field::new("tap_control_mode", DataType::Int32, true),
        Field::new("regulated_bus_id", DataType::Int32, true),
        Field::new("operation_time_min", DataType::Float64, true),
    ]
}

/// Null-padded tap-control columns for writers that do not invent a range.
pub fn null_tap_control_arrays(n: usize) -> Vec<ArrayRef> {
    tap_control_fields()
        .into_iter()
        .map(|field| new_null_array(field.data_type(), n))
        .collect()
}

/// Discrete step from RAW RMA/RMI/NTP. None when the grid is not defined.
#[inline]
pub fn derived_tap_step(tap_min: f64, tap_max: f64, n_positions: i32) -> Option<f64> {
    if n_positions > 1 && tap_max > tap_min && tap_min.is_finite() && tap_max.is_finite() {
        Some((tap_max - tap_min) / f64::from(n_positions - 1))
    } else {
        None
    }
}

/// Unit of `tap_min` / `tap_max`. Required on a usable movable block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapLimitUnit {
    Ratio,
    Degrees,
}

impl TapLimitUnit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ratio => TAP_LIMIT_UNIT_RATIO,
            Self::Degrees => TAP_LIMIT_UNIT_DEGREES,
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            TAP_LIMIT_UNIT_RATIO => Some(Self::Ratio),
            TAP_LIMIT_UNIT_DEGREES => Some(Self::Degrees),
            _ => None,
        }
    }
}

/// PSS/E writer **default** for `tap_limit_unit`: `|COD| == 3` → degrees, else ratio.
///
/// This is not a physics law and not a reader rule. Other decks can stamp
/// `degrees` on non-3 CODs. Readers must trust the on-wire `tap_limit_unit`
/// column. See `raptrix-psse-rs` for the RAW mapping.
#[must_use]
pub fn tap_limit_unit_from_cod(cod: i32) -> TapLimitUnit {
    if cod.unsigned_abs() == 3 {
        TapLimitUnit::Degrees
    } else {
        TapLimitUnit::Ratio
    }
}

/// Normalized tap-control block. All-null means fixed / incomplete.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TapControlBlock {
    pub tap_min: Option<f64>,
    pub tap_max: Option<f64>,
    pub tap_limit_unit: Option<TapLimitUnit>,
    pub n_positions: Option<i32>,
    pub tap_step: Option<f64>,
    pub tap_control_mode: Option<i32>,
    pub regulated_bus_id: Option<i32>,
    pub operation_time_min: Option<f64>,
}

impl TapControlBlock {
    #[must_use]
    pub fn is_usable(&self) -> bool {
        usable_tap_control(
            self.tap_min,
            self.tap_max,
            self.n_positions,
            self.tap_step,
            self.tap_limit_unit.map(TapLimitUnit::as_str),
            self.tap_control_mode,
        )
    }
}

fn tap_step_matches_derived(tap_step: f64, derived: f64) -> bool {
    tap_step.is_finite() && (tap_step - derived).abs() <= 1e-9 * derived.abs().max(1.0)
}

/// Shared validity predicate for writers, readers, and recovery.
///
/// Unknown `tap_limit_unit` tokens fail closed (not usable). Everything
/// else is fixed: ignore the range columns. Do not treat
/// `tap_control_mode = 0` with non-null RMA/RMI as a half-enabled lever.
#[must_use]
pub fn usable_tap_control(
    tap_min: Option<f64>,
    tap_max: Option<f64>,
    n_positions: Option<i32>,
    tap_step: Option<f64>,
    tap_limit_unit: Option<&str>,
    tap_control_mode: Option<i32>,
) -> bool {
    let Some(mode) = tap_control_mode else {
        return false;
    };
    if mode == 0 {
        return false;
    }
    let Some(unit) = tap_limit_unit else {
        return false;
    };
    if TapLimitUnit::parse(unit).is_none() {
        return false;
    }
    let (Some(min), Some(max), Some(n)) = (tap_min, tap_max, n_positions) else {
        return false;
    };
    let Some(derived) = derived_tap_step(min, max, n) else {
        return false;
    };
    match tap_step {
        None => true,
        Some(step) => tap_step_matches_derived(step, derived),
    }
}

/// Writer normalization: COD 0 / absent / incomplete grid → all eight null.
///
/// Do not write `tap_control_mode = 0` with non-null RMA/RMI.
#[must_use]
pub fn normalize_tap_control(
    tap_control_mode: Option<i32>,
    tap_min: Option<f64>,
    tap_max: Option<f64>,
    n_positions: Option<i32>,
    tap_step: Option<f64>,
    tap_limit_unit: Option<TapLimitUnit>,
    regulated_bus_id: Option<i32>,
    operation_time_min: Option<f64>,
) -> TapControlBlock {
    let unit_str = tap_limit_unit.map(TapLimitUnit::as_str);
    if !usable_tap_control(
        tap_min,
        tap_max,
        n_positions,
        tap_step,
        unit_str,
        tap_control_mode,
    ) {
        return TapControlBlock::default();
    }
    let min = tap_min.expect("usable_tap_control requires tap_min");
    let max = tap_max.expect("usable_tap_control requires tap_max");
    let n = n_positions.expect("usable_tap_control requires n_positions");
    let derived =
        derived_tap_step(min, max, n).expect("usable_tap_control requires a derived step");
    TapControlBlock {
        tap_min: Some(min),
        tap_max: Some(max),
        tap_limit_unit,
        n_positions: Some(n),
        tap_step: Some(tap_step.unwrap_or(derived)),
        tap_control_mode,
        regulated_bus_id,
        operation_time_min,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TAP_CONTROL_COLUMNS, TAP_LIMIT_UNIT_DEGREES, TAP_LIMIT_UNIT_RATIO, TapLimitUnit,
        derived_tap_step, normalize_tap_control, tap_control_fields, tap_limit_unit_from_cod,
        usable_tap_control,
    };

    #[test]
    fn columns_match_fields() {
        let fields = tap_control_fields();
        assert_eq!(fields.len(), TAP_CONTROL_COLUMNS.len());
        assert_eq!(fields.len(), 8);
        for (field, name) in fields.iter().zip(TAP_CONTROL_COLUMNS) {
            assert_eq!(field.name(), *name);
            assert!(field.is_nullable());
        }
        assert_eq!(fields[2].name(), "tap_limit_unit");
        assert_eq!(fields[5].name(), "tap_control_mode");
    }

    #[test]
    fn step_from_ntp() {
        let step = derived_tap_step(0.9, 1.1, 33).unwrap();
        assert!((step - 0.00625).abs() < 1e-12);
        assert!(derived_tap_step(0.9, 1.1, 1).is_none());
        assert!(derived_tap_step(1.1, 0.9, 17).is_none());
    }

    #[test]
    fn usable_requires_full_discrete_grid() {
        assert!(usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            None,
            Some(TAP_LIMIT_UNIT_RATIO),
            Some(1),
        ));
        assert!(usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            Some(0.00625),
            Some(TAP_LIMIT_UNIT_RATIO),
            Some(1),
        ));
        assert!(!usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            Some(0.05),
            Some(TAP_LIMIT_UNIT_RATIO),
            Some(1),
        ));
        assert!(!usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            None,
            Some(TAP_LIMIT_UNIT_RATIO),
            Some(0),
        ));
        assert!(!usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            None,
            None,
            Some(1),
        ));
        assert!(!usable_tap_control(
            Some(0.9),
            Some(1.1),
            Some(33),
            None,
            Some("pu"),
            Some(1),
        ));
    }

    #[test]
    fn normalize_cod_zero_is_all_null() {
        let block = normalize_tap_control(
            Some(0),
            Some(0.9),
            Some(1.1),
            Some(33),
            None,
            Some(TapLimitUnit::Ratio),
            Some(101),
            Some(1.0),
        );
        assert_eq!(block, Default::default());
        assert!(!block.is_usable());
    }

    #[test]
    fn normalize_incomplete_grid_is_all_null() {
        let block = normalize_tap_control(
            Some(1),
            Some(0.9),
            Some(1.1),
            Some(1),
            None,
            Some(TapLimitUnit::Ratio),
            Some(101),
            None,
        );
        assert_eq!(block, Default::default());
    }

    #[test]
    fn normalize_pst_cod_keeps_degrees() {
        let block = normalize_tap_control(
            Some(3),
            Some(-30.0),
            Some(30.0),
            Some(61),
            None,
            Some(tap_limit_unit_from_cod(3)),
            Some(0),
            None,
        );
        assert!(block.is_usable());
        assert_eq!(block.tap_limit_unit, Some(TapLimitUnit::Degrees));
        assert_eq!(
            block.tap_limit_unit.unwrap().as_str(),
            TAP_LIMIT_UNIT_DEGREES
        );
        assert_eq!(block.tap_control_mode, Some(3));
        assert!((block.tap_step.unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn voltage_cod_is_ratio() {
        assert_eq!(tap_limit_unit_from_cod(1), TapLimitUnit::Ratio);
        assert_eq!(tap_limit_unit_from_cod(-1), TapLimitUnit::Ratio);
        assert_eq!(tap_limit_unit_from_cod(3), TapLimitUnit::Degrees);
        assert_eq!(tap_limit_unit_from_cod(-3), TapLimitUnit::Degrees);
    }
}
