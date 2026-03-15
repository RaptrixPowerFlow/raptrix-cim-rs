// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Concrete CIM equipment types that compose [`BaseAttributes`] and implement
//! the [`IdentifiedObject`] / [`PowerSystemResource`] trait hierarchy.
//!
//! ## CIM hierarchy reflected here
//!
//! ```text
//! IdentifiedObject
//!   └─ PowerSystemResource
//!        └─ Equipment
//!             ├─ ConductingEquipment
//!             │    └─ ACLineSegment
//!             └─ EnergyConsumer
//! ```
//!
//! ## Serde design note
//!
//! `quick-xml`'s `#[serde(flatten)]` propagates XML *attributes* but not child
//! *elements*, so it cannot be used to flatten [`BaseAttributes`] (which
//! contains both an `rdf:ID` attribute and child elements such as
//! `IdentifiedObject.name`).
//!
//! Each concrete type therefore uses a private raw deserialization struct (e.g.
//! `RawACLineSegment`) that has all fields at the top level, and a manual
//! `Deserialize` implementation that constructs the public composed struct from
//! that flat representation.  The public struct preserves true field composition
//! via `pub base: BaseAttributes<'a>`.

use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize};

use super::base::{BaseAttributes, IdentifiedObject, PowerSystemResource};

// ---------------------------------------------------------------------------
// Equipment marker trait
// ---------------------------------------------------------------------------

/// Marker trait for CIM `Equipment`.
///
/// Equipment is a `PowerSystemResource` that is a physical piece of
/// conducting or non-conducting apparatus.
pub trait Equipment: PowerSystemResource {
    /// Indicates whether the equipment is normally in service.
    fn normally_in_service(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// ConductingEquipment marker trait
// ---------------------------------------------------------------------------

/// Marker trait for CIM `ConductingEquipment`.
///
/// `ConductingEquipment` is the root class for equipment that can carry
/// electrical current (e.g. lines, switches, transformers).
pub trait ConductingEquipment: Equipment {}

// ---------------------------------------------------------------------------
// ACLineSegment
// ---------------------------------------------------------------------------

/// CIM `ACLineSegment` – a single AC transmission-line segment.
///
/// In IEC 61970-301 an `ACLineSegment` is a `ConductingEquipment` with
/// per-unit-length series impedance and shunt admittance parameters.  Only
/// the fields relevant to a Newton-Raphson power flow engine are included
/// here; the full parameter set can be added incrementally.
///
/// ## XML example
///
/// ```xml
/// <cim:ACLineSegment rdf:ID="ACLineSegment_001">
///   <cim:IdentifiedObject.name>Line NW-1</cim:IdentifiedObject.name>
///   <cim:Conductor.length>42.5</cim:Conductor.length>
///   <cim:ACLineSegment.r>0.01</cim:ACLineSegment.r>
///   <cim:ACLineSegment.x>0.08</cim:ACLineSegment.x>
/// </cim:ACLineSegment>
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ACLineSegment<'a> {
    /// Inherited `IdentifiedObject` fields (mRID, name, description).
    pub base: BaseAttributes<'a>,

    /// Total conductor length in kilometres.
    pub length_km: Option<f64>,

    /// Positive-sequence series resistance (Ω).
    pub r: Option<f64>,

    /// Positive-sequence series reactance (Ω).
    pub x: Option<f64>,

    /// Positive-sequence shunt susceptance (S).
    pub bch: Option<f64>,
}

/// Private flat struct used only for serde deserialization.
///
/// `quick-xml` cannot flatten child-element fields, so we deserialize into
/// this flat representation first and then construct the composed public type.
#[derive(Deserialize)]
struct RawACLineSegment<'a> {
    /// Mapped from the `rdf:ID` XML attribute (namespace prefix stripped by
    /// quick-xml, so the serde rename is `@ID`).
    #[serde(rename = "@ID", borrow)]
    m_rid: Cow<'a, str>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "Conductor.length", default)]
    length_km: Option<f64>,
    #[serde(rename = "ACLineSegment.r", default)]
    r: Option<f64>,
    #[serde(rename = "ACLineSegment.x", default)]
    x: Option<f64>,
    #[serde(rename = "ACLineSegment.bch", default)]
    bch: Option<f64>,
}

impl<'de: 'a, 'a> Deserialize<'de> for ACLineSegment<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawACLineSegment::deserialize(deserializer)?;
        Ok(ACLineSegment {
            base: BaseAttributes {
                m_rid: raw.m_rid,
                name: raw.name,
                description: raw.description,
            },
            length_km: raw.length_km,
            r: raw.r,
            x: raw.x,
            bch: raw.bch,
        })
    }
}

/// Serializes `ACLineSegment` as a flat XML element.
///
/// Uses a private helper struct so that the public `base` composition field
/// is flattened back into the element's attribute and child-element space.
impl<'a> Serialize for ACLineSegment<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        // Count: @ID + optional name/desc + optional numeric fields
        let mut s = serializer.serialize_struct("cim:ACLineSegment", 5)?;
        s.serialize_field("@ID", &self.base.m_rid)?;
        if let Some(ref n) = self.base.name {
            s.serialize_field("IdentifiedObject.name", n)?;
        }
        if let Some(ref d) = self.base.description {
            s.serialize_field("IdentifiedObject.description", d)?;
        }
        if let Some(l) = self.length_km {
            s.serialize_field("Conductor.length", &l)?;
        }
        if let Some(r) = self.r {
            s.serialize_field("ACLineSegment.r", &r)?;
        }
        if let Some(x) = self.x {
            s.serialize_field("ACLineSegment.x", &x)?;
        }
        if let Some(b) = self.bch {
            s.serialize_field("ACLineSegment.bch", &b)?;
        }
        s.end()
    }
}

impl<'a> ACLineSegment<'a> {
    /// Creates a new [`ACLineSegment`] with only the required identity fields.
    pub fn new(base: BaseAttributes<'a>) -> Self {
        Self {
            base,
            length_km: None,
            r: None,
            x: None,
            bch: None,
        }
    }

    /// Converts to a fully-owned (`'static`) [`ACLineSegment`].
    pub fn into_owned(self) -> ACLineSegment<'static> {
        ACLineSegment {
            base: self.base.into_owned(),
            length_km: self.length_km,
            r: self.r,
            x: self.x,
            bch: self.bch,
        }
    }
}

impl<'a> IdentifiedObject for ACLineSegment<'a> {
    fn mrid(&self) -> &str {
        self.base.mrid()
    }

    fn name(&self) -> Option<&str> {
        self.base.name()
    }

    fn description(&self) -> Option<&str> {
        self.base.description()
    }
}

impl<'a> PowerSystemResource for ACLineSegment<'a> {}
impl<'a> Equipment for ACLineSegment<'a> {}
impl<'a> ConductingEquipment for ACLineSegment<'a> {}

// ---------------------------------------------------------------------------
// EnergyConsumer
// ---------------------------------------------------------------------------

/// CIM `EnergyConsumer` – a generic load connected to the network.
///
/// Represents a generic consumer of energy (residential, commercial, or
/// industrial load).  The active (`p_mw`) and reactive (`q_mvar`) power
/// consumption are the primary parameters needed by a power flow solver.
///
/// ## XML example
///
/// ```xml
/// <cim:EnergyConsumer rdf:ID="Load_A1">
///   <cim:IdentifiedObject.name>Substation A Load</cim:IdentifiedObject.name>
///   <cim:EnergyConsumer.p>12.5</cim:EnergyConsumer.p>
///   <cim:EnergyConsumer.q>3.2</cim:EnergyConsumer.q>
/// </cim:EnergyConsumer>
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct EnergyConsumer<'a> {
    /// Inherited `IdentifiedObject` fields (mRID, name, description).
    pub base: BaseAttributes<'a>,

    /// Active power demand (MW).
    pub p_mw: Option<f64>,

    /// Reactive power demand (Mvar).
    pub q_mvar: Option<f64>,
}

/// Private flat struct used only for serde deserialization.
#[derive(Deserialize)]
struct RawEnergyConsumer<'a> {
    /// Mapped from the `rdf:ID` XML attribute (namespace prefix stripped).
    #[serde(rename = "@ID", borrow)]
    m_rid: Cow<'a, str>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "EnergyConsumer.p", default)]
    p_mw: Option<f64>,
    #[serde(rename = "EnergyConsumer.q", default)]
    q_mvar: Option<f64>,
}

impl<'de: 'a, 'a> Deserialize<'de> for EnergyConsumer<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawEnergyConsumer::deserialize(deserializer)?;
        Ok(EnergyConsumer {
            base: BaseAttributes {
                m_rid: raw.m_rid,
                name: raw.name,
                description: raw.description,
            },
            p_mw: raw.p_mw,
            q_mvar: raw.q_mvar,
        })
    }
}

impl<'a> Serialize for EnergyConsumer<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("cim:EnergyConsumer", 3)?;
        s.serialize_field("@ID", &self.base.m_rid)?;
        if let Some(ref n) = self.base.name {
            s.serialize_field("IdentifiedObject.name", n)?;
        }
        if let Some(ref d) = self.base.description {
            s.serialize_field("IdentifiedObject.description", d)?;
        }
        if let Some(p) = self.p_mw {
            s.serialize_field("EnergyConsumer.p", &p)?;
        }
        if let Some(q) = self.q_mvar {
            s.serialize_field("EnergyConsumer.q", &q)?;
        }
        s.end()
    }
}

impl<'a> EnergyConsumer<'a> {
    /// Creates a new [`EnergyConsumer`] with only the required identity fields.
    pub fn new(base: BaseAttributes<'a>) -> Self {
        Self {
            base,
            p_mw: None,
            q_mvar: None,
        }
    }

    /// Converts to a fully-owned (`'static`) [`EnergyConsumer`].
    pub fn into_owned(self) -> EnergyConsumer<'static> {
        EnergyConsumer {
            base: self.base.into_owned(),
            p_mw: self.p_mw,
            q_mvar: self.q_mvar,
        }
    }
}

impl<'a> IdentifiedObject for EnergyConsumer<'a> {
    fn mrid(&self) -> &str {
        self.base.mrid()
    }

    fn name(&self) -> Option<&str> {
        self.base.name()
    }

    fn description(&self) -> Option<&str> {
        self.base.description()
    }
}

impl<'a> PowerSystemResource for EnergyConsumer<'a> {}
impl<'a> Equipment for EnergyConsumer<'a> {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_base(mrid: &'static str, name: &'static str) -> BaseAttributes<'static> {
        BaseAttributes::new(mrid, Some(name), None::<&str>)
    }

    #[test]
    fn ac_line_segment_identified_object() {
        let line = ACLineSegment {
            base: make_base("line-001", "Line NW-1"),
            length_km: Some(42.5),
            r: Some(0.01),
            x: Some(0.08),
            bch: None,
        };
        assert_eq!(line.mrid(), "line-001");
        assert_eq!(line.name(), Some("Line NW-1"));
        assert!(line.description().is_none());
        assert!(line.normally_in_service());
    }

    #[test]
    fn energy_consumer_identified_object() {
        let load = EnergyConsumer {
            base: make_base("load-001", "Substation A Load"),
            p_mw: Some(12.5),
            q_mvar: Some(3.2),
        };
        assert_eq!(load.mrid(), "load-001");
        assert_eq!(load.name(), Some("Substation A Load"));
        assert!(load.normally_in_service());
    }

    #[test]
    fn ac_line_segment_into_owned() {
        let mrid = String::from("line-owned");
        let name = String::from("Dynamic Line");
        let base = BaseAttributes::new(mrid.as_str(), Some(name.as_str()), None::<&str>);
        let line = ACLineSegment::new(base).into_owned();
        // Should compile and work as 'static without holding references.
        assert_eq!(line.mrid(), "line-owned");
        assert_eq!(line.name(), Some("Dynamic Line"));
    }

    #[test]
    fn energy_consumer_into_owned() {
        let mrid = String::from("load-owned");
        let base = BaseAttributes::new(mrid.as_str(), None::<&str>, None::<&str>);
        let load = EnergyConsumer::new(base).into_owned();
        assert_eq!(load.mrid(), "load-owned");
    }

    #[test]
    fn ac_line_segment_new_defaults() {
        let base = make_base("line-defaults", "Default Line");
        let line = ACLineSegment::new(base);
        assert!(line.length_km.is_none());
        assert!(line.r.is_none());
        assert!(line.x.is_none());
        assert!(line.bch.is_none());
    }

    #[test]
    fn ac_line_segment_base_composition() {
        let line = ACLineSegment {
            base: make_base("line-comp", "Composed Line"),
            length_km: Some(10.0),
            r: Some(0.005),
            x: Some(0.04),
            bch: Some(0.0001),
        };
        // Verify composition: base field is directly accessible
        assert_eq!(line.base.m_rid, "line-comp");
        assert_eq!(line.base.name.as_deref(), Some("Composed Line"));
    }
}

