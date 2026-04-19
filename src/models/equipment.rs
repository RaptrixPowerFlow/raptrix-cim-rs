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
//!             │    └─ SynchronousMachine
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

    /// In-service status if provided by profile payload.
    pub status: Option<bool>,
}

/// Private flat struct used only for serde deserialization.
#[derive(Deserialize)]
struct RawEnergyConsumer<'a> {
    /// Mapped from the `rdf:ID` XML attribute (namespace prefix stripped).
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "EnergyConsumer.p", default)]
    p_mw: Option<f64>,
    #[serde(rename = "EnergyConsumer.q", default)]
    q_mvar: Option<f64>,
    #[serde(rename = "Equipment.normallyInService", default)]
    status: Option<bool>,
}

impl<'de: 'a, 'a> Deserialize<'de> for EnergyConsumer<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawEnergyConsumer::deserialize(deserializer)?;

        let m_rid = if let Some(m_rid) = raw.m_rid {
            strip_hash_cow(m_rid)
        } else if let Some(about) = raw.about {
            let resolved = strip_hash_cow(about);
            #[cfg(debug_assertions)]
            eprintln!("Fallback: using rdf:about for EnergyConsumer mRID: {resolved}");
            resolved
        } else {
            return Err(serde::de::Error::missing_field("@ID or @about"));
        };

        Ok(EnergyConsumer {
            base: BaseAttributes {
                m_rid,
                name: raw.name,
                description: raw.description,
            },
            p_mw: raw.p_mw,
            q_mvar: raw.q_mvar,
            status: raw.status,
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
        if let Some(status) = self.status {
            s.serialize_field("Equipment.normallyInService", &status)?;
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
            status: None,
        }
    }

    /// Converts to a fully-owned (`'static`) [`EnergyConsumer`].
    pub fn into_owned(self) -> EnergyConsumer<'static> {
        EnergyConsumer {
            base: self.base.into_owned(),
            p_mw: self.p_mw,
            q_mvar: self.q_mvar,
            status: self.status,
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

fn strip_hash_cow<'a>(value: Cow<'a, str>) -> Cow<'a, str> {
    if value.starts_with('#') {
        Cow::Owned(value.trim_start_matches('#').to_string())
    } else {
        value
    }
}

// ---------------------------------------------------------------------------
// SvShuntCompensator
// ---------------------------------------------------------------------------

/// CIM `SvShuntCompensator` state values for switched-shunt export.
///
/// Tenet 2: maps directly to CIM state payload with no schema mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct SvShuntCompensator<'a> {
    /// Inherited `IdentifiedObject` fields (mRID, name, description).
    pub base: BaseAttributes<'a>,

    /// Lower voltage deadband bound (pu).
    pub v_low: Option<f64>,

    /// Upper voltage deadband bound (pu).
    pub v_high: Option<f64>,

    /// Cumulative susceptance steps (pu).
    pub b_steps: Option<Vec<f64>>,

    /// Active step index.
    pub current_step: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct RawSvResourceRef<'a> {
    #[serde(rename = "@resource", borrow)]
    resource: Cow<'a, str>,
}

#[derive(Deserialize)]
struct RawSvShuntCompensator<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "SvShuntCompensator.ShuntCompensator", default)]
    shunt_compensator: Option<RawSvResourceRef<'a>>,
    #[serde(rename = "SvShuntCompensator.vLow", default)]
    v_low: Option<f64>,
    #[serde(rename = "SvShuntCompensator.vHigh", default)]
    v_high: Option<f64>,
    #[serde(rename = "SvShuntCompensator.bSteps", default)]
    b_steps: Vec<f64>,
    #[serde(rename = "SvShuntCompensator.bPerSection", default)]
    b_per_section: Option<f64>,
    #[serde(rename = "SvShuntCompensator.currentSection", default)]
    current_step: Option<i32>,
    #[serde(rename = "SvShuntCompensator.sections", default)]
    sections: Option<f64>,
}

impl<'de: 'a, 'a> Deserialize<'de> for SvShuntCompensator<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawSvShuntCompensator::deserialize(deserializer)?;

        let m_rid = if let Some(shunt_ref) = raw.shunt_compensator {
            strip_hash_cow(shunt_ref.resource)
        } else if let Some(m_rid) = raw.m_rid {
            strip_hash_cow(m_rid)
        } else if let Some(about) = raw.about {
            strip_hash_cow(about)
        } else {
            return Err(serde::de::Error::missing_field(
                "SvShuntCompensator.ShuntCompensator or @ID or @about",
            ));
        };

        let current_step = raw
            .current_step
            .or_else(|| raw.sections.map(|value| value.round() as i32));
        let b_steps = if !raw.b_steps.is_empty() {
            Some(raw.b_steps)
        } else if let (Some(step), Some(per_section)) = (current_step, raw.b_per_section) {
            if step > 0 {
                Some((1..=step).map(|idx| per_section * (idx as f64)).collect())
            } else {
                None
            }
        } else {
            None
        };

        Ok(SvShuntCompensator {
            base: BaseAttributes {
                m_rid,
                name: raw.name,
                description: raw.description,
            },
            v_low: raw.v_low,
            v_high: raw.v_high,
            b_steps,
            current_step,
        })
    }
}

impl<'a> Serialize for SvShuntCompensator<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("cim:SvShuntCompensator", 5)?;
        s.serialize_field("@ID", &self.base.m_rid)?;
        if let Some(ref n) = self.base.name {
            s.serialize_field("IdentifiedObject.name", n)?;
        }
        if let Some(ref d) = self.base.description {
            s.serialize_field("IdentifiedObject.description", d)?;
        }
        if let Some(v_low) = self.v_low {
            s.serialize_field("SvShuntCompensator.vLow", &v_low)?;
        }
        if let Some(v_high) = self.v_high {
            s.serialize_field("SvShuntCompensator.vHigh", &v_high)?;
        }
        if let Some(ref b_steps) = self.b_steps {
            s.serialize_field("SvShuntCompensator.bSteps", b_steps)?;
        }
        if let Some(current_step) = self.current_step {
            s.serialize_field("SvShuntCompensator.currentSection", &current_step)?;
        }
        s.end()
    }
}

impl<'a> SvShuntCompensator<'a> {
    /// Creates a new [`SvShuntCompensator`] with identity only.
    pub fn new(base: BaseAttributes<'a>) -> Self {
        Self {
            base,
            v_low: None,
            v_high: None,
            b_steps: None,
            current_step: None,
        }
    }

    /// Converts to a fully-owned (`'static`) [`SvShuntCompensator`].
    pub fn into_owned(self) -> SvShuntCompensator<'static> {
        SvShuntCompensator {
            base: self.base.into_owned(),
            v_low: self.v_low,
            v_high: self.v_high,
            b_steps: self.b_steps,
            current_step: self.current_step,
        }
    }
}

impl<'a> IdentifiedObject for SvShuntCompensator<'a> {
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

impl<'a> PowerSystemResource for SvShuntCompensator<'a> {}
impl<'a> Equipment for SvShuntCompensator<'a> {}

// ---------------------------------------------------------------------------
// Transformer2W / Transformer3W
// ---------------------------------------------------------------------------

/// Canonical 2-winding transformer row model resolved from CIM ingestion.
///
/// Tenet 2: field names mirror `transformers_2w` semantics directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Transformer2W<'a> {
    pub base: BaseAttributes<'a>,
    pub from_bus_id: i32,
    pub to_bus_id: i32,
    pub r: Option<f64>,
    pub x: Option<f64>,
    pub g: Option<f64>,
    pub b: Option<f64>,
    pub tap_ratio: Option<f64>,
    pub phase_shift: Option<f64>,
    pub rate_a: Option<f64>,
    pub rate_b: Option<f64>,
    pub rate_c: Option<f64>,
    pub status: Option<bool>,
}

impl<'a> Transformer2W<'a> {
    /// Converts to a fully-owned (`'static`) [`Transformer2W`].
    pub fn into_owned(self) -> Transformer2W<'static> {
        Transformer2W {
            base: self.base.into_owned(),
            from_bus_id: self.from_bus_id,
            to_bus_id: self.to_bus_id,
            r: self.r,
            x: self.x,
            g: self.g,
            b: self.b,
            tap_ratio: self.tap_ratio,
            phase_shift: self.phase_shift,
            rate_a: self.rate_a,
            rate_b: self.rate_b,
            rate_c: self.rate_c,
            status: self.status,
        }
    }
}

impl<'a> IdentifiedObject for Transformer2W<'a> {
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

impl<'a> PowerSystemResource for Transformer2W<'a> {}
impl<'a> Equipment for Transformer2W<'a> {}

/// Canonical 3-winding transformer row model resolved from CIM ingestion.
///
/// Tenet 2: field names mirror `transformers_3w` semantics directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Transformer3W<'a> {
    pub base: BaseAttributes<'a>,
    pub bus_h_id: i32,
    pub bus_m_id: i32,
    pub bus_l_id: i32,
    pub r_hm: Option<f64>,
    pub x_hm: Option<f64>,
    pub r_hl: Option<f64>,
    pub x_hl: Option<f64>,
    pub r_ml: Option<f64>,
    pub x_ml: Option<f64>,
    pub tap_h: Option<f64>,
    pub tap_m: Option<f64>,
    pub tap_l: Option<f64>,
    pub phase_shift: Option<f64>,
    pub rate_a: Option<f64>,
    pub rate_b: Option<f64>,
    pub rate_c: Option<f64>,
    pub status: Option<bool>,
}

impl<'a> Transformer3W<'a> {
    /// Converts to a fully-owned (`'static`) [`Transformer3W`].
    pub fn into_owned(self) -> Transformer3W<'static> {
        Transformer3W {
            base: self.base.into_owned(),
            bus_h_id: self.bus_h_id,
            bus_m_id: self.bus_m_id,
            bus_l_id: self.bus_l_id,
            r_hm: self.r_hm,
            x_hm: self.x_hm,
            r_hl: self.r_hl,
            x_hl: self.x_hl,
            r_ml: self.r_ml,
            x_ml: self.x_ml,
            tap_h: self.tap_h,
            tap_m: self.tap_m,
            tap_l: self.tap_l,
            phase_shift: self.phase_shift,
            rate_a: self.rate_a,
            rate_b: self.rate_b,
            rate_c: self.rate_c,
            status: self.status,
        }
    }
}

impl<'a> IdentifiedObject for Transformer3W<'a> {
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

impl<'a> PowerSystemResource for Transformer3W<'a> {}
impl<'a> Equipment for Transformer3W<'a> {}

// ---------------------------------------------------------------------------
// Area / Zone / Owner
// ---------------------------------------------------------------------------

/// CIM area lookup row source.
///
/// Tenet 2: maps directly from CIM area profile payload to lookup table fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Area<'a> {
    pub base: BaseAttributes<'a>,
    pub interchange_mw: Option<f64>,
}

#[derive(Deserialize)]
struct RawArea<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "ControlArea.netInterchange", default)]
    interchange_mw: Option<f64>,
}

impl<'de: 'a, 'a> Deserialize<'de> for Area<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawArea::deserialize(deserializer)?;
        let m_rid = if let Some(m_rid) = raw.m_rid {
            strip_hash_cow(m_rid)
        } else if let Some(about) = raw.about {
            strip_hash_cow(about)
        } else {
            return Err(serde::de::Error::missing_field("@ID or @about"));
        };

        Ok(Area {
            base: BaseAttributes {
                m_rid,
                name: raw.name,
                description: raw.description,
            },
            interchange_mw: raw.interchange_mw,
        })
    }
}

impl<'a> Area<'a> {
    pub fn into_owned(self) -> Area<'static> {
        Area {
            base: self.base.into_owned(),
            interchange_mw: self.interchange_mw,
        }
    }
}

impl<'a> IdentifiedObject for Area<'a> {
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

impl<'a> PowerSystemResource for Area<'a> {}
impl<'a> Equipment for Area<'a> {}

/// CIM zone lookup row source.
#[derive(Debug, Clone, PartialEq)]
pub struct Zone<'a> {
    pub base: BaseAttributes<'a>,
}

#[derive(Deserialize)]
struct RawZone<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
}

impl<'de: 'a, 'a> Deserialize<'de> for Zone<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawZone::deserialize(deserializer)?;
        let m_rid = if let Some(m_rid) = raw.m_rid {
            strip_hash_cow(m_rid)
        } else if let Some(about) = raw.about {
            strip_hash_cow(about)
        } else {
            return Err(serde::de::Error::missing_field("@ID or @about"));
        };

        Ok(Zone {
            base: BaseAttributes {
                m_rid,
                name: raw.name,
                description: raw.description,
            },
        })
    }
}

impl<'a> Zone<'a> {
    pub fn into_owned(self) -> Zone<'static> {
        Zone {
            base: self.base.into_owned(),
        }
    }
}

impl<'a> IdentifiedObject for Zone<'a> {
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

impl<'a> PowerSystemResource for Zone<'a> {}
impl<'a> Equipment for Zone<'a> {}

/// CIM owner lookup row source.
#[derive(Debug, Clone, PartialEq)]
pub struct Owner<'a> {
    pub base: BaseAttributes<'a>,
}

#[derive(Deserialize)]
struct RawOwner<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
}

impl<'de: 'a, 'a> Deserialize<'de> for Owner<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawOwner::deserialize(deserializer)?;
        let m_rid = if let Some(m_rid) = raw.m_rid {
            strip_hash_cow(m_rid)
        } else if let Some(about) = raw.about {
            strip_hash_cow(about)
        } else {
            return Err(serde::de::Error::missing_field("@ID or @about"));
        };

        Ok(Owner {
            base: BaseAttributes {
                m_rid,
                name: raw.name,
                description: raw.description,
            },
        })
    }
}

impl<'a> Owner<'a> {
    pub fn into_owned(self) -> Owner<'static> {
        Owner {
            base: self.base.into_owned(),
        }
    }
}

impl<'a> IdentifiedObject for Owner<'a> {
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

impl<'a> PowerSystemResource for Owner<'a> {}
impl<'a> Equipment for Owner<'a> {}

// ---------------------------------------------------------------------------
// SynchronousMachine
// ---------------------------------------------------------------------------

/// CIM `SynchronousMachine` - a generator modelled as conducting equipment.
///
/// This profile only carries fields required by the locked v0.5 `generators`
/// table. Missing optional values are populated later by writer defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct SynchronousMachine<'a> {
    /// Inherited `IdentifiedObject` fields (mRID, name, description).
    pub base: BaseAttributes<'a>,

    /// Scheduled active power output (MW).
    pub p_sched_mw: Option<f64>,

    /// Minimum active power output (MW).
    pub p_min_mw: Option<f64>,

    /// Maximum active power output (MW).
    pub p_max_mw: Option<f64>,

    /// Minimum reactive power output (Mvar).
    pub q_min_mvar: Option<f64>,

    /// Maximum reactive power output (Mvar).
    pub q_max_mvar: Option<f64>,

    /// Machine base power (MVA).
    pub mbase_mva: Option<f64>,

    /// Inertia constant H.
    pub h: Option<f64>,

    /// Transient reactance Xd'.
    pub xd_prime: Option<f64>,

    /// Damping coefficient D.
    pub d: Option<f64>,

    /// Optional explicit upper operating limit (MW).
    pub uol_mw: Option<f64>,

    /// Optional explicit lower operating limit (MW).
    pub lol_mw: Option<f64>,

    /// Optional unit classification from CIM payload.
    pub unit_type: Option<Cow<'a, str>>,

    /// Optional fuel classification from CIM payload.
    pub fuel_type: Option<Cow<'a, str>>,

    /// Optional owner reference mRID from CIM payload.
    pub owner_mrid: Option<Cow<'a, str>>,

    /// Optional market resource identifier.
    pub market_resource_id: Option<Cow<'a, str>>,

    /// Optional IBR marker when explicitly declared in source CIM.
    pub is_ibr: Option<bool>,

    /// Optional IBR subtype from CIM payload.
    pub ibr_subtype: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct RawSynchronousMachine<'a> {
    #[serde(rename = "@ID", borrow)]
    m_rid: Cow<'a, str>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "RotatingMachine.p", default)]
    p_sched_mw: Option<f64>,
    #[serde(rename = "GeneratingUnit.minOperatingP", default)]
    p_min_mw: Option<f64>,
    #[serde(rename = "GeneratingUnit.maxOperatingP", default)]
    p_max_mw: Option<f64>,
    #[serde(rename = "SynchronousMachine.minQ", default)]
    q_min_mvar: Option<f64>,
    #[serde(rename = "SynchronousMachine.maxQ", default)]
    q_max_mvar: Option<f64>,
    #[serde(rename = "RotatingMachine.ratedS", default)]
    mbase_mva: Option<f64>,
    #[serde(rename = "SynchronousMachine.H", default)]
    h: Option<f64>,
    #[serde(rename = "SynchronousMachine.xdPrime", default)]
    xd_prime: Option<f64>,
    #[serde(rename = "SynchronousMachine.D", default)]
    d: Option<f64>,
    #[serde(rename = "SynchronousMachine.uol", default)]
    uol_mw: Option<f64>,
    #[serde(rename = "SynchronousMachine.lol", default)]
    lol_mw: Option<f64>,
    #[serde(rename = "SynchronousMachine.type", default, borrow)]
    sync_machine_type: Option<Cow<'a, str>>,
    #[serde(rename = "GeneratingUnit.genUnitType", default, borrow)]
    gen_unit_type: Option<Cow<'a, str>>,
    #[serde(rename = "GeneratingUnit.fuelType", default, borrow)]
    fuel_type: Option<Cow<'a, str>>,
    #[serde(rename = "PowerSystemResource.Owner", default)]
    owner: Option<RawSvResourceRef<'a>>,
    #[serde(rename = "PowerSystemResource.marketResource", default, borrow)]
    market_resource_id: Option<Cow<'a, str>>,
    #[serde(rename = "PowerElectronicsConnection.ibr", default)]
    is_ibr: Option<bool>,
    #[serde(rename = "PowerElectronicsConnection.type", default, borrow)]
    ibr_subtype: Option<Cow<'a, str>>,
}

impl<'de: 'a, 'a> Deserialize<'de> for SynchronousMachine<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawSynchronousMachine::deserialize(deserializer)?;
        Ok(SynchronousMachine {
            base: BaseAttributes {
                m_rid: raw.m_rid,
                name: raw.name,
                description: raw.description,
            },
            p_sched_mw: raw.p_sched_mw,
            p_min_mw: raw.p_min_mw,
            p_max_mw: raw.p_max_mw,
            q_min_mvar: raw.q_min_mvar,
            q_max_mvar: raw.q_max_mvar,
            mbase_mva: raw.mbase_mva,
            h: raw.h,
            xd_prime: raw.xd_prime,
            d: raw.d,
            uol_mw: raw.uol_mw,
            lol_mw: raw.lol_mw,
            unit_type: raw.gen_unit_type.or(raw.sync_machine_type),
            fuel_type: raw.fuel_type,
            owner_mrid: raw.owner.map(|owner| strip_hash_cow(owner.resource)),
            market_resource_id: raw.market_resource_id,
            is_ibr: raw.is_ibr,
            ibr_subtype: raw.ibr_subtype,
        })
    }
}

impl<'a> Serialize for SynchronousMachine<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("cim:SynchronousMachine", 10)?;
        s.serialize_field("@ID", &self.base.m_rid)?;
        if let Some(ref n) = self.base.name {
            s.serialize_field("IdentifiedObject.name", n)?;
        }
        if let Some(ref dsc) = self.base.description {
            s.serialize_field("IdentifiedObject.description", dsc)?;
        }
        if let Some(v) = self.p_sched_mw {
            s.serialize_field("RotatingMachine.p", &v)?;
        }
        if let Some(v) = self.p_min_mw {
            s.serialize_field("GeneratingUnit.minOperatingP", &v)?;
        }
        if let Some(v) = self.p_max_mw {
            s.serialize_field("GeneratingUnit.maxOperatingP", &v)?;
        }
        if let Some(v) = self.q_min_mvar {
            s.serialize_field("SynchronousMachine.minQ", &v)?;
        }
        if let Some(v) = self.q_max_mvar {
            s.serialize_field("SynchronousMachine.maxQ", &v)?;
        }
        if let Some(v) = self.mbase_mva {
            s.serialize_field("RotatingMachine.ratedS", &v)?;
        }
        if let Some(v) = self.h {
            s.serialize_field("SynchronousMachine.H", &v)?;
        }
        if let Some(v) = self.xd_prime {
            s.serialize_field("SynchronousMachine.xdPrime", &v)?;
        }
        if let Some(v) = self.d {
            s.serialize_field("SynchronousMachine.D", &v)?;
        }
        if let Some(v) = self.uol_mw {
            s.serialize_field("SynchronousMachine.uol", &v)?;
        }
        if let Some(v) = self.lol_mw {
            s.serialize_field("SynchronousMachine.lol", &v)?;
        }
        if let Some(v) = self.unit_type.as_deref() {
            s.serialize_field("GeneratingUnit.genUnitType", v)?;
        }
        if let Some(v) = self.fuel_type.as_deref() {
            s.serialize_field("GeneratingUnit.fuelType", v)?;
        }
        if let Some(v) = self.market_resource_id.as_deref() {
            s.serialize_field("PowerSystemResource.marketResource", v)?;
        }
        if let Some(v) = self.is_ibr {
            s.serialize_field("PowerElectronicsConnection.ibr", &v)?;
        }
        if let Some(v) = self.ibr_subtype.as_deref() {
            s.serialize_field("PowerElectronicsConnection.type", v)?;
        }
        s.end()
    }
}

impl<'a> SynchronousMachine<'a> {
    /// Creates a new [`SynchronousMachine`] with only required identity fields.
    pub fn new(base: BaseAttributes<'a>) -> Self {
        Self {
            base,
            p_sched_mw: None,
            p_min_mw: None,
            p_max_mw: None,
            q_min_mvar: None,
            q_max_mvar: None,
            mbase_mva: None,
            h: None,
            xd_prime: None,
            d: None,
            uol_mw: None,
            lol_mw: None,
            unit_type: None,
            fuel_type: None,
            owner_mrid: None,
            market_resource_id: None,
            is_ibr: None,
            ibr_subtype: None,
        }
    }

    /// Converts to a fully-owned (`'static`) [`SynchronousMachine`].
    pub fn into_owned(self) -> SynchronousMachine<'static> {
        SynchronousMachine {
            base: self.base.into_owned(),
            p_sched_mw: self.p_sched_mw,
            p_min_mw: self.p_min_mw,
            p_max_mw: self.p_max_mw,
            q_min_mvar: self.q_min_mvar,
            q_max_mvar: self.q_max_mvar,
            mbase_mva: self.mbase_mva,
            h: self.h,
            xd_prime: self.xd_prime,
            d: self.d,
            uol_mw: self.uol_mw,
            lol_mw: self.lol_mw,
            unit_type: self.unit_type.map(|value| Cow::Owned(value.into_owned())),
            fuel_type: self.fuel_type.map(|value| Cow::Owned(value.into_owned())),
            owner_mrid: self.owner_mrid.map(|value| Cow::Owned(value.into_owned())),
            market_resource_id: self
                .market_resource_id
                .map(|value| Cow::Owned(value.into_owned())),
            is_ibr: self.is_ibr,
            ibr_subtype: self.ibr_subtype.map(|value| Cow::Owned(value.into_owned())),
        }
    }
}

impl<'a> IdentifiedObject for SynchronousMachine<'a> {
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

impl<'a> PowerSystemResource for SynchronousMachine<'a> {}
impl<'a> Equipment for SynchronousMachine<'a> {}
impl<'a> ConductingEquipment for SynchronousMachine<'a> {}

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
            status: Some(true),
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
