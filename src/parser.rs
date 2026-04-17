// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RDF/XML parsing helpers for IEC 61970 CIM documents.
//!
//! This module wraps `quick-xml`'s `de::from_str` / `de::from_reader` to
//! deserialize top-level CIM elements into typed Rust structs defined in the
//! [`models`] module.
//!
//! ## Zero-copy usage
//!
//! When the entire XML document is already in memory as a `&str`, use the
//! `from_str` family of functions.  `quick-xml` can then hand string slices
//! directly into the [`Cow::Borrowed`] variants of [`BaseAttributes`],
//! avoiding any heap allocation for string data.
//!
//! ## Namespace prefix handling
//!
//! `quick-xml`'s serde deserialiser strips XML namespace prefixes from
//! attribute names, so the `rdf:ID="..."` attribute is exposed as `@ID`.
//! Element names including namespace prefixes (e.g. `cim:ACLineSegment`) are
//! matched as the element tag name; child element text content is accessed via
//! the local element name (e.g. `IdentifiedObject.name`).
//!
//! ```rust
//! use raptrix_cim_rs::parser;
//! use raptrix_cim_rs::models::base::IdentifiedObject;
//!
//! let xml = r#"<cim:ACLineSegment rdf:ID="Line1">
//!   <IdentifiedObject.name>Main Feeder</IdentifiedObject.name>
//!   <ACLineSegment.r>0.05</ACLineSegment.r>
//!   <ACLineSegment.x>0.12</ACLineSegment.x>
//! </cim:ACLineSegment>"#;
//!
//! let line = parser::ac_line_segment_from_str(xml).unwrap();
//! assert_eq!(line.mrid(), "Line1");
//! ```
//!
//! [`models`]: crate::models
//! [`BaseAttributes`]: crate::models::BaseAttributes
//! [`Cow::Borrowed`]: std::borrow::Cow::Borrowed

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::OnceLock;

use anyhow::{Result, bail};
use quick_xml::Reader;
use quick_xml::de::from_str;
use quick_xml::events::Event;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::models::base::BaseAttributes;
use crate::models::{
    ACLineSegment, Area, ConnectivityNodeGroup, EnergyConsumer, Owner, SvShuntCompensator,
    SynchronousMachine, TopologicalNode, Zone,
};

/// A parsing error returned by the helper functions in this module.
pub type ParseError = quick_xml::DeError;

fn should_log_rdf_about_fallback() -> bool {
    static SHOULD_LOG: OnceLock<bool> = OnceLock::new();
    *SHOULD_LOG.get_or_init(|| {
        std::env::var("RAPTRIX_LOG_RDF_ABOUT_FALLBACK")
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn log_rdf_about_fallback(mrid: &str) {
    if should_log_rdf_about_fallback() {
        eprintln!("Fallback: using rdf:about for mRID: {mrid}");
    }
}

// ---------------------------------------------------------------------------
// Generic helper
// ---------------------------------------------------------------------------

/// Deserialises a single XML fragment into any type `T` that implements
/// `serde::de::DeserializeOwned`.
///
/// Note: `T` must be `DeserializeOwned` (i.e. `for<'de> Deserialize<'de>`).
/// Borrowed CIM types such as `ACLineSegment<'_>` are NOT `DeserializeOwned`
/// because they borrow from the input buffer; use the typed helpers
/// ([`ac_line_segment_from_str`], [`energy_consumer_from_str`]) for zero-copy
/// parsing of those types.
pub fn from_xml_str<T: DeserializeOwned>(xml: &str) -> Result<T, ParseError> {
    from_str(xml)
}

// ---------------------------------------------------------------------------
// Typed parse helpers
// ---------------------------------------------------------------------------

/// Parses a single `<cim:ACLineSegment>` XML fragment.
///
/// The returned value borrows string data from `xml` where possible
/// (zero-copy, via [`Cow::Borrowed`]).  Call
/// [`.into_owned()`][ACLineSegment::into_owned] if you need a `'static` value
/// after `xml` is dropped.
///
/// [`Cow::Borrowed`]: std::borrow::Cow::Borrowed
pub fn ac_line_segment_from_str(xml: &str) -> Result<ACLineSegment<'_>, ParseError> {
    from_str(xml)
}

/// Parses a single `<cim:EnergyConsumer>` XML fragment.
///
/// The returned value borrows string data from `xml` where possible
/// (zero-copy, via [`Cow::Borrowed`]).  Call
/// [`.into_owned()`][EnergyConsumer::into_owned] if you need a `'static`
/// value after `xml` is dropped.
///
/// [`Cow::Borrowed`]: std::borrow::Cow::Borrowed
pub fn energy_consumer_from_str(xml: &str) -> Result<EnergyConsumer<'_>, ParseError> {
    from_str(xml)
}

/// Parses a single `<cim:SynchronousMachine>` XML fragment.
///
/// The returned value borrows string data from `xml` where possible
/// (zero-copy, via [`Cow::Borrowed`]).  Call
/// [`.into_owned()`][SynchronousMachine::into_owned] if you need a `'static`
/// value after `xml` is dropped.
///
/// [`Cow::Borrowed`]: std::borrow::Cow::Borrowed
pub fn synchronous_machine_from_str(xml: &str) -> Result<SynchronousMachine<'_>, ParseError> {
    from_str(xml)
}

/// Parses a single `<cim:SvShuntCompensator>` XML fragment.
pub fn sv_shunt_compensator_from_str(xml: &str) -> Result<SvShuntCompensator<'_>, ParseError> {
    from_str(xml)
}

/// Parses a single `<cim:TopologicalNode>` XML fragment.
pub fn topological_node_from_str(xml: &str) -> Result<TopologicalNode<'_>, ParseError> {
    from_str(xml)
}

/// Parses all `<cim:ACLineSegment>` elements from a CGMES RDF/XML reader.
///
/// This helper is intentionally small and practical for integration tests:
/// it reads the file into memory, extracts each `cim:ACLineSegment` XML
/// fragment, then reuses [`ac_line_segment_from_str`] so the existing typed
/// deserializer still does the actual CIM field parsing.
pub fn ac_line_segments_from_reader<R: Read>(mut reader: R) -> Result<Vec<ACLineSegment<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let fragments = extract_elements(&xml, "cim:ACLineSegment")?;
    let mut lines = Vec::with_capacity(fragments.len());

    for fragment in fragments {
        lines.push(ac_line_segment_from_str(fragment)?.into_owned());
    }

    Ok(lines)
}

/// Parsed branch row derived from live CGMES EQ content.
///
/// The `from_bus_id` and `to_bus_id` values are deterministic integer IDs
/// assigned to unique `ConnectivityNode` references found in terminal data.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchRow {
    pub line_mrid: String,
    pub from_bus_id: i32,
    pub to_bus_id: i32,
    pub r: f64,
    pub x: f64,
    pub b_shunt: f64,
}

/// Parsed linear shunt payload derived from EQ profile XML.
#[derive(Debug, Clone, PartialEq)]
pub struct FixedShuntSpec {
    pub equipment_mrid: String,
    pub status: Option<bool>,
    pub g_mw: Option<f64>,
    pub b_mvar: Option<f64>,
}

/// Parsed switch payload for optional node-breaker detail tables.
#[derive(Debug, Clone, PartialEq)]
pub struct SwitchSpec {
    pub switch_mrid: String,
    pub switch_type: String,
    pub name: Option<String>,
    pub is_open: Option<bool>,
    pub normal_open: Option<bool>,
    pub retained: Option<bool>,
}

/// Parsed ConnectivityNode payload for optional node-breaker detail tables.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectivityNodeSpec {
    pub connectivity_node_mrid: String,
    pub topological_node_mrid: Option<String>,
}

/// Parsed dynamic-model payload used for DY-profile-driven dynamics rows.
#[derive(Debug, Clone, PartialEq)]
pub struct DyModelSpec {
    pub equipment_mrid: String,
    pub model_type: String,
    pub params: Vec<(String, f64)>,
}

/// Parsed `Diagram` payload used for optional layout emission.
#[derive(Debug, Clone, PartialEq)]
pub struct DiagramRecord {
    pub diagram_rdf_id: String,
    pub name: Option<String>,
}

/// Parsed `DiagramObject` payload used for optional layout emission.
#[derive(Debug, Clone, PartialEq)]
pub struct DiagramObjectRecord {
    pub obj_rdf_id: String,
    pub diagram_rdf_id: String,
    pub identified_object_rdf_id: String,
    pub rotation: Option<f32>,
    pub drawing_order: Option<i32>,
}

/// Parsed `DiagramObjectPoint` payload used for optional layout emission.
#[derive(Debug, Clone, PartialEq)]
pub struct DiagramPointRecord {
    pub obj_rdf_id: String,
    pub seq: i32,
    pub x: f64,
    pub y: f64,
}

/// Parsed BaseVoltage payload keyed by mRID.
#[derive(Debug, Clone, PartialEq)]
pub struct BaseVoltageSpec {
    pub base_voltage_mrid: String,
    pub nominal_kv: f64,
}

/// Parsed association from equipment mRID to BaseVoltage mRID.
#[derive(Debug, Clone, PartialEq)]
pub struct EquipmentBaseVoltageRef {
    pub equipment_mrid: String,
    pub base_voltage_mrid: String,
}

/// Parses all `<cim:BaseVoltage>` elements from CGMES RDF/XML.
pub fn base_voltage_specs_from_reader<R: Read>(mut reader: R) -> Result<Vec<BaseVoltageSpec>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:BaseVoltage") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:BaseVoltage")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let raw: RawBaseVoltage = from_str(fragment)?;
        let Some(base_voltage_mrid) = raw.resolved_mrid() else {
            continue;
        };
        let Some(nominal_kv) = raw.nominal_voltage else {
            continue;
        };
        rows.push(BaseVoltageSpec {
            base_voltage_mrid,
            nominal_kv,
        });
    }

    rows.sort_unstable_by(|left, right| {
        left.base_voltage_mrid
            .cmp(&right.base_voltage_mrid)
            .then_with(|| left.nominal_kv.total_cmp(&right.nominal_kv))
    });
    rows.dedup_by(|left, right| left.base_voltage_mrid == right.base_voltage_mrid);

    Ok(rows)
}

/// Parses equipment to BaseVoltage references from common conducting equipment tags.
pub fn equipment_base_voltage_refs_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<EquipmentBaseVoltageRef>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let mut refs = Vec::new();
    for tag in [
        "cim:ACLineSegment",
        "cim:SynchronousMachine",
        "cim:EnergyConsumer",
        "cim:ConformLoad",
        "cim:NonConformLoad",
        "cim:PowerTransformer",
    ] {
        if !contains_exact_element_tag(&xml, tag) {
            continue;
        }

        let fragments = extract_elements(&xml, tag)?;
        for fragment in fragments {
            let raw: RawEquipmentBaseVoltageRef = from_str(fragment)?;
            let Some(equipment_mrid) = raw.resolved_mrid() else {
                continue;
            };
            let Some(base_voltage) = raw
                .conducting_equipment_base_voltage
                .or(raw.equipment_base_voltage)
            else {
                continue;
            };

            refs.push(EquipmentBaseVoltageRef {
                equipment_mrid,
                base_voltage_mrid: normalize_cgmes_ref(&base_voltage.resource),
            });
        }
    }

    refs.sort_unstable_by(|left, right| {
        left.equipment_mrid
            .cmp(&right.equipment_mrid)
            .then_with(|| left.base_voltage_mrid.cmp(&right.base_voltage_mrid))
    });
    refs.dedup_by(|left, right| {
        left.equipment_mrid == right.equipment_mrid
            && left.base_voltage_mrid == right.base_voltage_mrid
    });
    Ok(refs)
}

/// Parsed PowerTransformer with attached winding-end parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct PowerTransformer<'a> {
    pub base: BaseAttributes<'a>,
    pub status: Option<bool>,
    pub vector_group: Option<Cow<'a, str>>,
    pub rate_a: Option<f64>,
    pub rate_b: Option<f64>,
    pub rate_c: Option<f64>,
    pub ends: Vec<PowerTransformerEnd>,
}

impl<'a> PowerTransformer<'a> {
    pub fn into_owned(self) -> PowerTransformer<'static> {
        PowerTransformer {
            base: self.base.into_owned(),
            status: self.status,
            vector_group: self
                .vector_group
                .map(|value| Cow::Owned(value.into_owned())),
            rate_a: self.rate_a,
            rate_b: self.rate_b,
            rate_c: self.rate_c,
            ends: self.ends,
        }
    }
}

/// Parsed PowerTransformerEnd payload used for 2w/3w mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct PowerTransformerEnd {
    pub end_number: i32,
    pub r: Option<f64>,
    pub x: Option<f64>,
    pub g: Option<f64>,
    pub b: Option<f64>,
    pub tap_ratio: Option<f64>,
    pub phase_shift: Option<f64>,
    pub rate: Option<f64>,
}

/// Parses all `<cim:EnergyConsumer>` elements from a CGMES RDF/XML reader.
///
/// Tenet 1: extraction reuses fragment parsing helpers that preserve borrowed
/// string data until explicit ownership conversion.
/// Tenet 2: this helper maps directly to CIM `EnergyConsumer` without
/// reshaping semantics.
pub fn energy_consumers_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<EnergyConsumer<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    // Tenet 2: include CIM subtype payloads that carry EnergyConsumer fields.
    let mut fragments: Vec<&str> = Vec::new();
    for tag in [
        "cim:EnergyConsumer",
        "cim:ConformLoad",
        "cim:NonConformLoad",
    ] {
        if contains_exact_element_tag(&xml, tag) {
            let mut tag_fragments = extract_elements(&xml, tag)?;
            fragments.append(&mut tag_fragments);
        }
    }

    let mut loads = Vec::with_capacity(fragments.len());

    for fragment in fragments {
        loads.push(energy_consumer_from_str(fragment)?.into_owned());
    }

    Ok(loads)
}

/// Parses all `<cim:SynchronousMachine>` elements from a CGMES RDF/XML reader.
pub fn synchronous_machines_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<SynchronousMachine<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let fragments = extract_elements(&xml, "cim:SynchronousMachine")?;
    let mut machines = Vec::with_capacity(fragments.len());

    for fragment in fragments {
        machines.push(synchronous_machine_from_str(fragment)?.into_owned());
    }

    Ok(machines)
}

/// Parses all `<cim:PowerTransformer>` elements and their associated
/// `<cim:PowerTransformerEnd>` payloads from a CGMES RDF/XML reader.
///
/// Tenet 1: maintains borrowed ID fields until ownership is required at API
/// boundary.
/// Tenet 2: keeps direct CIM entity mapping for downstream 2w/3w conversion.
pub fn power_transformers_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<PowerTransformer<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:PowerTransformer") {
        return Ok(Vec::new());
    }

    let transformer_fragments = extract_elements(&xml, "cim:PowerTransformer")?;
    let mut transformers = Vec::with_capacity(transformer_fragments.len());
    for fragment in transformer_fragments {
        let raw: RawPowerTransformer = from_str(fragment)?;
        let Some(m_rid) = raw.resolved_mrid() else {
            continue;
        };

        transformers.push(PowerTransformer {
            base: BaseAttributes {
                m_rid: Cow::Owned(m_rid),
                name: raw.name,
                description: raw.description,
            },
            status: raw.status,
            vector_group: raw.vector_group,
            rate_a: raw.rate_a,
            rate_b: raw.rate_b,
            rate_c: raw.rate_c,
            ends: Vec::new(),
        });
    }

    if contains_exact_element_tag(&xml, "cim:PowerTransformerEnd") {
        let end_fragments = extract_elements(&xml, "cim:PowerTransformerEnd")?;
        let mut ends_by_transformer: HashMap<String, Vec<PowerTransformerEnd>> = HashMap::new();
        for fragment in end_fragments {
            let raw_end: RawPowerTransformerEnd = from_str(fragment)?;
            let Some(transformer_mrid) = raw_end.transformer_mrid() else {
                continue;
            };

            ends_by_transformer
                .entry(transformer_mrid)
                .or_default()
                .push(PowerTransformerEnd {
                    end_number: raw_end.end_number.unwrap_or(i32::MAX),
                    r: raw_end.r,
                    x: raw_end.x,
                    g: raw_end.g,
                    b: raw_end.b,
                    tap_ratio: raw_end.tap_ratio,
                    phase_shift: raw_end.phase_shift,
                    rate: raw_end.rated_s,
                });
        }

        for transformer in &mut transformers {
            if let Some(mut ends) = ends_by_transformer.remove(transformer.base.m_rid.as_ref()) {
                ends.sort_unstable_by_key(|end| end.end_number);
                transformer.ends = ends;
            }
        }
    }

    Ok(transformers
        .into_iter()
        .map(PowerTransformer::into_owned)
        .collect())
}

/// Parses all `<cim:ControlArea>` elements from CGMES RDF/XML.
///
/// Tenet 1: keeps borrowed XML fields until explicit ownership conversion.
/// Tenet 2: direct mapping of area lookup payload from CIM source.
pub fn areas_from_reader<R: Read>(mut reader: R) -> Result<Vec<Area<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:ControlArea") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:ControlArea")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        rows.push(from_str::<Area<'_>>(fragment)?.into_owned());
    }

    Ok(rows)
}

/// Parses all `<cim:SubGeographicalRegion>` elements from CGMES RDF/XML.
///
/// Tenet 2: direct mapping of zone lookup payload from CIM source.
pub fn zones_from_reader<R: Read>(mut reader: R) -> Result<Vec<Zone<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:SubGeographicalRegion") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:SubGeographicalRegion")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        rows.push(from_str::<Zone<'_>>(fragment)?.into_owned());
    }

    Ok(rows)
}

/// Parses all `<cim:Organisation>` elements from CGMES RDF/XML.
///
/// Tenet 2: direct mapping of owner lookup payload from CIM source.
pub fn owners_from_reader<R: Read>(mut reader: R) -> Result<Vec<Owner<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:Organisation") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:Organisation")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        rows.push(from_str::<Owner<'_>>(fragment)?.into_owned());
    }

    Ok(rows)
}

/// Parses all `<cim:LinearShuntCompensator>` elements from EQ RDF/XML.
///
/// Tenet 2: maps directly from CIM shunt fields with minimal derivation.
pub fn fixed_shunts_from_reader<R: Read>(mut reader: R) -> Result<Vec<FixedShuntSpec>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:LinearShuntCompensator") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:LinearShuntCompensator")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let shunt: RawLinearShuntCompensator = from_str(fragment)?;
        let Some(equipment_mrid) = shunt.resolved_mrid() else {
            continue;
        };

        let sections = shunt.sections.unwrap_or(1.0);
        let g_per_section = shunt
            .linear_g_per_section
            .or(shunt.shunt_g_per_section)
            .unwrap_or(0.0);
        let b_per_section = shunt
            .linear_b_per_section
            .or(shunt.shunt_b_per_section)
            .unwrap_or(0.0);

        rows.push(FixedShuntSpec {
            equipment_mrid,
            status: shunt.status,
            g_mw: Some(g_per_section * sections),
            b_mvar: Some(b_per_section * sections),
        });
    }

    Ok(rows)
}

/// Parses all `<cim:SvShuntCompensator>` elements from CGMES RDF/XML.
///
/// Tenet 1: deserializes borrowed XML fragments and only materializes owned
/// values at the API boundary.
/// Tenet 2: maps directly from CIM `SvShuntCompensator` to switched-shunt
/// writer rows.
pub fn sv_shunt_compensators_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<SvShuntCompensator<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:SvShuntCompensator") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:SvShuntCompensator")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        rows.push(sv_shunt_compensator_from_str(fragment)?.into_owned());
    }

    Ok(rows)
}

/// Parses all `<cim:TopologicalNode>` elements from a CGMES TP RDF/XML reader.
pub fn topological_nodes_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<TopologicalNode<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !xml.contains("<cim:TopologicalNode") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:TopologicalNode")?;
    let mut nodes = Vec::with_capacity(fragments.len());

    for fragment in fragments {
        nodes.push(topological_node_from_str(fragment)?.into_owned());
    }

    Ok(nodes)
}

/// Parses TopologicalNode -> ConnectivityNode group membership from TP/EQ links.
///
/// Supports both representations:
/// - `<TopologicalNode.ConnectivityNodes rdf:resource="#..."/>`
/// - `<ConnectivityNode.TopologicalNode rdf:resource="#..."/>`
pub fn connectivity_node_groups_from_reader<R: Read>(
    mut reader: R,
) -> Result<Vec<ConnectivityNodeGroup<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();

    if xml.contains("<cim:TopologicalNode") {
        let topological_fragments = extract_elements(&xml, "cim:TopologicalNode")?;
        for fragment in topological_fragments {
            let node: RawTopologicalNodeLinks = from_str(fragment)?;
            let Some(topological_mrid) = node.resolved_mrid() else {
                continue;
            };
            let entry = grouped.entry(topological_mrid).or_default();
            for link in node.connectivity_nodes {
                entry.push(normalize_cgmes_ref(&link.resource));
            }
        }
    }

    if xml.contains("<cim:ConnectivityNode") {
        let connectivity_fragments = extract_elements(&xml, "cim:ConnectivityNode")?;
        for fragment in connectivity_fragments {
            let node: RawConnectivityNode = from_str(fragment)?;
            let Some(connectivity_mrid) = node.resolved_mrid() else {
                continue;
            };
            let Some(topological_ref) = node.topological_node else {
                continue;
            };
            let topological_mrid = normalize_cgmes_ref(&topological_ref.resource);
            grouped
                .entry(topological_mrid)
                .or_default()
                .push(connectivity_mrid);
        }
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (topological_mrid, mut connectivity_nodes) in grouped {
        connectivity_nodes.sort_unstable();
        connectivity_nodes.dedup();
        out.push(ConnectivityNodeGroup {
            topological_node_mrid: Cow::Owned(topological_mrid),
            connectivity_node_mrids: connectivity_nodes.into_iter().map(Cow::Owned).collect(),
        });
    }

    Ok(out)
}

/// Parses switch-like CIM elements used for node-breaker detail emission.
///
/// Supported tags:
/// - `cim:Breaker`
/// - `cim:Disconnector`
/// - `cim:LoadBreakSwitch`
/// - `cim:Switch`
pub fn switch_specs_from_reader<R: Read>(mut reader: R) -> Result<Vec<SwitchSpec>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let mut rows = Vec::new();
    for (tag, switch_type) in [
        ("cim:Breaker", "Breaker"),
        ("cim:Disconnector", "Disconnector"),
        ("cim:LoadBreakSwitch", "LoadBreakSwitch"),
        ("cim:Switch", "Switch"),
    ] {
        if !contains_exact_element_tag(&xml, tag) {
            continue;
        }
        let fragments = extract_elements(&xml, tag)?;
        for fragment in fragments {
            let raw: RawSwitch<'_> = from_str(fragment)?;
            let Some(switch_mrid) = raw.resolved_mrid() else {
                continue;
            };
            rows.push(SwitchSpec {
                switch_mrid,
                switch_type: switch_type.to_string(),
                name: raw
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
                is_open: raw.open,
                normal_open: raw.normal_open,
                retained: raw.retained,
            });
        }
    }

    rows.sort_unstable_by(|left, right| left.switch_mrid.cmp(&right.switch_mrid));
    rows.dedup_by(|left, right| left.switch_mrid == right.switch_mrid);
    Ok(rows)
}

/// Parses raw `ConnectivityNode` rows for optional node-breaker detail tables.
pub fn connectivity_nodes_from_reader<R: Read>(mut reader: R) -> Result<Vec<ConnectivityNodeSpec>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    if !contains_exact_element_tag(&xml, "cim:ConnectivityNode") {
        return Ok(Vec::new());
    }

    let fragments = extract_elements(&xml, "cim:ConnectivityNode")?;
    let mut rows = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let node: RawConnectivityNode = from_str(fragment)?;
        let Some(connectivity_node_mrid) = node.resolved_mrid() else {
            continue;
        };
        rows.push(ConnectivityNodeSpec {
            connectivity_node_mrid,
            topological_node_mrid: node
                .topological_node
                .as_ref()
                .map(|reference| normalize_cgmes_ref(&reference.resource)),
        });
    }

    rows.sort_unstable_by(|left, right| {
        left.connectivity_node_mrid
            .cmp(&right.connectivity_node_mrid)
    });
    rows.dedup_by(|left, right| left.connectivity_node_mrid == right.connectivity_node_mrid);
    Ok(rows)
}

/// Parses IEC 61970-453 diagram layout payloads from CGMES RDF/XML.
pub fn diagram_layout_from_reader<R: Read>(
    mut reader: R,
) -> Result<(
    Vec<DiagramRecord>,
    Vec<DiagramObjectRecord>,
    Vec<DiagramPointRecord>,
)> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let mut diagrams = Vec::new();
    if contains_exact_element_tag(&xml, "cim:Diagram") {
        for fragment in extract_elements(&xml, "cim:Diagram")? {
            let raw: RawDiagram<'_> = from_str(fragment)?;
            let Some(diagram_rdf_id) = raw.resolved_mrid() else {
                continue;
            };

            diagrams.push(DiagramRecord {
                diagram_rdf_id,
                name: raw
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
            });
        }
    }

    let mut objects = Vec::new();
    if contains_exact_element_tag(&xml, "cim:DiagramObject") {
        for fragment in extract_elements(&xml, "cim:DiagramObject")? {
            let raw: RawDiagramObject<'_> = from_str(fragment)?;
            let Some(obj_rdf_id) = raw.resolved_mrid() else {
                continue;
            };
            let Some(diagram_ref) = raw.diagram else {
                continue;
            };
            let Some(identified_object) = raw.identified_object else {
                continue;
            };

            objects.push(DiagramObjectRecord {
                obj_rdf_id,
                diagram_rdf_id: normalize_cgmes_ref(&diagram_ref.resource),
                identified_object_rdf_id: normalize_cgmes_ref(&identified_object.resource),
                rotation: raw.rotation,
                drawing_order: raw.drawing_order,
            });
        }
    }

    let mut points = Vec::new();
    if contains_exact_element_tag(&xml, "cim:DiagramObjectPoint") {
        for fragment in extract_elements(&xml, "cim:DiagramObjectPoint")? {
            let raw: RawDiagramObjectPoint = from_str(fragment)?;
            let Some(diagram_object) = raw.diagram_object else {
                continue;
            };
            let (Some(x), Some(y)) = (raw.x, raw.y) else {
                continue;
            };

            points.push(DiagramPointRecord {
                obj_rdf_id: normalize_cgmes_ref(&diagram_object.resource),
                seq: raw.sequence_number.unwrap_or(0),
                x,
                y,
            });
        }
    }

    diagrams.sort_unstable_by(|left, right| left.diagram_rdf_id.cmp(&right.diagram_rdf_id));
    diagrams.dedup_by(|left, right| left.diagram_rdf_id == right.diagram_rdf_id);
    objects.sort_unstable_by(|left, right| {
        left.diagram_rdf_id
            .cmp(&right.diagram_rdf_id)
            .then_with(|| {
                left.identified_object_rdf_id
                    .cmp(&right.identified_object_rdf_id)
            })
            .then_with(|| left.obj_rdf_id.cmp(&right.obj_rdf_id))
    });
    objects.dedup_by(|left, right| left.obj_rdf_id == right.obj_rdf_id);
    points.sort_unstable_by(|left, right| {
        left.obj_rdf_id
            .cmp(&right.obj_rdf_id)
            .then_with(|| left.seq.cmp(&right.seq))
            .then_with(|| left.x.total_cmp(&right.x))
            .then_with(|| left.y.total_cmp(&right.y))
    });

    Ok((diagrams, objects, points))
}

/// Parses selected DY profile model blocks and resolves attached equipment mRID.
///
/// This first-pass parser extracts dynamic model identity and the equipment link
/// reference so writer logic can map DY rows onto generator dynamics output.
pub fn dy_model_specs_from_reader<R: Read>(mut reader: R) -> Result<Vec<DyModelSpec>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let mut rows = Vec::new();
    for (tag, model_type) in [
        (
            "cim:SynchronousMachineDynamics",
            "SynchronousMachineDynamics",
        ),
        (
            "cim:AsynchronousMachineDynamics",
            "AsynchronousMachineDynamics",
        ),
        ("cim:ExcitationSystemDynamics", "ExcitationSystemDynamics"),
        ("cim:TurbineGovernorDynamics", "TurbineGovernorDynamics"),
        (
            "cim:PowerSystemStabilizerDynamics",
            "PowerSystemStabilizerDynamics",
        ),
        // Extension example: Studio-originated custom dynamic model family.
        ("cim:RaptrixSmartValveDynamics", "raptrix.smart_valve.v1"),
    ] {
        if !contains_exact_element_tag(&xml, tag) {
            continue;
        }

        for fragment in extract_elements(&xml, tag)? {
            let raw: RawDyModel<'_> = from_str(fragment)?;
            let Some(equipment_mrid) = raw.resolved_equipment_mrid() else {
                continue;
            };
            let mut params = extract_numeric_dy_params(fragment, model_type);
            if params.is_empty() {
                // Keep a deterministic numeric marker so params map is non-empty.
                params.push(("dy_present".to_string(), 1.0));
            }

            rows.push(DyModelSpec {
                equipment_mrid,
                model_type: model_type.to_string(),
                params,
            });
        }
    }

    rows.sort_unstable_by(|left, right| {
        left.equipment_mrid
            .cmp(&right.equipment_mrid)
            .then_with(|| left.model_type.cmp(&right.model_type))
    });
    rows.dedup_by(|left, right| {
        left.equipment_mrid == right.equipment_mrid && left.model_type == right.model_type
    });

    Ok(rows)
}

fn extract_numeric_dy_params(fragment: &str, model_type: &str) -> Vec<(String, f64)> {
    let mut reader = Reader::from_str(fragment);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut current_tag: Option<String> = None;
    let mut params: BTreeMap<String, f64> = BTreeMap::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(tag)) => {
                let name = String::from_utf8_lossy(tag.name().as_ref()).into_owned();
                current_tag = Some(name);
            }
            Ok(Event::Text(text)) => {
                let Some(tag_name) = current_tag.as_deref() else {
                    buf.clear();
                    continue;
                };

                let Ok(raw_value) = text.xml10_content() else {
                    buf.clear();
                    continue;
                };
                let Ok(value) = raw_value.trim().parse::<f64>() else {
                    buf.clear();
                    continue;
                };

                let key = canonicalize_dy_param_name(model_type, tag_name);
                if !key.is_empty() {
                    // Last-wins keeps deterministic merge behavior for repeated keys.
                    params.insert(key, value);
                }
            }
            Ok(Event::End(_)) => {
                current_tag = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buf.clear();
    }

    params.into_iter().collect()
}

fn canonicalize_dy_param_name(model_type: &str, tag_name: &str) -> String {
    let local = tag_name.rsplit(':').next().unwrap_or(tag_name);
    let leaf = local.rsplit('.').next().unwrap_or(local);

    let snake = to_snake_case(leaf);
    if snake.is_empty() {
        return snake;
    }

    canonical_alias_for_model(model_type, &snake)
}

fn to_snake_case(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut prev_lower_or_digit = false;
    let mut prev_underscore = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && prev_lower_or_digit && !prev_underscore {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
            prev_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            prev_underscore = false;
        } else {
            if !prev_underscore && !normalized.is_empty() {
                normalized.push('_');
            }
            prev_lower_or_digit = false;
            prev_underscore = true;
        }
    }

    normalized.trim_matches('_').to_string()
}

fn canonical_alias_for_model(model_type: &str, key: &str) -> String {
    match model_type {
        // Keep common synchronous-machine keys stable for downstream consumers.
        "SynchronousMachineDynamics" => match key {
            "xdprime" => "xd_prime".to_string(),
            "xqprime" => "xq_prime".to_string(),
            "xdpp" | "xdoubleprime" | "xdouble_prime" => "xd_double_prime".to_string(),
            "xqpp" | "xdoubleprime_q" | "xqdoubleprime" | "xq_double_prime" => {
                "xq_double_prime".to_string()
            }
            _ => key.to_string(),
        },
        _ => key.to_string(),
    }
}

/// Parsed terminal endpoint linkage used for EQ topology joins.
#[derive(Debug, Clone, PartialEq)]
pub struct TerminalLink {
    pub sequence_number: i32,
    pub line_mrid: String,
    pub connectivity_node_mrid: String,
}

/// Parses all `<cim:Terminal>` elements from CGMES RDF/XML and resolves
/// references needed for topology joining.
pub fn terminal_links_from_reader<R: Read>(mut reader: R) -> Result<Vec<TerminalLink>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;
    terminals_from_xml(&xml)
}

/// Parses a single EQ XML payload once and extracts both line and terminal
/// components required for topology joins.
pub fn eq_lines_and_terminals_from_reader<R: Read>(
    mut reader: R,
) -> Result<(Vec<ACLineSegment<'static>>, Vec<TerminalLink>)> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let (lines, _machines, terminals) = eq_lines_machines_and_terminals_from_xml(&xml)?;
    Ok((lines, terminals))
}

/// Parses a single EQ XML payload once and extracts lines, machines and
/// terminals required for topology and generator joins.
pub fn eq_lines_machines_and_terminals_from_reader<R: Read>(
    mut reader: R,
) -> Result<(
    Vec<ACLineSegment<'static>>,
    Vec<SynchronousMachine<'static>>,
    Vec<TerminalLink>,
)> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;
    eq_lines_machines_and_terminals_from_xml(&xml)
}

fn eq_lines_machines_and_terminals_from_xml(
    xml: &str,
) -> Result<(
    Vec<ACLineSegment<'static>>,
    Vec<SynchronousMachine<'static>>,
    Vec<TerminalLink>,
)> {
    let line_fragments = if xml.contains("<cim:ACLineSegment") {
        extract_elements(xml, "cim:ACLineSegment")?
    } else {
        Vec::new()
    };
    let machine_fragments = if xml.contains("<cim:SynchronousMachine") {
        extract_elements(xml, "cim:SynchronousMachine")?
    } else {
        Vec::new()
    };
    let terminals = if xml.contains("<cim:Terminal") {
        terminals_from_xml(xml)?
    } else {
        Vec::new()
    };

    let mut lines = Vec::with_capacity(line_fragments.len());
    for fragment in line_fragments {
        lines.push(ac_line_segment_from_str(fragment)?.into_owned());
    }

    let mut machines = Vec::with_capacity(machine_fragments.len());
    for fragment in machine_fragments {
        machines.push(synchronous_machine_from_str(fragment)?.into_owned());
    }

    Ok((lines, machines, terminals))
}

/// Builds branch rows from live CGMES EQ RDF/XML data.
///
/// This function joins:
/// - `ACLineSegment` electrical parameters (`r`, `x`, `bch`)
/// - `Terminal` references (`Terminal.ConductingEquipment`)
/// - `Terminal.ConnectivityNode` references
///
/// into rows that can be written directly with [`crate::arrow_schema::branch_schema`].
pub fn branch_rows_from_eq_reader<R: Read>(mut reader: R) -> Result<Vec<BranchRow>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let line_fragments = extract_elements(&xml, "cim:ACLineSegment")?;
    let mut lines = Vec::with_capacity(line_fragments.len());
    for fragment in line_fragments {
        lines.push(ac_line_segment_from_str(fragment)?.into_owned());
    }

    let terminals = terminals_from_xml(&xml)?;
    let mut terminals_by_line: HashMap<String, Vec<TerminalLink>> = HashMap::new();
    for terminal in terminals {
        terminals_by_line
            .entry(terminal.line_mrid.clone())
            .or_default()
            .push(terminal);
    }

    let mut node_to_bus_id: BTreeMap<String, i32> = BTreeMap::new();
    let mut next_bus_id = 1_i32;
    for links in terminals_by_line.values() {
        for link in links {
            if !node_to_bus_id.contains_key(&link.connectivity_node_mrid) {
                node_to_bus_id.insert(link.connectivity_node_mrid.clone(), next_bus_id);
                next_bus_id += 1;
            }
        }
    }

    let mut rows = Vec::new();
    for line in lines {
        let line_id = line.base.m_rid.to_string();
        let Some(links) = terminals_by_line.get(&line_id) else {
            continue;
        };

        let mut unique_endpoints: Vec<&TerminalLink> = Vec::new();
        for link in links {
            if unique_endpoints
                .iter()
                .all(|existing| existing.connectivity_node_mrid != link.connectivity_node_mrid)
            {
                unique_endpoints.push(link);
            }
        }

        unique_endpoints.sort_by_key(|link| (link.sequence_number, &link.connectivity_node_mrid));
        if unique_endpoints.len() < 2 {
            continue;
        }

        let from_node = &unique_endpoints[0].connectivity_node_mrid;
        let to_node = &unique_endpoints[1].connectivity_node_mrid;

        let Some(from_bus_id) = node_to_bus_id.get(from_node).copied() else {
            continue;
        };
        let Some(to_bus_id) = node_to_bus_id.get(to_node).copied() else {
            continue;
        };

        rows.push(BranchRow {
            line_mrid: line_id,
            from_bus_id,
            to_bus_id,
            r: line.r.unwrap_or(0.0),
            x: line.x.unwrap_or(0.0),
            b_shunt: line.bch.unwrap_or(0.0),
        });
    }

    if rows.is_empty() {
        bail!("no branch rows could be built from ACLineSegment/Terminal data");
    }

    Ok(rows)
}

#[derive(Debug, Deserialize)]
struct RdfResourceRef {
    #[serde(rename = "@resource")]
    resource: String,
}

#[derive(Debug, Deserialize)]
struct RawTerminal {
    #[serde(rename = "Terminal.ConductingEquipment", default)]
    conducting_equipment: Option<RdfResourceRef>,
    #[serde(rename = "Terminal.ConnectivityNode", default)]
    connectivity_node: Option<RdfResourceRef>,
    #[serde(rename = "ACDCTerminal.sequenceNumber", default)]
    sequence_number: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct RawTopologicalNodeLinks {
    #[serde(rename = "@ID", default)]
    m_rid: Option<String>,
    #[serde(rename = "@about", default)]
    about: Option<String>,
    #[serde(rename = "TopologicalNode.ConnectivityNodes", default)]
    connectivity_nodes: Vec<RdfResourceRef>,
}

impl RawTopologicalNodeLinks {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            let mrid = normalize_cgmes_ref(about);
            log_rdf_about_fallback(&mrid);
            return Some(mrid);
        }
        None
    }
}

#[derive(Debug, Deserialize)]
struct RawConnectivityNode {
    #[serde(rename = "@ID", default)]
    m_rid: Option<String>,
    #[serde(rename = "@about", default)]
    about: Option<String>,
    #[serde(rename = "ConnectivityNode.TopologicalNode", default)]
    topological_node: Option<RdfResourceRef>,
}

#[derive(Debug, Deserialize)]
struct RawLinearShuntCompensator {
    #[serde(rename = "@ID", default)]
    m_rid: Option<String>,
    #[serde(rename = "@about", default)]
    about: Option<String>,
    #[serde(rename = "Equipment.normallyInService", default)]
    status: Option<bool>,
    #[serde(rename = "ShuntCompensator.sections", default)]
    sections: Option<f64>,
    #[serde(rename = "ShuntCompensator.gPerSection", default)]
    shunt_g_per_section: Option<f64>,
    #[serde(rename = "ShuntCompensator.bPerSection", default)]
    shunt_b_per_section: Option<f64>,
    #[serde(rename = "LinearShuntCompensator.gPerSection", default)]
    linear_g_per_section: Option<f64>,
    #[serde(rename = "LinearShuntCompensator.bPerSection", default)]
    linear_b_per_section: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawBaseVoltage {
    #[serde(rename = "@ID", default)]
    m_rid: Option<String>,
    #[serde(rename = "@about", default)]
    about: Option<String>,
    #[serde(rename = "BaseVoltage.nominalVoltage", default)]
    nominal_voltage: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawEquipmentBaseVoltageRef {
    #[serde(rename = "@ID", default)]
    m_rid: Option<String>,
    #[serde(rename = "@about", default)]
    about: Option<String>,
    #[serde(rename = "ConductingEquipment.BaseVoltage", default)]
    conducting_equipment_base_voltage: Option<RdfResourceRef>,
    #[serde(rename = "Equipment.BaseVoltage", default)]
    equipment_base_voltage: Option<RdfResourceRef>,
}

#[derive(Debug, Deserialize)]
struct RawPowerTransformer<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
    #[serde(rename = "Equipment.normallyInService", default)]
    status: Option<bool>,
    #[serde(rename = "PowerTransformer.vectorGroup", default, borrow)]
    vector_group: Option<Cow<'a, str>>,
    #[serde(rename = "PowerTransformer.rateA", default)]
    rate_a: Option<f64>,
    #[serde(rename = "PowerTransformer.rateB", default)]
    rate_b: Option<f64>,
    #[serde(rename = "PowerTransformer.rateC", default)]
    rate_c: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawSwitch<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "Switch.open", default)]
    open: Option<bool>,
    #[serde(rename = "Switch.normalOpen", default)]
    normal_open: Option<bool>,
    #[serde(rename = "Switch.retained", default)]
    retained: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RawDiagram<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
struct RawDiagramObject<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "DiagramObject.Diagram", default)]
    diagram: Option<RdfResourceRef>,
    #[serde(rename = "DiagramObject.IdentifiedObject", default)]
    identified_object: Option<RdfResourceRef>,
    #[serde(rename = "DiagramObject.rotation", default)]
    rotation: Option<f32>,
    #[serde(rename = "DiagramObject.drawingOrder", default)]
    drawing_order: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct RawDiagramObjectPoint {
    #[serde(rename = "DiagramObjectPoint.DiagramObject", default)]
    diagram_object: Option<RdfResourceRef>,
    #[serde(rename = "DiagramObjectPoint.sequenceNumber", default)]
    sequence_number: Option<i32>,
    #[serde(rename = "DiagramObjectPoint.xPosition", default)]
    x: Option<f64>,
    #[serde(rename = "DiagramObjectPoint.yPosition", default)]
    y: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawDyModel<'a> {
    #[serde(rename = "DynamicsFunctionBlock.PowerSystemResource", default)]
    power_system_resource: Option<RdfResourceRef>,
    #[serde(rename = "RotatingMachineDynamics.RotatingMachine", default)]
    rotating_machine: Option<RdfResourceRef>,
    #[serde(rename = "SynchronousMachineDynamics.SynchronousMachine", default)]
    synchronous_machine: Option<RdfResourceRef>,
    #[serde(rename = "AsynchronousMachineDynamics.AsynchronousMachine", default)]
    asynchronous_machine: Option<RdfResourceRef>,
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
}

impl RawPowerTransformer<'_> {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawSwitch<'_> {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawDiagram<'_> {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawDiagramObject<'_> {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawDyModel<'_> {
    fn resolved_equipment_mrid(&self) -> Option<String> {
        self.power_system_resource
            .as_ref()
            .or(self.rotating_machine.as_ref())
            .or(self.synchronous_machine.as_ref())
            .or(self.asynchronous_machine.as_ref())
            .map(|reference| normalize_cgmes_ref(&reference.resource))
            .or_else(|| self.m_rid.as_ref().map(|value| normalize_cgmes_ref(value)))
            .or_else(|| self.about.as_ref().map(|value| normalize_cgmes_ref(value)))
    }
}

#[derive(Debug, Deserialize)]
struct RawPowerTransformerEnd {
    #[serde(rename = "PowerTransformerEnd.PowerTransformer", default)]
    transformer: Option<RdfResourceRef>,
    #[serde(rename = "TransformerEnd.endNumber", default)]
    end_number: Option<i32>,
    #[serde(rename = "PowerTransformerEnd.r", default)]
    r: Option<f64>,
    #[serde(rename = "PowerTransformerEnd.x", default)]
    x: Option<f64>,
    #[serde(rename = "PowerTransformerEnd.g", default)]
    g: Option<f64>,
    #[serde(rename = "PowerTransformerEnd.b", default)]
    b: Option<f64>,
    #[serde(rename = "PowerTransformerEnd.ratedS", default)]
    rated_s: Option<f64>,
    #[serde(rename = "TapChanger.stepVoltageIncrement", default)]
    tap_ratio: Option<f64>,
    #[serde(rename = "PowerTransformerEnd.phaseAngleClock", default)]
    phase_shift: Option<f64>,
}

impl RawPowerTransformerEnd {
    fn transformer_mrid(&self) -> Option<String> {
        self.transformer
            .as_ref()
            .map(|reference| normalize_cgmes_ref(&reference.resource))
    }
}

impl RawLinearShuntCompensator {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawBaseVoltage {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawEquipmentBaseVoltageRef {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            return Some(normalize_cgmes_ref(about));
        }
        None
    }
}

impl RawConnectivityNode {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            let mrid = normalize_cgmes_ref(about);
            log_rdf_about_fallback(&mrid);
            return Some(mrid);
        }
        None
    }
}

fn terminals_from_xml(xml: &str) -> Result<Vec<TerminalLink>> {
    let terminal_fragments = extract_elements(xml, "cim:Terminal")?;
    let mut links = Vec::new();

    for fragment in terminal_fragments {
        let terminal: RawTerminal = from_str(fragment)?;
        let (Some(line_ref), Some(node_ref)) =
            (terminal.conducting_equipment, terminal.connectivity_node)
        else {
            continue;
        };

        links.push(TerminalLink {
            sequence_number: terminal.sequence_number.unwrap_or(i32::MAX),
            line_mrid: normalize_cgmes_ref(&line_ref.resource),
            connectivity_node_mrid: normalize_cgmes_ref(&node_ref.resource),
        });
    }

    Ok(links)
}

fn normalize_cgmes_ref(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(index) = trimmed.rfind('#') {
        return trimmed[index + 1..].to_string();
    }
    trimmed.trim_start_matches('#').to_string()
}

fn contains_exact_element_tag(xml: &str, tag_name: &str) -> bool {
    let opening = format!("<{tag_name}");
    let mut cursor = 0;

    while let Some(relative_start) = xml[cursor..].find(&opening) {
        let start = cursor + relative_start;
        let boundary_idx = start + opening.len();
        let boundary_ok = xml[boundary_idx..]
            .chars()
            .next()
            .map(|ch| ch == '>' || ch == '/' || ch.is_whitespace())
            .unwrap_or(false);
        if boundary_ok {
            return true;
        }
        cursor = boundary_idx;
    }

    false
}

fn extract_elements<'a>(xml: &'a str, tag_name: &str) -> Result<Vec<&'a str>> {
    let opening = format!("<{tag_name}");
    let closing = format!("</{tag_name}>");
    let mut fragments = Vec::new();
    let mut cursor = 0;

    while let Some(relative_start) = xml[cursor..].find(&opening) {
        let start = cursor + relative_start;

        // Require an exact tag boundary so `<cim:EnergyConsumer` does not
        // match `<cim:EnergyConsumer.LoadResponse .../>`.
        let boundary_idx = start + opening.len();
        let boundary_ok = xml[boundary_idx..]
            .chars()
            .next()
            .map(|ch| ch == '>' || ch == '/' || ch.is_whitespace())
            .unwrap_or(false);
        if !boundary_ok {
            cursor = boundary_idx;
            continue;
        }

        let start_tag_end = xml[start..]
            .find('>')
            .map(|offset| start + offset)
            .ok_or_else(|| anyhow::anyhow!("unterminated start tag for {tag_name}"))?;

        let start_tag = &xml[start..=start_tag_end];
        if start_tag.trim_end().ends_with("/>") {
            fragments.push(&xml[start..=start_tag_end]);
            cursor = start_tag_end + 1;
            continue;
        }

        let body_start = start_tag_end + 1;
        let relative_end = xml[body_start..]
            .find(&closing)
            .ok_or_else(|| anyhow::anyhow!("missing closing tag for {tag_name}"))?;
        let end = body_start + relative_end + closing.len();
        fragments.push(&xml[start..end]);
        cursor = end;
    }

    if fragments.is_empty() {
        bail!("no {tag_name} elements found in XML document");
    }

    Ok(fragments)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::base::IdentifiedObject;
    use std::time::Instant;

    #[test]
    fn parse_ac_line_segment_full() {
        let xml = r#"<cim:ACLineSegment rdf:ID="ACLineSegment_001">
  <IdentifiedObject.name>Line NW-1</IdentifiedObject.name>
  <IdentifiedObject.description>Northern feeder segment</IdentifiedObject.description>
  <Conductor.length>42.5</Conductor.length>
  <ACLineSegment.r>0.01</ACLineSegment.r>
  <ACLineSegment.x>0.08</ACLineSegment.x>
  <ACLineSegment.bch>0.0002</ACLineSegment.bch>
</cim:ACLineSegment>"#;

        let line = ac_line_segment_from_str(xml).expect("parse should succeed");
        assert_eq!(line.mrid(), "ACLineSegment_001");
        assert_eq!(line.name(), Some("Line NW-1"));
        assert_eq!(line.description(), Some("Northern feeder segment"));
        assert_eq!(line.length_km, Some(42.5));
        assert_eq!(line.r, Some(0.01));
        assert_eq!(line.x, Some(0.08));
        assert_eq!(line.bch, Some(0.0002));
    }

    #[test]
    fn parse_ac_line_segment_minimal() {
        let xml = r#"<cim:ACLineSegment rdf:ID="line-min"/>"#;
        let line = ac_line_segment_from_str(xml).expect("parse should succeed");
        assert_eq!(line.mrid(), "line-min");
        assert!(line.name().is_none());
        assert!(line.r.is_none());
    }

    #[test]
    fn parse_energy_consumer_full() {
        let xml = r#"<cim:EnergyConsumer rdf:ID="Load_A1">
  <IdentifiedObject.name>Substation A Load</IdentifiedObject.name>
  <EnergyConsumer.p>12.5</EnergyConsumer.p>
  <EnergyConsumer.q>3.2</EnergyConsumer.q>
</cim:EnergyConsumer>"#;

        let load = energy_consumer_from_str(xml).expect("parse should succeed");
        assert_eq!(load.mrid(), "Load_A1");
        assert_eq!(load.name(), Some("Substation A Load"));
        assert_eq!(load.p_mw, Some(12.5));
        assert_eq!(load.q_mvar, Some(3.2));
    }

    #[test]
    fn parse_energy_consumer_minimal() {
        let xml = r#"<cim:EnergyConsumer rdf:ID="load-min"/>"#;
        let load = energy_consumer_from_str(xml).expect("parse should succeed");
        assert_eq!(load.mrid(), "load-min");
        assert!(load.p_mw.is_none());
        assert!(load.q_mvar.is_none());
    }

    #[test]
    fn parse_zero_copy_borrow_check() {
        let xml = String::from(concat!(
            r#"<cim:ACLineSegment rdf:ID="borrow-001">"#,
            r#"<IdentifiedObject.name>Borrowing Line</IdentifiedObject.name>"#,
            r#"</cim:ACLineSegment>"#,
        ));
        let line = ac_line_segment_from_str(&xml).expect("parse should succeed");
        // The string data should be borrowed from `xml` – no heap allocation.
        assert!(
            matches!(line.base.m_rid, std::borrow::Cow::Borrowed(_)),
            "m_rid should borrow from the input buffer"
        );
        assert_eq!(line.mrid(), "borrow-001");
        assert_eq!(line.name(), Some("Borrowing Line"));
    }

    #[test]
    fn typed_parser_helper_round_trip() {
        // Demonstrates that the typed helper correctly deserializes and
        // exposes the composed `base` field.
        let xml = r#"<cim:ACLineSegment rdf:ID="gen-001">
  <IdentifiedObject.name>Generic</IdentifiedObject.name>
  <ACLineSegment.r>0.1</ACLineSegment.r>
</cim:ACLineSegment>"#;

        let line = ac_line_segment_from_str(xml).expect("parse should succeed");
        assert_eq!(line.mrid(), "gen-001");
        assert_eq!(line.base.m_rid, "gen-001");
        assert_eq!(line.r, Some(0.1));
    }

    #[test]
    fn benchmark_fragment_parse_speed() {
        let line_xml = r#"<cim:ACLineSegment rdf:ID="BenchLine">
  <IdentifiedObject.name>Bench Line</IdentifiedObject.name>
  <ACLineSegment.r>0.01</ACLineSegment.r>
  <ACLineSegment.x>0.08</ACLineSegment.x>
  <ACLineSegment.bch>0.0002</ACLineSegment.bch>
</cim:ACLineSegment>"#;
        let load_xml = r#"<cim:EnergyConsumer rdf:ID="BenchLoad">
  <IdentifiedObject.name>Bench Load</IdentifiedObject.name>
  <EnergyConsumer.p>12.5</EnergyConsumer.p>
  <EnergyConsumer.q>3.2</EnergyConsumer.q>
</cim:EnergyConsumer>"#;

        let iterations: u32 = 50_000;

        let line_start = Instant::now();
        for _ in 0..iterations {
            let parsed = ac_line_segment_from_str(line_xml).expect("line parse should succeed");
            assert_eq!(parsed.base.m_rid, "BenchLine");
        }
        let line_elapsed = line_start.elapsed();

        let load_start = Instant::now();
        for _ in 0..iterations {
            let parsed = energy_consumer_from_str(load_xml).expect("load parse should succeed");
            assert_eq!(parsed.base.m_rid, "BenchLoad");
        }
        let load_elapsed = load_start.elapsed();

        println!(
            "ACLineSegment: {} parses in {:.3?} ({:.0} parses/s)",
            iterations,
            line_elapsed,
            iterations as f64 / line_elapsed.as_secs_f64()
        );
        println!(
            "EnergyConsumer: {} parses in {:.3?} ({:.0} parses/s)",
            iterations,
            load_elapsed,
            iterations as f64 / load_elapsed.as_secs_f64()
        );
    }

    #[test]
    fn parse_base_voltage_specs() {
        let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
    <cim:BaseVoltage rdf:ID="BV_230">
        <BaseVoltage.nominalVoltage>230.0</BaseVoltage.nominalVoltage>
    </cim:BaseVoltage>
    <cim:BaseVoltage rdf:about="#BV_115">
        <BaseVoltage.nominalVoltage>115</BaseVoltage.nominalVoltage>
    </cim:BaseVoltage>
</rdf:RDF>"##;

        let rows = base_voltage_specs_from_reader(xml.as_bytes()).expect("parse should succeed");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].base_voltage_mrid, "BV_115");
        assert_eq!(rows[0].nominal_kv, 115.0);
        assert_eq!(rows[1].base_voltage_mrid, "BV_230");
        assert_eq!(rows[1].nominal_kv, 230.0);
    }

    #[test]
    fn parse_equipment_base_voltage_refs() {
        let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
    <cim:ACLineSegment rdf:ID="Line1">
        <ConductingEquipment.BaseVoltage rdf:resource="#BV_230"/>
    </cim:ACLineSegment>
    <cim:EnergyConsumer rdf:ID="Load1">
        <ConductingEquipment.BaseVoltage rdf:resource="#BV_115"/>
    </cim:EnergyConsumer>
    <cim:PowerTransformer rdf:about="#Tx1">
        <Equipment.BaseVoltage rdf:resource="#BV_345"/>
    </cim:PowerTransformer>
</rdf:RDF>"##;

        let refs =
            equipment_base_voltage_refs_from_reader(xml.as_bytes()).expect("parse should succeed");
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].equipment_mrid, "Line1");
        assert_eq!(refs[0].base_voltage_mrid, "BV_230");
        assert_eq!(refs[1].equipment_mrid, "Load1");
        assert_eq!(refs[1].base_voltage_mrid, "BV_115");
        assert_eq!(refs[2].equipment_mrid, "Tx1");
        assert_eq!(refs[2].base_voltage_mrid, "BV_345");
    }

    #[test]
    fn parse_dy_model_specs_extracts_numeric_params() {
        let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
    <cim:SynchronousMachineDynamics rdf:ID="SMD1">
        <SynchronousMachineDynamics.SynchronousMachine rdf:resource="#GEN_A"/>
        <SynchronousMachineDynamics.H>3.2</SynchronousMachineDynamics.H>
        <SynchronousMachineDynamics.D>0.15</SynchronousMachineDynamics.D>
        <SynchronousMachineDynamics.xdPrime>0.27</SynchronousMachineDynamics.xdPrime>
    </cim:SynchronousMachineDynamics>
</rdf:RDF>"##;

        let rows = dy_model_specs_from_reader(xml.as_bytes()).expect("parse should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].equipment_mrid, "GEN_A");
        assert_eq!(rows[0].model_type, "SynchronousMachineDynamics");

        let params: BTreeMap<_, _> = rows[0].params.iter().cloned().collect();
        assert_eq!(params.get("h"), Some(&3.2));
        assert_eq!(params.get("d"), Some(&0.15));
        assert_eq!(params.get("xd_prime"), Some(&0.27));
    }

    #[test]
    fn parse_custom_dy_model_type_for_smart_valve() {
        let xml = r##"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#">
    <cim:RaptrixSmartValveDynamics rdf:ID="SV1">
        <DynamicsFunctionBlock.PowerSystemResource rdf:resource="#GEN_SMART"/>
        <RaptrixSmartValveDynamics.kGain>2.5</RaptrixSmartValveDynamics.kGain>
        <RaptrixSmartValveDynamics.tOpenS>0.3</RaptrixSmartValveDynamics.tOpenS>
    </cim:RaptrixSmartValveDynamics>
</rdf:RDF>"##;

        let rows = dy_model_specs_from_reader(xml.as_bytes()).expect("parse should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].equipment_mrid, "GEN_SMART");
        assert_eq!(rows[0].model_type, "raptrix.smart_valve.v1");

        let params: BTreeMap<_, _> = rows[0].params.iter().cloned().collect();
        assert_eq!(params.get("k_gain"), Some(&2.5));
        assert_eq!(params.get("t_open_s"), Some(&0.3));
    }
}
