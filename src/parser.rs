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

use anyhow::{bail, Result};
use quick_xml::de::from_str;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::models::{
    ACLineSegment, ConnectivityNodeGroup, EnergyConsumer, SynchronousMachine, TopologicalNode,
};

/// A parsing error returned by the helper functions in this module.
pub type ParseError = quick_xml::DeError;

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

/// Parses all `<cim:EnergyConsumer>` elements from a CGMES RDF/XML reader.
pub fn energy_consumers_from_reader<R: Read>(mut reader: R) -> Result<Vec<EnergyConsumer<'static>>> {
    let mut xml = String::new();
    reader.read_to_string(&mut xml)?;

    let fragments = extract_elements(&xml, "cim:EnergyConsumer")?;
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
            connectivity_node_mrids: connectivity_nodes
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        });
    }

    Ok(out)
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
            #[cfg(debug_assertions)]
            eprintln!("Fallback: using rdf:about for mRID: {mrid}");
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

impl RawConnectivityNode {
    fn resolved_mrid(&self) -> Option<String> {
        if let Some(mrid) = &self.m_rid {
            return Some(normalize_cgmes_ref(mrid));
        }
        if let Some(about) = &self.about {
            let mrid = normalize_cgmes_ref(about);
            #[cfg(debug_assertions)]
            eprintln!("Fallback: using rdf:about for mRID: {mrid}");
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

fn extract_elements<'a>(xml: &'a str, tag_name: &str) -> Result<Vec<&'a str>> {
    let opening = format!("<{tag_name}");
    let closing = format!("</{tag_name}>");
    let mut fragments = Vec::new();
    let mut cursor = 0;

    while let Some(relative_start) = xml[cursor..].find(&opening) {
        let start = cursor + relative_start;
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
}
