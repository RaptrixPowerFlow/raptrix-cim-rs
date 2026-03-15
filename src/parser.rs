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

use std::io::Read;

use anyhow::{bail, Result};
use quick_xml::de::from_str;
use serde::de::DeserializeOwned;

use crate::models::{ACLineSegment, EnergyConsumer};

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
}
