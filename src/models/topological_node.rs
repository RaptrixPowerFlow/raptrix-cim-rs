// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CIM `TopologicalNode` model.
//!
//! Tenet 1: string identity fields use `Cow<'a, str>` via composed
//! `BaseAttributes` for zero-copy deserialization from RDF/XML buffers.

use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize};

use super::base::{BaseAttributes, IdentifiedObject, PowerSystemResource};

/// CIM `TopologicalNode` used by TP profile for bus collapsing.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologicalNode<'a> {
    /// Inherited `IdentifiedObject` fields (mRID, name, description).
    pub base: BaseAttributes<'a>,
}

#[derive(Deserialize)]
struct RawTopologicalNode<'a> {
    #[serde(rename = "@ID", default, borrow)]
    m_rid: Option<Cow<'a, str>>,
    #[serde(rename = "@about", default, borrow)]
    about: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.name", default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(rename = "IdentifiedObject.description", default, borrow)]
    description: Option<Cow<'a, str>>,
}

impl<'a> RawTopologicalNode<'a> {
    fn into_topological_node<E>(self) -> Result<TopologicalNode<'a>, E>
    where
        E: serde::de::Error,
    {
        let m_rid = match (self.m_rid, self.about) {
            (Some(m_rid), _) => m_rid,
            (None, Some(about)) => {
                let mrid = strip_hash_cow(about);
                #[cfg(debug_assertions)]
                eprintln!("Fallback: using rdf:about for mRID: {}", mrid);
                mrid
            }
            (None, None) => {
                return Err(E::custom(
                    "TopologicalNode requires either rdf:ID or rdf:about",
                ));
            }
        };

        Ok(TopologicalNode {
            base: BaseAttributes {
                m_rid,
                name: self.name,
                description: self.description,
            },
        })
    }
}

fn strip_hash_cow<'a>(value: Cow<'a, str>) -> Cow<'a, str> {
    match value {
        Cow::Borrowed(borrowed) => {
            Cow::Borrowed(borrowed.strip_prefix('#').unwrap_or(borrowed))
        }
        Cow::Owned(owned) => Cow::Owned(owned.trim_start_matches('#').to_string()),
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for TopologicalNode<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawTopologicalNode::deserialize(deserializer)?;
        raw.into_topological_node()
    }
}

impl<'a> Serialize for TopologicalNode<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("cim:TopologicalNode", 3)?;
        s.serialize_field("@ID", &self.base.m_rid)?;
        if let Some(ref n) = self.base.name {
            s.serialize_field("IdentifiedObject.name", n)?;
        }
        if let Some(ref d) = self.base.description {
            s.serialize_field("IdentifiedObject.description", d)?;
        }
        s.end()
    }
}

impl<'a> TopologicalNode<'a> {
    /// Converts to a fully-owned (`'static`) [`TopologicalNode`].
    pub fn into_owned(self) -> TopologicalNode<'static> {
        TopologicalNode {
            base: self.base.into_owned(),
        }
    }
}

impl<'a> IdentifiedObject for TopologicalNode<'a> {
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

impl<'a> PowerSystemResource for TopologicalNode<'a> {}
