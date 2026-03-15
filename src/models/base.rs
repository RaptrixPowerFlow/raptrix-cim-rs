// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Base traits and the shared [`BaseAttributes`] struct used by every CIM
//! type.
//!
//! ## Zero-copy design
//!
//! [`BaseAttributes`] stores string fields as [`Cow<'a, str>`].  When the
//! XML input lives in a buffer that outlives the parsed value the strings
//! borrow directly from that buffer (`Cow::Borrowed`), avoiding any heap
//! allocation.  When ownership is required (e.g. after the buffer has been
//! released) the strings are cloned lazily via [`Cow::Owned`].
//!
//! `serde` integration is provided through `quick-xml`'s `serialize`
//! feature.  Use [`serde(borrow)`] on each `Cow` field so that the
//! deserializer can hand out borrows rather than copies.

use std::borrow::Cow;

use serde::Serialize;

// ---------------------------------------------------------------------------
// IdentifiedObject – root of the CIM class hierarchy
// ---------------------------------------------------------------------------

/// Core trait for every CIM object.
///
/// In IEC 61970-301 `IdentifiedObject` is the root super-class.  Every
/// modelled entity—lines, buses, transformers, measurements—is an
/// `IdentifiedObject`.
pub trait IdentifiedObject {
    /// Master Resource Identifier (mRID).  Globally unique UUID-like string.
    fn mrid(&self) -> &str;

    /// Human-readable name, if present.
    fn name(&self) -> Option<&str>;

    /// Free-text description, if present.
    fn description(&self) -> Option<&str>;
}

// ---------------------------------------------------------------------------
// PowerSystemResource – direct sub-class of IdentifiedObject
// ---------------------------------------------------------------------------

/// Marker trait for CIM `PowerSystemResource`.
///
/// A `PowerSystemResource` is any part of a power system that is a physical
/// object (equipment, locations, …) or a logical grouping of such objects.
/// It inherits all properties from [`IdentifiedObject`].
pub trait PowerSystemResource: IdentifiedObject {}

// ---------------------------------------------------------------------------
// BaseAttributes – composable struct carrying IdentifiedObject fields
// ---------------------------------------------------------------------------

/// Shared fields present on every `IdentifiedObject`.
///
/// Concrete CIM types embed this struct (via composition) rather than
/// duplicating the field declarations.  The struct is the public API for
/// programmatic access to identity fields; serde integration on concrete types
/// is handled through private raw-deserialization helper structs (see
/// [`equipment`](super::equipment)) to work around `quick-xml`'s limitation
/// that `#[serde(flatten)]` only propagates XML attributes, not child elements.
///
/// ## Lifetime parameter `'a`
///
/// The `'a` lifetime ties the borrowed string slices to the input XML buffer.
/// Use [`BaseAttributes::into_owned`] to obtain a `'static` version when you
/// need to store the value beyond the buffer's lifetime.
///
/// ## RDF/XML attribute mapping
///
/// | Rust field    | XML representation                          |
/// |---------------|---------------------------------------------|
/// | `m_rid`       | `rdf:ID` attribute on the element           |
/// | `name`        | `<cim:IdentifiedObject.name>` child element |
/// | `description` | `<cim:IdentifiedObject.description>` child  |
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BaseAttributes<'a> {
    /// Master Resource Identifier – maps to the `rdf:ID` XML attribute.
    pub m_rid: Cow<'a, str>,

    /// Optional human-readable name.
    pub name: Option<Cow<'a, str>>,

    /// Optional free-text description.
    pub description: Option<Cow<'a, str>>,
}

impl<'a> BaseAttributes<'a> {
    /// Constructs a new [`BaseAttributes`] from owned strings.
    pub fn new(
        m_rid: impl Into<Cow<'a, str>>,
        name: Option<impl Into<Cow<'a, str>>>,
        description: Option<impl Into<Cow<'a, str>>>,
    ) -> Self {
        Self {
            m_rid: m_rid.into(),
            name: name.map(Into::into),
            description: description.map(Into::into),
        }
    }

    /// Converts this value into a fully-owned `BaseAttributes<'static>` by
    /// cloning any borrowed string slices.
    pub fn into_owned(self) -> BaseAttributes<'static> {
        BaseAttributes {
            m_rid: Cow::Owned(self.m_rid.into_owned()),
            name: self.name.map(|n| Cow::Owned(n.into_owned())),
            description: self.description.map(|d| Cow::Owned(d.into_owned())),
        }
    }
}

// ---------------------------------------------------------------------------
// Blanket implementation helpers
// ---------------------------------------------------------------------------

/// Convenience macro that implements [`IdentifiedObject`] for any type that
/// exposes a `base` field of type `BaseAttributes`.
///
/// Note: This macro is useful for equipment types that store identity fields
/// via composition in a `pub base: BaseAttributes<'a>` field.
#[macro_export]
macro_rules! impl_identified_object {
    ($ty:ty) => {
        impl<'a> $crate::models::base::IdentifiedObject for $ty {
            fn mrid(&self) -> &str {
                &self.base.m_rid
            }

            fn name(&self) -> Option<&str> {
                self.base.name.as_deref()
            }

            fn description(&self) -> Option<&str> {
                self.base.description.as_deref()
            }
        }
    };
}

/// Convenience macro that implements [`PowerSystemResource`] for any type
/// that already implements [`IdentifiedObject`].
#[macro_export]
macro_rules! impl_power_system_resource {
    ($ty:ty) => {
        impl<'a> $crate::models::base::PowerSystemResource for $ty {}
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_attributes_new_owned() {
        let attrs = BaseAttributes::new(
            "uuid-001",
            Some("Bus A"),
            Some("HV busbar in substation 1"),
        );
        assert_eq!(attrs.mrid(), "uuid-001");
        assert_eq!(attrs.name(), Some("Bus A"));
        assert_eq!(attrs.description(), Some("HV busbar in substation 1"));
    }

    #[test]
    fn base_attributes_new_no_optional_fields() {
        let attrs: BaseAttributes<'_> =
            BaseAttributes::new("uuid-002", None::<&str>, None::<&str>);
        assert_eq!(attrs.mrid(), "uuid-002");
        assert!(attrs.name().is_none());
        assert!(attrs.description().is_none());
    }

    #[test]
    fn base_attributes_into_owned_is_static() {
        let owned: BaseAttributes<'static> = {
            let data = String::from("dynamic-string");
            let attrs = BaseAttributes::new(data.as_str(), None::<&str>, None::<&str>);
            attrs.into_owned()
        };
        assert_eq!(owned.mrid(), "dynamic-string");
    }

    #[test]
    fn base_attributes_cow_borrow() {
        let mrid = String::from("uuid-borrow");
        let attrs = BaseAttributes {
            m_rid: Cow::Borrowed(mrid.as_str()),
            name: None,
            description: None,
        };
        // Should still borrow (no clone needed yet).
        assert!(matches!(attrs.m_rid, Cow::Borrowed(_)));
        assert_eq!(attrs.mrid(), "uuid-borrow");
    }
}

// Make IdentifiedObject accessible on BaseAttributes itself for convenience.
impl<'a> IdentifiedObject for BaseAttributes<'a> {
    fn mrid(&self) -> &str {
        &self.m_rid
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

impl<'a> PowerSystemResource for BaseAttributes<'a> {}
