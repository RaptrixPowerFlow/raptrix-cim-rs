// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CIM data-model types, base traits, and equipment definitions.
//!
//! The module mirrors the semantic hierarchy defined by IEC 61970-301:
//!
//! ```text
//! IdentifiedObject (trait)
//!   └─ PowerSystemResource (trait)
//!        └─ Equipment (trait)
//!             ├─ ConductingEquipment (trait)
//!             │    └─ ACLineSegment
//!             │    └─ SynchronousMachine
//!             └─ EnergyConsumer
//! ```
//!
//! Each concrete type holds a [`base::BaseAttributes`] that carries the
//! fields common to every `IdentifiedObject`.

pub mod base;
pub mod connectivity_node_group;
pub mod equipment;
pub mod topological_node;

pub use base::{BaseAttributes, IdentifiedObject, PowerSystemResource};
pub use connectivity_node_group::ConnectivityNodeGroup;
pub use equipment::{
	ACLineSegment, EnergyConsumer, SvShuntCompensator, SynchronousMachine, Transformer2W,
	Transformer3W,
};
pub use topological_node::TopologicalNode;
