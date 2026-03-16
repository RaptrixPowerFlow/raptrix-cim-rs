// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper grouping for TP/EQ merge.
//!
//! Tenet 6: keeps detailed ConnectivityNode membership while defaulting bus
//! solving to TopologicalNode compatibility.

use std::borrow::Cow;

/// Groups connectivity nodes under one topological node.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectivityNodeGroup<'a> {
    pub topological_node_mrid: Cow<'a, str>,
    pub connectivity_node_mrids: Vec<Cow<'a, str>>,
}

impl<'a> ConnectivityNodeGroup<'a> {
    /// Converts to fully-owned values.
    pub fn into_owned(self) -> ConnectivityNodeGroup<'static> {
        ConnectivityNodeGroup {
            topological_node_mrid: Cow::Owned(self.topological_node_mrid.into_owned()),
            connectivity_node_mrids: self
                .connectivity_node_mrids
                .into_iter()
                .map(|value| Cow::Owned(value.into_owned()))
                .collect(),
        }
    }
}
