// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Small helpers shared by integration tests.

use std::path::PathBuf;

/// Returns the expected path to an external CGMES profile file.
///
/// The root comes from `RAPTRIX_TEST_DATA_ROOT`, for example:
/// `C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0`
pub fn get_external_cgmes_path(model: &str, profile: &str) -> Option<PathBuf> {
    let root = std::env::var("RAPTRIX_TEST_DATA_ROOT").ok()?;

    Some(
        PathBuf::from(root)
            .join(model)
            .join(format!("{model}-Merged"))
            .join(format!("{model}_{profile}.xml")),
    )
}
