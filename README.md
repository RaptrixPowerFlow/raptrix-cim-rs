# raptrix-cim-rs
High-performance Rust implementation of the IEC 61970 Common Information Model (CIM). Optimized for zero-copy RDF/XML parsing for real-time power flow and SCED applications.

## Test Data Layout

- `tests/data/fixtures/` contains tiny committed XML snippets that are safe to keep in Git.
- `tests/data/external/` is reserved for local symlinks or placeholders and is ignored by default.
- Large CGMES datasets should stay outside the repository, for example under `C:\tmp`.

## External CGMES Setup

1. Download the ENTSO-E CGMES v3.0 test configurations from the CGMES Library:
	 https://www.entsoe.eu/data/cim/cim-for-grid-models-exchange/
2. Unzip the archive to a local path such as:
	 `C:\tmp\CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3\v3.0`
3. Set `RAPTRIX_TEST_DATA_ROOT` to that `v3.0` folder.

The integration helper expects paths shaped like:

`<RAPTRIX_TEST_DATA_ROOT>\SmallGrid\SmallGrid-Merged\SmallGrid_EQ.xml`

If `RAPTRIX_TEST_DATA_ROOT` is not set, external-data tests skip cleanly.

## Running Integration Tests

- Run normal unit tests without external data:
	`cargo test`
- Run ignored integration tests that need CGMES models:
	`cargo test -- --ignored`
- Run only the SmallGrid EQ parser integration test:
	`cargo test parse_smallgrid_eq_aclinesegment -- --ignored --nocapture`
