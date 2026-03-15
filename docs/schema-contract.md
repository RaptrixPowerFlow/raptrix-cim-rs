# Schema Contract

## Contract Policy

- Schema changes are explicit and versioned.
- Column order is stable and treated as part of the contract.
- Column type and nullability changes require a version bump and migration note.

## File Metadata Keys

Parquet files written by this project include:

- raptrix.branding
- raptrix.version

## Current Schema Version

- v0.1.1

## Branch Table Contract

Name | Arrow Type | Nullable | Source
--- | --- | --- | ---
from | Int32 | false | mapped endpoint bus id
to | Int32 | false | mapped endpoint bus id
r | Float64 | false | ACLineSegment.r (default 0.0)
x | Float64 | false | ACLineSegment.x (default 0.0)
b_shunt | Float64 | false | ACLineSegment.bch (default 0.0)
tap | Float64 | false | default 1.0 (until richer profile mapping)
phase | Float64 | false | default 0.0 (until richer profile mapping)
rate_a | Float64 | false | default 250.0 (placeholder)
status | Boolean | false | default true (placeholder)

## Bus Table Contract

See src/arrow_schema.rs for the full bus schema used by powerflow_schema().

## Compatibility Rules

- Additive columns should be appended and documented.
- Renaming or reordering columns is breaking.
- Removing columns is breaking.
- Type widening or narrowing is breaking unless consumers are migrated in lockstep.

## Change Checklist

1. Update src/arrow_schema.rs.
2. Update this file with version and column docs.
3. Add or update test coverage for schema construction and writer outputs.
4. Update README capability and known-limits sections.
