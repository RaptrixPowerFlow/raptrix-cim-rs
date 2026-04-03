# Cross-Repo Release Sync Workflow

This repository is the canonical source of truth for the Raptrix `.rpf` schema contract and CIM-to-RPF mapping behavior.

## Scope

- Master contract owner: `raptrix-cim-rs`
- Downstream consumers:
  - `raptrix-psse-rs`
  - `raptrix-core`
  - `raptrix-studio`

## Release Triggers

Run this workflow on any of the following changes:

- `raptrix-cim-arrow/src/schema.rs`
- `docs/schema-contract.md`
- `src/rpf_writer.rs`
- `src/parser.rs`
- Any CLI behavior that affects profile detection or metadata emission

## Versioning Rules

- PATCH: non-structural fixes (bug fixes, docs, metadata text fixes)
- MINOR: additive format changes (new optional fields/tables/metadata keys)
- MAJOR: breaking wire-shape changes (required field or table rename/removal/reorder/type change)

## Canonical Release Steps

1. Validate this repo on main:
   - `cargo fmt --all -- --check`
   - `cargo check --workspace --all-targets`
   - `cargo test --workspace --all-targets`
2. Tag release:
   - `vX.Y.Z` for crate release
   - optional `schema-vX.Y.Z` for explicit contract milestones
3. Ensure GitHub action `Master Contract CI` publishes the contract artifact.
4. Publish release notes with:
   - schema/contract impact summary
   - compatibility statement (CGMES 2.4.x and 3.x)
   - migration notes for downstream repos

## Downstream Sync Checklist

### raptrix-psse-rs

1. Update dependency to latest `raptrix-cim-arrow` source (path or registry).
2. Re-run parser and output tests.
3. Confirm no local schema fork or duplicate contract files remain.

### raptrix-core

1. Update embedded or vendored schema references to current contract.
2. Re-run CMake configure/build and import validation for `.rpf` samples.
3. Verify metadata keys and optional table behavior still parse correctly.

### raptrix-studio

1. Validate `.rpf` loading against current release artifact.
2. Confirm optional table handling remains non-breaking.
3. Re-run typecheck/test/build validation.

## Compatibility Guardrails

- Readers must tolerate unknown trailing root columns and unknown metadata keys.
- Writers in this repo must preserve canonical required root table ordering.
- Any planned breaking contract change must include:
  - MAJOR version bump
  - migration guide section in release notes
  - downstream update tasks in all three consumer repos
