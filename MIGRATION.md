# Workspace Migration

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix PowerFlow

Copyright (c) 2026 Raptrix PowerFlow

## What Changed

This repository was refactored from a single crate into a Cargo workspace with two responsibilities:

- `raptrix-cim-rs`: CIM parsing, CGMES profile handling, row mapping, and CLI orchestration
- `raptrix-cim-arrow`: locked canonical schema definitions and generic `.rpf` Arrow IPC infrastructure

## What Moved Into `raptrix-cim-arrow`

- all schema definitions previously in `src/arrow_schema.rs`
- branding and version metadata constants
- canonical table ordering and lookup helpers
- root `.rpf` Arrow IPC assembly logic
- root `.rpf` validation helpers
- generic `.rpf` readback, summary, and metadata inspection helpers

## What Stayed In `raptrix-cim-rs`

- CIM model types in `src/models`
- RDF/XML parsing helpers in `src/parser.rs`
- CGMES-specific row construction in `src/rpf_writer.rs`
- CLI behavior in `src/main.rs`

This boundary is intentional: the shared crate should not know how CIM, PSS/E, MATLAB, or any future format is parsed. It should only know the canonical contract and how to emit and validate a compliant `.rpf` file.

## Why The Split Was Done

- keeps the locked RPF contract in one source of truth
- reduces duplication for future converter repositories
- lets format-specific bugs and parser changes stay isolated from Arrow contract changes
- makes contract fixes available to every converter that depends on the shared crate

## How Future Converter Crates Should Reuse It

For a future converter such as `raptrix-psse-rs`:

1. Depend on `raptrix-cim-arrow`
2. Parse the source format into canonical table rows or `RecordBatch` values
3. Use the schema helpers from `raptrix-cim-arrow` when constructing batches
4. Call `write_root_rpf` to emit the final `.rpf` file
5. Use `read_rpf_tables`, `summarize_rpf`, and `rpf_file_metadata` in tests to verify compatibility

That keeps all converters aligned on one exact Arrow schema contract and one exact root-file layout.

---

## Transformer Representation Contract (v0.8.7)

**Schema version**: v0.8.7 | **Crate version**: 0.2.7

### What changed

Every RPF file produced by v0.2.7+ now contains the required file-level metadata key:

```
rpf.transformer_representation_mode = native_3w | expanded
```

### Producer obligations

All writers must insert this key in the root Arrow IPC metadata before calling `write_root_rpf`.
`WriteOptions::default()` already sets the mode to `native_3w`, so existing callers that do not
opt in to `Expanded` receive the key automatically with no code changes required.

### Consumer / reader fallback semantics

Files produced before v0.8.7 will not contain the key.  Readers should treat a missing key as
`native_3w` (3-winding rows appear in `transformers_3w`, no synthetic star buses in
`transformers_2w`).

```rust
use raptrix_cim_arrow::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;

let mode = metadata
		.get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
		.map(String::as_str)
		.unwrap_or("native_3w");   // pre-v0.8.7 files default to native_3w
```

The reader helper `validate_transformer_representation_mode_value(value)` is available in
`raptrix_cim_arrow::schema` to validate the string before branching.

### Expanded mode — star bus range

When a file carries `rpf.transformer_representation_mode = expanded`:

- `transformers_3w` contains **zero active rows** (inactive rows may remain as bookkeeping).
- `transformers_2w` contains three synthetic legs per original 3-winding transformer.
- Synthetic star bus IDs are > 10 000 000 and should not appear in the `buses` table.
- Impedance conversion follows the delta→wye formula:
	- `r_h = (r_hm + r_hl − r_ml) / 2`
	- `r_m = (r_hm + r_ml − r_hl) / 2`
	- `r_l = (r_hl + r_ml − r_hm) / 2`
	- (same for `x`)

### Companion change (raptrix-psse-rs)

`raptrix-psse-rs` currently carries a local copy of `METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE`
at `src/lib.rs`.  Once v0.2.7 of `raptrix-cim-arrow` is published, that local copy should be
replaced with:

```rust
use raptrix_cim_arrow::METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE;
```

---

## Schema Contract 0.8.9 (Breaking)

**Schema version**: v0.8.9 | **Crate version**: 0.2.9

### 2026 First-Principles Mandate

v0.8.9 formalizes a modern-grid-first contract. The schema now treats IBR-heavy operation,
distributed flexibility, Smart Valve-style controls, and modern DC workflows as core model
features. This is reflected in required root tables and required metadata, not optional add-ons.

### Beyond Parity

This release is not a parity-first redesign around legacy interchange formats. Compatibility with
legacy workflows can still be achieved where practical, but contract design is anchored in
first-principles network physics and IEC 61970 CIM semantics.

### Breaking support policy

- Reader support for contracts below v0.8.9 is deprecated and removed in this repository.
- `SUPPORTED_RPF_VERSIONS` now accepts only `v0.8.9` / `0.8.9`.
- Any file produced at v0.8.8 or below must be re-exported/migrated to v0.8.9 before ingestion.

### Required table changes

New required tables in canonical root order:

- `multi_section_lines`
- `dc_lines_2w`
- `ibr_devices`
- `switched_shunt_banks`

Arrow typing constraints for these tables are part of the wire contract:

- `multi_section_lines.section_branch_ids`: `list<int32>`
- `ibr_devices.params`: `map<string, float64>`
- Nullable fields remain nullable per schema contract for additive compatibility where possible.

### Required metadata changes

`metadata` row now requires:

- `modern_grid_profile` (bool)
- `has_ibr` (bool)
- `has_smart_valve` (bool)
- `has_multi_terminal_dc` (bool)

New nullable metadata fields:

- `ibr_penetration_pct` (float64)
- `study_purpose` (utf8)
- `scenario_tags` (list<utf8>)

### Branch table changes

`branches` now includes additive linkage fields:

- `parent_line_id` (int32, nullable)
- `section_index` (int32, nullable)

### Switched shunt semantics

- `switched_shunts.b_steps` is now capacitive-only (positive values).
- Inductive steps must be represented in `switched_shunt_banks`.

### Writer obligations for modern-grid metadata

Writers must populate required v0.8.9 metadata flags:

- `modern_grid_profile`
- `has_ibr`
- `has_smart_valve`
- `has_multi_terminal_dc`

Writers should populate nullable context fields when known:

- `ibr_penetration_pct`
- `study_purpose`
- `scenario_tags`

### Backward compatibility boundaries

- Reader compatibility is intentionally strict at v0.8.9 only.
- Backward compatibility remains for additive nullable columns and empty required-table materialization within the v0.8.9 contract shape.
