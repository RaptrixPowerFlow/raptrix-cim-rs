# Schema Contract (Locked v0.5)

## Contract Policy

- Schema changes are explicit and versioned.
- Column order is stable and treated as part of the contract.
- Column type and nullability changes require a version bump and migration note.

## File Metadata Keys

Every `.rpf` file must include:

- `raptrix.branding`
- `raptrix.version`

Current locked values:

- `raptrix.version = v0.5`
- `raptrix.branding = Raptrix CIM-Arrow / PowerFlow Interchange v0.5 - High-performance open profile by Musto Technologies LLC. Copyright (c) 2026 Musto Technologies LLC.`

## Canonical Schema Source

The executable contract is defined in `src/arrow_schema.rs` and exported through:

- `all_table_schemas()` for canonical ordering
- `table_schema(name)` for table lookup

## Locked Tables

Required tables (empty tables allowed):

- `metadata`
- `buses`
- `branches`
- `generators`
- `loads`
- `fixed_shunts`
- `switched_shunts`
- `transformers_2w`
- `transformers_3w`
- `areas`
- `zones`
- `owners`
- `contingencies`
- `interfaces`
- `dynamics_models`

## Blocker Fixes Incorporated in v0.5

### 1) Expanded transformer detail

`transformers_2w` includes winding-level and vector fields:

- `winding1_r`, `winding1_x`
- `winding2_r`, `winding2_x`
- `nominal_tap_ratio`
- `vector_group` (dictionary string)

`transformers_3w` includes:

- per-leg impedance fields (`r_hm/x_hm`, `r_hl/x_hl`, `r_ml/x_ml`)
- `star_bus_id` (nullable Int32, fictitious star bus when present)
- `vector_group` (dictionary string)

### 2) Dynamics model table formalized

`dynamics_models` is locked with:

- `bus_id` (Int32)
- `gen_id` (dictionary string)
- `model_type` (dictionary string)
- `params` (Map<String, Float64>)

Compatibility alias: `dynamics` is accepted by `table_schema(name)`.

### 3) Contingency element payload tightened

`contingencies.elements` is a list of struct with explicit fields:

- `element_type` (dictionary string)
- `branch_id` (nullable Int32)
- `bus_id` (nullable Int32)
- `gen_id` (nullable dictionary string)
- `load_id` (nullable dictionary string)
- `amount_mw` (nullable Float64)
- `status_change` (Boolean)

Allowed `element_type` values are locked to:

- `branch_outage`
- `gen_trip`
- `load_shed`
- `shunt_switch`

### 4) Solved-result contingency scoping

Export-only solved-result tables must include:

- `contingency_id` (nullable dictionary string)

Semantics:

- `null` means base-case result.
- non-null values key each row to a contingency case.

The reusable schema helper is `solved_results_contingency_id_field()`.

## Compatibility Rules

- Additive columns should be appended and documented.
- Renaming or reordering columns is breaking.
- Removing columns is breaking.
- Type widening or narrowing is breaking unless consumers are migrated in lockstep.

## Change Checklist

1. Update `src/arrow_schema.rs`.
2. Update this file with version and column docs.
3. Add or update test coverage for schema construction and writer outputs.
4. Update README capability and known-limits sections.
