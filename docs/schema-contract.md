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

Optional detail table (emitted only in connectivity-detail mode):

- `connectivity_groups`

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

### 5) TP merge policy (EQ + TP)

Default solver-facing bus construction is at TP `TopologicalNode` level:

- EQ `Terminal.ConnectivityNode` references are mapped to TP topological groups.
- Dense `buses.bus_id` values are assigned by sorted TopologicalNode mRID.
- `branches.from_bus_id` / `branches.to_bus_id` and `generators.bus_id` follow the collapsed topology.

This policy improves interoperability and reduces matrix dimensions versus raw
ConnectivityNode granularity while preserving CIM semantics.

Identifier compatibility note:

- TP parsing accepts either `rdf:ID` or `rdf:about` for `TopologicalNode` and
	`ConnectivityNode` identity extraction.
- When `rdf:about` is used, a leading `#` is stripped before mRID mapping.

### 6) Split-bus preservation via `connectivity_groups`

When connectivity-detail mode is requested, writers may emit
`connectivity_groups` with:

- `topological_bus_id` (Int32)
- `topological_node_mrid` (dictionary string)
- `connectivity_node_mrids` (List<Utf8>)
- `connectivity_count` (Int32)

This table preserves switchyard-level split-bus structure for ML and detailed
contingency analysis without changing core v0.5 table schemas.

### 7) `split_bus` contingency stub element

`contingencies.elements.element_type` now also permits:

- `split_bus`

Current writer behavior is stub-only (no breaker-status parsing yet). Stub
payload encodes:

- `topological_node_id`
- `connectivity_node_a`
- `connectivity_node_b`
- `breaker_mrid` (`stub` placeholder)

These values are serialized in the existing `gen_id` slot as a compact string
to preserve strict v0.5 field layout.

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
