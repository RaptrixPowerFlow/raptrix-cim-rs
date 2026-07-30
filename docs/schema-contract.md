<!--
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
-->

# Schema Contract (Locked contract: v0.13.0 — CGMES 3.0+ Only)

This repository is the authoritative source of truth for the Raptrix Power Interchange (`.rpf`) wire contract used by CIM-first conversion pipelines.

**v0.13.0** is the current contract release. `SUPPORTED_RPF_VERSIONS` accepts **only** **`v0.13.0`** / **`0.13.0`** (clean cut — no pre-0.13 dual-read).

## Identity model (hybrid solver flat-profile)

`.rpf` is a **denormalized solver flat-profile**, not a pure CGMES mRID join graph:

- **Dense `Int32 bus_id`** is the relational foreign key used by solvers, contingencies, and RAS action targets.
- **`buses.bus_uuid`** is required; equipment **`mrid`** is optional (nullable) and must not be required for PSS/E/PSLF ingest.
- Optional file metadata: `rpf.identity.model = hybrid_solver_flat_v1`.
- Optional `metadata.source_identity_scheme`: `dense_bus_id` \| `mrid` \| `mixed` \| `synthetic_mrid`.

## Units matrix (intentional hybrid)

| Domain | Convention |
| --- | --- |
| Bus P/Q, ZIP loads, branch r/x/b | PU on `metadata.base_mva` |
| Generator setpoints / limits | **MW / MVAr** |
| `computational_load_profiles` power fields | **Physical MW** |
| Classical `xd_prime` | pu on machine base `mbase_mva` |
| GIS lat/lon | WGS84 degrees (Float64) |

## v0.13.0 Breaking Changes

- **Remove** required `metadata.psse_version`.
- **Add** optional `source_format` (Dictionary: `psse_raw` \| `pslf_epc` \| `cgmes` \| `powerworld` \| `rpf` \| `other`), `source_format_version` (Utf8), `source_identity_scheme` (Dictionary).
- **Rename** `original_sentinel_case_id` → `baseline_source_case_id`.
- **Timestamps** (`timestamp_utc`, `snapshot_timestamp_utc`, `sal_enhancement_timestamp`, `scenario_context.created_timestamp_utc`): Arrow `Timestamp(Microsecond, Some("UTC"))`.
- **`buses.type`**: Dictionary tokens `PQ` \| `PV` \| `Slack` (not Int8 1/2/3).
- **`generators.controlled_bus_id`**: nullable Int32; **null = local regulation**.
- Optional **`mrid`** on `loads`, `fixed_shunts`, `switched_shunts`.
- **`dynamics_models.classical_params`**: nullable struct `{H, D, xd_prime, mbase_mva}` — prefer over map keys when both present.
- **`computational_load_profiles`**: additive large-load columns including `priority` (1 = highest), `max_step_drop_mw`, `trip_study_percentiles` (**0–100 percentage points**, e.g. 60 and 100 — **not** 0–1 fractions), `common_mode_group`, facility/ride-through fields. **Null/empty `trip_study_percentiles` means no auto-generated percentiles from the case file**; downstream study tools may apply their own configurable defaults.
- **Version gate**: only v0.13.0.

## v0.12.5 Additive Changes

- **`buses.latitude` / `buses.longitude`**: nullable trailing `Float64` columns (WGS84 degrees). Optional GIS coordinates for operator-oriented relative layout (north→south / west→east ranking). Null when the source model has no CIM `Location` / `PositionPoint` data. Purely additive — v0.12.4 files remain readable.
- **CIM GeographicalLocation ingest**: converters accept an optional GL profile (`--gl` / auto-detect `_GL`). `Location.PowerSystemResources` may reference a bus resource (`TopologicalNode` / `ConnectivityNode`) or an `ACLineSegment`. For line routes, the first `PositionPoint` maps to the from-bus and the last maps to the to-bus (via Terminal sequence 1/2); multiple contributions to one bus are averaged. Diagram layout (DL) coordinates are never copied into these fields.
- **EQBD ingest (related converter fix)**: optional Equipment Boundary profile (`--eqbd` / `_EQBD`) supplies shared `BaseVoltage` definitions referenced by TP but omitted from MAS EQ files (required for SmallGrid/FullGrid/MiniGrid/Svedala Merged CAS packages).
- **Version gate**: `SUPPORTED_RPF_VERSIONS` accepts v0.12.5 and retains v0.12.4, v0.12.3, v0.12.2, and v0.12.1.
- **Backward compatibility**: readers null-pad absent trailing geo columns when reading older bus tables.

## v0.12.4 Additive Changes

- **Optional solved-state table `q_limits_solved`** (emitted when `case_mode = solved_snapshot` and the solver recorded per-bus reactive-limit targets):

| Field | Type | Notes |
| --- | --- | --- |
| `bus_id` | Int32, required | Foreign key into `buses.bus_id` |
| `q_net_target_pu` | Float64, required | Net reactive target after limit enforcement |

- **Optional table `feasibility_certificate_buses`** (emitted when a post-solve feasibility/complementarity certificate is present; mirrors the certificate bus audit rows for typed consumers): `bus_id` (Int32, required), `is_pv`, `switched_to_pq`, `complementarity_ok`, `voltage_box_ok` (Boolean, nullable), `q_gen_pu` (Float64, nullable), `violation_kind` (Dictionary\<Int32, Utf8\>, nullable).
- **Version gate**: `SUPPORTED_RPF_VERSIONS` accepts v0.12.4 and retains v0.12.3, v0.12.2, and v0.12.1. The gate remains an explicit allowlist; unknown future versions are rejected.
- **Backward compatibility**: v0.12.4 solved-snapshot files may omit the ten nullable trailing `metadata` provenance columns introduced in v0.12.3; readers null-pad absent fields and reconstruct the canonical 45-column `metadata` shape.

### v0.12.4 Reader Compatibility Policy

Generic readers (`raptrix-cim-arrow::read_rpf_tables`) apply the following tolerances so that files from all conforming writer implementations load identically. Writers must always target the canonical layouts.

- **Name-based root matching.** Every canonical required table must be present as a root struct column, but fixed root column ordering is no longer required. Unknown trailing root columns are ignored.
- **Nested-type tolerance.** List/map item field names (`item` vs `element`) and the nullability of nested fields may differ between writers; value types and top-level field names are still enforced.
- **Pad-row trimming.** Writers pad all root struct columns to a common row count; the logical (real) row count for each table is declared in `rpf.rows.<table>` file metadata and readers slice to it before returning table batches. Pad rows carrying unmasked nulls in non-nullable child arrays are tolerated because they are discarded by trimming.
- **Snapshot dialects.** Current solved-snapshot exports use alternate layouts for four tables. Readers match struct layouts by field name against the canonical layout first, then the documented dialect:

| Table | Canonical layout | Snapshot dialect (read-only) |
| --- | --- | --- |
| `multi_section_lines` | 12 cols, per-line rows with `section_branch_ids` list | 8 cols, per-section rows (`parent_line_id`, `section_index`, `section_branch_id`) |
| `dc_lines_2w` | 15 cols, electrical parameters (`r_ohm`, `l_henry`, setpoints) | 11 cols, MW-setpoint solver form (`is_vsc`, `p_min_mw`, `p_max_mw`, `loss_factor`) |
| `switched_shunt_banks` | 5 cols, per-bank step rows | 8 cols, per-bank control rows (`v_low`, `v_high`, `b_steps`, `current_step`) |
| `generators_solved` | 9 cols incl. per-unit outputs | 5 cols, MW/MVAR only (`bus_id`, `id`, `p_mw`, `q_mvar`, `status`) |

Dialect layouts are read-compatibility surfaces only; they are expected to converge to the canonical layouts in a future writer release.

## v0.12.3 Additive Changes

- **baseline provenance** on `metadata`: ten nullable trailing columns document source-case → baseline upgrades (source case ID, model versions, enhancement timestamp, convergence stats, planning-ready flag, and human-readable upgrade summary). Null in standard CIM exports.
- **Change tracking** on optional `topology_changes`: nullable `change_source` and `applied_phase` dictionary columns (`Dictionary<Int32, Utf8>`) record why and when topology deltas were applied (e.g. `SAL_CIM_Upgrade`, `Jan_to_June_Baseline`).
- **Version gate**: `SUPPORTED_RPF_VERSIONS` accepts v0.12.3 and retains v0.12.2 and v0.12.1.
- **Backward compatibility**: v0.12.2 files without SAL columns remain valid; readers null-pad missing trailing metadata and topology_changes fields.

| Field | Type | Example tokens / values |
| --- | --- | --- |
| `metadata.original_sentinel_case_id` | Utf8, nullable | Original source case identifier (field name retained for compatibility) |
| `metadata.original_model_version` | Utf8, nullable | `"2026-01"` |
| `metadata.target_baseline_version` | Utf8, nullable | `"2026-06"` |
| `metadata.is_sal_enhanced` | Boolean, nullable | `true` when SAL enhancement applied |
| `metadata.sal_enhancement_timestamp` | Utf8, nullable | RFC 3339 UTC (same pattern as `timestamp_utc`) |
| `metadata.cim_model_version_used` | Utf8, nullable | CIM model version used during upgrade |
| `metadata.planning_ready` | Boolean, nullable | Case ready for planning studies |
| `metadata.upgrade_summary` | Utf8, nullable | Human-readable upgrade narrative |
| `metadata.convergence_time_ms` | Float64, nullable | Solver convergence wall time |
| `metadata.convergence_iterations` | Int32, nullable | Solver iteration count |
| `topology_changes.change_source` | Dictionary\<Int32, Utf8\>, nullable | `SAL_CIM_Upgrade`, `Model_Alignment` |
| `topology_changes.applied_phase` | Dictionary\<Int32, Utf8\>, nullable | `Jan_to_June_Baseline`, `Planning_Study_Prep` |

## v0.12.2 Additive Changes

- **Nullable `mrid` column** on `branches`, `generators`, `transformers_2w`, and `transformers_3w`: stable CIM-compatible equipment identifiers populated from source mRIDs on export.
- **Schema metadata key `rpf.mrid_support = v1`**: indicates stable equipment identifier column support.
- **Version gate**: `SUPPORTED_RPF_VERSIONS` accepts v0.12.2 and retains v0.12.1.
- **Downstream guidance**: New `mrid` columns provide stable CIM-compatible identifiers. Downstream tools should prefer `mrid` for equipment_id mapping.

| Table | Source mRID | Notes |
| --- | --- | --- |
| `branches` | `ACLineSegment.base.m_rid` | Populated on CIM export |
| `generators` | `SynchronousMachine.base.m_rid` | Distinct from `market_resource_id` |
| `transformers_2w` | `PowerTransformer.base.m_rid` | Populated on CIM export |
| `transformers_3w` | `PowerTransformer.base.m_rid` | Star-expanded legs use `{mrid}_H` / `_M` / `_L` |

## v0.12.1 Additive Changes

- **Optional root table `contingency_island_analysis`** (enabled via `RootWriteOptions.include_contingency_island_analysis`; file metadata `raptrix.features.contingency_island_analysis=true` when present): optional contingency topology filter audit rows keyed by `contingency_id`.
- **Unified optional extensions**: merges public v0.12.0 `remedial_action_schemes` with `contingency_island_analysis` under a single v0.12.1 contract.
- **Version gate**: `SUPPORTED_RPF_VERSIONS` accepts only v0.12.1.

## v0.12.0 Additive Changes

- **Optional root table `remedial_action_schemes`** (enabled via `RootWriteOptions.include_remedial_action_schemes`; file metadata `raptrix.features.remedial_action_schemes=true` when present): canonical single-table representation for executable RAS/SPS data, including arming/trigger conditions, sequenced actions, delay/priority/merit order metadata, and action targets.
- **New optional file metadata key `rpf.ras.schema_mode`**: defaults to `canonical_v12` when `remedial_action_schemes` is emitted.
- **Single-model write policy**: new RAS writes use `remedial_action_schemes` as the authoritative schema.
- **Legacy compatibility policy**: `protection_contingencies` and `topology_changes` remain optional when explicitly enabled; deprecated for new RAS writes.
- **Public-safety requirement**: public examples and fixtures must be synthetic demonstration data only; no CEII or utility-specific topology identifiers.

### v0.11 -> v0.12 Migration Mapping (Deterministic)

| Legacy v0.11 field | Canonical v0.12 field | Rule |
| --- | --- | --- |
| `protection_contingencies.contingency_id` | `remedial_action_schemes.applicable_contingency_ids` | Copy as list member |
| `protection_contingencies.scheme_type` | `remedial_action_schemes.scheme_kind` | Copy token; tolerate unknown values |
| `protection_contingencies.tripped_elements` | `remedial_action_schemes.sequence_steps[].action_set` | Map each tripped element to one action entry |
| `protection_contingencies.sequence` | `remedial_action_schemes.sequence_steps` | Preserve order and `delay_ms` values |
| `topology_changes.change_type` + `affected_bus_ids` | `remedial_action_schemes.sequence_steps[].action_set[].params` | Encode as topology action params in canonical action set |
| `protection_contingencies.data_confidence` | `remedial_action_schemes.data_confidence` | Pass through unchanged |

Migration is intended for compatibility import paths and tests. New v0.12 writes should emit canonical `remedial_action_schemes` directly.

## v0.11.0 Additive Changes

Design rationale and the cross-repo consumption contract live in [adr/0001-protection-informed-contingencies.md](adr/0001-protection-informed-contingencies.md).

- **Optional root table `protection_contingencies`** (enabled via `RootWriteOptions.include_protection_contingencies`; file metadata `raptrix.features.protection_contingencies=true` when present): captures protection-driven contingencies using a layered model — a logical protection-group baseline (`tripped_elements`, `scheme_type`, `data_confidence`) that works on bus-branch data, plus optional breaker-level refinement (`breaker_ids`). Keyed to `contingencies.contingency_id`. `tripped_elements` reuses the exact element struct shape of `contingencies.elements`.
- **Optional root table `topology_changes`** (enabled via `RootWriteOptions.include_topology_changes`, which requires `include_protection_contingencies=true`; file metadata `raptrix.features.topology_changes=true` when present): captures the resulting topology delta (bus split, island formation, substation/partial isolation). The `provenance` column discriminates planning intent (`declared`, emitted by Phase 0 producers) from a future solver-derived delta (`solved`).
- **New optional file metadata key `rpf.protection.fidelity`** (`logical` | `breaker_level` | `mixed`): declares how protection rows are resolved. Defaults to `logical` when `protection_contingencies` is emitted without an explicit override.
- **`contingencies.elements.element_type`** gains a doc-level token **`protection_event`**: marks a `contingencies` row whose protection detail lives in the matching `protection_contingencies` row (joined on `contingency_id`). The wire type is unchanged (`Dictionary<Int32,Utf8>`).
- **Backward compatibility:** no required table/column/metadata changes. Files with neither optional table are byte-for-byte v0.10.0-compatible; v0.10.0 readers ignore the new trailing root columns.

## v0.10.0 Additive Changes

- **`metadata.computational_load_mode`** (Boolean, nullable, Arrow field name `computational_load_mode`): when `true`, consumers apply the computational-load runtime validation contract (for example: non-empty `computational_load_profiles` where required by the interchange profile).
- **Optional root table `computational_load_profiles`** (enabled via `RootWriteOptions.include_computational_load_profiles`; file metadata `raptrix.features.computational_load_profiles=true` when present):
  - **`seasonal_envelope`**: `List<Struct>` where each list element has exactly these child fields (order, names, types):
    - `season`: **Utf8** (non-null within the struct row)
    - `min_mw`: **Float32**
    - `max_mw`: **Float32**
    - `pf`: **Float32**
  - **`buildout_schedule`**: `List<Struct>` where each list element has:
    - `year`: **Int32**
    - `mw`: **Float32**
  - Additional nullable columns: `bus_id`, `load_id`, ramp rates, IT split, `it_allocation_mode`, `ups_config` / `pcc_relay_settings` / `facility_use_case_percent` as `Map<Utf8, Float64>`, and onsite BESS fields (see `computational_load_profiles_schema()` in `raptrix-cim-arrow/src/schema.rs`).
- **`dynamics_models.perc1_params`**: nullable **Struct** with named Float64 child fields: `perc1_voltage_ride_through_pu`, `perc1_frequency_ride_through_hz`, `perc1_reactive_power_ceiling_pu`, `perc1_active_power_recovery_rate_pu_per_s`, `perc1_voltage_support_time_sec`, `perc1_frequency_support_time_sec` (each nullable within the struct).

## v0.9.5 Additive Changes

- **`generators.controlled_bus_id`** (Int32, required, 25th column): Remote voltage regulation target bus in the same dense `bus_id` numbering as `generators.bus_id`. Semantics: **`0` or `bus_id`** = local regulation at the generator terminal bus; any other valid `bus_id` = remote **IREG** / **RegulatingControl** target (denormalized from CIM so consumers need not join `RegulatingControl` at load time). **PSS/E mapping:** machine IREG bus number → `controlled_bus_id`. **CIM mapping:** `RegulatingControl` (voltage-regulating) target terminal’s topological / connectivity resolution → `controlled_bus_id`.
- **Backward compatibility:** 24-column `generators` tables (v0.9.4 shape ending at `params`) continue to load. Canonical readers (for example `raptrix-cim-arrow::read_rpf_tables`) synthesize missing `controlled_bus_id` as **`0`** (local regulation) when extending short structs to the locked schema — **zero-copy, zero allocation** aside from the pre-sized padding column slice.
- **`metadata.default_shunt_control_mode`** (Dictionary\<Int32, Utf8\>, nullable) and optional file-level **`rpf.default_shunt_control_mode`**: When present, downstream solvers will default to this shunt mode (`planning_full` \| `real_time_hot_start` \| `real_time_frozen`). Enables fully declarative **planning ↔ real-time** handoff alongside `case_mode` (which remains the authoritative planning vs. solved snapshot discriminator).

## v0.9.3 Breaking Changes

- `buses.nominal_kv` is now required (`Float64`, non-null).
- `branches.from_nominal_kv` and `branches.to_nominal_kv` are now required (`Float64`, non-null).
- `transformers_2w.from_nominal_kv` and `transformers_2w.to_nominal_kv` are now required (`Float64`, non-null).
- `transformers_3w.nominal_kv_h`, `nominal_kv_m`, `nominal_kv_l` are now required (`Float64`, non-null).
- Writer validation now enforces these fields as finite and strictly greater than `0.0`.
- `SUPPORTED_RPF_VERSIONS` accepts only `v0.9.3` / `0.9.3`.

## v0.9.2 Additive Changes

- **`generators` table extended** with required field:
  - `q_sched_mvar` (scheduled reactive power setpoint, MVAr)
- `q_sched_mvar` is required on read and always emitted on write.
- `SUPPORTED_RPF_VERSIONS` now accepts `v0.9.2` / `0.9.2` in addition to prior supported versions.

## v0.9.1 Additive Changes

- **`loads` table extended** with 4 new nullable ZIP-fidelity columns appended after `q_pu`:
  - `p_i_pu` (constant-current active component, per-unit on system base)
  - `q_i_pu` (constant-current reactive component, per-unit on system base)
  - `p_y_pu` (constant-admittance active component, per-unit on system base)
  - `q_y_pu` (constant-admittance reactive component, per-unit on system base)
- Existing `loads.p_pu` / `loads.q_pu` remain constant-power components with unchanged semantics.
- `SUPPORTED_RPF_VERSIONS` now accepts `v0.9.1` / `0.9.1`.
- v0.9.0 files remain structurally compatible for additive readers that tolerate missing trailing nullable `loads` fields.

## v0.9.0 Breaking Changes

- **`ibr_devices` table removed.** IBRs are now modeled exclusively in the `generators` table using `is_ibr = true` and `ibr_subtype`. Files claiming v0.9.0 must not include an `ibr_devices` root column.
- **`contingencies` table extended** with 6 new nullable operational-outcome columns: `risk_score`, `cleared_by_reserves`, `voltage_collapse_flag`, `recovery_possible`, `recovery_time_min`, `greedy_reserve_summary`. These are null in standard planning files.
- **`metadata` table extended** with 5 new nullable analysis-readiness fields. `case_mode` now accepts the additional value `"hour_ahead_advisory"`.
- **New optional table `scenario_context`** for structured analysis context (real-time, hour-ahead advisory, planning feedback).
- **Canonical table count**: 18 required tables (was 19).
- `SUPPORTED_RPF_VERSIONS` now accepts `v0.9.1` / `0.9.1` and keeps `v0.9.0` / `0.9.0` for backward-compatible reads.

## Contract Design Rationale

- The v0.9.1 contract is designed for current grid models, including inverter-based resources (IBR), DER-heavy operation, advanced flow-control devices, and modern DC workflows.
- Required modern-grid tables (`multi_section_lines`, `dc_lines_2w`, `switched_shunt_banks`) are first-class contract elements, not side extensions.
- IBR modeling is unified in the `generators` table (`is_ibr = true`, `ibr_subtype`); no separate `ibr_devices` table.
- Arrow-native list and map types are used deliberately so parsers and solvers can ingest table payloads without lossy flattening.

## Compatibility Rationale

- This contract is not designed as a parity-first schema for any single legacy format.
- Interoperability with legacy toolchains may be achieved where practical, but the primary design goal is a stable, physically consistent interchange contract.
- The normative source remains IEC 61970 CIM semantics mapped into a stable Arrow contract for deterministic downstream ingestion.

## Table Ownership (solve / re-export contract)

The solver (`raptrix-core`) holds a **projection** of an `.rpf` — only the electrical state needed for powerflow. Rebuilding a full `.rpf` from that projection is lossy (GIS, contingencies, RAS/SPS, diagram layout, node-breaker detail, unknown future tables, …).

**Canonical rule:** the source `.rpf` is the authoritative document. After a solve, consumers apply a **solve patch** via `raptrix-cim-arrow::apply_rpf_patch` (FFI: `apply_rpf_patch_c`). Untouched tables/columns are passed through (Arrow buffer reuse). Unknown root tables default to converter-owned passthrough so older solvers cannot destroy newer file richness.

| Ownership | Rule | Tables |
| --- | --- | --- |
| **Converter-owned** | Always taken from the source file when present. Patch copies are ignored. | All required structural tables (`buses`, `branches`, `generators`, `loads`, shunts, transformers, areas/zones/owners, `contingencies`, `interfaces`, `dynamics_models`, …), plus optional enrichment: `protection_contingencies`, `topology_changes`, `remedial_action_schemes`, `contingency_island_analysis`, `scenario_context`, `computational_load_profiles`, `facts_devices`, node-breaker / connectivity / diagram tables. Includes `buses.latitude` / `buses.longitude`. |
| **Solver-owned** | Taken from the patch when present; otherwise retained from source. | `buses_solved`, `generators_solved`, `switched_shunts_solved`, `facts_solved`, `q_limits_solved`, `feasibility_certificate_buses` |
| **Shared** | Explicit merge. | `metadata` table: per-column, prefer patch when non-null else source. File-level keys under `rpf.solver.*`, `rpf.case_mode`, `rpf.solved_state_presence`, and related solved-feature flags overlay from the patch; converter feature flags and unknown keys from source are preserved. `rpf.rows.*` is always recomputed for the output. |

**Workflows**

1. **Solve → export (preferred):** `apply_rpf_patch(source_rpf, patch_rpf, output_rpf)` where `patch_rpf` may be a full core export; only solver-owned tables (and shared metadata overlays) are taken from it.
2. **No source file** (e.g. RAW→Network never loaded from `.rpf`): full rebuild from the solver model remains allowed; richness that was never in an `.rpf` cannot be invented.
3. **Structural edits in Studio:** persist them to the working `.rpf` first (new source), then solve and patch. Do not expect Network-only structural edits to survive a source-passthrough export.

**Guardrail:** regression tests must assert that `apply_rpf_patch(source, empty_or_solver_only_patch, out)` preserves every converter-owned table (including GIS and contingencies) bit-for-bit at the RecordBatch level.

## Contract Policy

- Schema changes are explicit and versioned.
- Column order is stable and treated as part of the contract.
- Column type and nullability changes require a version bump and migration note.

## Compatibility Policy

- **CGMES Ingest Target**: v3.0 and later only (complete merged profiles with EQ, TP, SV, DL, GL, SSH, etc.).
- **Legacy Support Dropped**: CGMES 2.4.x support was removed in v0.8.0. All ingest is now CGMES 3.0+ only. This enables cleaner parsing logic, better performance, and full alignment with ENTSO-E Conformity Assessment Scheme (v3.0.3 current).
- **CIM baseline**: raptrix-cim-rs targets IEC 61970 CIM 17+ classes and RDF/XML profile exchange directly.
- **Public validation corpus**: ENTSO-E CGMES v3.0.3 CAS remains the canonical public regression dataset.
- The `.rpf` contract is forward compatible for additive changes only. Readers must ignore unknown trailing root columns and unknown file metadata keys.
- Breaking file-format changes (required column rename/removal/reorder, required table rename/removal/reorder, type change for required fields) require a MAJOR contract bump.
- Additive changes (new optional columns, new optional tables, new optional metadata keys) require at least a MINOR bump.
- PATCH bumps are reserved for non-structural fixes: bug fixes, metadata text fixes, and documentation clarifications without wire-shape changes.

## 0.9.3 Nominal-kV Guidance

- Nominal voltage is a required physical base for per-unit conversion and solver interoperability.
- Writers must not emit null or non-positive values for required nominal-kV columns.
- If source payload cannot provide BaseVoltage / BASKV for these fields, writers must fail with a validation error.

## File Metadata Keys

Every `.rpf` file must include:

- `raptrix.branding`
- `raptrix.version`

Current locked values:

- `raptrix.version = 0.13.0` (also accepted as `v0.13.0` only)
- `raptrix.branding = Raptrix CIM-Arrow / Raptrix Power Interchange v0.13.0 - High-performance open CIM profile (CGMES 3.0+) by Raptrix Power. Copyright (c) 2026 Raptrix Power.`
- `rpf.case_fingerprint = <required deterministic case identity fingerprint>`
- `rpf.validation_mode = topology_only | solved_ready`
- `rpf.case_mode = flat_start_planning | warm_start_planning | solved_snapshot | hour_ahead_advisory` (v0.8.4+, required; `hour_ahead_advisory` added in v0.9.0)
- `rpf.solved_state_presence = actual_solved | not_available | not_computed | seed_only` (v0.8.4+, required; `seed_only` added in v0.9.6 — warm-start initial conditions in `buses_solved` without solver provenance, valid only with `case_mode = warm_start_planning`)

Optional file-level metadata keys:

- `raptrix.features.node_breaker = true` when optional node-breaker detail tables are emitted
- `raptrix.features.diagram_layout = true` when optional IEC 61970-453 diagram layout tables are emitted
- `raptrix.features.contingencies_stub = true` when contingencies table is populated by placeholder/stub rows
- `raptrix.features.dynamics_stub = true` when dynamics_models table is populated by placeholder/stub rows
- `raptrix.features.facts = true` when optional FACTS metadata table(s) are emitted (v0.8.6+)
- `rpf.default_shunt_control_mode = planning_full | real_time_hot_start | real_time_frozen` (v0.9.5+, optional) — mirrors nullable `metadata.default_shunt_control_mode` when writers stamp file-level keys for tooling that inspects IPC metadata only
- `raptrix.features.facts_solved = true` when optional `facts_solved` table is emitted (v0.8.6+)
- `raptrix.features.protection_contingencies = true` when optional `protection_contingencies` table is emitted (v0.11.0+)
- `raptrix.features.topology_changes = true` when optional `topology_changes` table is emitted (v0.11.0+)
- `rpf.protection.fidelity = logical | breaker_level | mixed` (v0.11.0+, optional; defaults to `logical` when `protection_contingencies` is present without an explicit value)
- `raptrix.features.remedial_action_schemes = true` when optional `remedial_action_schemes` table is emitted (v0.12.1+)
- `raptrix.features.contingency_island_analysis = true` when optional `contingency_island_analysis` table is emitted (v0.12.1+)
- `rpf.ras.schema_mode = canonical_v12` when canonical v0.12 RAS rows are emitted (v0.12.1+)
- `rpf.mrid_support = v1` when stable equipment `mrid` columns are present in table schemas (v0.12.2+)
- `rpf.rows.<table_name> = <row_count>` for each emitted table
- `rpf.solver.version = <string>` solver software version (only when `solved_state_presence = actual_solved`)
- `rpf.solver.iterations = <int>` Newton-Raphson iteration count (only when solved)
- `rpf.solver.accuracy = <float>` final mismatch residual (only when solved)
- `rpf.solver.mode = <string>` bus control mode, e.g. `PV`, `PV_to_PQ` (only when solved)
- `rpf.solver.slack_bus_id = <int>` the bus_id used as the angle reference (slack bus) in the solve (v0.8.5+, only when solved)
- `rpf.solver.angle_reference_deg = <float>` angle reference value in degrees, typically 0.0 (v0.8.5+, only when solved)
- `rpf.solver.solved_shunt_state_presence = actual_solved | not_available` (v0.8.5+, only when solved)
- `rpf.facts_solved_state_presence = actual_solved | not_available` (v0.8.6+, optional; defaults to `not_available` when `facts_devices` is present and `facts_solved` is absent)
- `rpf.transformer_representation_mode = native_3w | expanded` (v0.8.7+, **required**; readers treating files from pre-v0.8.7 producers should default to `native_3w` when the key is absent)
- `rpf.loads.zip_fidelity_presence = not_available | partial | complete` (v0.9.1+, optional; indicates whether `loads` ZIP fidelity columns are populated by source export path)

## File Container Layout

`.rpf` is a standard Arrow IPC File container, not a custom binary framing. A compliant reader must:

1. Open the file as Arrow IPC File format.
2. Read the root schema metadata.
3. Read one root record batch.
4. Interpret each root column as one table encoded as a nullable `StructArray`.

Current writer behavior emits exactly one root record batch. A future writer may emit more than one root batch, so readers should iterate record batches and reconstruct tables by root column name rather than assuming a single batch forever.

## Root Column Ordering

Required root columns are in this exact order:

1. `metadata`
2. `buses`
3. `branches`
4. `multi_section_lines`
5. `dc_lines_2w`
6. `generators`
7. `loads`
8. `fixed_shunts`
9. `switched_shunts`
10. `switched_shunt_banks`
11. `transformers_2w`
12. `transformers_3w`
13. `areas`
14. `zones`
15. `owners`
16. `contingencies`
17. `interfaces`
18. `dynamics_models`

Optional root columns, when present, are appended after the required columns in this order:

19. `node_breaker_detail`
20. `switch_detail`
21. `connectivity_nodes`
22. `diagram_objects`
23. `diagram_points`
24. `buses_solved`
25. `generators_solved`
26. `switched_shunts_solved`
27. `facts_devices`
28. `facts_solved`
29. `scenario_context` (v0.9.0+, optional analysis context)
30. `protection_contingencies` (v0.11.0+, optional protection-informed contingencies)
31. `topology_changes` (v0.11.0+, optional post-event topology metadata)
32. `remedial_action_schemes` (v0.12.1+, optional canonical RAS/SPS schema)
33. `contingency_island_analysis` (v0.12.1+, optional contingency topology filter audit rows)

`connectivity_groups` is an optional detail table emitted only in connectivity-detail mode and is appended after the required root columns when that mode is active.

## Table Reconstruction Rules

Each root struct column may be null-padded to the maximum row count of any emitted table in the root batch. A compliant parser must use `rpf.rows.<table_name>` metadata, when present, as the authoritative logical row count for each table and trim any padded null tail beyond that count.

Recommended read algorithm:

1. Open Arrow IPC file and collect root schema metadata.
2. For each root column name, look up the expected schema by table name.
3. Downcast the root column to `StructArray`.
4. Trim each child array to `rpf.rows.<table_name>` rows.
5. Reconstruct the logical table record batch from the trimmed child arrays.

Readers should ignore unknown trailing root columns for forward compatibility, but they must reject reordered or renamed required root columns.

## Canonical Schema Source

The executable contract is defined in `raptrix-cim-arrow/src/schema.rs` and exported through the shared `raptrix-cim-arrow` crate:

- `all_table_schemas()` for canonical ordering
- `table_schema(name)` for table lookup

Generic root Arrow IPC file assembly, validation, readback, and metadata inspection live beside the schema in `raptrix-cim-arrow/src/io.rs`.

## Locked Tables

Required tables (empty tables allowed):

- `metadata`
- `buses`
- `branches`
- `multi_section_lines`
- `dc_lines_2w`
- `generators`
- `loads`
- `fixed_shunts`
- `switched_shunts`
- `switched_shunt_banks`
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

Optional detail tables (emitted only when `raptrix.features.node_breaker = true`):

- `node_breaker_detail`
- `switch_detail`
- `connectivity_nodes`

Optional diagram layout tables (emitted only when `raptrix.features.diagram_layout = true`):

- `diagram_objects`
- `diagram_points`

Optional solved-state tables (emitted when `case_mode = solved_snapshot`, v0.8.4+, or when `case_mode = warm_start_planning` with `solved_state_presence = seed_only`, v0.9.6+):

- `buses_solved` — populated for both `solved_snapshot` and `seed_only`
- `generators_solved` — populated for `solved_snapshot`; zero rows under `seed_only`
- `switched_shunts_solved` (v0.8.5+) — populated for `solved_snapshot`; zero rows under `seed_only`

Optional FACTS tables (v0.8.6+, emitted only when FACTS metadata is present):

- `facts_devices`
- `facts_solved` (optional solved snapshot replay companion)

Optional protection-informed tables (v0.11.0+, emitted only when explicitly enabled):

- `protection_contingencies`
- `topology_changes` (requires `protection_contingencies`)

Optional canonical RAS/SPS table (v0.12.1+):

- `remedial_action_schemes`

Optional contingency topology filter audit table (v0.12.1+):

- `contingency_island_analysis`

## Column Reference

This section is normative for external parser authors.

### metadata

- `base_mva`: Float64, required
- `frequency_hz`: Float64, required
- `source_format`: Dictionary<Int32, Utf8>, nullable (v0.13.0+) — `psse_raw` \| `pslf_epc` \| `cgmes` \| `powerworld` \| `rpf` \| `other`
- `source_format_version`: Utf8, nullable (v0.13.0+)
- `source_identity_scheme`: Dictionary<Int32, Utf8>, nullable (v0.13.0+) — `dense_bus_id` \| `mrid` \| `mixed` \| `synthetic_mrid`
- `study_name`: Dictionary<Int32, Utf8>, required
- `timestamp_utc`: Timestamp(us, UTC), required (v0.13.0+)
- `raptrix_version`: Utf8, required
- `is_planning_case`: Boolean, required
- `source_case_id`: Dictionary<Int32, Utf8>, required
- `snapshot_timestamp_utc`: Timestamp(us, UTC), required (v0.13.0+)
- `case_fingerprint`: Utf8, required
- `validation_mode`: Dictionary<Int32, Utf8>, required
- `custom_metadata`: Map<String, String>, nullable
- `case_mode`: Dictionary<Int32, Utf8>, required — `flat_start_planning` | `warm_start_planning` | `solved_snapshot` | `hour_ahead_advisory` (v0.8.4+; `hour_ahead_advisory` added in v0.9.0)
- `solved_state_presence`: Dictionary<Int32, Utf8>, nullable — `actual_solved` | `not_available` | `not_computed` | `seed_only` (v0.8.4+; `seed_only` added in v0.9.6 — warm-start initial conditions in `buses_solved` without solver provenance, valid only with `case_mode = warm_start_planning`)
- `solver_version`: Utf8, nullable — populated only when `solved_state_presence = actual_solved` (v0.8.4+)
- `solver_iterations`: Int32, nullable — Newton-Raphson iteration count (v0.8.4+)
- `solver_accuracy`: Float64, nullable — final mismatch residual norm (v0.8.4+)
- `solver_mode`: Dictionary<Int32, Utf8>, nullable — e.g. `PV`, `PV_to_PQ` (v0.8.4+)
- `slack_bus_id_solved`: Int32, nullable — bus_id of the angle reference (slack) bus used in the solve; prevents silent reference-frame mismatch when snapshots are re-used (v0.8.5+)
- `angle_reference_deg`: Float64, nullable — angle reference value in degrees applied at the slack bus; typically 0.0 (v0.8.5+)
- `solved_shunt_state_presence`: Dictionary<Int32, Utf8>, nullable — `actual_solved` | `not_available`; lets loaders fail fast or warn if solved snapshot claims solved but lacks full shunt state (v0.8.5+)
- `modern_grid_profile`: Boolean, required (v0.8.9+)
- `ibr_penetration_pct`: Float64, nullable (v0.8.9+)
- `has_ibr`: Boolean, required (v0.8.9+)
- `has_smart_valve`: Boolean, required (v0.8.9+)
- `has_multi_terminal_dc`: Boolean, required (v0.8.9+)
- `study_purpose`: Utf8, nullable (v0.8.9+)
- `scenario_tags`: List<Utf8>, nullable (v0.8.9+)
- `hour_ahead_uncertainty_band`: Float64, nullable (v0.9.0+) — load forecast uncertainty band as a percentage, e.g. `2.0` = ±2%
- `commitment_source`: Utf8, nullable (v0.9.0+) — e.g. `"day_ahead_market"`, `"operator_plan"`
- `solver_q_limit_infeasible_count`: Int32, nullable (v0.9.0+) — number of buses where Q-limit infeasibility was detected
- `pv_to_pq_switch_count`: Int32, nullable (v0.9.0+) — number of PV→PQ bus-type switches during solve
- `real_time_discovery`: Boolean, nullable (v0.9.0+) — `true` if this case originated from live State Estimator analysis
- `default_shunt_control_mode`: Dictionary<Int32, Utf8>, nullable (v0.9.5+) — optional declarative default shunt control mode; see v0.9.5 additive section
- `computational_load_mode`: Boolean, nullable (v0.10.0+) — when `true`, consumers apply the computational-load runtime validation contract
- `baseline_source_case_id`: Utf8, nullable (v0.13.0+; renamed from `original_sentinel_case_id`) — original source case identifier for baseline provenance
- `original_model_version`: Utf8, nullable (v0.12.3+) — e.g. `"2026-01"`
- `target_baseline_version`: Utf8, nullable (v0.12.3+) — e.g. `"2026-06"`
- `is_sal_enhanced`: Boolean, nullable (v0.12.3+) — `true` when SAL enhancement was applied
- `sal_enhancement_timestamp`: Timestamp(us, UTC), nullable (v0.13.0+)
- `cim_model_version_used`: Utf8, nullable (v0.12.3+) — CIM model version used during upgrade
- `planning_ready`: Boolean, nullable (v0.12.3+) — case ready for planning studies
- `upgrade_summary`: Utf8, nullable (v0.12.3+) — human-readable upgrade narrative
- `convergence_time_ms`: Float64, nullable (v0.12.3+) — solver convergence wall time in milliseconds
- `convergence_iterations`: Int32, nullable (v0.12.3+) — solver iteration count

### buses

- `bus_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `type`: Dictionary<Int32, Utf8>, required — `PQ` \| `PV` \| `Slack` (v0.13.0+)
- `p_sched`: Float64, required
- `q_sched`: Float64, required — net scheduled reactive injection = `qg_sched_pu − qd_load_pu` (all bus types)
- `v_mag_set`: Float64, required
- `v_ang_set`: Float64, required
- `q_min`: Float64, required
- `q_max`: Float64, required
- `g_shunt`: Float64, required
- `b_shunt`: Float64, required
- `area`: Int32, required
- `zone`: Int32, required
- `owner`: Int32, required
- `v_min`: Float64, required
- `v_max`: Float64, required
- `p_min_agg`: Float64, required
- `p_max_agg`: Float64, required
- `owner_id`: Int32, nullable
- `nominal_kv`: Float64, required (`> 0`)
- `bus_uuid`: Dictionary<Int32, Utf8>, required
- `qd_load_pu`: Float64, required (v0.9.4+) — Σ(in-service load QL) / SBASE; signed (positive for inductive load, negative when PSS/E load QL < 0); zero for buses with no load
- `qg_sched_pu`: Float64, required (v0.9.4+) — Σ(in-service generator QG) / SBASE; any sign; zero for buses with no generation
- `latitude`: Float64, nullable (v0.12.5+) — WGS84 latitude in degrees from CIM `Location`/`PositionPoint` (yPosition). Null when unavailable.
- `longitude`: Float64, nullable (v0.12.5+) — WGS84 longitude in degrees from CIM `Location`/`PositionPoint` (xPosition). Null when unavailable.

### branches

- `branch_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Dictionary<Int32, Utf8>, required
- `r`: Float64, required
- `x`: Float64, required
- `b_shunt`: Float64, required
- `tap`: Float64, required
- `phase`: Float64, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `owner_id`: Int32, nullable
- `from_nominal_kv`: Float64, required (`> 0`)
- `to_nominal_kv`: Float64, required (`> 0`)
- `device_type`: Dictionary<Int32, Utf8>, nullable (v0.8.6+) — canonical token for SmartValve is `smartvalve`; reader normalization must accept alias `SV` (case-insensitive) and canonicalize to `smartvalve`.
- `control_mode`: Dictionary<Int32, Utf8>, nullable (v0.8.6+) — open vocabulary; recommended values include `series_impedance`, `phase_shift`, `voltage_injection`, `bypass`.
- `control_target_flow_mw`: Float64, nullable (v0.8.6+) — flow target used by flow-controlling FACTS.
- `x_min_pu`: Float64, nullable (v0.8.6+) — lower bound for effective series reactance in per-unit.
- `x_max_pu`: Float64, nullable (v0.8.6+) — upper bound for effective series reactance in per-unit.
- `injected_voltage_mag_pu`: Float64, nullable (v0.8.6+) — injected series-voltage magnitude in per-unit.
- `injected_voltage_angle_deg`: Float64, nullable (v0.8.6+) — injected series-voltage angle in degrees.
- `facts_params`: Map<String, Float64>, nullable (v0.8.6+) — additive vendor or model-specific scalar parameters.
- `parent_line_id`: Int32, nullable (v0.8.9+) — links branch sections to `multi_section_lines.line_id`.
- `section_index`: Int32, nullable (v0.8.9+) — ordered section index within a multi-section logical line.

### multi_section_lines

- `line_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Utf8, required
- `section_branch_ids`: List<Int32>, required
- `total_r_pu`: Float64, required
- `total_x_pu`: Float64, required
- `total_b_pu`: Float64, required
- `rate_a_mva`: Float64, required
- `rate_b_mva`: Float64, nullable
- `status`: Boolean, required
- `name`: Utf8, nullable

### dc_lines_2w

- `dc_line_id`: Int32, required
- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Utf8, required
- `r_ohm`: Float64, required
- `l_henry`: Float64, nullable
- `control_mode`: Utf8, required
- `p_setpoint_mw`: Float64, nullable
- `i_setpoint_ka`: Float64, nullable
- `v_setpoint_kv`: Float64, nullable
- `q_from_mvar`: Float64, nullable
- `q_to_mvar`: Float64, nullable
- `status`: Boolean, required
- `name`: Utf8, nullable
- `converter_type`: Utf8, required

Recommended `control_mode` tokens for `dc_lines_2w` are `power`, `current`, `voltage`, and `droop`.

### generators

- `generator_id`: Int32, required
- `bus_id`: Int32, required
- `name`: Utf8, nullable
- `unit_type`: Utf8, required
- `hierarchy_level`: Utf8, required
- `parent_generator_id`: Int32, nullable
- `aggregation_count`: Int32, nullable
- `status`: Boolean, required
- `is_ibr`: Boolean, required
- `ibr_subtype`: Utf8, nullable
- `p_sched_mw`: Float64, required
- `q_sched_mvar`: Float64, required
- `p_min_mw`: Float64, required
- `p_max_mw`: Float64, required
- `q_min_mvar`: Float64, required
- `q_max_mvar`: Float64, required
- `mbase_mva`: Float64, required
- `uol_mw`: Float64, nullable
- `lol_mw`: Float64, nullable
- `ramp_rate_up_mw_min`: Float64, nullable
- `ramp_rate_down_mw_min`: Float64, nullable
- `owner_id`: Int32, nullable
- `market_resource_id`: Utf8, nullable
- `params`: Map<String, Float64>, nullable
- `controlled_bus_id`: Int32, nullable (v0.13.0+) — `null` = local voltage regulation; non-null dense `bus_id` = remote regulated bus (**PSS/E IREG** / **CIM RegulatingControl** target).

### ibr_devices

> **Removed in v0.9.0.** IBRs are now modeled in the `generators` table using `is_ibr = true` and `ibr_subtype`. Writers must not emit an `ibr_devices` root column in v0.9.0+ files.

### loads

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `p_pu`: Float64, required — constant-power active component (P term), per-unit on system base
- `q_pu`: Float64, required — constant-power reactive component (Q term), per-unit on system base
- `p_i_pu`: Float64, nullable (v0.9.1+) — constant-current active component, per-unit on system base
- `q_i_pu`: Float64, nullable (v0.9.1+) — constant-current reactive component, per-unit on system base
- `p_y_pu`: Float64, nullable (v0.9.1+) — constant-admittance active component, per-unit on system base
- `q_y_pu`: Float64, nullable (v0.9.1+) — constant-admittance reactive component, per-unit on system base
- `name`: Dictionary<UInt32, Utf8>, nullable

ZIP mapping semantics for PSS/E LOAD records (system base `S_base`):

- `p_pu = PL / S_base`
- `q_pu = QL / S_base`
- `p_i_pu = IP / S_base`
- `q_i_pu = IQ / S_base`
- `p_y_pu = YP / S_base`
- `q_y_pu = YQ / S_base`

Sign convention:

- Positive values represent net demand (load consumption) for both active and reactive components.
- Negative values represent net injection.
- Writers must preserve source sign without normalization.

Null/default behavior:

- When source data lacks a ZIP component (or the source format does not provide it), writers must emit `null` for that component.
- Writers must not fabricate zero values to imply absent source data.
- Legacy files without these fields remain readable; readers should treat missing columns as all-null for backward compatibility.
- Writers should stamp `rpf.loads.zip_fidelity_presence` as:
  - `not_available` when source/export path does not provide ZIP decomposition terms
  - `partial` when ZIP terms are populated for only a subset of load rows
  - `complete` when ZIP terms are populated (or explicitly zero-valued from source) for all load rows

### fixed_shunts

- `bus_id`: Int32, required
- `id`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `g_pu`: Float64, required
- `b_pu`: Float64, required

### switched_shunts

- `bus_id`: Int32, required
- `status`: Boolean, required
- `v_low`: Float64, required
- `v_high`: Float64, required
- `b_steps`: List<Float64>, required
- `current_step`: Int32, required
- `b_init_pu`: Float64, nullable — authoritative initial susceptance in per-unit (v0.8.3+). PSS/E source: `BINIT / base_mva`. CIM source: `b_steps[current_step - 1]`. Readers should prefer this field over reconstructing from `b_steps + current_step`. Nullable for backward compatibility; writers must populate this field.
- `shunt_id`: Dictionary<Int32, Utf8>, nullable — stable per-bank identity to disambiguate multiple banks at the same bus (v0.8.5+). CIM path: `ShuntCompensator` mRID. PSS/E path: synthesized as `"{bus_id}_shunt_{n}"` (1-indexed). Nullable for backward compatibility; writers must populate when available.

For v0.8.9+, `switched_shunts.b_steps` must contain strictly capacitive (positive) values.
Inductive steps must be represented in `switched_shunt_banks`.

### switched_shunt_banks

- `shunt_id`: Int32, required
- `bank_id`: Int32, required
- `b_mvar`: Float64, required
- `status`: Boolean, required
- `step`: Int32, required

### transformers_2w

- `from_bus_id`: Int32, required
- `to_bus_id`: Int32, required
- `ckt`: Dictionary<Int32, Utf8>, required
- `r`: Float64, required
- `x`: Float64, required
- `winding1_r`: Float64, required
- `winding1_x`: Float64, required
- `winding2_r`: Float64, required
- `winding2_x`: Float64, required
- `g`: Float64, required
- `b`: Float64, required
- `tap_ratio`: Float64, required
- `nominal_tap_ratio`: Float64, required
- `phase_shift`: Float64, required
- `vector_group`: Dictionary<Int32, Utf8>, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `from_nominal_kv`: Float64, required (`> 0`)
- `to_nominal_kv`: Float64, required (`> 0`)

### transformers_3w

- `bus_h_id`: Int32, required
- `bus_m_id`: Int32, required
- `bus_l_id`: Int32, required
- `star_bus_id`: Int32, nullable
- `ckt`: Dictionary<Int32, Utf8>, required
- `r_hm`: Float64, required
- `x_hm`: Float64, required
- `r_hl`: Float64, required
- `x_hl`: Float64, required
- `r_ml`: Float64, required
- `x_ml`: Float64, required
- `tap_h`: Float64, required
- `tap_m`: Float64, required
- `tap_l`: Float64, required
- `phase_shift`: Float64, required
- `vector_group`: Dictionary<Int32, Utf8>, required
- `rate_a`: Float64, required
- `rate_b`: Float64, required
- `rate_c`: Float64, required
- `status`: Boolean, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `nominal_kv_h`: Float64, required (`> 0`)
- `nominal_kv_m`: Float64, required (`> 0`)
- `nominal_kv_l`: Float64, required (`> 0`)

### areas

- `area_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `interchange_mw`: Float64, nullable

### zones

- `zone_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required

### owners

- `owner_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required

### contingencies

- `contingency_id`: Dictionary<Int32, Utf8>, required
- `elements`: List<Struct>, required
- `risk_score`: Float64, nullable (v0.9.0+) — composite risk score
- `cleared_by_reserves`: Boolean, nullable (v0.9.0+) — true if contingency was cleared by greedy reserve dispatch
- `voltage_collapse_flag`: Boolean, nullable (v0.9.0+) — true if voltage collapse was detected
- `recovery_possible`: Boolean, nullable (v0.9.0+) — true if system recovery is achievable within NERC criteria
- `recovery_time_min`: Float64, nullable (v0.9.0+) — estimated recovery time in minutes
- `greedy_reserve_summary`: Utf8, nullable (v0.9.0+) — short text description of greedy reserve dispatch actions

`elements` fields:

- `element_type`: Dictionary<Int32, Utf8>, required
- `branch_id`: Int32, nullable
- `bus_id`: Int32, nullable
- `gen_id`: Dictionary<Int32, Utf8>, nullable
- `load_id`: Dictionary<Int32, Utf8>, nullable
- `amount_mw`: Float64, nullable
- `status_change`: Boolean, required
- `equipment_kind`: Dictionary<Int32, Utf8>, nullable
- `equipment_id`: Dictionary<Int32, Utf8>, nullable

### contingency_island_analysis

Optional contingency topology filter audit rows (v0.12.1+):

- `contingency_id`: Dictionary<Int32, Utf8>, required — FK to `contingencies.contingency_id`
- `classification`: Dictionary<Int32, Utf8>, nullable
- `filter_reason`: Dictionary<Int32, Utf8>, nullable
- `island_load_mw`: Float64, nullable
- `island_gen_mw`: Float64, nullable
- `bus_count`: Int32, nullable
- `max_kv`: Float64, nullable
- `is_main_island`: Boolean, nullable
- `excluded_from_events`: Boolean, nullable
- `params_snapshot_json`: Utf8, nullable

### interfaces

- `interface_id`: Int32, required
- `name`: Dictionary<Int32, Utf8>, required
- `monitored_branches`: List<Int32>, required
- `transfer_limit_mw`: Float64, required

### dynamics_models

- `bus_id`: Int32, required
- `gen_id`: Dictionary<Int32, Utf8>, required
- `model_type`: Dictionary<Int32, Utf8>, required
- `params`: Map<String, Float64>, required
- `perc1_params`: Struct, nullable (v0.10.0+)
- `classical_params`: Struct, nullable (v0.13.0+) — children `H`, `D`, `xd_prime`, `mbase_mva` (each Float64, nullable). Prefer over map keys when both present.

Dynamics population rules for downstream consumers:

- Writers SHOULD prefer DY-profile-linked model rows when CGMES DY input is present and references a known generator.
- Writers SHOULD include parsed numeric DY parameters in `params` using normalized lowercase keys derived from CIM field names.
- When DY coverage is partial, writers SHOULD fall back to EQ-derived rows for unmatched generators to preserve generator coverage.
- When no generator-linked dynamics can be derived from DY or EQ, writers MAY emit a placeholder row and set `raptrix.features.dynamics_stub = true`.
- `model_type` is an open string vocabulary. Writers MAY emit CIM class names (for example `SynchronousMachineDynamics`) or extension names (for example `raptrix.smart_valve.v1`).
- For non-CIM extensions, writers SHOULD use namespaced `model_type` values and namespaced `params` keys to avoid collisions.
- Provenance keys currently emitted in `params` are:
  - `source_dy = 1.0` for DY-linked rows
  - `source_eq_fallback = 1.0` for EQ fallback rows
  - `source_stub = 1.0` for placeholder rows

### facts_devices

- `device_id`: Dictionary<Int32, Utf8>, required
- `branch_id`: Int32, nullable (null when bus-coupled)
- `bus_id`: Int32, nullable (null when branch-coupled)
- `device_type`: Dictionary<Int32, Utf8>, required
- `status`: Boolean, required
- `control_mode`: Dictionary<Int32, Utf8>, nullable
- `target_flow_mw`: Float64, nullable
- `x_min_pu`: Float64, nullable
- `x_max_pu`: Float64, nullable
- `voltage_injection_mag_pu`: Float64, nullable
- `voltage_injection_angle_deg`: Float64, nullable
- `response_time_ms`: Float64, nullable
- `rating_mva`: Float64, nullable
- `dynamics_model_ref`: Dictionary<Int32, Utf8>, nullable
- `params`: Map<String, Float64>, nullable
- `name`: Dictionary<Int32, Utf8>, nullable

Token rules for `facts_devices.device_type` and `branches.device_type`:

- Canonical SmartValve token: `smartvalve`.
- Accepted ingestion alias: `SV` (case-insensitive).
- Writers must emit canonical `smartvalve`.
- Readers must normalize `SV` to `smartvalve`.

### facts_solved

- `device_id`: Dictionary<Int32, Utf8>, required
- `effective_x_pu`: Float64, nullable
- `injected_voltage_mag_pu`: Float64, nullable
- `injected_voltage_angle_deg`: Float64, nullable
- `p_effect_mw`: Float64, nullable
- `q_effect_mvar`: Float64, nullable
- `status`: Boolean, nullable

Solved presence convention (v0.8.6+):

- `rpf.facts_solved_state_presence = actual_solved` when `facts_solved` is emitted.
- `rpf.facts_solved_state_presence = not_available` when `facts_devices` is emitted but solved replay values are not present.

### scenario_context (optional, v0.9.0+)

Stores structured context for flagged or exported analysis cases. This table is optional — present in analysis exports, absent in standard planning files.

- `scenario_context_id`: Int32, required — primary key
- `case_id`: Utf8, required — links to `metadata.case_fingerprint`
- `source_type`: Utf8, required — `"real_time"` | `"hour_ahead_advisory"` | `"planning_study"`
- `priority`: Utf8, required — `"critical"` | `"high"` | `"medium"` | `"low"`
- `violation_type`: Utf8, nullable — e.g. `"voltage_collapse"`, `"q_limit_infeasible"`, `"unrecoverable_n2"`, `"limit_violation"`
- `nerc_recovery_status`: Utf8, nullable — `"recoverable_15min_lte"` | `"not_recoverable"` | `"unknown"`
- `recovery_time_min`: Float64, nullable — estimated recovery time in minutes
- `cleared_by_reserves`: Boolean, nullable — true if cleared by greedy reserve dispatch
- `planning_feedback_flag`: Boolean, required — true if this case should trigger a planning study review
- `planning_assumption_violated`: Utf8, nullable — description of the violated planning assumption
- `recommended_action`: Utf8, nullable — operator-readable recommended corrective action
- `investigation_summary`: Utf8, nullable — analysis narrative
- `load_forecast_error_pct`: Float64, nullable — forecast error contribution for hour-ahead cases
- `created_timestamp_utc`: Utf8, required — ISO 8601 UTC timestamp when this context record was created
- `params`: Map<String, Float64>, nullable — extensible key/value parameters

Schema-level example: parallel PST + SmartValve on one corridor

- `transformers_2w` row carries the PST tap/phase state for the physical transformer branch.
- `branches` row for the same electrical corridor may carry additive FACTS metadata (`device_type=smartvalve`, control/limits fields).
- `facts_devices` carries the authoritative device identity, linkage (`branch_id` or `bus_id`), and richer control metadata.
- `facts_solved` (when present) carries solved replay outputs (`effective_x_pu`, injected voltage, effective P/Q impact).
- Loaders should treat PST and SmartValve effects as composable controls on the same path, not mutually exclusive equipment classes.

### protection_contingencies (optional, v0.11.0+)

Captures protection-driven contingencies (breaker failure, bus-differential lockout, transfer
trip, automatic sequences, etc.) using a layered model: a logical protection-group baseline
that works on bus-branch data, plus optional breaker-level refinement. One row per protection
event, keyed to a `contingencies.contingency_id`. Present in EMS / operations exports; absent
in standard planning files. See [adr/0001-protection-informed-contingencies.md](adr/0001-protection-informed-contingencies.md).

- `contingency_id`: Dictionary<Int32,Utf8>, required — FK to `contingencies.contingency_id`
- `protection_group_id`: Dictionary<Int32,Utf8>, required — stable identity of the protection scheme/group
- `name`: Utf8, nullable — human-readable label
- `scheme_type`: Dictionary<Int32,Utf8>, required — open vocabulary; recommended tokens: `breaker_failure`, `stuck_breaker`, `relay_misoperation`, `bus_differential`, `zone_protection`, `line_protection`, `transfer_trip`, `sympathetic_trip`, `auto_reclose`. Consumers must tolerate unknown tokens.
- `initiating_equipment_kind`: Dictionary<Int32,Utf8>, nullable — kind of the fault/trigger element
- `initiating_equipment_id`: Dictionary<Int32,Utf8>, nullable — id of the fault/trigger element
- `tripped_elements`: List<Struct>, required — the resulting outage set; **identical struct shape** to `contingencies.elements` (see that section)
- `sequence`: List<Struct>, nullable — automatic sequence ordering/timing; fields: `step` (Int32, required), `delay_ms` (Float64, nullable), `equipment_kind` (Dictionary<Int32,Utf8>, nullable), `equipment_id` (Dictionary<Int32,Utf8>, nullable)
- `topology_change_id`: Int32, nullable — FK to `topology_changes.topology_change_id`
- `data_confidence`: Dictionary<Int32,Utf8>, required — `modeled` | `inferred` | `assumed`; producer honesty about the outage set
- `breaker_ids`: List<Utf8>, nullable — optional breaker-level refinement; references `switch_detail.switch_id` / `node_breaker_detail.switch_id`
- `params`: Map<String,Float64>, nullable — extensible scalar parameters

### topology_changes (optional, v0.11.0+)

One row per resulting topology delta produced by a contingency (typically a protection event).

- `topology_change_id`: Int32, required — primary key
- `contingency_id`: Dictionary<Int32,Utf8>, nullable — contingency that produced the change
- `change_type`: Dictionary<Int32,Utf8>, required — open vocabulary; recommended tokens: `bus_split`, `island_formation`, `substation_isolation`, `partial_isolation`, `element_isolation`
- `affected_bus_ids`: List<Int32>, required — buses involved in the change
- `resulting_islands`: List<Struct>, nullable — islands formed; fields: `island_index` (Int32, required), `bus_ids` (List<Int32>, required), `energized` (Boolean, nullable)
- `isolated_element_count`: Int32, nullable — count of de-energized elements
- `summary`: Utf8, nullable — operator-readable narrative
- `provenance`: Dictionary<Int32,Utf8>, nullable — `declared` (planning intent; Phase 0) | `solved` (solver-derived; future)
- `params`: Map<String,Float64>, nullable — extensible scalar parameters
- `change_source`: Dictionary<Int32,Utf8>, nullable (v0.12.3+) — why the change was made; e.g. `SAL_CIM_Upgrade`, `Model_Alignment`
- `applied_phase`: Dictionary<Int32,Utf8>, nullable (v0.12.3+) — when/which upgrade phase applied it; e.g. `Jan_to_June_Baseline`, `Planning_Study_Prep`

Referential integrity: every non-null `protection_contingencies.topology_change_id` must resolve
to a `topology_changes.topology_change_id` (enforced by `validate_rpf_file()` when both tables
are emitted).

### connectivity_groups

- `topological_bus_id`: Int32, required
- `topological_node_mrid`: Dictionary<Int32, Utf8>, required
- `connectivity_node_mrids`: List<Utf8>, required
- `connectivity_count`: Int32, required

### node_breaker_detail

- `switch_id`: Dictionary<Int32, Utf8>, required
- `switch_type`: Dictionary<Int32, Utf8>, required
- `from_bus_id`: Int32, nullable
- `to_bus_id`: Int32, nullable
- `connectivity_node_a`: Dictionary<Int32, Utf8>, nullable
- `connectivity_node_b`: Dictionary<Int32, Utf8>, nullable
- `is_open`: Boolean, nullable
- `normal_open`: Boolean, nullable
- `status`: Boolean, nullable

### switch_detail

- `switch_id`: Dictionary<Int32, Utf8>, required
- `name`: Dictionary<UInt32, Utf8>, nullable
- `switch_type`: Dictionary<Int32, Utf8>, required
- `is_open`: Boolean, nullable
- `normal_open`: Boolean, nullable
- `retained`: Boolean, nullable

### connectivity_nodes

- `connectivity_node_mrid`: Dictionary<Int32, Utf8>, required
- `topological_node_mrid`: Dictionary<Int32, Utf8>, nullable
- `bus_id`: Int32, nullable

## Optional Tables: diagram_objects and diagram_points

RPF v0.8.0 includes two optional Arrow tables for persisted one-line layout, aligned with IEC 61970-453 `DiagramObject` and `DiagramObjectPoint`. These tables are intended for viewer/editor workflows and are additive only: when absent, downstream tools may synthesize layout at runtime; when present, tools should restore the saved layout exactly. The payload is carried inside the standard Apache Arrow IPC `.rpf` root container and may be derived from CGMES RDF/XML diagram layout content commonly exchanged under IEC 61970-501 CGMES profile sets.

The two tables must be present together or both absent. A file with `diagram_objects` but no `diagram_points`, or vice versa, is malformed.

### diagram_objects

- `element_id`: Utf8, required. RPF-resolved layout key in namespaced form such as `bus:1`, `branch:1`, `generator:G1`, `fixed_shunt:SH1`, `breaker:BR1`, or `connectivity_node:CN1`.
- `element_type`: Utf8, required. Allowed values currently emitted by this writer include `bus`, `branch`, `generator`, `load`, `fixed_shunt`, `breaker`, and `connectivity_node`.
- `diagram_id`: Utf8, required. Named diagram view aligned with `cim:Diagram.name`; writers should prefer `overview` for the full-system one-line and use area/substation names for detail views.
- `rotation`: Float32, nullable. Clockwise rotation in degrees; null should be interpreted as zero.
- `visible`: Boolean, required. Whether the element is visible in the named diagram.
- `draw_order`: Int32, nullable. Z-order / drawing order; null should be interpreted as zero.

### diagram_points

- `element_id`: Utf8, required. Foreign key to `diagram_objects.element_id`.
- `diagram_id`: Utf8, required. Foreign key to `diagram_objects.diagram_id`.
- `seq`: Int32, required. Point ordering key aligned with IEC 61970-453 `DiagramObjectPoint.sequenceNumber`.
- `x`: Float64, required. Viewer-space X coordinate.
- `y`: Float64, required. Viewer-space Y coordinate.

### seq conventions by element_type

- `bus`: `seq=0` left endpoint of the bus bar, `seq=1` right endpoint.
- `branch`: `seq=0` from-end terminal, `seq=N` to-end terminal, intermediate values are bend vertices.
- `generator`, `load`, `fixed_shunt`: `seq=0` symbol center or connection point.
- `breaker`: `seq=0` terminal-1 side, `seq=1` terminal-2 side.
- `connectivity_node`: `seq=0` connection point.

### Coordinate convention

IEC 61970-453 uses an inverted-Y convention where larger Y values are lower on screen. Writers store the raw CIM values unchanged in `diagram_points.y`; renderers using a standard screen-space coordinate system should invert Y during display.

### Standard alignment and version

- Standard alignment: IEC 61970-453 `Diagram`, `DiagramObject`, and `DiagramObjectPoint`
- Exchange context: IEC 61970-501 CGMES RDF/XML profile sets, including merged datasets that carry diagram layout payloads
- Container format: Apache Arrow columnar IPC file layout already used by `.rpf`
- Introduced in: RPF v0.8.0

## Optional Tables: buses_solved, generators_solved, switched_shunts_solved

These tables are emitted when:

- `case_mode = solved_snapshot` (v0.8.4+/v0.8.5+) — full post-solve payload, all three tables populated; or
- `case_mode = warm_start_planning` with `solved_state_presence = seed_only` (v0.9.6+) — warm-start initial conditions in `buses_solved` only (`v_mag_pu` / `v_ang_deg` copied from the source case); `generators_solved` and `switched_shunts_solved` are emitted as zero-row, structurally valid placeholders.

When `case_mode = flat_start_planning` (or `warm_start_planning` without `seed_only`), all three tables must be absent.

### buses_solved

- `bus_id`: Int32, non-null — FK into `buses`.
- `v_mag_pu`: Float64, nullable — post-solve voltage magnitude in per-unit.
- `v_ang_deg`: Float64, nullable — post-solve voltage angle in degrees.
- `p_inj_pu`: Float64, nullable — net active power injection in per-unit.
- `q_inj_pu`: Float64, nullable — net reactive power injection in per-unit.
- `bus_type_solved`: Int8, nullable — effective bus type after convergence: 1=PQ, 2=PV, 3=slack.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

### generators_solved

- `bus_id`: Int32, non-null — FK into `generators`.
- `id`: Dictionary<Int32, Utf8>, non-null — FK into `generators`.
- `p_actual_pu`: Float64, nullable — post-solve real power output in per-unit.
- `q_actual_pu`: Float64, nullable — post-solve reactive power output in per-unit.
- `p_mw`: Float64, nullable — actual real power in MW (`= p_actual_pu × base_mva`); solver-native unit convenience (v0.8.5+).
- `q_mvar`: Float64, nullable — actual reactive power in MVAR (`= q_actual_pu × base_mva`) (v0.8.5+).
- `status`: Boolean, nullable — in-service status at solve time (v0.8.5+).
- `pv_to_pq`: Boolean, nullable — true when this unit's bus switched PV→PQ during solve.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

### switched_shunts_solved

Emitted only when `solved_shunt_state_presence = actual_solved` (v0.8.5+).

- `bus_id`: Int32, non-null — FK into `switched_shunts`.
- `shunt_id`: Dictionary<Int32, Utf8>, nullable — FK into `switched_shunts.shunt_id`.
- `current_step_solved`: Int32, nullable — energized step after convergence (1-indexed).
- `b_pu_solved`: Float64, nullable — post-solve total susceptance in per-unit.
- `provenance`: Dictionary<Int32, Utf8>, nullable.

## Blocker Fixes Incorporated in Locked contract: v0.7.1

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
- `protection_event` (v0.11.0+) — marks a contingency whose protection detail lives in the matching `protection_contingencies` row (joined on `contingency_id`)

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
contingency analysis without changing core Locked contract: v0.7.0 table schemas.

### 7) `split_bus` contingency stub element

`contingencies.elements.element_type` now also permits:

- `split_bus`

Current writer behavior is stub-only (no breaker-status parsing yet). Stub
payload encodes:

- `topological_node_id`
- `connectivity_node_a`
- `connectivity_node_b`
- `breaker_mrid` (`stub` placeholder)

These values are serialized in the additive `equipment_kind` and `equipment_id`
fields to preserve strict Locked contract: v0.7.0 field layout while giving
switch and split-bus workflows a stable generic equipment identifier.

Current writer behavior for contingencies is hybrid:

- prefers switch-derived contingency rows when switch/open-state payloads are present
- emits `split_bus` placeholder elements only when split-bus topology hints are present
- emits `raptrix.features.contingencies_stub=true` only when placeholder contingency rows are present in the file

### 8) Optional node-breaker detail tables (opt-in only)

Locked contract: v0.7.0 adds optional node-breaker detail tables (`node_breaker_detail`, `switch_detail`, and `connectivity_nodes`) for operational CGMES fidelity and viewer workflows while preserving the strict core solver path. These tables are emitted only when explicitly requested with `--node-breaker` and are advertised in `.rpf` file-level Arrow IPC metadata with `raptrix.features.node_breaker=true`, so default power-flow ingest remains core tables only and preserves zero-copy performance semantics end-to-end (memory-mapped Arrow IPC to Arrow arrays with no additional allocations or copies on the default path).

## Parser Author Checklist

An independent parser is considered compliant if it:

1. Opens `.rpf` as Arrow IPC File format.
2. Verifies `raptrix.version` is in the set of supported contract versions (current: `0.12.3` / `v0.12.3`; retains `0.12.2` / `v0.12.2` and `0.12.1` / `v0.12.1`).
3. Verifies required root columns appear in canonical order.
4. Uses `rpf.rows.<table_name>` metadata to trim padded null tails.
5. Treats the 15 required root columns as mandatory even when their logical row counts are zero.
6. Detects optional tables by root column presence and feature metadata, not by guesswork.
7. Ignores unknown future trailing root columns for forward compatibility.
8. Reads and validates `rpf.case_mode` (required since v0.8.4): must be `flat_start_planning`, `warm_start_planning`, `solved_snapshot`, or `hour_ahead_advisory` (v0.9.0+).
9. When `case_mode = solved_snapshot`: expects `rpf.solved_state_presence = actual_solved` and treats `buses_solved` and `generators_solved` as required; treats `switched_shunts_solved` as required when `rpf.solver.solved_shunt_state_presence = actual_solved`.
10. When `case_mode = warm_start_planning` with `rpf.solved_state_presence = seed_only` (v0.9.6+): expects a populated `buses_solved` table carrying warm-start initial conditions; `generators_solved` and `switched_shunts_solved` may be present as zero-row placeholders or absent. Solver provenance keys (`rpf.solver.*`) MUST be absent or null.
11. When `case_mode = flat_start_planning` (or `warm_start_planning` without `seed_only`): treats `buses_solved`, `generators_solved`, and `switched_shunts_solved` as absent; if found, the file is malformed.
11. Reads solver provenance keys (`rpf.solver.*`) only when `solved_state_presence = actual_solved`; ignores them otherwise.
12. When `rpf.solver.solved_shunt_state_presence = not_available`: warns that switched-shunt solved state is absent; does not fail (v0.8.5+).
13. When `facts_devices.device_type` or `branches.device_type` contains `SV` (case-insensitive), canonicalizes to `smartvalve`.
14. Treats `facts_devices` and `facts_solved` as optional additive tables; if `rpf.facts_solved_state_presence = actual_solved`, expects `facts_solved` to be present.
15. Treats `protection_contingencies` and `topology_changes` as optional additive tables (v0.11.0+) detected by `raptrix.features.protection_contingencies` / `raptrix.features.topology_changes`; when both are present, expects every non-null `protection_contingencies.topology_change_id` to resolve to a `topology_changes.topology_change_id`. Tolerates unknown `scheme_type` / `change_type` tokens.
16. Treats `remedial_action_schemes` as the canonical optional v0.12 RAS schema when `raptrix.features.remedial_action_schemes=true` and `rpf.ras.schema_mode=canonical_v12`; expects this table to be used for new-write RAS semantics.
17. Treats `contingency_island_analysis` as an optional additive table (v0.12.1+) detected by `raptrix.features.contingency_island_analysis`; each row keys to `contingencies.contingency_id`.

For a plain-English explanation of all fields see [rpf-field-guide.md](rpf-field-guide.md).

## Compatibility Rules

- Additive columns should be appended and documented.
- Renaming or reordering columns is breaking.
- Removing columns is breaking.
- Type widening or narrowing is breaking unless consumers are migrated in lockstep.

## Change Checklist

1. Update `raptrix-cim-arrow/src/schema.rs` and any affected helpers in `raptrix-cim-arrow/src/io.rs`.
2. Update this file with version and column docs.
3. Add or update test coverage for schema construction and writer outputs.
4. Update README capability and known-limits sections.

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power

