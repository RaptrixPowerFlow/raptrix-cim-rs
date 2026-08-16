<!--
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
-->

# ADR 0002: RPF v0.14.0 Funnel Portability and Identity Honesty

- **Status**: Accepted
- **Target contract**: RPF v0.14.0 (MINOR — additive columns + one optional table)
- **Scope**: `raptrix-cim-rs` / `raptrix-cim-arrow` wire contract and authoring builders.
- **Amends**: [0001-protection-informed-contingencies.md](0001-protection-informed-contingencies.md) (core consumption of `tripped_elements` as reserved simultaneous).

## 1. Context

v0.13.x already represents simultaneous multi-element outages (`contingencies.elements`) and
protection-driven N-k (`protection_contingencies.tripped_elements`). Sequential N-1-1
(TPL P3/P6: primary → intervening adjustment → secondary) has no in-file shape. Generator
identity is split across an Int32 PK (`generators.generator_id`) and string labels
(`dynamics_models.gen_id`, `generators_solved.id`, `contingencies.elements.gen_id`), and
docs still refer to a non-existent `generators.id`.

This ADR ships a contained cleanup + additive pass. It is not a redesign of the electrical
core, required-table set, hybrid identity model, RAS table, or CLP surface.

## 2. Decision

### 2.1 Application inference (no `application_mode`)

| Shape | Funnel track |
| --- | --- |
| 1 element | N-1 parent (and sequential secondary **by id pair**) |
| 2+ elements | Simultaneous application (tower / common-mode / P7-shaped) |
| `protection_contingencies` row | Reserved simultaneous; apply `tripped_elements` |
| `contingency_sequences` row or study-JSON pair | Sequential N-1-1 |

`protection_contingencies.sequence` (`delay_ms`) is millisecond-scale protection clearing.
It is **not** a P3/P6 intervening window.

### 2.2 Wire additives (v0.14.0)

Trailing nullable columns on the required `contingencies` table:

| Column | Type | Meaning |
| --- | --- | --- |
| `tpl_category` | Dictionary\<Int32, Utf8\>, nullable | Optional NERC-oriented annotation. Closed set: `P1`…`P7` / `unspecified`. **Null = untagged, not invalid.** Structural meaning stays element count / protection / sequences. |
| `reserved` | Boolean, nullable | `true` = never-trim; `false` = not reserved; **null** = infer from protection table / study list |

Optional root table `contingency_sequences` (feature flag
`raptrix.features.contingency_sequences`):

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `sequence_id` | Dictionary\<Int32, Utf8\> | required | Stable id |
| `primary_contingency_id` | Dictionary\<Int32, Utf8\> | required | FK → `contingencies.contingency_id` |
| `secondary_contingency_id` | Dictionary\<Int32, Utf8\> | required | FK → `contingencies.contingency_id` |
| `intervening_window_min` | Int32 | nullable | Adjustment window; null = consumer default |
| `tpl_category` | Dictionary\<Int32, Utf8\> | nullable | Usually `P3` or `P6` |
| `provenance` | Dictionary\<Int32, Utf8\> | nullable | `planner_file` \| `ems_export` \| `rpf` \| `autonomous` |

Sequence endpoints **should** be single-element contingencies. Writers do **not** hard-fail
multi-element ends: a multi-element endpoint is simultaneous physics and is rare.

v0.14 writers **may omit** the sequences table entirely. CIM converters leave the new
contingency columns null and do not invent protection or sequence rows.

### 2.3 Dual-read

Readers accept `v0.14.0` / `0.14.0` and retain `v0.13.1` / `0.13.1` / `v0.13.0` / `0.13.0`.
Pre-0.13 remains rejected.

0.13.x files load as 0.14 with `tpl_category` / `reserved` **null** (trailing-column pad).
The `contingency_sequences` table **may be absent**. 0.14 writers may omit sequences.

### 2.4 Generator identity (docs + write alias; no nested FK)

- Machine PK: `generators.generator_id` (Int32). There is no `generators.id` on the wire.
- String label used by `dynamics_models.gen_id`, `contingencies.elements.gen_id`, and
  `generators_solved.id`: prefer `generators.mrid` when present; else a deterministic
  synthetic such as `"{bus_id}:{name}"`.
- Apply path: writers that can resolve Int32 also set `equipment_kind=generator` and
  `equipment_id` to the decimal `generator_id` string.
- Canonical element token: **`gen_trip`**. Accept `generator_trip` as a reader alias;
  normalize to `gen_trip` on write.

No `elements.generator_id` Int32 is added in 0.14 (nested-struct dual-read cost).

### 2.5 Outcome columns stay on the definition table

`risk_score`, `cleared_by_reserves`, `voltage_collapse_flag`, `recovery_possible`,
`recovery_time_min`, `greedy_reserve_summary` are **analysis-only**. Planning /
interchange files: always null. Analysis exports may populate them.
`scenario_context` remains the structured ops→planning path. No `contingency_results`
table in 0.14.

### 2.6 Defaults

- Leaf `hierarchy_level` token: `unit`.
- `buses.p_min_agg` / `p_max_agg`: `0` when unknown / not aggregated.
- Prefer `case_mode` over `is_planning_case` in documentation; keep the boolean.

## 3. Non-goals

- Nested `elements.generator_id` Int32
- `application_mode`
- Separate `contingency_results` multi-run table
- Richer `interfaces` ratings / SOL
- Node-breaker auto-expansion of stuck-breaker zones
- Demoting required tables; removing `is_planning_case` or outcome columns
- Merging protection outage sets into `remedial_action_schemes`
- PSS/E-shaped required fields
- Claiming TPL-001 Table 1 compliance from the schema alone

## 4. Downstream handshake

- **raptrix-core:** PR-D first on existing `protection_contingencies.tripped_elements` +
  study-JSON pairs (works on v0.13.1). `reserved` / `contingency_sequences` are additive
  after 0.14. PR-G `study_profile` may read `tpl_category`; still no “TPL compliant”
  claim from the file alone.
- **raptrix-studio / raptrix-psse-rs:** bump to crate 0.7.0 when they **write** the new
  fields. Dual-read of 0.13.x does not require a writer bump.

## 5. Consequences

- Partners get a published gen-key join rule and a portable sequential-pair table
  without a breaking clean cut.
- 0.13.x case libraries remain readable without re-export.
- Funnel sequential physics remains a study procedure until producers populate
  `contingency_sequences` or study JSON.

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
