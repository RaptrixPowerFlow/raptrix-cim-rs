# RPF case health model

The case-health inspector lives in `raptrix-cim-arrow` (`inspect_rpf_case`, `inspect_rpf_file`) and provides a **read-only**, **deterministic** view of an existing `.rpf` file. It does not modify the schema or require new tables.

## API

| Symbol | Role |
|--------|------|
| `RpfTables` | In-memory model: `HashMap` of canonical table batches + file-level metadata (spec name `RpfNetwork` refers to this) |
| `inspect_rpf_case(&RpfTables)` | Core inspection |
| `inspect_rpf_file(path)` | `read_rpf_tables` + `rpf_file_metadata` + inspect |
| `RpfCaseHealth` | Full result: grade, reasons, counts, aggregates, topology, convergence |
| `format_health_report` | Human-readable summary for CLI and logs |

CLI: `raptrix-cim-rs view --input case.rpf --health`

## Grades

| Grade | Meaning |
|-------|---------|
| `Healthy` | No elevated rules fired (informational reason only) |
| `Caution` | Large case, many taps/shunts, planning-only export, weak initial voltages, partial ZIP, etc. |
| `Stressed` | Detached active network islands, heavy Q-limit pressure, many buses out of voltage band, high PV→PQ counts |
| `Pathological` | Detached islands with active load/gen, topology-only with material injection, fragmented islands, solved-metadata contradictions |

Rules are evaluated in severity order; `grade` is the maximum severity among triggered rules. `reasons` lists all triggered rules (most severe first).

### Calibration thresholds

Constants in `raptrix-cim-arrow/src/health/mod.rs` are tuned against:

- **IEEE 14** — small, should stay `Healthy` / low bus count
- **Texas2k** (~2.7k buses) — `LARGE_CASE_BUS_THRESHOLD = 1500` triggers `Caution` for size
- **NYISO** (~1.5k buses) — same size threshold; many taps/shunts may add further `Caution` reasons

Adjust thresholds only with corresponding updates to `raptrix-cim-arrow/tests/case_health.rs` and PSS/E golden RPFs under `raptrix-psse-rs/tests/golden`.

## Aggregates

`RpfCaseAggregates` fields:

- **Load/gen totals** — in-service rows only; load P/Q from `loads.p_pu` / `q_pu` × `base_mva`; gen from `p_sched_mw` / `q_sched_mvar`.
- **Reserve P/Q** — per in-service generator: sum of `max(0, p_max - p_sched)`, `max(0, p_sched - p_min)`, and the Q analogs using `q_*_mvar` columns.
- **`reactive_support_headroom_mvar`** — per in-service generator: `max(q_max - q_sched, q_sched - q_min)` (headroom to the nearer Q limit), summed over units.

## Topology

Prefer `rpf.topology.*` file metadata when present; otherwise recompute islands from in-service branches and transformers (same graph rules as CGMES export in `rpf_writer.rs`).

## Convergence hints

Every field in `RpfConvergenceHints` is `Option` and is set **only when the underlying data exists**:

- Solver provenance from metadata / `metadata` table when written
- `initial_mismatch_rms` only when both `buses` and non-empty `buses_solved` are present
- `q_violation_count` only when the `generators` table has rows
- `pv_to_pq_from_generators_solved` only when `generators_solved` has rows
- `contraction_ratio_first_step` / `stall_or_oscillation` only when a future writer stores them in `custom_metadata` or `scenario_context.params` (not inferred)

`RpfSolverHints` (profile, `max_q_switch_per_iter`, etc.) are deferred to a follow-up PR after `raptrix-core` profile names are fixed.

## Tests

- `tests/case_health.rs` — CGMES fixture export → RPF → inspect
- `raptrix-cim-arrow/tests/case_health.rs` — optional golden RPFs via `RAPTRIX_PSSE_GOLDEN_DIR` (default: `../raptrix-psse-rs/tests/golden`)

## Future work

- Persist signed health snapshot in `metadata.custom_metadata` at export time (e.g. PSS/E build) to skip re-scan
- `RpfSolverHints` for `raptrix-core` default profile and escalation
