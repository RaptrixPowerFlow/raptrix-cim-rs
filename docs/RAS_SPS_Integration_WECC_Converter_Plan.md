# RAS/SPS Interchange (Public Summary)

This document summarizes the public, schema-level direction for remedial action scheme (RAS)
and system protection scheme (SPS) data in the open RPF contract. Detailed converter
implementation plans for proprietary solver products are maintained internally.

## Public scope (raptrix-cim-rs)

- **v0.12.0** adds optional canonical `remedial_action_schemes` for executable RAS/SPS
  sequences (arming/trigger conditions, ordered actions, and action targets).
- Legacy v0.11.0 `protection_contingencies` / `topology_changes` remain readable for
  migration; new writes should prefer the v0.12 canonical table.
- All RAS examples and fixtures in this public repository are **synthetic demonstration
  data only** — no CEII, utility identifiers, or protected topology.

## Out of scope for this public repository

- WECC- or utility-specific protection logic, proprietary solver execution semantics, and
  closed-product ingestion paths are not documented here.
- For production-grid RAS/SPS execution, contact Raptrix for commercial Sentinel and Forge
  integration options.

See also: `docs/schema-contract.md`, `docs/architecture.md`, and `CHANGELOG.md` (v0.12.0).
