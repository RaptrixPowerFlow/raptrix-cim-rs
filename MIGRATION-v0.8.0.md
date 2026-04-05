# Migration Guide: raptrix-cim-rs v0.8.0 (Schema Contract v0.8.0)

**Released**: April 5, 2026  
**Schema Version**: v0.8.0 (Breaking Change)  
**Crate Version**: raptrix-cim-arrow 0.2.1 | raptrix-cim-rs 0.2.1

---

## Executive Summary

raptrix-cim-rs now targets **CGMES v3.0 and later only**. Support for CGMES 2.4.x was dropped as a deliberate breaking change to align with current ENTSO-E production standards and eliminate legacy parsing complexity.

**For v0.7.1 → v0.8.0 migration**:
- ✅ If you use CGMES v3.0+: No action needed other than dependency update.
- ❌ If you use CGMES 2.4.x: Upgrade your test datasets to v3.0+ before updating.

---

## Why CGMES 3.0+ Only?

### Market Status (April 2026)
- **CGMES 3.0** is the current, mature, production-grade standard (IEC TS 61970-600-1, Ed. 1).
- **CGMES 2.4.15** is legacy — retained in a few older CGM processes but explicitly marked as obsolete in ENTSO-E documentation.
- **Vendor Adoption**: DIgSILENT PowerFactory 2024 SP5, Siemens PSS/ODMS, PowerInfo CIMdesk, DNV CIMbion, and others have official CGMES 3.0 Conformity Attestations (2024–2025).
- **Regulatory Push**: UK NESO mandates CGMES v3 for Planning Code exchanges (effective 2027).

### Technical Benefits
1. **Performance**: Eliminates dual-track heuristics, difference-file assembly logic, and conditional parsing branches.
2. **Simplicity**: Single-track XSD validation, no multi-pattern naming fallbacks, cleaner row mapping.
3. **Compliance**: 100% alignment with ENTSO-E Conformity Assessment Scheme (v3.0.3 current).
4. **Interoperability**: Downstream tools (GridCal, pandapower, PowerModels, etc.) already target v3.0+.

---

## What Changed

### Schema Contract v0.8.0

| Aspect | v0.7.1 | v0.8.0 |
|--------|--------|--------|
| **CGMES Support** | v2.4.x + v3.x (dual) | v3.0+ only (single) |
| **Profile Ingest** | Conditional pathways | Deterministic merged profiles |
| **Optional Tables** | Diagram layout (new) | Diagram layout (stable) |
| **Branding** | `v0.7.1` | **`v0.8.0` — CGMES 3.0+** |
| **Backward Compat** | N/A | Can read v0.7.1 RPF files |

### Code Changes

**Files Updated**:
- `raptrix-cim-arrow/src/schema.rs`: v0.8.0 constants, CGMES 3.0+ documentation.
- `tests/inspect_rpf.py`: Schema validator updated to v0.8.0.
- `docs/schema-contract.md`: Removed dual-track language, added v3.0+ clarity.
- `src/parser.rs`: DY profile parser now extracts numeric model parameters and normalizes keys.
- `src/rpf_writer.rs`: `dynamics_models` now records DY-linked rows plus EQ fallback coverage counters.
- `tests/generate_rpf_matrix.py`: Regression reports now aggregate dynamics row provenance (DY-linked vs EQ fallback).
- `docs/roadmap.md`: Removed v2.4.x dual-compatibility goal.
- `CHANGELOG.md`: Added v0.8.0 release notes with breaking change summary.

**No Removals from Public API**: The `table_schema()`, `all_table_schemas()`, and `diagram_layout_table_schemas()` functions remain unchanged. Only internal profile-detection heuristics and difference-file logic were removed.

---

## Migration Steps for Downstream Repos

### Step 1: Update Dependency

**In `raptrix-psse-rs`, `raptrix-studio`, and other dependents**:

```toml
# Cargo.toml
[dependencies]
raptrix-cim-arrow = { path = "../raptrix-cim-arrow", version = "0.2.1" }
```

Or, if using the registry:

```toml
raptrix-cim-arrow = "0.2.1"
raptrix-cim-rs = "0.2.1"
```

### Step 2: Update Test Data

**If your tests use CGMES 2.4.x**:

1. Migrate datasets to CGMES v3.0+ using official vendor converters (most provide free batch tools).
2. Update `RAPTRIX_TEST_DATA_ROOT` paths to point to v3.0+ merged profiles.
3. Re-run integration tests.

**If your tests already use CGMES v3.0+**:

- No test data changes needed. Re-run suite to confirm.

### Step 3: Update Documentation

In your repo README and docs, update compatibility claims:

**Before (v0.7.1)**:
```
Supports CGMES 2.4.x and 3.x profile sets (auto-detect).
```

**After (v0.8.0)**:
```
Supports CGMES v3.0+ (v3.0, v3.0.1, v3.0.2, v3.0.3) with complete merged profiles.
```

### Step 4: Verify & Test

```bash
cd /path/to/dependent/repo
export RAPTRIX_TEST_DATA_ROOT="path/to/cgmes/v3.0/test/data"
cargo build --release
cargo test
```

---

## v0.8.0 Feature Additions

### DY Semantics and Provenance (Top Priority)

`dynamics_models` now supports richer provenance for strict SSH/DY regression workflows:

- DY-linked rows include numeric parameters parsed from DY profile payload.
- Partial DY coverage keeps generator completeness by falling back to EQ-derived rows for unmatched generators.
- `params` may include provenance keys:
  - `source_dy = 1.0`
  - `source_eq_fallback = 1.0`
  - `source_stub = 1.0`

This keeps topology-first Studio workflows stable while enabling incremental dynamics fidelity upgrades.

### Studio Extensibility Beyond CIM

Studio can add new model families not currently represented in CIM without breaking the v0.8.0 contract:

- `dynamics_models.model_type` is an open string vocabulary.
- For custom models, use namespaced identifiers, for example:
  - `raptrix.smart_valve.v1`
- Store model parameters as numeric entries in `dynamics_models.params`, for example:
  - `raptrix.smart_valve.k_gain`
  - `raptrix.smart_valve.t_open_s`
  - `raptrix.smart_valve.t_close_s`

Downstream tools should treat unknown `model_type` values as extension payloads rather than errors.

### Diagram Layout Support (Continued from v0.7.1)

When the CGMES DL (Diagram Layout) profile is present, raptrix-cim-rs emits two optional tables:
- `diagram_objects`: Element references (buses, branches, transformers) with page assignment.
- `diagram_points`: Coordinate sequences for graphical rendering.

**Metadata flag**: `raptrix.features.diagram_layout=true` when tables are emitted.

**CLI Usage**:
```bash
cargo run --release -- convert \
  --eq SmallGrid_EQ.xml \
  --tp SmallGrid_TP.xml \
  --dl SmallGrid_DL.xml \
  --output output.rpf
```

---

## Backward Compatibility

### Can Read

- ✅ v0.8.0 RPF files
- ✅ v0.7.1 RPF files (backward compat reads)
- ✅ v0.7.0 RPF files (backward compat reads)

### Cannot Read

- ❌ v0.6.0 and earlier (core refactor; see `raptrix-core` for conversion)

### Downstream Writer Compatibility

Readers using `raptrix-cim-arrow` can conditionally skip unknown optional tables:

```rust
// Pseudo-code
match table_name {
    "diagram_objects" | "diagram_points" => {
        // Skip or delegate to DL handler
    }
    _ => { /* required table logic */ }
}
```

---

## FAQ

### Q: Can I still use CGMES 2.4.x datasets?

**A**: Not directly with v0.8.0+. You must:
1. Convert 2.4.x to 3.0+ using vendor tools (PowerFactory, PSS/ODMS, CIMdesk all support this).
2. Or pin to raptrix-cim-rs v0.7.1 if you cannot migrate datasets.

### Q: Is this a MAJOR version bump?

**A**: Schema contract v0.8.0 is a MINOR bump (feature + ingest target change). Downstream serialization format remains stable; new optional tables are additive. However, *ingest capability* changed (2.4.x → 3.0+), so it's a breaking change for repos using v2.4.

### Q: What if we find bugs in v0.8.0?

**A**: Report to the raptrix-cim-rs GitHub issues. Patches will be issued as v0.8.1, v0.8.2, etc. v0.7.1 remains available for backports if needed.

### Q: Can raptrix-core still read v0.8.0 RPF files?

**A**: Yes. raptrix-core's Arrow IPC reader is version-agnostic; it reads the schema from file metadata and adapts. Schema v0.8.0 is fully readable by v0.7.x readers (additive changes only).

---

## Contact & Support

- **GitHub Issues**: https://github.com/MustoTechnologies/raptrix-cim-rs
- **Documentation**: [docs/schema-contract.md](docs/schema-contract.md)
- **Release Sync Workflow**: [docs/release-sync-workflow.md](docs/release-sync-workflow.md)

---

## Timeline

| Date | Event |
|------|-------|
| **Apr 5, 2026** | v0.8.0 released. CGMES 2.4.x support dropped. |
| **Apr 5, 2026** | Downstream repos notified to update. |
| **Apr 12, 2026** | Recommended deadline for dependent repos to merge v0.8.0 updates. |
| **Apr 30, 2026** | Archive period for v0.7.1; community backport support ends. |

---

**Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC**  
**Copyright (c) 2026 Musto Technologies LLC**
