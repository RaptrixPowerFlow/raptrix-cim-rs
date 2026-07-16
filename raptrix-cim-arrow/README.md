# raptrix-cim-arrow

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power

Part of the Raptrix Powerflow ecosystem.

This crate supports the shared open converter suite published at [RaptrixPowerFlow](https://github.com/RaptrixPowerFlow/).

Copyright (c) 2026 Raptrix Power

`raptrix-cim-arrow` is the shared crate for the locked Raptrix Power Interchange (`.rpf`) contract.

It owns:

- canonical Arrow schema definitions
- metadata and branding constants
- deterministic table ordering and lookup helpers
- generic Arrow IPC `.rpf` root-file assembly
- patch-based re-export (`apply_rpf_patch` / FFI `apply_rpf_patch_c`) so solvers can overlay `*_solved` tables without dropping converter-owned enrichment (GIS, contingencies, RAS, diagrams, …)
- generic `.rpf` readback, summary, and metadata inspection helpers

It does not parse CIM, PSS/E, or any other source format. Upstream converter crates are expected to map source formats into canonical Arrow `RecordBatch` values and then call the shared writer helpers from this crate. Table ownership for solve→re-export is documented in `docs/schema-contract.md`.

