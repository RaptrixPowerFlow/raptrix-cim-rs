# raptrix-cim-arrow

Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix PowerFlow

Part of the Raptrix Powerflow ecosystem.

This crate supports the shared open converter suite published at [RaptrixPowerFlow](https://github.com/RaptrixPowerFlow/).

Copyright (c) 2026 Raptrix PowerFlow

`raptrix-cim-arrow` is the shared crate for the locked Raptrix PowerFlow Interchange (`.rpf`) contract.

It owns:

- canonical Arrow schema definitions
- metadata and branding constants
- deterministic table ordering and lookup helpers
- generic Arrow IPC `.rpf` root-file assembly
- generic `.rpf` readback, summary, and metadata inspection helpers

It does not parse CIM, PSS/E, or any other source format. Upstream converter crates are expected to map source formats into canonical Arrow `RecordBatch` values and then call the shared writer helpers from this crate.

