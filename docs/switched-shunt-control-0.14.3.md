<!--
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
-->

# Proposal: switched-shunt control on RPF (shape of a v0.14.3 cut)

**Status: proposal. Not locked. Live contract remains v0.14.2.**

This is the contract note for an additive trailing cut on `switched_shunts`. It is **not** a request to stamp `v0.14.3` and push every writer today. Implement only after this note is accepted. Dual-read of v0.14.2 is mandatory.

The goal is **alignment and reproducibility**: a PSS/E RAW and the converter `.rpf` of the same case must carry the same switched-shunt control model, so independent engines and Raptrix see one network, not two. Today they do not, because the mode never leaves the parser.

PSS/E already parses `MODSW` and `SWREG`/`SWREM` and then writes `*(not stored)*` (`raptrix-psse-rs` `docs/psse-mapping.md` §17). That is a **wire hole**. Downstream readers that were not given the mode reconstruct locked / discrete-V / continuous-V from N/B bank shape. Independent lab inventory (2026-09-08, RAW offsets v33/v34 vs v35):

| Case | PSS/E rev | n shunts | MODSW | N/B shape ≠ MODSW | Remote SWREG |
| --- | --- | --- | --- | --- | --- |
| Memphis Mar7 | 35 | 61 | all 2 (continuous V) | 3 (buses 55, 76, 638) | 2 (575→88, 731→20) |
| Texas2k series241 | 34 | 153 | 152×2, **1×3** | 3 (1007, 1010, and the MODSW=3 row at 2127) | 0 |
| NYISO off-peak 2019 | 33 | 33 | all 2 | 4 | 1 (621→627) |
| ACTIVSg10k | 33 | 387 | 386×1 (discrete V), **1×3** | 8 | 2 |
| IEEE 14 | 33 | 0 | — | — | — |

The N/B heuristic (`one N=1 block → continuous-V, else discrete-V`) is wrong whenever MODSW=2 on a stepped bank (Memphis 76 is N=5, MODSW=2) or MODSW=3 (discrete Q, not a voltage mode). That is a **model-identity** miss, not a reason to chase a single worst-bus |dV| against another engine.

`metadata.default_shunt_control_mode` / `rpf.default_shunt_control_mode` (`planning_full` \| `real_time_hot_start` \| `real_time_frozen`) is a **file-level solver SVD loop** knob. Per-device MODSW is “is this bank locked, discrete-V, or continuous-V.” Downstream (raptrix-core, Studio, other RPF readers) must not conflate them.

## Proposed columns (additive, trailing, nullable)

On `switched_shunts` only:

| Column | Type | Meaning |
| --- | --- | --- |
| `shunt_control_mode` | Dictionary\<Int32, Utf8\>, nullable | Per-device bank mode. **Not** `branches.control_mode`. **Not** `tap_control_mode`. |
| `regulated_bus_id` | Int32, nullable | RAW `SWREG` / `SWREM` (dense `bus_id`). **null = local.** Do not write `0`. |

Name `shunt_control_mode` is table-scoped on purpose. `branches.control_mode` is FACTS Utf8. `tap_control_mode` is RAW COD Int32. 0.14.2 already warns: do not join those. A third generic `control_mode` on shunts will get joined by accident.

`regulated_bus_id` matches `transformers_2w/3w.regulated_bus_id` (CONT). Generators already shipped `controlled_bus_id` for IREG; do not rename that column. Document the table: shunts use `regulated_bus_id`.

Remote shunt is rarer than MODSW. Do not block a voltage-mode cut on SWREG if the defect is the missing mode; still prefer to land both in one additive cut because the parser already has the field.

## Dictionary tokens (voltage modes only)

Allowed non-null values:

| Token | PSS/E `MODSW` | Meaning |
| --- | --- | --- |
| `locked` | 0 | Bank does not switch. |
| `discrete_voltage` | 1 | Stepped voltage control. |
| `continuous_voltage` | 2 | Continuous voltage control. |

**MODSW is not a 3-value enum.** Texas2k bus 2127 and ACTIVSg10k bus 20387 are `MODSW=3` (discrete reactive; VSWHI/VSWLO used as a Q band on ACTIVSg). A 0.14.3 that only names voltage modes must say so:

- Writer (PSS/E): map 0/1/2 as above. **Any other code → null + warning** (`unknown_modsw`, include I and the integer). Do not invent `discrete_reactive` in 0.14.3 unless we are ready to specify Q-band units.
- Reader: null means “no per-device mode on the wire.” Keep today’s N/B heuristic or treat as unspecified. Do not assume `locked`.

Do not store raw MODSW 0/1/2 as Int32. That paints a 0.14.4 when Q modes arrive.

## Who fills what

| Writer | `shunt_control_mode` | `regulated_bus_id` |
| --- | --- | --- |
| **raptrix-psse-rs** | From MODSW 0/1/2; else null + warning | From SWREG/SWREM; 0 or I → **null** (local) |
| **raptrix-pslf-rs** | **null** (EPC SVD is stepped banks + a voltage band; do not invent PSS/E MODSW) | null unless EPC has an honest remote-bus field |
| **raptrix-cim-rs** CIM path | null unless a `RegulatingControl` join is honest (same bar as tap control) | null unless the CIM regulating node join is honest |
| Dual-read **v0.14.2** | column absent → null | column absent → null |

Do not force PSLF to populate either column. “Update pslf-rs to use the new schema” means pad nulls and dual-read.

## Dual-read / blast radius

Same pattern as 0.14.2 tap columns:

1. `raptrix-cim-arrow` schema + `SUPPORTED_RPF_VERSIONS` (accept `v0.14.3`, retain 0.14.2…0.13.0).
2. `raptrix-psse-rs` emit + golden regen.
3. `raptrix-pslf-rs` pad + smoke.
4. `raptrix-cim-rs` CIM writer null.
5. raptrix-core, Studio, and other RPF readers dual-read: null → keep today’s heuristic.

That is a coordinated cut, not three git tags.

## Out of scope (do not mix into this cut)

- Dynamics, ZIP, IBR, HVDC, CTG, and other tables this revision does not add.
- File-level `default_shunt_control_mode`.
- Using another engine’s solved voltages as Raptrix goldens.
