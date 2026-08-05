# RPF v0.13.1 — Voltage Transfer Curve Research & Field Validation Matrix

**Status:** research freeze for additive CLP fields  
**Contract stamp:** `v0.13.1` (compatibility-extension exception; dual-read `v0.13.0`)  
**Audience:** schema / core / Sentinel implementers continuing Ashburn-class large-load work

---

## 1. Purpose

Close the gap that turns a routine fault into a multi-GW data-center transfer by adding **typed, optional** multi-stage `(V, t)` facility protection fields on `computational_load_profiles`, without breaking `v0.13.0` files.

This release is **PERC1-mappable facility protection screening**, not full PERC1-compliant dynamics. Full compliance requires the published PERC1 parameter set, filtered measurement, P/Q exponents and current limits, frequency path, cease/reconnect state machine, and cross-tool validation.

---

## 2. Evidence sources

| Source | Role |
|--------|------|
| Internal Ashburn cascade docs / Hungerford demo | Product baseline (scalar Phase D) |
| ITIC / CBEMA, SEMI F47, IEEE 1668 | Equipment **immunity test envelopes** — not site UPS settings |
| PowerWorld / WECC PERC1 docs (full param list) | Aggregate PE load cease/reconnect model |
| NERC Data Center Load Modeling TRD (draft), LMWG/LLTF | Modeling guidance; PERC2 backlog |
| NERC Incident Review (Jan 2025) — July 10 2024 Dominion event | Authoritative multi-reclose timing fixture |
| Public July 22 2026 Ashburn press | Confirms >3 GW transfer; **no** public clearing/reclose timing yet |
| UPS OEM manuals (Vertiv, Schneider, Eaton, Delta, DRUPS) | Distinguish mode-transfer, switching time, grid cessation, critical-load interruption |

### 2.1 2024 Dominion / NERC timing fixture (golden)

Relative times from first depression (`t=0`):

| t (s) | Event | Duration (ms) |
|------:|-------|--------------:|
| 0.000 | Initial fault (arrestor) | 42 |
| 0.532 | Simultaneous first reclose both terminals | 66 |
| 15.860 | 2nd reclose terminal A | 58 |
| 21.279 | 2nd reclose terminal B | 50 |
| 76.249 | 3rd reclose terminal A | 66 |
| 81.665 | 3rd reclose terminal B | 59 |

Published outcomes: residual V ≈ 0.25–0.40 pu in load-loss area; ~1,260 MW sustained loss near the third depression interaction; f_peak ≈ 60.047 Hz; V_peak ≈ 1.07 pu; operators removed shunt caps. Typical facility latch: **3 strikes / ~60 s**, manual return.

Strike counting is **facility-configured episode logic**, not hard-coded event ordinals. Six physical depressions ≠ six strikes if the facility merges staggered terminal operations.

### 2.2 2026 Ashburn evidence discipline

Public reporting confirms >3 GW transfer and a wide-area voltage/frequency disturbance. Do **not** label 50–70 ms values as the “actual 2026 sequence” until an official PJM/Dominion technical report exists. Use the 2024 sequence as the calibration case.

---

## 3. Field validation matrix (shipped in v0.13.1)

| Field / child | Observable? | Needed for Ashburn screening? | Without EMT? | Distinct from existing? | Null-safe? | Decision |
|---------------|-------------|-------------------------------|--------------|-------------------------|------------|----------|
| `voltage_transfer_curve` list | UPS/ATS commissioned stages | Yes — scalar 0.90 cannot distinguish 0.70 vs 0.85 | Yes (filtered RMS + duration) | Yes vs scalar threshold | Fall back to scalar | **SHIP** |
| `.v_pu` / `.t_ms` | Yes | Yes | Yes | — | — | **SHIP** |
| `.polarity` under\|over | Over-voltage observed (1.07 pu) | Yes for CAP/swell | Yes | — | — | **SHIP** |
| `.action` transfer\|trip\|partial_transfer | UPS vs DRUPS vs partial | Yes | Yes | — | — | **SHIP** |
| `.mw_fraction` cumulative | Fcease-like | Yes | Yes | Distinct from percentile | null→1.0 | **SHIP** |
| `.load_class` it\|mechanical\|all | NERC IT vs non-IT | Yes | Yes | Uses existing % columns | null→all | **SHIP** |
| `disturbance_counter` | NERC 3-strike | Yes for multi-reclose | Yes | No prior field | null=off | **SHIP** |
| `reconnection_params` | Vrecon/Trecon/Tramp | Yes for return path | Yes | Typed vs opaque map | null=off | **SHIP** |
| `voltage_measurement` | PERC1 Tv≈0.02 s | Yes — filter changes timing | Yes | New | defaults | **SHIP** |
| `protection_settings_provenance` | Study hygiene | Yes — avoid fake “site” data | N/A | New | null ok | **SHIP** |
| Full PERC1 P/Q exponents, washouts, Ip/Iq limits | Model | Dynamics, not schema v0.13.1 | Needs RMS | — | — | **DEFER** (sibling later) |
| High-V / frequency cease (PERC2) | Emerging | Future | — | — | — | **DEFER** |
| Mutating `perc1_params` children | — | — | — | Breaks strict readers | — | **REJECT** |

### 3.1 Compatibility semantics (locked)

- Null curve → identical to v0.13.0 scalar Phase D.
- Non-null curve wins for arming/timing; scalar may remain as derived summary.
- Do not rename/remove existing CLP or `perc1_params` fields.
- Decisions use first-order filtered RMS by default; instantaneous residual is a labeled sensitivity mode.
- Common-mode: evaluate each row locally, then aggregate by `common_mode_group`. One row does not auto-force peers unless group trigger semantics say so.
- Cumulative `mw_fraction`: max of satisfied stages, never additive double-count.
- Inequalities: under → `V_filt < v_pu`; over → `V_filt > v_pu`.

---

## 4. Complete PERC1 parameter cross-walk

Published PERC1 inputs → RPF placement:

| PERC1 param | Default (PW) | RPF today | v0.13.1 | Notes |
|-------------|--------------|-----------|---------|-------|
| Lfm | 0.80 | — | deferred | Loading factor / MVA base |
| QPratio | 0.66 | — | deferred | Q/P init |
| Dbfl / Dbfh | 0 | — | deferred | Freq deadband |
| Kdroop | 0 | — | deferred | Freq droop |
| Kvp/Tvp, Kvq/Tvq | 0 / 0.1 | — | deferred | Washouts |
| Tap/Tbp, Taq/Tbq | 0 | — | deferred | Lead/lag |
| nP / nQ | 0 / 1 | — | deferred | Voltage exponents |
| Ipmax/Ipmin | 1 / 0 | — | deferred | Active current limits |
| Iqmax/Iqmin | ±0.66 | approx `perc1_reactive_power_ceiling_pu` | carriage only | Incomplete mapping |
| Fcease | 1.0 | — | via curve `mw_fraction` | Cumulative stages |
| Vcease | 0.50 | `perc1_voltage_ride_through_pu` + CLP scalar | curve `v_pu` | Multi-stage on CLP |
| Tcease | 0.01 s | `perc1_voltage_support_time_sec` (misaligned) | curve `t_ms` | Prefer ms on CLP |
| Tdelay | 0 | — | may fold into t_ms | Small post-detect delay |
| Vrecon | 0.60 | opaque reconnection map | `reconnection_params.v_recover_pu` | Typed |
| Trecon | 0.05 s | opaque | `reconnection_params.delay_ms` | Typed |
| Tramp | 1.0 s | opaque / BESS ramp | `reconnection_params.ramp_mw_per_min` | Facility MW/min |
| Frecon | 1.0 | — | deferred / Frecon via action | Partial reconnect |
| Tt | 0.02 | — | deferred | Output lag |
| Tv | 0.02 | — | `voltage_measurement.filter_time_constant_ms` | **Required for timing fidelity** |
| Tf | 0.02 | — | deferred | Freq filter |

Existing six-field `perc1_params` remains a **Raptrix subset** (carriage + NR λ-bump). Do not mutate its Arrow struct children in v0.13.1.

---

## 5. Voltage measurement semantics

- Default: positive-sequence RMS, first-order filter `Tv = 20 ms`.
- Exact update for piecewise-constant sample:  
  `Vf_next = V + (Vf_prev - V) * exp(-dt / Tv)`.
- Initialize `Vf` from pre-fault RMS.
- `facility_lv` requires a distribution equivalent; otherwise emit unsupported measurement location.
- Instantaneous residual (no filter) is a conservative sensitivity mode, not the default claim.

---

## 6. Industry envelopes vs facility settings

| Envelope | Example knees | Use in RPF |
|----------|---------------|------------|
| ITIC / CBEMA | Equipment immunity | Comparison only — do **not** invert into transfer settings |
| SEMI F47 / IEEE 1668 | 50%/0.2 s, 70%/0.5 s, 80%/1–2 s | Comparison only |
| WECC/LMWG OEM PERC1 examples | Vcease ~0.70, Vrecon ~0.90, Trecon 4–10 s, Tramp 10–15 s | Inform defaults |
| NERC 3-strike | 3 / ~60 s, manual reset | `disturbance_counter` |

Illustrative AI/HPC campus enhance-spec curve (`study_assumption` only — **not frozen as recommended settings**):

- under 0.70 pu → 30 ms → transfer  
- under 0.80 pu → 80 ms  
- under 0.90 pu → 250 ms  
- under 0.95 pu → 1500 ms  
- over 1.10 pu → 100 ms  
- disturbance_counter: 3 / 60–120 s / qualifying 0.90 pu / 50 ms / latch_permanent  
- reconnection_params: v_recover 0.95, delay 5000 ms, ramp 50 MW/min, manual_reset from latch  

---

## 7. Worked examples (evaluator acceptance)

1. **Deep short sag:** 0.70 pu for 30 ms → near-instant transfer when stage matches.  
2. **Shallow longer sag:** 0.85 pu for 200 ms → may ride through if no stage fires.  
3. **3-strike latch:** 2024 six-depression play-in with strike_limit=3 → sustained transfer after configured episodes; not automatic on depression #3 ordinal.  
4. **Reconnection:** recover above `v_recover_pu` for `delay_ms`, then ramp; second sag interrupts ramp.

---

## 8. Go / no-go checklist (schema PR)

- [x] Evidence matrix complete; rejected alternatives listed  
- [x] Every PERC1 param mapped existing / new / deferred  
- [x] Filtered vs instantaneous measurement documented  
- [x] 2024 fixture encoded; 2026 timing not fabricated  
- [x] Over-voltage stages retained for post-rejection CAP  
- [x] `common_mode_group` is first-class for ranking  
- [x] Illustrative curve stamped `study_assumption` only  
- [x] `v0.13.1` documented as compatibility-extension exception to MINOR-bump rule  

---

## 9. Related docs

- [`schema-contract.md`](schema-contract.md)  
- [`rpf-field-guide.md`](rpf-field-guide.md)  
- Core: `RPF_V0130_ASHBURN_CASCADE.md`, `NERC_COMPUTATIONAL_LOAD_BEHAVIOR.md`  
- NERC: *Incident Review Considering Simultaneous Voltage-Sensitive Load Reductions* (Jan 2025)  
- PERC1: PowerWorld Load Characteristic PERC1; WECC LMWG data-center presentations  
