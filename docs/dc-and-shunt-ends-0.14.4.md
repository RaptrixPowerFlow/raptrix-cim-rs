<!--
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
-->

# RPF v0.14.4 — converter ends, branch-end shunts, load ride-through

**Status: column shape locked in v0.14.4. Current readers accept v0.14.5 only (crate 0.7.6).**

This note describes the v0.14.4 columns. v0.14.5 keeps them and locks the dynamics parameter vocabulary. Readers accept `v0.14.5` and `0.14.5` only. `v0.15.0` is reserved for the first supported product freeze. Files stamped with any other version must be re-emitted.

## Two-terminal DC

`dc_lines_2w` stays the 15-column tie:

`dc_line_id`, `from_bus_id`, `to_bus_id`, `ckt`, `r_ohm`, `l_henry`, `control_mode`, `p_setpoint_mw`, `i_setpoint_ka`, `v_setpoint_kv`, `q_from_mvar`, `q_to_mvar`, `status`, `name`, `converter_type`.

`p_setpoint_mw` is rectifier DC power for power control (`MDC = 1`), including when the metering flag names the inverter. `q_from_mvar` and `q_to_mvar` are fixed injections. They stay null when the reactive power is voltage-dependent. Do not copy the DC schedule into bus `p_sched`.

The 11-column solved-snapshot dialect of `dc_lines_2w` is unchanged and read-only.

### `dc_converters`

Every v0.14.4 writer emits this table, including a zero-row table when the case has no terminals. `RootWriteOptions.include_dc_converters` defaults to true. Stamp `raptrix.features.dc_converters=true` when the table is present. A v0.14.4 file that omits the table fails validation. The table is not one of the 18 equipment tables in `all_table_schemas()`. Ownership is converter-owned, so a solver patch keeps the source table.

Two rows per line that has terminal records, keyed by `dc_line_id` plus `role`. A line has two AC ends. A shorthand one-line DC record produces a `dc_lines_2w` row and no converter row. Multi-terminal DC is not modeled here.

| Column | Type | Null | Use |
| --- | --- | --- | --- |
| `dc_line_id` | Int32 | no | Line this end belongs to |
| `bus_id` | Int32 | no | AC terminal. Rectifier matches `from_bus_id`. Inverter matches `to_bus_id` |
| `role` | Utf8 | no | `rectifier` or `inverter` |
| `converter_kind` | Utf8 | no | `lcc` or `vsc`. Must match `dc_lines_2w.converter_type` |
| `n_bridges` | Int32 | yes | LCC bridge count, 1 through 12. Q(V) input |
| `ebas_kv` | Float64 | yes | Commutating-transformer AC base, kV. Q(V) input |
| `tr` | Float64 | yes | Transformer ratio. Q(V) input |
| `tap` | Float64 | yes | Operating tap. Q(V) input |
| `tap_min`, `tap_max` | Float64 | yes | Recorded tap band. Not an equation input |
| `xc_ohm` | Float64 | yes | Commutating reactance. Stored, not used by the v0.14.4 equation |
| `alpha_deg`, `gamma_deg` | Float64 | yes | Firing and extinction angles. Stored, not used |
| `is_meter_end` | Boolean | no | True on the end named by the meter flag. Does not move `p_setpoint_mw` |
| `ac_control` | Utf8 | yes | VSC: `v_ac` or `q`. Null for LCC |
| `ac_setpoint` | Float64 | yes | VSC AC voltage setpoint in pu, or Q setpoint in Mvar |
| `q_min_mvar`, `q_max_mvar` | Float64 | yes | VSC reactive limits |
| `s_max_mva`, `i_max_ka` | Float64 | yes | VSC ratings |
| `loss_const_mw`, `loss_i_mw_per_ka` | Float64 | yes | VSC losses. Not `r_ohm` |

If any of `n_bridges`, `ebas_kv`, `tr`, or `tap` is null, that end has real power only. The schema does not invent `1.0` from a null. For `converter_kind = lcc`, the eight VSC columns are null. There is no VSC power-flow equation in this revision.

At most one row per `(dc_line_id, role)`. At most one `is_meter_end` true on a line. Unknown meter → both false, and `p_setpoint_mw` stays rectifier power.

Worked LCC row (synthetic, unlabeled): `from_bus_id` 58540, `to_bus_id` 55247, `converter_type` `lcc`, `control_mode` `power`, `r_ohm` 10, `p_setpoint_mw` 550, `v_setpoint_kv` 250, both Q null, name `DC Line 1`. Both ends: `n_bridges` 2, `ebas_kv` 345, `tr` 0.55784, `tap` 1.5, tap band 1.5 / 0.51, `xc_ohm` 10, `alpha_deg` 15, `gamma_deg` 15. `is_meter_end` is true only on bus 55247. The line still has 15 columns.

The ideal Q(V) term, for a solver that chooses to apply it:

```text
ell = V_ac_pu * ebas_kv * tr / tap
Vdio = (3 * sqrt(2) / pi) * n_bridges * ell
cos_phi = clamp(Vdc_kv / Vdio, 0.05, 0.999)
Q_mvar = P_mw * sin(phi) / cos(phi)
```

`Q_mvar` is absorbed from the AC bus. `xc_ohm`, `alpha_deg`, and `gamma_deg` are not inputs to this equation.

## Branch-end shunts

`branches` gains four trailing nullable Float64 columns after `is_bptf`: `g_from`, `b_from`, `g_to`, `b_to`. They are PSS/E `GI` / `BI` / `GJ` / `BJ` divided by `metadata.base_mva`. Positive susceptance is capacitive. Null means the source has no per-end fields. A parsed 0 is stored as 0.

`branches.b_shunt` remains total line charging. It is not the sum of the end shunts.

`buses.g_shunt` and `buses.b_shunt` stay the derived injection:

```text
g_shunt = bus GL/SBASE + fixed g + in-service g_from at the from bus + in-service g_to at the to bus
b_shunt = bus BL/SBASE + fixed b + in-service b_from at the from bus + in-service b_to at the to bus
```

A solver that already injects the bus aggregates must not also add the new branch columns. CIM exports write the four columns null. Line length and zero-sequence are not added.

## Computational-load ride-through

No new load table. On `computational_load_profiles`:

- `frequency_transfer_curve`: same stage shape as `voltage_transfer_curve`, with `f_hz` instead of `v_pu`. Actions stay `transfer`, `trip`, `partial_transfer`. This is what the facility does.
- `reconnection_params.restore_fraction` and `restore_time_s`: both null, or both set. The fraction is in (0, 1]. The time is seconds after voltage is back in the continuous band. `ramp_mw_per_min` stays the plant ramp.
- `ride_through_requirement`: nullable struct of `voltage_bands` and `frequency_bands`. Each band is `{low, high, t_ms?}`. Null `t_ms` means continuous inside that closed interval. This is a must-remain-connected envelope. It is not checked against the facility curves. A facility curve may transfer inside a band the requirement still calls continuous.

Applicability gates, tariff section numbers, and filing prose are not columns. A label belongs in `protection_settings_provenance` or study metadata. Converters leave the new load fields null. One synthetic requirement band may be authored with `source = study_assumption`.

`perc1_params` is unchanged, including scalar `perc1_frequency_ride_through_hz`.
