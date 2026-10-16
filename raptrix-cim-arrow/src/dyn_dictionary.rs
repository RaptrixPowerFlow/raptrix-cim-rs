// PSS/E DYR column order for the models this crate names.
//
// ICON/CON counts and names follow ANDES 1.9.3 `andes/io/psse-dyr.yaml`
// (Cui et al. validation set) where that file lists the model, checked
// against Texas2k/Texas7k/Memphis record lengths. A model that is not in
// this table stays positional in the converter. Do not invent a name.

/// One PSS/E dynamic model whose DYR columns are named.
#[derive(Debug, Clone, Copy)]
pub struct DynModelDef {
    pub psse_name: &'static str,
    /// sync_machine | exciter | governor | pss | regc | reec | repc | wind_mech
    pub family: &'static str,
    pub icons: &'static [&'static str],
    pub cons: &'static [&'static str],
    /// CIM class that maps 1:1 onto this PSS/E model, when one exists.
    pub cim_class: Option<&'static str>,
    pub code: u16,
}

impl DynModelDef {
    pub fn data_len(self) -> usize {
        self.icons.len() + self.cons.len()
    }
}

const fn def(
    code: u16,
    psse_name: &'static str,
    family: &'static str,
    icons: &'static [&'static str],
    cons: &'static [&'static str],
    cim_class: Option<&'static str>,
) -> DynModelDef {
    DynModelDef {
        psse_name,
        family,
        icons,
        cons,
        cim_class,
        code,
    }
}

// Synchronous machines. H and D keep that spelling. xd_prime is the legacy key.
const GENROU_CONS: &[&str] = &[
    "tpdo",
    "tppdo",
    "tpqo",
    "tppqo",
    "H",
    "D",
    "xd",
    "xq",
    "xd_prime",
    "xq_prime",
    "xd_dprime",
    "xl",
    "s10",
    "s12",
];
const GENSAL_CONS: &[&str] = &[
    "tpdo",
    "tppdo",
    "tppqo",
    "H",
    "D",
    "xd",
    "xq",
    "xd_prime",
    "xd_dprime",
    "xl",
    "s10",
    "s12",
];
const GENCLS_CONS: &[&str] = &["H", "D"];

const ESST4B_CONS: &[&str] = &[
    "tr", "kpr", "kir", "vrmax", "vrmin", "ta", "kpm", "kim", "vmmax", "vmmin", "kg", "kp", "ki",
    "vbmax", "kc", "xl", "thetap",
];
const EXST1_CONS: &[&str] = &[
    "tr", "vimax", "vimin", "tc", "tb", "ka", "ta", "vrmax", "vrmin", "kc", "kf", "tf",
];
const ESST1A_ICONS: &[&str] = &["uel", "vos"];
const ESST1A_CONS: &[&str] = &[
    "tr", "vimax", "vimin", "tb", "tc", "tb1", "tc1", "vamax", "vamin", "ka", "ta", "ilr", "klr",
    "vrmax", "vrmin", "kf", "tf", "kc",
];
const IEEET1_CONS: &[&str] = &[
    "tr", "ka", "ta", "vrmax", "vrmin", "ke", "te", "kf", "tf", "sw", "e1", "se1", "e2", "se2",
];
const IEEEX1_CONS: &[&str] = &[
    "tr", "ka", "ta", "tb", "tc", "vrmax", "vrmin", "ke", "te", "kf", "tf", "sw", "e1", "se1",
    "e2", "se2",
];
const SEXS_CONS: &[&str] = &["tatb", "tb", "k", "te", "emin", "emax"];
const SCRX_CONS: &[&str] = &["tatb", "tb", "k", "te", "emin", "emax", "cswitch", "rcrfd"];
const EXPIC1_CONS: &[&str] = &[
    "tr", "ka", "ta1", "vr1", "vr2", "ta2", "ta3", "ta4", "vrmax", "vrmin", "kf", "tf1", "tf2",
    "efdmax", "efdmin", "ke", "te", "e1", "se1", "e2", "se2", "kp", "ki", "kc",
];
const EXAC1_CONS: &[&str] = &[
    "tr", "tb", "tc", "ka", "ta", "vrmax", "vrmin", "te", "kf", "tf", "kc", "kd", "ke", "e1",
    "se1", "e2", "se2",
];
const EXAC2_CONS: &[&str] = &[
    "tr", "tb", "tc", "ka", "ta", "vamax", "vamin", "kb", "vrmax", "vrmin", "te", "kl", "kh", "kf",
    "tf", "kc", "kd", "ke", "vlr", "e1", "se1", "e2", "se2",
];
const ESAC1A_CONS: &[&str] = &[
    "tr", "tb", "tc", "vamax", "vamin", "ka", "ta", "vrmax", "vrmin", "te", "e1", "se1", "e2",
    "se2", "kc", "kd", "ke", "kf", "tf",
];
const ESAC6A_CONS: &[&str] = &[
    "tr", "ka", "ta", "tk", "tb", "tc", "vamax", "vamin", "vrmax", "vrmin", "te", "vfelim", "kh",
    "vhmax", "th", "tj", "kc", "kd", "ke", "e1", "se1", "e2", "se2",
];
const ESDC_CONS: &[&str] = &[
    "tr", "ka", "ta", "tb", "tc", "vrmax", "vrmin", "ke", "te", "kf", "tf", "sw", "e1", "se1",
    "e2", "se2",
];
const ESST3A_CONS: &[&str] = &[
    "tr", "vimax", "vimin", "km", "tc", "tb", "ka", "ta", "vrmax", "vrmin", "kg", "kp", "ki",
    "vbmax", "kc", "xl", "vgmax", "thetap", "tm", "vmmax", "vmmin",
];

const GGOV1_ICONS: &[&str] = &["rselect", "flag"];
const GGOV1_CONS: &[&str] = &[
    "r", "tpelec", "maxerr", "minerr", "kpgov", "kigov", "kdgov", "tdgov", "vmax", "vmin", "tact",
    "kturb", "wfnl", "tb", "tc", "teng", "tfload", "kpload", "kiload", "ldref", "dm", "ropen",
    "rclose", "kimw", "aset", "ka", "ta", "trate", "db", "tsa", "tsb", "rup", "rdown",
];
const IEEEG1_ICONS: &[&str] = &["jbus", "jid"];
const IEEEG1_CONS: &[&str] = &[
    "k", "t1", "t2", "t3", "uo", "uc", "pmax", "pmin", "t4", "k1", "k2", "t5", "k3", "k4", "t6",
    "k5", "k6", "t7", "k7", "k8",
];
const HYGOV_CONS: &[&str] = &[
    "r", "r_temp", "tr", "tf", "tg", "velm", "gmax", "gmin", "tw", "at", "dturb", "qnl",
];
const TGOV1_CONS: &[&str] = &["r", "t1", "vmax", "vmin", "t2", "t3", "dt"];
const GAST_CONS: &[&str] = &["r", "t1", "t2", "t3", "at", "kt", "vmax", "vmin", "dt"];

const PSS2A_ICONS: &[&str] = &["ic1", "rembus1", "ic2", "rembus2", "m", "n"];
const PSS2A_CONS: &[&str] = &[
    "tw1", "tw2", "t6", "tw3", "tw4", "t7", "ks2", "ks3", "t8", "t9", "ks1", "t1", "t2", "t3",
    "t4", "vstmax", "vstmin",
];
const IEEEST_ICONS: &[&str] = &["mode", "rembus"];
const IEEEST_CONS: &[&str] = &[
    "a1", "a2", "a3", "a4", "a5", "a6", "t1", "t2", "t3", "t4", "t5", "t6", "ks", "lsmax", "lsmin",
    "vcu", "vcl",
];

const REGCA1_ICONS: &[&str] = &["lvplsw"];
const REGCA1_CONS: &[&str] = &[
    "tg", "rrpwr", "brkpt", "zerox", "lvpl1", "volim", "lvpnt1", "lvpnt0", "iolim", "tfltr", "khv",
    "iqrmax", "iqrmin", "accel",
];
const REECA1_ICONS: &[&str] = &[
    "remote_bus_id",
    "pfflag",
    "vflag",
    "qflag",
    "pflag",
    "pqflag",
];
const REECA1_CONS: &[&str] = &[
    "vdip", "vup", "trv", "dbd1", "dbd2", "kqv", "iqh1", "iql1", "vref0", "iqfrz", "thld", "thld2",
    "tp", "qmax", "qmin", "vmax", "vmin", "kqp", "kqi", "kvp", "kvi", "vbias", "tiq", "dpmax",
    "dpmin", "pmax", "pmin", "imax", "tpord", "vq1", "iq1", "vq2", "iq2", "vq3", "iq3", "vq4",
    "iq4", "vp1", "ip1", "vp2", "ip2", "vp3", "ip3", "vp4", "ip4",
];
const REPCA1_ICONS: &[&str] = &[
    "remote_bus_id",
    "mon_from_bus",
    "mon_to_bus",
    "mon_ckt",
    "vcflag",
    "refflag",
    "fflag",
];
const REPCA1_CONS: &[&str] = &[
    "tfltr", "kp", "ki", "tft", "tfv", "vfrz", "rc", "xc", "kc", "emax", "emin", "dbd1", "dbd2",
    "qmax", "qmin", "kpg", "kig", "tp", "fdbd1", "fdbd2", "femax", "femin", "pmax", "pmin", "tg",
    "ddn", "dup",
];

const WTARA1_CONS: &[&str] = &["ka", "theta0"];
const WTPTA1_CONS: &[&str] = &[
    "kiw", "kpw", "kic", "kpc", "kcc", "tp", "tetamax", "tetamin", "rtetamax", "rtetamin",
];
const WTTQA1_ICONS: &[&str] = &["tflag"];
// Power breakpoints are pwr1..pwr4. ANDES names them p1..p4, which collide
// with the converter's positional keys params["p1"]..params["pN"].
const WTTQA1_CONS: &[&str] = &[
    "kpp", "kip", "tp", "twref", "temax", "temin", "pwr1", "spd1", "pwr2", "spd2", "pwr3", "spd3",
    "pwr4", "spd4", "trate",
];

/// Named models. `code` is stable for a given `psse_name`.
pub static DYN_MODELS: &[DynModelDef] = &[
    def(1, "GENROU", "sync_machine", &[], GENROU_CONS, None),
    def(2, "GENROE", "sync_machine", &[], GENROU_CONS, None),
    def(3, "GENSAL", "sync_machine", &[], GENSAL_CONS, None),
    def(4, "GENSAE", "sync_machine", &[], GENSAL_CONS, None),
    def(5, "GENCLS", "sync_machine", &[], GENCLS_CONS, None),
    def(
        10,
        "ESST4B",
        "exciter",
        &[],
        ESST4B_CONS,
        Some("ExcIEEEST4B"),
    ),
    def(11, "EXST1", "exciter", &[], EXST1_CONS, Some("ExcST1")),
    def(
        12,
        "ESST1A",
        "exciter",
        ESST1A_ICONS,
        ESST1A_CONS,
        Some("ExcIEEEST1A"),
    ),
    def(13, "IEEET1", "exciter", &[], IEEET1_CONS, Some("ExcIEEET1")),
    def(14, "IEEEX1", "exciter", &[], IEEEX1_CONS, Some("ExcIEEEX1")),
    def(15, "SEXS", "exciter", &[], SEXS_CONS, Some("ExcSEXS")),
    def(16, "SCRX", "exciter", &[], SCRX_CONS, Some("ExcSCRX")),
    def(17, "EXPIC1", "exciter", &[], EXPIC1_CONS, Some("ExcPIC")),
    def(18, "EXAC1", "exciter", &[], EXAC1_CONS, Some("ExcAC1A")),
    def(19, "EXAC2", "exciter", &[], EXAC2_CONS, Some("ExcAC2A")),
    def(
        20,
        "ESAC1A",
        "exciter",
        &[],
        ESAC1A_CONS,
        Some("ExcIEEEAC1A"),
    ),
    def(
        21,
        "ESAC6A",
        "exciter",
        &[],
        ESAC6A_CONS,
        Some("ExcIEEEAC6A"),
    ),
    def(22, "ESDC1A", "exciter", &[], ESDC_CONS, Some("ExcIEEEDC1A")),
    def(23, "ESDC2A", "exciter", &[], ESDC_CONS, Some("ExcIEEEDC2A")),
    def(
        24,
        "ESST3A",
        "exciter",
        &[],
        ESST3A_CONS,
        Some("ExcIEEEST3A"),
    ),
    def(
        30,
        "GGOV1",
        "governor",
        GGOV1_ICONS,
        GGOV1_CONS,
        Some("GovCT1"),
    ),
    def(
        31,
        "IEEEG1",
        "governor",
        IEEEG1_ICONS,
        IEEEG1_CONS,
        Some("GovSteamIEEE1"),
    ),
    def(32, "HYGOV", "governor", &[], HYGOV_CONS, None),
    def(33, "TGOV1", "governor", &[], TGOV1_CONS, Some("GovSteam0")),
    def(34, "GAST", "governor", &[], GAST_CONS, Some("GovGAST")),
    def(40, "PSS2A", "pss", PSS2A_ICONS, PSS2A_CONS, None),
    def(41, "IEEEST", "pss", IEEEST_ICONS, IEEEST_CONS, None),
    def(50, "REGCA1", "regc", REGCA1_ICONS, REGCA1_CONS, None),
    def(51, "REECA1", "reec", REECA1_ICONS, REECA1_CONS, None),
    def(52, "REPCA1", "repc", REPCA1_ICONS, REPCA1_CONS, None),
    def(60, "WTARA1", "wind_mech", &[], WTARA1_CONS, None),
    def(61, "WTPTA1", "wind_mech", &[], WTPTA1_CONS, None),
    def(62, "WTTQA1", "wind_mech", WTTQA1_ICONS, WTTQA1_CONS, None),
];

/// CIM attribute on `SynchronousMachineTimeConstantReactance` / rotating-machine
/// dynamics, and the canonical key it writes. Rotor type picks GENROU vs GENSAL
/// in the CIM converter; it is not a column of this table.
pub static MACHINE_CIM_ATTRS: &[(&str, &str)] = &[
    ("RotatingMachineDynamics.inertia", "H"),
    ("RotatingMachineDynamics.damping", "D"),
    (
        "SynchronousMachineTimeConstantReactance.xDirectTrans",
        "xd_prime",
    ),
    (
        "SynchronousMachineTimeConstantReactance.xQuadTrans",
        "xq_prime",
    ),
    (
        "SynchronousMachineTimeConstantReactance.xDirectSubtrans",
        "xd_dprime",
    ),
    ("SynchronousMachineTimeConstantReactance.tpdo", "tpdo"),
    ("SynchronousMachineTimeConstantReactance.tppdo", "tppdo"),
    ("SynchronousMachineTimeConstantReactance.tpqo", "tpqo"),
    ("SynchronousMachineTimeConstantReactance.tppqo", "tppqo"),
    ("SynchronousMachineTimeConstantReactance.xDirectSync", "xd"),
    ("SynchronousMachineTimeConstantReactance.xQuadSync", "xq"),
    (
        "SynchronousMachineTimeConstantReactance.satDirectSubtransX",
        "xl",
    ),
];

pub fn lookup_psse(name: &str) -> Option<&'static DynModelDef> {
    let upper = name.to_ascii_uppercase();
    DYN_MODELS.iter().find(|m| m.psse_name == upper)
}

pub fn lookup_cim_class(class_name: &str) -> Option<&'static DynModelDef> {
    let bare = class_name.rsplit(':').next().unwrap_or(class_name);
    DYN_MODELS.iter().find(|m| m.cim_class == Some(bare))
}

/// JSON array used by the C++ parity test. One object per model.
pub fn dictionary_json() -> String {
    let mut out = String::from("[\n");
    for (i, m) in DYN_MODELS.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str("  {");
        out.push_str(&format!("\"psse_name\":\"{}\",", m.psse_name));
        out.push_str(&format!("\"family\":\"{}\",", m.family));
        out.push_str(&format!("\"code\":{},", m.code));
        out.push_str(&format!(
            "\"cim_class\":{},",
            match m.cim_class {
                Some(c) => format!("\"{c}\""),
                None => "null".to_string(),
            }
        ));
        out.push_str("\"icons\":[");
        for (j, name) in m.icons.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&format!("\"{name}\""));
        }
        out.push_str("],\"cons\":[");
        for (j, name) in m.cons.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&format!("\"{name}\""));
        }
        out.push_str("]}");
    }
    out.push_str("\n]\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_shapes() {
        let expect = [
            ("GENROU", 0, 14),
            ("REGCA1", 1, 14),
            ("REECA1", 6, 45),
            ("REPCA1", 7, 27),
            ("GGOV1", 2, 33),
            ("PSS2A", 6, 17),
            ("ESST1A", 2, 18),
            ("IEEEG1", 2, 20),
            ("WTTQA1", 1, 15),
            ("ESST4B", 0, 17),
            ("GENSAL", 0, 12),
        ];
        for (name, icons, cons) in expect {
            let m = lookup_psse(name).unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!(m.icons.len(), icons, "{name} icons");
            assert_eq!(m.cons.len(), cons, "{name} cons");
        }
    }

    #[test]
    fn cim_class_round_trip() {
        let m = lookup_cim_class("ExcIEEEST4B").expect("ESST4B");
        assert_eq!(m.psse_name, "ESST4B");
    }

    #[test]
    fn reeca_names_vdip() {
        let m = lookup_psse("REECA1").unwrap();
        assert_eq!(m.cons[0], "vdip");
        assert!(m.icons.contains(&"remote_bus_id"));
    }

    #[test]
    fn wttqa1_power_breakpoints_do_not_reuse_positional_keys() {
        let m = lookup_psse("WTTQA1").unwrap();
        assert_eq!(
            &m.cons[6..14],
            &[
                "pwr1", "spd1", "pwr2", "spd2", "pwr3", "spd3", "pwr4", "spd4"
            ]
        );
    }

    #[test]
    fn column_names_do_not_use_reserved_param_keys() {
        const RESERVED: &[&str] = &[
            "source_dyr",
            "params_named",
            "icon_count",
            "con_count",
            "dict_code",
            "token_count_mismatch",
            "source_dy",
            "source_cim_class",
            "psse_equiv",
            "model_mapped",
        ];
        let reserved_shape = |name: &str, prefix: &str| {
            let Some(rest) = name.get(prefix.len()..) else {
                return false;
            };
            name.len() > prefix.len()
                && name[..prefix.len()].eq_ignore_ascii_case(prefix)
                && !rest.is_empty()
                && rest.bytes().all(|c| c.is_ascii_digit())
        };
        for model in DYN_MODELS {
            for name in model.icons.iter().chain(model.cons.iter()) {
                assert!(
                    !RESERVED.iter().any(|key| key.eq_ignore_ascii_case(name)),
                    "{} column {name} reuses a metadata key",
                    model.psse_name
                );
                assert!(
                    !reserved_shape(name, "p")
                        && !reserved_shape(name, "icon")
                        && !reserved_shape(name, "con"),
                    "{} column {name} collides with a positional key",
                    model.psse_name
                );
            }
        }
    }
}
