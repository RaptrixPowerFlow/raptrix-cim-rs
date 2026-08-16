/*
Raptrix CIM-Arrow — High-performance open CIM profile by Raptrix Power
Copyright (c) 2026 Raptrix Power
*/

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Production CLI entrypoint for CGMES -> Raptrix CIM-Arrow conversion.
//!
//! Tenet references:
//! - Tenet 1: the CLI delegates directly to `write_complete_rpf` and avoids
//!   any readback or post-write parsing.
//! - Tenet 4: all user-facing output includes the required Musto branding.
//! - Tenet 6: the CLI is the primary entrypoint while the library API remains
//!   stable through `raptrix_cim_rs::write_complete_rpf`.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};

use raptrix_cim_rs::arrow_schema::{
    BRANDING, METADATA_KEY_FEATURE_CONTINGENCIES_STUB, METADATA_KEY_FEATURE_CONTINGENCY_SEQUENCES,
    METADATA_KEY_FEATURE_DIAGRAM_LAYOUT, METADATA_KEY_FEATURE_DYNAMICS_STUB,
    METADATA_KEY_FEATURE_NODE_BREAKER, METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES,
    METADATA_KEY_FEATURE_TOPOLOGY_CHANGES, format_health_report, inspect_rpf_file,
};
use raptrix_cim_rs::rpf_writer::{
    BusResolutionMode, DetachedIslandPolicy, WriteOptions, rpf_file_metadata, summarize_rpf,
    write_complete_rpf_with_options,
};

const ENHANCE_LONG_ABOUT: &str = "Read an existing v0.13.x/v0.14.0 .rpf, apply a JSON enhancement spec, and \
write a new .rpf. Pure authoring: no schema is invented, and every table other than the ones the \
spec touches is preserved unchanged.\n\n\
Spec JSON shape:\n\
{\n  \
  \"computational_load_profiles\": [ { \"bus_id\": 110013, \"facility_class\": \"ai_hpc\",\n    \
    \"common_mode_group\": \"ashburn_campus_a\", \"priority\": 1, \"max_step_drop_mw\": 800.0,\n    \
    \"trip_study_percentiles\": [60.0, 100.0], \"transfer_to_backup_threshold_pu\": 0.90,\n    \
    \"transfer_delay_ms\": 50.0, \"poi_name\": \"Campus A POI 1\" } ],\n  \
  \"dynamics_models\": [ { \"bus_id\": 1, \"gen_id\": \"1\", \"model_type\": \"GENCLS\",\n    \
    \"params\": {}, \"classical_params\": { \"H\": 5.0, \"D\": 0.0, \"xd_prime\": 0.25, \"mbase_mva\": 100.0 } } ],\n  \
  \"load_overrides\": [ { \"bus_id\": 110013, \"p_mw\": 800.0, \"q_mw\": 200.0 },\n    \
    { \"bus_id\": 110123, \"scale_p\": 10.0 } ],\n  \
  \"computational_load_mode\": true\n\
}\n\n\
Rules: any field on computational_load_profiles rows may be set by name (bus_id XOR load_id \
required when mode ends up true). Omitting \"computational_load_profiles\" preserves the input's \
existing table unchanged; including it (even as []) fully replaces it. Omitting \"dynamics_models\" \
preserves the input's existing table unchanged; including it fully replaces it (each row needs \
bus_id, gen_id, model_type; params/classical_params default to empty/absent). \
Optional \"load_overrides\" scale and/or set absolute load MW on the loads table: p_mw/q_mw \
convert via metadata.base_mva (default 100); scale_p/scale_q multiply existing p_pu/q_pu (and \
ZIP I/Y proportionally when present) on all load rows at that bus_id; scale first, then absolute \
wins for that quantity; absolute clears ZIP I/Y; a missing load row is created (id=DC1) only when \
p_mw is set. \"computational_load_mode\" defaults to true once the resolved \
computational_load_profiles table is non-empty, unless set explicitly.";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DetachedIslandPolicyArg {
    Permissive,
    Strict,
    PruneDetached,
}

impl From<DetachedIslandPolicyArg> for DetachedIslandPolicy {
    fn from(value: DetachedIslandPolicyArg) -> Self {
        match value {
            DetachedIslandPolicyArg::Permissive => DetachedIslandPolicy::Permissive,
            DetachedIslandPolicyArg::Strict => DetachedIslandPolicy::Strict,
            DetachedIslandPolicyArg::PruneDetached => DetachedIslandPolicy::PruneDetached,
        }
    }
}

const COPYRIGHT: &str = "Copyright (c) 2026 Raptrix Power";
const CANONICAL_TABLE_COUNT: usize = 15;

/// Command-line interface for Raptrix CIM-Arrow conversion.
#[derive(Debug, Parser)]
#[command(
    name = "raptrix-cim-rs",
    about = BRANDING,
    long_about = BRANDING,
    after_help = COPYRIGHT,
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Convert CGMES profiles into a canonical `.rpf` Arrow IPC artifact.
    Convert(ConvertArgs),
    /// One-line helper: re-ingest source profiles into strict schema v0.9.3 output.
    #[command(name = "migrate-0.9.2-to-0.9.3")]
    Migrate092To093(ConvertArgs),
    /// View summary stats from an existing `.rpf` Arrow IPC artifact.
    View(ViewArgs),
    /// Patch an existing `.rpf` with a JSON computational-load / dynamics / load-override spec.
    #[command(long_about = ENHANCE_LONG_ABOUT)]
    Enhance(EnhanceArgs),
}

/// Conversion arguments for explicit profile paths or auto-detection.
#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("profile_mode")
    .args(["input_dir", "eq", "eqbd", "tp", "sv", "ssh", "dy", "dl", "gl"])
        .multiple(true)
))]
struct ConvertArgs {
    /// EQ profile path. Required when `--input-dir` is not used.
    #[arg(long)]
    eq: Option<PathBuf>,

    /// EQBD (Equipment Boundary) profile path — shared BaseVoltage definitions.
    #[arg(long)]
    eqbd: Option<PathBuf>,

    /// TP profile path.
    #[arg(long)]
    tp: Option<PathBuf>,

    /// SV profile path.
    #[arg(long)]
    sv: Option<PathBuf>,

    /// SSH profile path.
    #[arg(long)]
    ssh: Option<PathBuf>,

    /// DY profile path.
    #[arg(long)]
    dy: Option<PathBuf>,

    /// DL profile path carrying IEC 61970-453 diagram layout payloads.
    #[arg(long)]
    dl: Option<PathBuf>,

    /// GL profile path carrying CIM GeographicalLocation (Location + PositionPoint).
    #[arg(long)]
    gl: Option<PathBuf>,

    /// Auto-detect CGMES profiles in a directory via case-insensitive filename matching.
    #[arg(long)]
    input_dir: Option<PathBuf>,

    /// Output `.rpf` path.
    #[arg(long)]
    output: PathBuf,

    /// Print resolved input/output paths.
    #[arg(long)]
    verbose: bool,

    /// Use TP TopologicalNode collapsing for bus IDs (default interoperability mode).
    #[arg(long)]
    topological: bool,

    /// Keep ConnectivityNode granularity and emit optional connectivity_groups detail.
    #[arg(long)]
    connectivity_detail: bool,

    /// Emit optional node-breaker detail tables for viewer/operations workflows.
    #[arg(long)]
    node_breaker: bool,

    /// Suppress optional diagram layout tables even when CIM DiagramLayout is present.
    #[arg(long)]
    no_diagram: bool,

    /// Detached island handling policy: permissive, strict, or prune-detached.
    #[arg(long, value_enum, default_value_t = DetachedIslandPolicyArg::Permissive)]
    detached_island_policy: DetachedIslandPolicyArg,

    /// Default system base MVA used when CGMES profile metadata is unavailable.
    #[arg(long, default_value_t = 100.0)]
    base_mva: f64,

    /// Default nominal system frequency used when profile metadata is unavailable.
    #[arg(long, default_value_t = 60.0)]
    frequency_hz: f64,

    /// Optional study name override written into metadata.
    #[arg(long)]
    study_name: Option<String>,

    /// Optional UTC timestamp override written into metadata (RFC3339 recommended).
    #[arg(long)]
    timestamp_utc: Option<String>,
}

/// Arguments for the `enhance` subcommand.
#[derive(Debug, clap::Args)]
struct EnhanceArgs {
    /// Input v0.13.x / v0.14.0 `.rpf` file to enhance.
    #[arg(long)]
    input: PathBuf,

    /// JSON enhancement spec path. See `--help` for the accepted shape.
    #[arg(long)]
    spec: PathBuf,

    /// Output `.rpf` path.
    #[arg(long)]
    output: PathBuf,

    /// Print resolved input/spec/output paths.
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, clap::Args)]
struct ViewArgs {
    /// Input `.rpf` file to inspect.
    #[arg(long)]
    input: PathBuf,

    /// Print file-level RPF metadata entries.
    #[arg(long)]
    verbose: bool,

    /// Print deterministic case-health grade and reasons.
    #[arg(long)]
    health: bool,
}

#[derive(Debug, Clone, Copy)]
enum DetectionMode {
    Explicit,
    Auto,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{BRANDING}");
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Convert(args) => run_convert(args),
        Commands::Migrate092To093(args) => run_convert(args),
        Commands::View(args) => run_view(args),
        Commands::Enhance(args) => run_enhance_cmd(args),
    }
}

fn run_enhance_cmd(args: EnhanceArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let input_path = normalize_existing_path(&args.input, &cwd)?;
    validate_rpf_input_path(&input_path)?;
    let spec_path = normalize_existing_path(&args.spec, &cwd)?;
    let output_path = normalize_output_path(&args.output, &cwd);
    validate_output_path(&output_path)?;

    if args.verbose {
        println!("{BRANDING}");
        println!("Resolved input path: {}", input_path.display());
        println!("Resolved spec path: {}", spec_path.display());
        println!("Resolved output path: {}", output_path.display());
    }

    let summary = raptrix_cim_rs::enhance::run_enhance(&input_path, &spec_path, &output_path)
        .with_context(|| {
            format!(
                "failed to enhance {} with spec {} into {}",
                input_path.display(),
                spec_path.display(),
                output_path.display()
            )
        })?;

    let file_size = fs::metadata(&output_path)
        .with_context(|| {
            format!(
                "failed to read output metadata for {}",
                output_path.display()
            )
        })?
        .len();

    println!("{BRANDING}");
    println!("Input: {}", input_path.display());
    println!("Spec: {}", spec_path.display());
    println!("Output: {}", output_path.display());
    println!("File size: {} bytes", file_size);
    println!("Tables written: {}", summary.tables_written);
    println!(
        "dynamics_models: {} row(s){}",
        summary.dynamics_models_rows,
        if summary.dynamics_models_replaced {
            " (replaced by spec)"
        } else {
            " (preserved from input)"
        }
    );
    if summary.computational_load_profiles_included {
        println!(
            "computational_load_profiles: {} row(s){}",
            summary.computational_load_profiles_rows,
            if summary.computational_load_profiles_replaced {
                " (replaced by spec)"
            } else {
                " (preserved from input)"
            }
        );
    } else {
        println!("computational_load_profiles: absent");
    }
    println!(
        "metadata.computational_load_mode: {}",
        match summary.computational_load_mode {
            Some(true) => "true",
            Some(false) => "false",
            None => "null",
        }
    );
    if summary.load_overrides_applied > 0 {
        println!(
            "load_overrides: {} applied ({} load row(s) created)",
            summary.load_overrides_applied, summary.load_rows_created
        );
    } else {
        println!("load_overrides: none");
    }

    Ok(())
}

fn run_view(args: ViewArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let input_path = normalize_existing_path(&args.input, &cwd)?;
    validate_rpf_input_path(&input_path)?;

    let summary = summarize_rpf(&input_path).with_context(|| {
        format!(
            "failed to read Raptrix CIM-Arrow input from {}",
            input_path.display()
        )
    })?;
    let metadata = rpf_file_metadata(&input_path).with_context(|| {
        format!(
            "failed to read Raptrix CIM-Arrow metadata from {}",
            input_path.display()
        )
    })?;

    println!("{BRANDING}");
    println!("Input: {}", input_path.display());

    if args.health {
        let health = inspect_rpf_file(&input_path).with_context(|| {
            format!("failed to inspect case health for {}", input_path.display())
        })?;
        println!("{}", format_health_report(&health));
        return Ok(());
    }

    println!("Tables: {}", summary.tables.len());
    println!("Total record batches: {}", summary.total_batches);
    println!("Total rows: {}", summary.total_rows);
    println!(
        "Canonical tables expected: {}",
        summary.canonical_table_count
    );
    println!(
        "Canonical coverage: {}",
        if summary.has_all_canonical_tables {
            "complete"
        } else {
            "partial"
        }
    );

    for table in &summary.tables {
        println!(
            "Table {}: {} batch(es), {} row(s)",
            table.table_name, table.batches, table.rows
        );
    }

    let mut enabled_features = Vec::new();
    if metadata
        .get(METADATA_KEY_FEATURE_NODE_BREAKER)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("node-breaker");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_CONTINGENCIES_STUB)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("contingencies-stub");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_DYNAMICS_STUB)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("dynamics-stub");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_DIAGRAM_LAYOUT)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("diagram-layout");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_PROTECTION_CONTINGENCIES)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("protection-contingencies");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_TOPOLOGY_CHANGES)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("topology-changes");
    }
    if metadata
        .get(METADATA_KEY_FEATURE_CONTINGENCY_SEQUENCES)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        enabled_features.push("contingency-sequences");
    }

    println!(
        "Feature flags: {}",
        if enabled_features.is_empty() {
            "none".to_string()
        } else {
            enabled_features.join(", ")
        }
    );

    if args.verbose {
        let mut metadata_entries: Vec<(String, String)> = metadata.into_iter().collect();
        metadata_entries.sort_by(|left, right| left.0.cmp(&right.0));

        println!("Metadata:");
        for (key, value) in metadata_entries {
            println!("  {key} = {value}");
        }
    }

    Ok(())
}

fn run_convert(args: ConvertArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let output_path = normalize_output_path(&args.output, &cwd);
    validate_output_path(&output_path)?;

    let (mode, profile_paths) = collect_profile_paths(&args, &cwd)?;

    if args.verbose {
        println!("{BRANDING}");
        println!("Resolved output path: {}", output_path.display());
        for (profile, path) in &profile_paths {
            println!("Resolved {profile} path: {}", path.display());
        }
    }

    let input_strings: Vec<String> = profile_paths
        .iter()
        .map(|(_, path)| path.to_string_lossy().into_owned())
        .collect();
    let input_refs: Vec<&str> = input_strings.iter().map(String::as_str).collect();
    let output = output_path.to_string_lossy().into_owned();
    if args.topological && args.connectivity_detail {
        bail!("Use either --topological or --connectivity-detail, not both");
    }

    let write_options = if args.connectivity_detail {
        WriteOptions {
            bus_resolution_mode: BusResolutionMode::ConnectivityDetail,
            detached_island_policy: args.detached_island_policy.into(),
            emit_connectivity_groups: true,
            emit_node_breaker_detail: args.node_breaker,
            emit_diagram_layout: !args.no_diagram,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: args.base_mva,
            frequency_hz: args.frequency_hz,
            study_name: args.study_name.clone(),
            timestamp_utc: args.timestamp_utc.clone(),
            modern_grid_profile: true,
            ibr_penetration_pct: None,
            study_purpose: None,
            scenario_tags: Vec::new(),
            case_mode: raptrix_cim_rs::rpf_writer::CaseMode::FlatStartPlanning,
            solver_provenance: None,
            transformer_representation_mode:
                raptrix_cim_rs::rpf_writer::TransformerRepresentationMode::Native3W,
            default_shunt_control_mode: None,
            computational_load_mode: None,
            emit_computational_load_profiles: false,
            emit_remedial_action_schemes: false,
        }
    } else {
        WriteOptions {
            // Topological is default for interoperability.
            bus_resolution_mode: BusResolutionMode::Topological,
            detached_island_policy: args.detached_island_policy.into(),
            emit_connectivity_groups: false,
            emit_node_breaker_detail: args.node_breaker,
            emit_diagram_layout: !args.no_diagram,
            contingencies_are_stub: false,
            dynamics_are_stub: false,
            base_mva: args.base_mva,
            frequency_hz: args.frequency_hz,
            study_name: args.study_name.clone(),
            timestamp_utc: args.timestamp_utc.clone(),
            modern_grid_profile: true,
            ibr_penetration_pct: None,
            study_purpose: None,
            scenario_tags: Vec::new(),
            case_mode: raptrix_cim_rs::rpf_writer::CaseMode::FlatStartPlanning,
            solver_provenance: None,
            transformer_representation_mode:
                raptrix_cim_rs::rpf_writer::TransformerRepresentationMode::Native3W,
            default_shunt_control_mode: None,
            computational_load_mode: None,
            emit_computational_load_profiles: false,
            emit_remedial_action_schemes: false,
        }
    };

    let summary = write_complete_rpf_with_options(&input_refs, &output, &write_options)
        .with_context(|| format!("failed to write Raptrix CIM-Arrow output to {output}"))?;

    let file_size = fs::metadata(&output_path)
        .with_context(|| {
            format!(
                "failed to read output metadata for {}",
                output_path.display()
            )
        })?
        .len();

    println!("{BRANDING}");
    println!("Mode: {}", mode_label(mode));
    println!("Profiles: {}", format_profile_summary(&profile_paths));
    println!(
        "Bus resolution: {}",
        match write_options.bus_resolution_mode {
            BusResolutionMode::Topological => "topological",
            BusResolutionMode::ConnectivityDetail => "connectivity-detail",
        }
    );
    println!("Output: {}", output_path.display());
    println!("File size: {} bytes", file_size);
    let emitted_tables = if write_options.emit_connectivity_groups {
        CANONICAL_TABLE_COUNT + 1
    } else {
        CANONICAL_TABLE_COUNT
    } + if write_options.emit_node_breaker_detail {
        3
    } else {
        0
    } + if write_options.emit_diagram_layout && summary.diagram_object_rows > 0
    {
        2
    } else {
        0
    };
    println!("Tables emitted: {emitted_tables}");
    if summary.tp_merged {
        println!(
            "TP merge bus reduction: {} -> {} ({:.1}% reduction)",
            summary.connectivity_bus_count,
            summary.final_bus_count,
            100.0
                * (summary
                    .connectivity_bus_count
                    .saturating_sub(summary.final_bus_count)) as f64
                / summary.connectivity_bus_count as f64
        );
    }
    if write_options.emit_connectivity_groups {
        println!(
            "Connectivity groups emitted: {}",
            summary.connectivity_groups_rows
        );
    }
    if write_options.emit_node_breaker_detail {
        println!("Node-breaker detail rows: {}", summary.node_breaker_rows);
        println!("Switch detail rows: {}", summary.switch_detail_rows);
        println!(
            "Connectivity nodes emitted: {}",
            summary.connectivity_node_rows
        );
    }
    if write_options.emit_diagram_layout && summary.diagram_object_rows > 0 {
        println!("Diagram objects emitted: {}", summary.diagram_object_rows);
        println!("Diagram points emitted: {}", summary.diagram_point_rows);
    }
    println!("Dynamics rows emitted: {}", summary.dynamics_rows_total);
    println!(
        "Dynamics DY-linked rows: {}",
        summary.dynamics_rows_dy_linked
    );
    println!(
        "Dynamics EQ-fallback rows: {}",
        summary.dynamics_rows_eq_fallback
    );

    Ok(())
}

fn validate_output_path(output: &Path) -> Result<()> {
    let extension = output
        .extension()
        .and_then(OsStr::to_str)
        .map(|value| value == "rpf")
        .unwrap_or(false);

    if !extension {
        bail!("Output must end with .rpf");
    }

    Ok(())
}

fn validate_rpf_input_path(input: &Path) -> Result<()> {
    let extension = input
        .extension()
        .and_then(OsStr::to_str)
        .map(|value| value == "rpf")
        .unwrap_or(false);

    if !extension {
        bail!("Input must end with .rpf");
    }

    Ok(())
}

fn collect_profile_paths(
    args: &ConvertArgs,
    cwd: &Path,
) -> Result<(DetectionMode, Vec<(String, PathBuf)>)> {
    match (&args.input_dir, has_explicit_profiles(args)) {
        (Some(_), true) => bail!("Use either --input-dir or explicit profile flags, not both"),
        (Some(input_dir), false) => {
            let profiles = detect_profiles(input_dir, cwd)?;
            Ok((DetectionMode::Auto, profiles))
        }
        (None, true) => {
            let profiles = explicit_profiles(args, cwd)?;
            Ok((DetectionMode::Explicit, profiles))
        }
        (None, false) => bail!("Provide --input-dir or at least --eq <PATH>"),
    }
}

fn has_explicit_profiles(args: &ConvertArgs) -> bool {
    args.eq.is_some()
        || args.eqbd.is_some()
        || args.tp.is_some()
        || args.sv.is_some()
        || args.ssh.is_some()
        || args.dy.is_some()
        || args.dl.is_some()
        || args.gl.is_some()
}

fn explicit_profiles(args: &ConvertArgs, cwd: &Path) -> Result<Vec<(String, PathBuf)>> {
    let eq = args.eq.as_ref().context("No EQ profile found")?;
    let mut profiles = Vec::with_capacity(8);
    profiles.push(("EQ".to_string(), normalize_existing_path(eq, cwd)?));

    for (profile_name, path) in [
        ("EQBD", args.eqbd.as_ref()),
        ("TP", args.tp.as_ref()),
        ("SV", args.sv.as_ref()),
        ("SSH", args.ssh.as_ref()),
        ("DY", args.dy.as_ref()),
        ("DL", args.dl.as_ref()),
        ("GL", args.gl.as_ref()),
    ] {
        if let Some(path) = path {
            profiles.push((
                profile_name.to_string(),
                normalize_existing_path(path, cwd)?,
            ));
        }
    }

    Ok(profiles)
}

fn detect_profiles(input_dir: &Path, cwd: &Path) -> Result<Vec<(String, PathBuf)>> {
    let resolved_input_dir = resolve_from_cwd(input_dir, cwd);
    let canonical_input_dir = canonicalize_or_original(&resolved_input_dir);

    if !canonical_input_dir.is_dir() {
        bail!(
            "Input directory does not exist or is not a directory: {}",
            canonical_input_dir.display()
        );
    }

    let mut detected = Vec::with_capacity(8);
    // EQBD before EQ so filename token matching stays unambiguous in listings.
    for profile in ["EQ", "EQBD", "TP", "SV", "SSH", "DY", "DL", "GL"] {
        if let Some(path) = find_profile_file(&canonical_input_dir, profile)? {
            detected.push((profile.to_string(), path));
        }
    }

    if !detected.iter().any(|(profile, _)| profile == "EQ") {
        bail!("No EQ profile found");
    }

    Ok(detected)
}

fn find_profile_file(input_dir: &Path, profile: &str) -> Result<Option<PathBuf>> {
    let candidate_files = collect_profile_candidate_files(input_dir)?;
    for path in candidate_files {
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if filename_matches_profile(name, profile) {
            return Ok(Some(canonicalize_or_original(&path)));
        }
    }

    Ok(None)
}

fn collect_profile_candidate_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![input_dir.to_path_buf()];

    while let Some(directory) = stack.pop() {
        let entries = fs::read_dir(&directory)
            .with_context(|| format!("failed to read input directory {}", directory.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| {
                format!(
                    "failed to enumerate input directory {}",
                    directory.display()
                )
            })?;

        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.is_file() && is_supported_profile_file(&path) {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

fn is_supported_profile_file(path: &Path) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .map(|ext| {
            let lowered = ext.to_ascii_lowercase();
            lowered == "xml" || lowered == "rdf"
        })
        .unwrap_or(false)
}

fn filename_matches_profile(name: &str, profile: &str) -> bool {
    let needle = profile.to_ascii_lowercase();
    let lowered = name.to_ascii_lowercase();

    lowered
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|token| token == needle)
}

fn normalize_existing_path(path: &Path, cwd: &Path) -> Result<PathBuf> {
    let resolved = resolve_from_cwd(path, cwd);
    let canonical = canonicalize_or_original(&resolved);
    if !canonical.is_file() {
        bail!("Input file does not exist: {}", canonical.display());
    }
    Ok(canonical)
}

fn normalize_output_path(path: &Path, cwd: &Path) -> PathBuf {
    let resolved = resolve_from_cwd(path, cwd);
    if let Some(file_name) = resolved.file_name()
        && let Some(parent) = resolved.parent()
        && let Ok(canonical_parent) = parent.canonicalize()
    {
        return canonical_parent.join(file_name);
    }
    resolved
}

fn resolve_from_cwd(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn canonicalize_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn mode_label(mode: DetectionMode) -> &'static str {
    match mode {
        DetectionMode::Explicit => "explicit",
        DetectionMode::Auto => "auto-detect",
    }
}

fn format_profile_summary(profile_paths: &[(String, PathBuf)]) -> String {
    profile_paths
        .iter()
        .map(|(profile, path)| format!("{profile}={}", path.display()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{detect_profiles, filename_matches_profile, is_supported_profile_file};
    use anyhow::Result;
    use std::fs;

    #[test]
    fn cgmes_profile_detection_filename_tokens_cover_24x_and_3x() {
        assert!(filename_matches_profile("SmallGrid_EQ.xml", "EQ"));
        assert!(filename_matches_profile("SmallGrid_EQBD.xml", "EQBD"));
        assert!(!filename_matches_profile("SmallGrid_EQBD.xml", "EQ"));
        assert!(filename_matches_profile("SmallGrid-TP.rdf", "TP"));
        assert!(filename_matches_profile("CGMES_2.4.15_case_SV.XML", "SV"));
        assert!(filename_matches_profile("CGMES-v3.0.3-SSH.xml", "SSH"));
        assert!(filename_matches_profile("2026_CASE_DY.rdf", "DY"));
        assert!(filename_matches_profile("SmallGrid_GL.xml", "GL"));
        assert!(!filename_matches_profile("SmallGrid_EQUIPMENT.xml", "EQ"));
    }

    #[test]
    fn cgmes_profile_detection_accepts_xml_and_rdf_extensions() {
        assert!(is_supported_profile_file(std::path::Path::new("a_EQ.xml")));
        assert!(is_supported_profile_file(std::path::Path::new("a_EQ.XML")));
        assert!(is_supported_profile_file(std::path::Path::new("a_EQ.rdf")));
        assert!(is_supported_profile_file(std::path::Path::new("a_EQ.RDF")));
        assert!(!is_supported_profile_file(std::path::Path::new("a_EQ.txt")));
    }

    #[test]
    fn cgmes_profile_detection_recurses_nested_directories() -> Result<()> {
        let temp_dir = std::env::temp_dir().join("raptrix_cgmes_detection_tests");
        let nested = temp_dir.join("v3").join("SmallGrid-Merged");
        fs::create_dir_all(&nested)?;

        fs::write(nested.join("SmallGrid_EQ.xml"), "<rdf:RDF/>")?;
        fs::write(nested.join("SmallGrid_TP.rdf"), "<rdf:RDF/>")?;
        fs::write(nested.join("SmallGrid_SV.xml"), "<rdf:RDF/>")?;

        let detected = detect_profiles(&temp_dir, &temp_dir)?;
        let names: Vec<String> = detected.iter().map(|(name, _)| name.clone()).collect();

        assert!(names.contains(&"EQ".to_string()));
        assert!(names.contains(&"TP".to_string()));
        assert!(names.contains(&"SV".to_string()));

        let _ = fs::remove_dir_all(&temp_dir);
        Ok(())
    }
}
