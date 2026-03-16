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

use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Parser, Subcommand};

use raptrix_cim_rs::arrow_schema::BRANDING;
use raptrix_cim_rs::rpf_writer::{
    write_complete_rpf_with_options, BusResolutionMode, WriteOptions,
};

const COPYRIGHT: &str = "Copyright (c) 2026 Musto Technologies LLC";
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
}

/// Conversion arguments for explicit profile paths or auto-detection.
#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("profile_mode")
        .args(["input_dir", "eq", "tp", "sv", "ssh", "dy"])
        .multiple(true)
))]
struct ConvertArgs {
    /// EQ profile path. Required when `--input-dir` is not used.
    #[arg(long)]
    eq: Option<PathBuf>,

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
    }
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
            emit_connectivity_groups: true,
        }
    } else {
        WriteOptions {
            // Topological is default for interoperability.
            bus_resolution_mode: BusResolutionMode::Topological,
            emit_connectivity_groups: false,
        }
    };

    let summary = write_complete_rpf_with_options(&input_refs, &output, &write_options)
        .with_context(|| format!("failed to write Raptrix CIM-Arrow output to {output}"))?;

    let file_size = fs::metadata(&output_path)
        .with_context(|| format!("failed to read output metadata for {}", output_path.display()))?
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
    };
    println!("Tables emitted: {emitted_tables}");
    if summary.tp_merged {
        println!(
            "TP merge bus reduction: {} -> {} ({:.1}% reduction)",
            summary.connectivity_bus_count,
            summary.final_bus_count,
            100.0
                * (summary.connectivity_bus_count.saturating_sub(summary.final_bus_count)) as f64
                / summary.connectivity_bus_count as f64
        );
    }
    if write_options.emit_connectivity_groups {
        println!(
            "Connectivity groups emitted: {}",
            summary.connectivity_groups_rows
        );
    }

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
    args.eq.is_some() || args.tp.is_some() || args.sv.is_some() || args.ssh.is_some() || args.dy.is_some()
}

fn explicit_profiles(args: &ConvertArgs, cwd: &Path) -> Result<Vec<(String, PathBuf)>> {
    let eq = args.eq.as_ref().context("No EQ profile found")?;
    let mut profiles = Vec::with_capacity(5);
    profiles.push(("EQ".to_string(), normalize_existing_path(eq, cwd)?));

    for (profile_name, path) in [
        ("TP", args.tp.as_ref()),
        ("SV", args.sv.as_ref()),
        ("SSH", args.ssh.as_ref()),
        ("DY", args.dy.as_ref()),
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

    let mut detected = Vec::with_capacity(5);
    for profile in ["EQ", "TP", "SV", "SSH", "DY"] {
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
    let mut entries = fs::read_dir(input_dir)
        .with_context(|| format!("failed to read input directory {}", input_dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to enumerate input directory {}", input_dir.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    let needle = profile.to_ascii_lowercase();
    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let lower_name = name.to_ascii_lowercase();
        if lower_name.contains(&needle) && lower_name.ends_with(".xml") {
            return Ok(Some(canonicalize_or_original(&path)));
        }
    }

    Ok(None)
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
    if let Some(file_name) = resolved.file_name() {
        if let Some(parent) = resolved.parent() {
            if let Ok(canonical_parent) = parent.canonicalize() {
                return canonical_parent.join(file_name);
            }
        }
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
