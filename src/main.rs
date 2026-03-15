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
use raptrix_cim_rs::write_complete_rpf;

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
    validate_output_path(&args.output)?;
    let (mode, profile_paths) = collect_profile_paths(&args)?;

    let input_refs: Vec<&str> = profile_paths.iter().map(|(_, path)| path.as_str()).collect();
    let output = args.output.to_string_lossy().into_owned();
    write_complete_rpf(&input_refs, &output)
        .with_context(|| format!("failed to write Raptrix CIM-Arrow output to {output}"))?;

    let file_size = fs::metadata(&args.output)
        .with_context(|| format!("failed to read output metadata for {}", args.output.display()))?
        .len();

    println!("{BRANDING}");
    println!("Mode: {}", mode_label(mode));
    println!("Profiles: {}", format_profile_summary(&profile_paths));
    println!("Output: {}", args.output.display());
    println!("File size: {} bytes", file_size);
    println!("Tables emitted: {CANONICAL_TABLE_COUNT}");

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

fn collect_profile_paths(args: &ConvertArgs) -> Result<(DetectionMode, Vec<(String, String)>)> {
    match (&args.input_dir, has_explicit_profiles(args)) {
        (Some(_), true) => bail!("Use either --input-dir or explicit profile flags, not both"),
        (Some(input_dir), false) => {
            let profiles = detect_profiles(input_dir)?;
            Ok((DetectionMode::Auto, profiles))
        }
        (None, true) => {
            let profiles = explicit_profiles(args)?;
            Ok((DetectionMode::Explicit, profiles))
        }
        (None, false) => bail!("Provide --input-dir or at least --eq <PATH>"),
    }
}

fn has_explicit_profiles(args: &ConvertArgs) -> bool {
    args.eq.is_some() || args.tp.is_some() || args.sv.is_some() || args.ssh.is_some() || args.dy.is_some()
}

fn explicit_profiles(args: &ConvertArgs) -> Result<Vec<(String, String)>> {
    let eq = args.eq.as_ref().context("No EQ profile found")?;
    let mut profiles = Vec::with_capacity(5);
    profiles.push(("EQ".to_string(), normalize_existing_path(eq)?));

    for (profile_name, path) in [
        ("TP", args.tp.as_ref()),
        ("SV", args.sv.as_ref()),
        ("SSH", args.ssh.as_ref()),
        ("DY", args.dy.as_ref()),
    ] {
        if let Some(path) = path {
            profiles.push((profile_name.to_string(), normalize_existing_path(path)?));
        }
    }

    Ok(profiles)
}

fn detect_profiles(input_dir: &Path) -> Result<Vec<(String, String)>> {
    if !input_dir.is_dir() {
        bail!("Input directory does not exist or is not a directory: {}", input_dir.display());
    }

    let mut detected = Vec::with_capacity(5);
    for profile in ["EQ", "TP", "SV", "SSH", "DY"] {
        if let Some(path) = find_profile_file(input_dir, profile)? {
            detected.push((profile.to_string(), path));
        }
    }

    if !detected.iter().any(|(profile, _)| profile == "EQ") {
        bail!("No EQ profile found");
    }

    Ok(detected)
}

fn find_profile_file(input_dir: &Path, profile: &str) -> Result<Option<String>> {
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
            return Ok(Some(path.to_string_lossy().into_owned()));
        }
    }

    Ok(None)
}

fn normalize_existing_path(path: &Path) -> Result<String> {
    if !path.is_file() {
        bail!("Input file does not exist: {}", path.display());
    }
    Ok(path.to_string_lossy().into_owned())
}

fn mode_label(mode: DetectionMode) -> &'static str {
    match mode {
        DetectionMode::Explicit => "explicit",
        DetectionMode::Auto => "auto-detect",
    }
}

fn format_profile_summary(profile_paths: &[(String, String)]) -> String {
    profile_paths
        .iter()
        .map(|(profile, path)| format!("{profile}={path}"))
        .collect::<Vec<_>>()
        .join(", ")
}
