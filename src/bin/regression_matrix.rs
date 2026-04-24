use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};

fn print_usage() {
    eprintln!(
        "Usage:\n  cargo rpf-regression -- --data-root <CGMES_v3_root> [matrix args]\n\nExamples:\n  cargo rpf-regression -- --data-root C:\\raptrix-cim-tests\\...\\v3.0 --profiles both --clean\n  cargo rpf-regression -- --profiles both --clean"
    );
}

fn parse_args(args: Vec<String>) -> (Option<String>, Vec<String>) {
    let mut data_root: Option<String> = None;
    let mut forwarded: Vec<String> = Vec::new();

    let mut idx = 0;
    while idx < args.len() {
        if args[idx] == "--data-root" {
            if idx + 1 >= args.len() {
                eprintln!("missing value for --data-root");
                print_usage();
                std::process::exit(2);
            }
            data_root = Some(args[idx + 1].clone());
            idx += 2;
            continue;
        }

        forwarded.push(args[idx].clone());
        idx += 1;
    }

    if forwarded.is_empty() {
        forwarded = vec![
            "--profiles".to_string(),
            "both".to_string(),
            "--clean".to_string(),
        ];
    }

    (data_root, forwarded)
}

fn run_python(
    python_cmd: &str,
    script: &PathBuf,
    forwarded: &[String],
    data_root: &str,
) -> std::io::Result<std::process::ExitStatus> {
    Command::new(python_cmd)
        .arg(script)
        .args(forwarded)
        .env("RAPTRIX_TEST_DATA_ROOT", data_root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
}

fn resolve_test_data_root() -> Option<String> {
    if let Ok(value) = env::var("RAPTRIX_TEST_DATA_ROOT") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let fallback = fs::read_to_string(".raptrix-test-data-root").ok()?;
    let trimmed = fallback.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn main() -> ExitCode {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = repo_root.join("tests").join("generate_rpf_matrix.py");
    if !script.is_file() {
        eprintln!("matrix script not found: {}", script.display());
        return ExitCode::from(1);
    }

    let cli_args: Vec<String> = env::args().skip(1).collect();
    let (data_root_override, forwarded) = parse_args(cli_args);

    let data_root = data_root_override.or_else(resolve_test_data_root);
    let Some(data_root) = data_root else {
        eprintln!(
            "RAPTRIX_TEST_DATA_ROOT is not set. Pass --data-root, set the env var, or create .raptrix-test-data-root."
        );
        print_usage();
        return ExitCode::from(2);
    };

    let status = run_python("python", &script, &forwarded, &data_root)
        .or_else(|_| run_python("py", &script, &forwarded, &data_root));

    match status {
        Ok(exit_status) => {
            if exit_status.success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(exit_status.code().unwrap_or(1) as u8)
            }
        }
        Err(err) => {
            eprintln!("failed to run Python for matrix generation: {err}");
            ExitCode::from(1)
        }
    }
}
