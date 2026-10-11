//! Write schemas/dynamics_model_dictionary.json for the C++ parity test.
//!
//! Usage: cargo run -p raptrix-cim-arrow --bin export-dyn-dictionary -- <out.json>

use std::env;
use std::fs;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: export-dyn-dictionary <out.json>");
        return ExitCode::from(2);
    };
    let json = raptrix_cim_arrow::dyn_dictionary::dictionary_json();
    if let Err(err) = fs::write(&path, json) {
        eprintln!("write {path}: {err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
