use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use raptrix_cim_rs::arrow_schema::{SCHEMA_VERSION, all_table_schemas};
use raptrix_cim_rs::rpf_writer::{
    DetachedIslandPolicy, WriteOptions, rpf_file_metadata, summarize_rpf,
    write_complete_rpf_with_options,
};

static OUTPUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct FixtureCase {
    name: String,
    eq: PathBuf,
    dy: Option<PathBuf>,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixtures_dir() -> PathBuf {
    repo_root().join("tests").join("data").join("fixtures")
}

fn unique_temp_rpf_path(label: &str) -> PathBuf {
    let seq = OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "raptrix_fixture_v091_{}_{}_{}.rpf",
        label,
        std::process::id(),
        seq
    ))
}

fn has_terminal_payload(path: &Path) -> bool {
    let text = fs::read_to_string(path).unwrap_or_default();
    text.contains("Terminal.ConductingEquipment") || text.contains("<cim:Terminal")
}

fn discover_fixture_cases() -> Result<Vec<FixtureCase>> {
    let mut cases = Vec::new();
    let base = fixtures_dir();

    for entry in
        fs::read_dir(&base).with_context(|| format!("failed to read {}", base.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("xml") {
            continue;
        }
        if !has_terminal_payload(&path) {
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if file_name.ends_with("_DY.xml") {
            continue;
        }

        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if let Some(prefix) = stem.strip_suffix("_EQ") {
                let dy_path = base.join(format!("{prefix}_DY.xml"));
                cases.push(FixtureCase {
                    name: prefix.to_string(),
                    eq: path,
                    dy: if dy_path.is_file() {
                        Some(dy_path)
                    } else {
                        None
                    },
                });
                continue;
            }
        }

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("fixture")
            .to_string();
        cases.push(FixtureCase {
            name,
            eq: path,
            dy: None,
        });
    }

    cases.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(cases)
}

#[test]
fn all_workspace_fixture_cim_cases_emit_v091_compliant_rpf() -> Result<()> {
    let cases = discover_fixture_cases()?;
    assert!(
        !cases.is_empty(),
        "expected at least one local fixture case"
    );

    let required_table_names: Vec<String> = all_table_schemas()
        .into_iter()
        .map(|(name, _)| name.to_string())
        .collect();

    for case in cases {
        let output = unique_temp_rpf_path(&case.name);

        let mut owned_inputs = vec![case.eq.to_string_lossy().into_owned()];
        if let Some(ref dy) = case.dy {
            owned_inputs.push(dy.to_string_lossy().into_owned());
        }
        let input_refs: Vec<&str> = owned_inputs.iter().map(String::as_str).collect();

        write_complete_rpf_with_options(
            &input_refs,
            output.to_string_lossy().as_ref(),
            &WriteOptions {
                detached_island_policy: DetachedIslandPolicy::Permissive,
                emit_diagram_layout: true,
                ..Default::default()
            },
        )
        .with_context(|| format!("failed writing RPF for fixture case '{}'", case.name))?;

        let summary = summarize_rpf(&output)
            .with_context(|| format!("failed to summarize RPF for fixture case '{}'", case.name))?;
        assert!(
            summary.has_all_canonical_tables,
            "missing canonical table(s) for case '{}'; found {} tables",
            case.name,
            summary.tables.len()
        );
        assert_eq!(
            summary.canonical_table_count,
            required_table_names.len(),
            "canonical table count mismatch for case '{}'",
            case.name
        );

        let present: HashSet<String> = summary
            .tables
            .iter()
            .map(|t| t.table_name.clone())
            .collect();
        for required in &required_table_names {
            assert!(
                present.contains(required),
                "missing required table '{}' for case '{}'",
                required,
                case.name
            );
        }

        let metadata = rpf_file_metadata(&output)?;
        assert_eq!(
            metadata.get("rpf_version"),
            Some(&SCHEMA_VERSION.to_string()),
            "unexpected rpf_version for case '{}'",
            case.name
        );
        assert_eq!(
            metadata.get("raptrix.version"),
            Some(&SCHEMA_VERSION.to_string()),
            "unexpected raptrix.version for case '{}'",
            case.name
        );
    }

    Ok(())
}
