"""Generate persistent RPF outputs for all discoverable CIM sources.

Usage:
  python tests/generate_rpf_matrix.py --profiles both

Outputs:
  - RPF files under tests/data/external/results/{debug|release}/
  - Timing + status report at tests/data/external/results/report.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "tests" / "data" / "external" / "results"
FIXTURES_DIR = REPO_ROOT / "tests" / "data" / "fixtures"


@dataclass
class Case:
    case_name: str
    source_kind: str
    eq: Path
    tp: Path | None = None
    sv: Path | None = None
    ssh: Path | None = None
    dy: Path | None = None
    dl: Path | None = None


@dataclass
class RunResult:
    mode: str
    case_name: str
    variant: str
    command: list[str]
    output_path: str
    elapsed_seconds: float
    succeeded: bool
    return_code: int
    stdout_tail: str
    stderr_tail: str
    dynamics_rows_emitted: int
    dynamics_rows_dy_linked: int
    dynamics_rows_eq_fallback: int


def _extract_metric(stdout_text: str, label: str) -> int:
    pattern = rf"^{re.escape(label)}:\s*(\d+)\s*$"
    match = re.search(pattern, stdout_text, flags=re.MULTILINE)
    if not match:
        return 0
    return int(match.group(1))


def _safe_name(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in text)


def discover_cases() -> list[Case]:
    cases: list[Case] = []

    def has_terminal_payload(path: Path) -> bool:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8", errors="ignore")
        return "Terminal.ConductingEquipment" in text or "<cim:Terminal" in text

    # Always include local fixture cases that are committed with the repo.
    for eq in sorted(FIXTURES_DIR.glob("*.xml")):
        if not has_terminal_payload(eq):
            continue
        if eq.name.endswith("_EQ.xml"):
            # Treat as CGMES-style naming if present.
            model = eq.stem[:-3]
            base = eq.parent
            cases.append(
                Case(
                    case_name=f"fixture_{_safe_name(model)}",
                    source_kind="workspace-fixture",
                    eq=eq,
                    tp=base / f"{model}_TP.xml" if (base / f"{model}_TP.xml").is_file() else None,
                    sv=base / f"{model}_SV.xml" if (base / f"{model}_SV.xml").is_file() else None,
                    ssh=base / f"{model}_SSH.xml" if (base / f"{model}_SSH.xml").is_file() else None,
                    dy=base / f"{model}_DY.xml" if (base / f"{model}_DY.xml").is_file() else None,
                    dl=base / f"{model}_DL.xml" if (base / f"{model}_DL.xml").is_file() else None,
                )
            )
        else:
            # Generic XML fixture, run as EQ-only case.
            cases.append(
                Case(
                    case_name=f"fixture_{_safe_name(eq.stem)}",
                    source_kind="workspace-fixture",
                    eq=eq,
                )
            )

    # Optionally include external CGMES corpora when configured.
    data_root = os.environ.get("RAPTRIX_TEST_DATA_ROOT")
    if data_root:
        root = Path(data_root)
        if root.is_dir():
            # Discover every directory that directly contains *_EQ.xml files.
            # This covers *-Merged assembled cases as well as standalone cases
            # (e.g. PowerFlow/PowerFlow and PST/*) that use the same naming
            # convention but live outside Merged directories.
            eq_dirs: dict[Path, list[Path]] = {}
            for eq_file in sorted(root.glob("**/*_EQ.xml")):
                eq_dirs.setdefault(eq_file.parent, []).append(eq_file)

            for dir_path, eq_files_in_dir in sorted(eq_dirs.items()):
                for eq in sorted(eq_files_in_dir):
                    model = eq.stem[:-3]
                    cases.append(
                        Case(
                            case_name=f"external_{_safe_name(model)}",
                            source_kind="external-cgmes",
                            eq=eq,
                            tp=dir_path / f"{model}_TP.xml" if (dir_path / f"{model}_TP.xml").is_file() else None,
                            sv=dir_path / f"{model}_SV.xml" if (dir_path / f"{model}_SV.xml").is_file() else None,
                            ssh=dir_path / f"{model}_SSH.xml" if (dir_path / f"{model}_SSH.xml").is_file() else None,
                            dy=dir_path / f"{model}_DY.xml" if (dir_path / f"{model}_DY.xml").is_file() else None,
                            dl=dir_path / f"{model}_DL.xml" if (dir_path / f"{model}_DL.xml").is_file() else None,
                        )
                    )

    # Deduplicate by case name and eq path.
    dedup: dict[tuple[str, str], Case] = {}
    for case in cases:
        dedup[(case.case_name, str(case.eq.resolve()))] = case
    return sorted(dedup.values(), key=lambda c: c.case_name)


def variants_for_case(case: Case, *, include_ssh_dy: bool) -> list[tuple[str, list[str]]]:
    base = ["convert", "--eq", str(case.eq)]
    if case.tp:
        base += ["--tp", str(case.tp)]
    if case.sv:
        base += ["--sv", str(case.sv)]
    if include_ssh_dy and case.ssh:
        base += ["--ssh", str(case.ssh)]
    if include_ssh_dy and case.dy:
        base += ["--dy", str(case.dy)]
    if case.dl:
        base += ["--dl", str(case.dl)]

    variants: list[tuple[str, list[str]]] = []
    variants.append(("topological", base.copy()))
    variants.append(("connectivity_detail", base + ["--connectivity-detail"]))
    variants.append(("node_breaker", base + ["--connectivity-detail", "--node-breaker"]))
    variants.append(("no_diagram", base + ["--no-diagram"]))
    return variants


def run_cli(mode: str, case: Case, variant: str, args: list[str], output_dir: Path) -> RunResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{case.case_name}_{variant}.rpf"

    cmd = ["cargo", "run", "--bin", "raptrix-cim-rs"]
    if mode == "release":
        cmd.append("--release")
    cmd += ["--"] + args + ["--output", str(output_path), "--verbose"]

    started = time.perf_counter()
    completed = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    elapsed = time.perf_counter() - started
    dynamics_rows_emitted = _extract_metric(completed.stdout, "Dynamics rows emitted")
    dynamics_rows_dy_linked = _extract_metric(completed.stdout, "Dynamics DY-linked rows")
    dynamics_rows_eq_fallback = _extract_metric(completed.stdout, "Dynamics EQ-fallback rows")

    return RunResult(
        mode=mode,
        case_name=case.case_name,
        variant=variant,
        command=cmd,
        output_path=str(output_path),
        elapsed_seconds=elapsed,
        succeeded=completed.returncode == 0,
        return_code=completed.returncode,
        stdout_tail="\n".join(completed.stdout.strip().splitlines()[-20:]),
        stderr_tail="\n".join(completed.stderr.strip().splitlines()[-20:]),
        dynamics_rows_emitted=dynamics_rows_emitted,
        dynamics_rows_dy_linked=dynamics_rows_dy_linked,
        dynamics_rows_eq_fallback=dynamics_rows_eq_fallback,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate RPF matrix for discovered CIM sources")
    parser.add_argument(
        "--profiles",
        choices=["debug", "release", "both"],
        default="both",
        help="Build profile(s) to run",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing results directory before generating outputs",
    )
    parser.add_argument(
        "--include-ssh-dy",
        action="store_true",
        help="Include SSH and DY profiles in conversion inputs (strict multi-profile mode)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.clean and RESULTS_DIR.exists():
        shutil.rmtree(RESULTS_DIR)

    cases = discover_cases()
    if not cases:
        print("No CIM source files discovered. Nothing to run.")
        return 1

    modes = ["debug", "release"] if args.profiles == "both" else [args.profiles]

    all_results: list[RunResult] = []
    for mode in modes:
        mode_dir = RESULTS_DIR / mode
        for case in cases:
            for variant, variant_args in variants_for_case(case, include_ssh_dy=args.include_ssh_dy):
                result = run_cli(mode, case, variant, variant_args, mode_dir)
                all_results.append(result)
                status = "PASS" if result.succeeded else "FAIL"
                print(
                    f"[{mode}] {case.case_name}/{variant}: {status} "
                    f"({result.elapsed_seconds:.3f}s) -> {result.output_path}"
                )

    # Summaries by mode.
    summary: dict[str, dict[str, float | int]] = {}
    for mode in modes:
        selected = [r for r in all_results if r.mode == mode]
        passed = [r for r in selected if r.succeeded]
        elapsed_total = sum(r.elapsed_seconds for r in selected)
        elapsed_pass = sum(r.elapsed_seconds for r in passed)
        summary[mode] = {
            "runs": len(selected),
            "passed": len(passed),
            "failed": len(selected) - len(passed),
            "elapsed_total_seconds": round(elapsed_total, 6),
            "elapsed_passed_seconds": round(elapsed_pass, 6),
            "dynamics_rows_emitted": sum(r.dynamics_rows_emitted for r in passed),
            "dynamics_rows_dy_linked": sum(r.dynamics_rows_dy_linked for r in passed),
            "dynamics_rows_eq_fallback": sum(r.dynamics_rows_eq_fallback for r in passed),
        }

    serialized_cases = []
    for case in cases:
        data = asdict(case)
        for key in ("eq", "tp", "sv", "ssh", "dy", "dl"):
            value = data.get(key)
            data[key] = str(value) if value is not None else None
        serialized_cases.append(data)

    report = {
        "repo_root": str(REPO_ROOT),
        "results_dir": str(RESULTS_DIR),
        "raptrix_test_data_root": os.environ.get("RAPTRIX_TEST_DATA_ROOT"),
        "include_ssh_dy": args.include_ssh_dy,
        "cases_with_dy_profile": sum(1 for case in cases if case.dy is not None),
        "cases": serialized_cases,
        "summary": summary,
        "results": [asdict(result) for result in all_results],
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = RESULTS_DIR / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Write a compact markdown summary for quick downstream inspection.
    lines = [
        "# RPF Matrix Report",
        "",
        f"Results directory: {RESULTS_DIR}",
        f"RAPTRIX_TEST_DATA_ROOT: {os.environ.get('RAPTRIX_TEST_DATA_ROOT', '<unset>')}",
        f"Include SSH/DY: {args.include_ssh_dy}",
        f"Cases with DY profile: {sum(1 for case in cases if case.dy is not None)}",
        "",
        "## Mode Summary",
        "",
        "| Mode | Runs | Passed | Failed | Total Seconds | Dynamics Rows | DY-linked | EQ-fallback |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for mode in modes:
        s = summary[mode]
        lines.append(
            f"| {mode} | {s['runs']} | {s['passed']} | {s['failed']} | {s['elapsed_total_seconds']:.3f} | {s['dynamics_rows_emitted']} | {s['dynamics_rows_dy_linked']} | {s['dynamics_rows_eq_fallback']} |"
        )
    lines += ["", "## Failed Runs", ""]
    failed = [r for r in all_results if not r.succeeded]
    if not failed:
        lines.append("None")
    else:
        lines.append("| Mode | Case | Variant | Return Code |")
        lines.append("|---|---|---|---:|")
        for item in failed:
            lines.append(
                f"| {item.mode} | {item.case_name} | {item.variant} | {item.return_code} |"
            )

    (RESULTS_DIR / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote report: {report_path}")
    return 0 if all(r.succeeded for r in all_results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
