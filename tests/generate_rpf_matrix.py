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
    diagram_objects_emitted: int
    diagram_points_emitted: int
    node_breaker_detail_rows: int
    switch_detail_rows: int


def _extract_metric(stdout_text: str, label: str) -> int:
    pattern = rf"^{re.escape(label)}:\s*(\d+)\s*$"
    match = re.search(pattern, stdout_text, flags=re.MULTILINE)
    if not match:
        return 0
    return int(match.group(1))


def _sanitize_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        rel = path.resolve().relative_to(REPO_ROOT.resolve())
        return rel.as_posix()
    except ValueError:
        return f"<external>/{path.name}"


def _sanitize_env_path(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        rel = Path(value).resolve().relative_to(REPO_ROOT.resolve())
        return rel.as_posix()
    except ValueError:
        return "<external>"


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
    diagram_objects_emitted = _extract_metric(completed.stdout, "Diagram objects emitted")
    diagram_points_emitted = _extract_metric(completed.stdout, "Diagram points emitted")
    node_breaker_detail_rows = _extract_metric(completed.stdout, "Node-breaker detail rows")
    switch_detail_rows = _extract_metric(completed.stdout, "Switch detail rows")

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
        diagram_objects_emitted=diagram_objects_emitted,
        diagram_points_emitted=diagram_points_emitted,
        node_breaker_detail_rows=node_breaker_detail_rows,
        switch_detail_rows=switch_detail_rows,
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
        data = {
            "case_name": case.case_name,
            "source_kind": case.source_kind,
            "eq": _sanitize_path(case.eq),
            "tp": _sanitize_path(case.tp),
            "sv": _sanitize_path(case.sv),
            "ssh": _sanitize_path(case.ssh),
            "dy": _sanitize_path(case.dy),
            "dl": _sanitize_path(case.dl),
        }
        serialized_cases.append(data)

    serialized_results = []
    for result in all_results:
        data = asdict(result)
        data["output_path"] = _sanitize_path(Path(result.output_path))
        serialized_results.append(data)

    report = {
        "repo_root": ".",
        "results_dir": str(RESULTS_DIR.relative_to(REPO_ROOT)).replace("\\", "/"),
        "raptrix_test_data_root": _sanitize_env_path(os.environ.get("RAPTRIX_TEST_DATA_ROOT")),
        "include_ssh_dy": args.include_ssh_dy,
        "cases_with_dy_profile": sum(1 for case in cases if case.dy is not None),
        "cases": serialized_cases,
        "summary": summary,
        "results": serialized_results,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = RESULTS_DIR / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Write a compact markdown summary for quick downstream inspection.
    lines = [
        "# RPF Matrix Report",
        "",
        f"Results directory: {RESULTS_DIR.relative_to(REPO_ROOT).as_posix()}",
        f"RAPTRIX_TEST_DATA_ROOT: {_sanitize_env_path(os.environ.get('RAPTRIX_TEST_DATA_ROOT')) or '<unset>'}",
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

    # Studio-focused ranking for external corpus models.
    by_case: dict[str, list[RunResult]] = {}
    for item in all_results:
        if item.mode != "release" or not item.case_name.startswith("external_"):
            continue
        by_case.setdefault(item.case_name, []).append(item)

    ranking: list[tuple[str, int, int, int, int]] = []
    for case_name, case_runs in by_case.items():
        best = max(
            case_runs,
            key=lambda r: (
                r.diagram_points_emitted,
                r.diagram_objects_emitted,
                r.node_breaker_detail_rows,
                r.switch_detail_rows,
            ),
        )
        score = (
            (best.diagram_points_emitted * 10)
            + (best.diagram_objects_emitted * 5)
            + best.node_breaker_detail_rows
            + best.switch_detail_rows
        )
        ranking.append(
            (
                case_name,
                score,
                best.diagram_objects_emitted,
                best.diagram_points_emitted,
                best.node_breaker_detail_rows,
            )
        )

    ranking.sort(key=lambda x: (x[1], x[3], x[2], x[4]), reverse=True)
    lines += ["", "## Studio Visualization Shortlist (release, external cases)", ""]
    lines.append("| Rank | Case | Score | Diagram Objects | Diagram Points | Node-Breaker Rows |")
    lines.append("|---:|---|---:|---:|---:|---:|")
    for idx, item in enumerate(ranking, start=1):
        lines.append(
            f"| {idx} | {item[0]} | {item[1]} | {item[2]} | {item[3]} | {item[4]} |"
        )

    (RESULTS_DIR / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote report: {report_path}")
    return 0 if all(r.succeeded for r in all_results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
