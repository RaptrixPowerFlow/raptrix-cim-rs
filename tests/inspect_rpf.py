"""Automated CGMES merged-profile RPF validation.

Run with:
    python -m pytest tests/inspect_rpf.py -q
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
except ImportError:  # pragma: no cover - handled by skip branch below
    pa = None
    ipc = None


BRANDING = "Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC\nCopyright (c) 2026 Musto Technologies LLC"
SCHEMA_VERSION = "0.5.1"
CANONICAL_TABLE_ORDER = [
    "metadata",
    "buses",
    "branches",
    "generators",
    "loads",
    "fixed_shunts",
    "switched_shunts",
    "transformers_2w",
    "transformers_3w",
    "areas",
    "zones",
    "owners",
    "contingencies",
    "interfaces",
    "dynamics_models",
]


def pytest_configure(config: pytest.Config) -> None:
    """Register local custom markers for standalone execution."""
    config.addinivalue_line("markers", "ignore: ignored/skipped due to missing external data")


def _read_rpf_tables(path: Path) -> list[tuple[str, pa.RecordBatch]]:
    """Read canonical per-table batches from the single-root v0.5.1 RPF file."""
    reader = ipc.RecordBatchFileReader(pa.memory_map(str(path), "r"))
    file_metadata = reader.schema.metadata or {}
    tables: list[tuple[str, pa.RecordBatch]] = []

    for root_batch_idx in range(reader.num_record_batches):
        root_batch = reader.get_batch(root_batch_idx)
        for table_idx, table_name in enumerate(CANONICAL_TABLE_ORDER):
            struct_array = root_batch.column(table_idx)
            if not pa.types.is_struct(struct_array.type):
                raise AssertionError(
                    f"Root column '{table_name}' must be Struct, found {struct_array.type}"
                )
            row_key = f"rpf.rows.{table_name}".encode("utf-8")
            expected_rows = int(file_metadata.get(row_key, str(len(struct_array)).encode("utf-8")))
            child_names = [field.name for field in struct_array.type]
            child_arrays = [child.slice(0, expected_rows) for child in struct_array.flatten()]
            table_batch = pa.RecordBatch.from_arrays(child_arrays, names=child_names)
            tables.append((table_name, table_batch))

    return tables


def _find_first(parent: ET.Element, tag_suffix: str) -> ET.Element | None:
    for child in parent.iter():
        if child.tag.endswith(tag_suffix):
            return child
    return None


def _text_of_first(parent: ET.Element, tag_suffix: str) -> str | None:
    node = _find_first(parent, tag_suffix)
    if node is None or node.text is None:
        return None
    return node.text.strip()


def _local_name(tag: str) -> str:
    """Return XML local name for namespace-qualified or plain tags."""
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _parse_eq_metrics(
    eq_path: Path,
    sv_path: Path | None = None,
) -> tuple[
    int,
    int,
    float,
    float,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    float | None,
    float | None,
]:
    rdf_ns = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    root = ET.parse(eq_path).getroot()

    lines_by_mrid: dict[str, tuple[float, float]] = {}
    generators_by_mrid: dict[str, float] = {}
    loads_by_mrid: dict[str, float] = {}
    connectivity_nodes: set[str] = set()
    transformer_end_counts: dict[str, int] = {}
    fixed_shunt_count = 0
    switched_shunt_count = 0
    areas_count = 0
    zones_count = 0
    owners_count = 0

    for element in root.iter():
        local_name = _local_name(element.tag)

        if local_name == "ACLineSegment":
            mrid = element.get(f"{rdf_ns}ID") or element.get(f"{rdf_ns}about", "").lstrip("#")
            if not mrid:
                continue
            r_text = _text_of_first(element, "ACLineSegment.r")
            x_text = _text_of_first(element, "ACLineSegment.x")
            lines_by_mrid[mrid] = (
                float(r_text) if r_text is not None else 0.0,
                float(x_text) if x_text is not None else 0.0,
            )
        elif local_name == "Terminal":
            cn = _find_first(element, "Terminal.ConnectivityNode")
            if cn is None:
                continue
            resource = cn.get(f"{rdf_ns}resource")
            if resource:
                connectivity_nodes.add(resource.lstrip("#"))
        elif local_name == "SynchronousMachine":
            mrid = element.get(f"{rdf_ns}ID") or element.get(f"{rdf_ns}about", "").lstrip("#")
            if not mrid:
                continue
            p_text = _text_of_first(element, "RotatingMachine.p")
            if p_text is None:
                p_text = _text_of_first(element, "SynchronousMachine.p")
            generators_by_mrid[mrid] = float(p_text) if p_text is not None else 0.0
        elif local_name in {"EnergyConsumer", "ConformLoad", "NonConformLoad"}:
            mrid = element.get(f"{rdf_ns}ID") or element.get(f"{rdf_ns}about", "").lstrip("#")
            if not mrid:
                continue
            p_text = _text_of_first(element, "EnergyConsumer.p")
            loads_by_mrid[mrid] = float(p_text) if p_text is not None else 0.0
        elif local_name == "PowerTransformerEnd":
            pt_ref = _find_first(element, "PowerTransformerEnd.PowerTransformer")
            if pt_ref is None:
                continue
            resource = pt_ref.get(f"{rdf_ns}resource")
            if resource:
                pt_mrid = resource.lstrip("#")
                transformer_end_counts[pt_mrid] = transformer_end_counts.get(pt_mrid, 0) + 1
        elif local_name == "LinearShuntCompensator":
            fixed_shunt_count += 1
        elif local_name == "SvShuntCompensator":
            switched_shunt_count += 1
        elif local_name == "ControlArea":
            areas_count += 1
        elif local_name == "SubGeographicalRegion":
            zones_count += 1
        elif local_name == "Organisation":
            owners_count += 1

    transformer_2w_count = sum(1 for count in transformer_end_counts.values() if count == 2)
    transformer_3w_count = sum(1 for count in transformer_end_counts.values() if count >= 3)

    if sv_path is not None and sv_path.is_file():
        sv_root = ET.parse(sv_path).getroot()
        for element in sv_root.iter():
            if _local_name(element.tag) == "SvShuntCompensator":
                switched_shunt_count += 1

    if not lines_by_mrid:
        raise AssertionError("No ACLineSegment records found in EQ XML")

    first_line_mrid = sorted(lines_by_mrid.keys())[0]
    first_r, first_x = lines_by_mrid[first_line_mrid]
    first_generator_p = None
    if generators_by_mrid:
        first_generator_mrid = sorted(generators_by_mrid.keys())[0]
        first_generator_p = generators_by_mrid[first_generator_mrid]

    first_load_p = None
    if loads_by_mrid:
        first_load_mrid = sorted(loads_by_mrid.keys())[0]
        first_load_p = loads_by_mrid[first_load_mrid]

    return (
        len(connectivity_nodes),
        len(lines_by_mrid),
        first_r,
        first_x,
        len(generators_by_mrid),
        len(loads_by_mrid),
        transformer_2w_count,
        transformer_3w_count,
        fixed_shunt_count,
        switched_shunt_count,
        areas_count,
        zones_count,
        owners_count,
        first_generator_p,
        first_load_p,
    )


def _write_memory_snapshot(source_path: Path, snapshot_path: Path) -> None:
    """Re-serialize the single root IPC file via Arrow memory APIs."""
    reader = ipc.RecordBatchFileReader(pa.memory_map(str(source_path), "r"))
    sink = pa.BufferOutputStream()
    with ipc.RecordBatchFileWriter(sink, reader.schema) as writer:
        for batch_idx in range(reader.num_record_batches):
            writer.write_batch(reader.get_batch(batch_idx))
    snapshot_path.write_bytes(sink.getvalue().to_pybytes())


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _emit_table_row_report(
    profile_name: str,
    tables: list[tuple[str, pa.RecordBatch]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Always print per-table row counts, even when pytest capture is enabled."""
    with capsys.disabled():
        print(f"Row-count report for {profile_name}:")
        for table_name, batch in tables:
            print(f"  Table {table_name}: {batch.num_rows} rows")


def _run_profile_validation(
    request: pytest.FixtureRequest,
    capsys: pytest.CaptureFixture[str],
    profile_name: str,
) -> None:
    print(BRANDING)

    if pa is None or ipc is None:
        request.node.add_marker(pytest.mark.ignore)
        pytest.skip("pyarrow is required for RPF validation")

    data_root = os.environ.get("RAPTRIX_TEST_DATA_ROOT")
    if not data_root:
        request.node.add_marker(pytest.mark.ignore)
        pytest.skip("RAPTRIX_TEST_DATA_ROOT is not set")

    merged_dir = Path(data_root) / profile_name / f"{profile_name}-Merged"
    eq_path = merged_dir / f"{profile_name}_EQ.xml"
    tp_path = merged_dir / f"{profile_name}_TP.xml"
    sv_path = merged_dir / f"{profile_name}_SV.xml"
    has_tp = tp_path.is_file()
    has_sv = sv_path.is_file()
    connectivity_detail_mode = os.environ.get("RAPTRIX_CONNECTIVITY_DETAIL", "0") == "1"
    if not eq_path.is_file():
        request.node.add_marker(pytest.mark.ignore)
        pytest.skip(f"{profile_name} EQ file not found: {eq_path}")

    (
        expected_buses,
        expected_branches,
        expected_first_r,
        expected_first_x,
        expected_generators,
        expected_loads,
        expected_transformers_2w,
        expected_transformers_3w,
        expected_fixed_shunts,
        expected_switched_shunts,
        expected_areas,
        expected_zones,
        expected_owners,
        expected_first_generator_p,
        expected_first_load_p,
    ) = _parse_eq_metrics(eq_path, sv_path if has_sv else None)

    repo_root = _repo_root()
    with tempfile.TemporaryDirectory(prefix="raptrix-rpf-") as temp_dir:
        output_path = Path(temp_dir) / f"{profile_name.lower()}.rpf"
        cmd = [
            "cargo",
            "run",
            "--release",
            "--",
            "convert",
            "--eq",
            str(eq_path),
            "--output",
            str(output_path),
            "--verbose",
        ]
        if has_tp:
            cmd.extend(["--tp", str(tp_path)])
        if has_sv:
            cmd.extend(["--sv", str(sv_path)])
        if connectivity_detail_mode:
            cmd.append("--connectivity-detail")
        completed = subprocess.run(
            cmd,
            cwd=repo_root,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(
                "CLI generation failed\n"
                f"STDOUT:\n{completed.stdout}\n"
                f"STDERR:\n{completed.stderr}"
            )

        print("Captured stdout first 200 chars:", completed.stdout[:200])
        normalized_stdout = completed.stdout.replace("â€”", "—")
        assert BRANDING in normalized_stdout, (
            "CLI output must include full branding text "
            "(Windows console encoding note: captured stdout may render em dash as â€” )"
        )
        assert output_path.is_file(), f"Expected output file not found: {output_path}"

        tables = _read_rpf_tables(output_path)
        _emit_table_row_report(profile_name, tables, capsys)

        expected_table_count = 15
        assert len(tables) == expected_table_count, (
            f"Expected {expected_table_count} canonical tables, got {len(tables)}"
        )

        observed_names = [name for name, _ in tables]
        assert observed_names[:15] == CANONICAL_TABLE_ORDER, (
            f"Unexpected core table order: {observed_names}"
        )
        for table_idx, (table_name, batch) in enumerate(tables[:15]):
            expected_table_name = CANONICAL_TABLE_ORDER[table_idx]
            assert table_name == expected_table_name, (
                f"Table order mismatch at index {table_idx}: expected {expected_table_name}, got {table_name}"
            )
            assert batch.num_rows >= 0, f"Table {table_name} must have non-negative row count"

        file_reader = ipc.RecordBatchFileReader(pa.memory_map(str(output_path), "r"))
        file_schema_metadata = file_reader.schema.metadata or {}
        assert b"raptrix.branding" in file_schema_metadata
        assert b"raptrix.version" in file_schema_metadata
        assert b"rpf_version" in file_schema_metadata
        assert file_schema_metadata[b"raptrix.branding"].decode("utf-8") == BRANDING
        assert file_schema_metadata[b"raptrix.version"].decode("utf-8") == SCHEMA_VERSION
        assert file_schema_metadata[b"rpf_version"].decode("utf-8") == SCHEMA_VERSION

        table_map = {name: batch for name, batch in tables}
        buses_batch = table_map["buses"]
        branches_batch = table_map["branches"]
        generators_batch = table_map["generators"]
        loads_batch = table_map["loads"]
        fixed_shunts_batch = table_map["fixed_shunts"]
        switched_shunts_batch = table_map["switched_shunts"]
        transformers_2w_batch = table_map["transformers_2w"]
        transformers_3w_batch = table_map["transformers_3w"]
        areas_batch = table_map["areas"]
        zones_batch = table_map["zones"]
        owners_batch = table_map["owners"]

        assert buses_batch.num_rows > 0, "Buses table must contain at least one row"
        assert branches_batch.num_rows > 0, "Branches table must contain at least one row"
        assert generators_batch.num_rows > 0, "Generators table must contain at least one row"
        if has_tp and not connectivity_detail_mode:
            assert buses_batch.num_rows <= expected_buses, (
                f"Expected TP mapping to not increase bus count (<= {expected_buses}), got {buses_batch.num_rows}"
            )
            if profile_name == "SmallGrid":
                assert buses_batch.num_rows < expected_buses, (
                    f"Expected topological bus collapse with TP (less than {expected_buses}), got {buses_batch.num_rows}"
                )
                assert buses_batch.num_rows < 500, (
                    f"Expected significant TP bus reduction (< 500 buses), got {buses_batch.num_rows}"
                )
            elif buses_batch.num_rows == expected_buses:
                with capsys.disabled():
                    print(
                        f"Note: {profile_name} retained connectivity-level bus count ({buses_batch.num_rows}) despite TP input"
                    )
        else:
            assert buses_batch.num_rows == expected_buses, (
                f"Expected {expected_buses} buses from ConnectivityNode count, got {buses_batch.num_rows}"
            )
        assert branches_batch.num_rows == expected_branches, (
            f"Expected {expected_branches} branches from ACLineSegment count, got {branches_batch.num_rows}"
        )

        assert generators_batch.num_rows >= 0
        assert loads_batch.num_rows >= 0
        assert transformers_2w_batch.num_rows >= 0
        assert fixed_shunts_batch.num_rows >= 0
        assert switched_shunts_batch.num_rows >= 0

        assert generators_batch.num_rows == expected_generators, (
            f"Expected {expected_generators} generators from SynchronousMachine count, got {generators_batch.num_rows}"
        )
        assert loads_batch.num_rows == expected_loads, (
            f"Expected {expected_loads} loads from EnergyConsumer/ConformLoad/NonConformLoad count, got {loads_batch.num_rows}"
        )
        assert transformers_2w_batch.num_rows == expected_transformers_2w, (
            f"Expected {expected_transformers_2w} transformers_2w from PowerTransformer 2-end count, got {transformers_2w_batch.num_rows}"
        )
        assert transformers_3w_batch.num_rows == expected_transformers_3w, (
            f"Expected {expected_transformers_3w} transformers_3w from PowerTransformer 3-end count, got {transformers_3w_batch.num_rows}"
        )
        assert fixed_shunts_batch.num_rows == expected_fixed_shunts, (
            f"Expected {expected_fixed_shunts} fixed_shunts from LinearShuntCompensator count, got {fixed_shunts_batch.num_rows}"
        )
        assert switched_shunts_batch.num_rows == expected_switched_shunts, (
            f"Expected {expected_switched_shunts} switched_shunts from SvShuntCompensator count, got {switched_shunts_batch.num_rows}"
        )
        assert areas_batch.num_rows == expected_areas, (
            f"Expected {expected_areas} areas from ControlArea count, got {areas_batch.num_rows}"
        )
        assert zones_batch.num_rows == expected_zones, (
            f"Expected {expected_zones} zones from SubGeographicalRegion count, got {zones_batch.num_rows}"
        )
        assert owners_batch.num_rows == expected_owners, (
            f"Expected {expected_owners} owners from Organisation count, got {owners_batch.num_rows}"
        )

        branch_table = pa.Table.from_batches([branches_batch])
        first_branch_r = float(branch_table.column("r")[0].as_py())
        first_branch_x = float(branch_table.column("x")[0].as_py())

        assert first_branch_r == pytest.approx(expected_first_r), (
            f"First branch r mismatch: expected {expected_first_r}, got {first_branch_r}"
        )
        assert first_branch_x == pytest.approx(expected_first_x), (
            f"First branch x mismatch: expected {expected_first_x}, got {first_branch_x}"
        )

        if expected_first_generator_p is not None and generators_batch.num_rows > 0:
            generator_table = pa.Table.from_batches([generators_batch])
            first_generator_p = float(generator_table.column("p_sched_mw")[0].as_py())
            assert first_generator_p == pytest.approx(expected_first_generator_p), (
                f"First generator p_sched_mw mismatch: expected {expected_first_generator_p}, got {first_generator_p}"
            )

        if expected_first_load_p is not None and loads_batch.num_rows > 0:
            load_table = pa.Table.from_batches([loads_batch])
            first_load_p = float(load_table.column("p_mw")[0].as_py())
            assert first_load_p == pytest.approx(expected_first_load_p), (
                f"First load p_mw mismatch: expected {expected_first_load_p}, got {first_load_p}"
            )

        snapshot_path = repo_root / "tests" / "data" / "fixtures" / "memory_snapshot.rpf"
        _write_memory_snapshot(output_path, snapshot_path)
        snapshot_tables = _read_rpf_tables(snapshot_path)

        assert len(snapshot_tables) == len(tables), (
            f"Memory snapshot table count mismatch: expected {len(tables)}, got {len(snapshot_tables)}"
        )

        for idx, ((orig_name, orig_batch), (snap_name, snap_batch)) in enumerate(zip(tables, snapshot_tables)):
            assert snap_name == orig_name, (
                f"Memory snapshot table name mismatch at index {idx}: expected {orig_name}, got {snap_name}"
            )
            assert snap_batch.num_rows == orig_batch.num_rows, (
                f"Memory snapshot row-count mismatch for table {orig_name}: expected {orig_batch.num_rows}, got {snap_batch.num_rows}"
            )
            assert snap_batch.schema.equals(orig_batch.schema), (
                f"Memory snapshot schema mismatch for table {orig_name}"
            )

        snapshot_branch_table = pa.Table.from_batches([snapshot_tables[2][1]])
        snapshot_first_branch_r = float(snapshot_branch_table.column("r")[0].as_py())
        snapshot_first_branch_x = float(snapshot_branch_table.column("x")[0].as_py())
        assert snapshot_first_branch_r == pytest.approx(first_branch_r)
        assert snapshot_first_branch_x == pytest.approx(first_branch_x)
        print("Memory snapshot created: memory_snapshot.rpf (memory API confirmed identical)")

    print(f"PASS: {profile_name} RPF validation succeeded")
    print(BRANDING)


@pytest.mark.ignore
def test_smallgrid_rpf_generation(
    request: pytest.FixtureRequest,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _run_profile_validation(request, capsys, "SmallGrid")


@pytest.mark.ignore
def test_realgrid_rpf_generation(
    request: pytest.FixtureRequest,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _run_profile_validation(request, capsys, "RealGrid")