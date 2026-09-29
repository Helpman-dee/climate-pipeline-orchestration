import json
import subprocess

from climate_pipeline.benchmark import _persistent_submission_command, _write_partition_items
from climate_pipeline.persistent_submitter import _load_items


def test_large_partition_input_uses_a_short_mounted_file_argument(tmp_path) -> None:
    """A full 120-partition workload must not be expanded into Windows argv."""
    items = [
        {
            "location_id": f"location_{index % 5}",
            "year": 2001 + index // 5,
            "run_id": "large-run",
            "replay_base_url": "http://replay:8000",
            "fault_scenario": None,
            "seed": 42,
            "data_root": "/app/data/benchmarks/state/large-run",
        }
        for index in range(120)
    ]
    path = _write_partition_items(tmp_path, items)
    assert json.loads(path.read_text(encoding="utf-8")) == items
    assert _load_items(None, str(path)) == items

    command = _persistent_submission_command(
        "airflow", "/app/data/benchmarks/state/large-run/partition-items.v1.json", "/app/data/benchmarks/state/large-run", 72000,
    )
    assert "--items-file" in command
    assert "--items-json" not in command
    # Well below the Windows CreateProcess command-line limit that the old
    # inline 120-item JSON payload exceeded.
    assert len(subprocess.list2cmdline(command)) < 4096
