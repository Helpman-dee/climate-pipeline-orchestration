from __future__ import annotations
from pathlib import Path
import yaml

def load_config(path: str | Path = "configs/dataset.yaml") -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)

def location(config: dict, location_id: str) -> dict:
    for item in config["locations"]:
        if item["id"] == location_id:
            return item
    raise KeyError(f"Unknown configured location: {location_id}")

def years(config: dict) -> range:
    period = config["period"]
    return range(period["start_year"], period["end_year"] + 1)

