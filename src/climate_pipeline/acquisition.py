from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
import requests

from .config import load_config, location, years
from .models import DatasetManifest
from .util import partition_dates, sha256_bytes, write_json

def _params(config: dict, place: dict, year: int) -> dict[str, str | float]:
    start, end = partition_dates(year)
    return {"parameters": ",".join(config["parameters"]), "community": config["source"]["community"], "longitude": place["longitude"], "latitude": place["latitude"], "start": start, "end": end, "format": config["source"]["format"], "time-standard": config["source"]["time_standard"]}

def _validate_response(payload: dict, requested: set[str]) -> None:
    available = set(payload.get("properties", {}).get("parameter", {}))
    missing = requested - available
    if missing:
        raise ValueError(f"NASA POWER response did not validate requested parameter(s): {sorted(missing)}")

def acquire_canonical_dataset(data_root: str | Path = "data", config_path: str | Path = "configs/dataset.yaml", timeout_seconds: float = 30.0) -> DatasetManifest:
    """Acquire each configured location-year once; never overwrites differing canonical bytes."""
    config = load_config(config_path)
    root = Path(data_root)
    entries: list[dict] = []
    requested = set(config["parameters"])
    for place in config["locations"]:
        for year in years(config):
            params = _params(config, place, year)
            url = f"{config['source']['base_url']}?{urlencode(params)}"
            response = requests.get(config["source"]["base_url"], params=params, timeout=timeout_seconds)
            response.raise_for_status()
            content = response.content
            payload = response.json()
            _validate_response(payload, requested)
            target = root / "raw" / "canonical" / config["dataset_version"] / place["id"] / f"{year}.json"
            checksum = sha256_bytes(content)
            if target.exists() and sha256_bytes(target.read_bytes()) != checksum:
                raise FileExistsError(f"Immutable canonical snapshot differs: {target}")
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            entries.append({
                "dataset_version": config["dataset_version"],
                "location_id": place["id"],
                "location_name": place["name"],
                "latitude": place["latitude"],
                "longitude": place["longitude"],
                "year": year,
                "date_range": {"start": params["start"], "end": params["end"]},
                "variables_requested": list(config["parameters"]),
                "source_url": url,
                "request_parameters": params,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "http_status": response.status_code,
                "sha256": checksum,
                "byte_size": len(content),
                "raw_path": str(target),
                "response_header": payload.get("header", {}),
            })
    manifest = DatasetManifest(config["dataset_version"], entries, datetime.now(timezone.utc).isoformat())
    write_json(root / "manifests" / f"{config['dataset_version']}.json", {"dataset_version": manifest.dataset_version, "created_at": manifest.created_at, "entries": entries})
    return manifest

def canonical_path(data_root: str | Path, config: dict, location_id: str, year: int) -> Path:
    return Path(data_root) / "raw" / "canonical" / config["dataset_version"] / location_id / f"{year}.json"
