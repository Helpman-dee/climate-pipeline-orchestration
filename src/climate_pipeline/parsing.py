from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

SOURCE_TO_COLUMN = {"PRECTOTCORR": "precipitation_corrected_mm_day", "T2M": "temperature_mean_c", "T2M_MIN": "temperature_min_c", "T2M_MAX": "temperature_max_c"}

def parse_power_payload(payload: dict, location_id: str) -> pd.DataFrame:
    parameters = payload.get("properties", {}).get("parameter")
    if not isinstance(parameters, dict):
        raise ValueError("Malformed NASA POWER response: properties.parameter is absent")
    absent = set(SOURCE_TO_COLUMN) - set(parameters)
    if absent:
        raise ValueError(f"Schema drift: missing parameter(s) {sorted(absent)}")
    frame = pd.DataFrame({SOURCE_TO_COLUMN[key]: pd.Series(values) for key, values in parameters.items() if key in SOURCE_TO_COLUMN})
    frame.index.name = "date_key"
    frame = frame.reset_index()
    frame["observation_date"] = pd.to_datetime(frame.pop("date_key"), format="%Y%m%d", errors="coerce")
    frame["location_id"] = location_id
    for column in SOURCE_TO_COLUMN.values():
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame.loc[frame[column] <= -999, column] = pd.NA
    return frame[["location_id", "observation_date", *SOURCE_TO_COLUMN.values()]].sort_values("observation_date").reset_index(drop=True)

def parse_power_file(path: str | Path, location_id: str) -> pd.DataFrame:
    return parse_power_payload(json.loads(Path(path).read_text(encoding="utf-8")), location_id)

