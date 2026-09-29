from __future__ import annotations
import pandas as pd
from .models import QualityResult
from .util import days_in_year

try:
    import pandera.pandas as pa
except ImportError:  # Allows static adapter imports before the core environment is installed.
    pa = None

REQUIRED_COLUMNS = {"location_id", "observation_date", "precipitation_corrected_mm_day", "temperature_mean_c", "temperature_min_c", "temperature_max_c"}

def validate_observations(frame: pd.DataFrame, location_id: str, year: int, config: dict) -> list[QualityResult]:
    issues: list[QualityResult] = []
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        return [QualityResult("hard", "required_columns", f"Missing columns: {sorted(missing)}", location_id, year)]
    if pa is not None:
        schema = pa.DataFrameSchema({"location_id": pa.Column(str, nullable=False), "observation_date": pa.Column("datetime64[ns]", nullable=False)}, strict=False)
        try:
            schema.validate(frame, lazy=True)
        except Exception as exc:
            issues.append(QualityResult("hard", "pandera_schema", f"Pandera contract failed: {exc}", location_id, year, len(frame)))
    if frame["observation_date"].isna().any() or not pd.api.types.is_datetime64_any_dtype(frame["observation_date"]):
        issues.append(QualityResult("hard", "invalid_dates", "Observation dates are invalid", location_id, year, len(frame)))
    if set(frame["location_id"].dropna()) != {location_id}:
        issues.append(QualityResult("hard", "location_identity", "Rows do not match requested location", location_id, year, len(frame)))
    if frame.duplicated(["location_id", "observation_date"]).any():
        issues.append(QualityResult("hard", "duplicate_keys", "Duplicate natural keys detected", location_id, year, len(frame)))
    if len(frame) != days_in_year(year):
        issues.append(QualityResult("hard", "partition_completeness", f"Expected {days_in_year(year)} dates, found {len(frame)}", location_id, year, len(frame)))
    quality = config["quality"]
    if (frame["temperature_mean_c"] < quality["min_temperature_c"]).any() or (frame["temperature_mean_c"] > quality["max_temperature_c"]).any():
        issues.append(QualityResult("hard", "temperature_bounds", "Mean temperature outside configured plausible bounds", location_id, year, len(frame)))
    if (frame["precipitation_corrected_mm_day"] < 0).any() or (frame["precipitation_corrected_mm_day"] > quality["max_precipitation_mm_day"]).any():
        issues.append(QualityResult("hard", "precipitation_bounds", "Precipitation outside configured plausible bounds", location_id, year, len(frame)))
    fraction = frame[["precipitation_corrected_mm_day", "temperature_mean_c", "temperature_min_c", "temperature_max_c"]].isna().mean().max()
    if fraction > quality["max_missing_fraction_warning"]:
        issues.append(QualityResult("warning", "missing_values", f"Maximum missing fraction {fraction:.3f}", location_id, year, len(frame)))
    return issues

def hard_failures(issues: list[QualityResult]) -> list[QualityResult]:
    return [issue for issue in issues if issue.severity == "hard"]
