"""Versioned, platform-independent checksums for curated climate output."""
from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


LOGICAL_CHECKSUM_VERSION = "climate-logical-v1"
SOURCE_CHECKSUM_VERSION = "climate-source-v1"
LOGICAL_OUTPUT_COLUMNS = [
    "location_id", "observation_date", "precipitation_corrected_mm_day",
    "temperature_mean_c", "temperature_min_c", "temperature_max_c",
    "daily_temperature_range_c", "dry_day", "precipitation_30d_mm",
    "precipitation_90d_mm",
]
SOURCE_OBSERVATION_COLUMNS = LOGICAL_OUTPUT_COLUMNS[:6]


def _canonical_value(column: str, value: Any) -> str | int | None:
    """Return a type-normalized logical value for a single output cell."""
    if pd.isna(value):
        return None
    if column == "location_id":
        return str(value)
    if column == "observation_date":
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert("UTC")
        return timestamp.date().isoformat()
    if column == "dry_day":
        return int(bool(value))
    number = float(value)
    # IEEE-754 distinguishes signed zero, but climate values do not.
    return (0.0 if number == 0.0 else number).hex()


def _content_checksum(frame: pd.DataFrame, columns: list[str], version: str) -> str:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Checksum input is missing columns: {missing}")
    ordered = frame[columns].sort_values(["location_id", "observation_date"]).reset_index(drop=True)
    digest = hashlib.sha256()
    digest.update((version + "\n").encode("utf-8"))
    digest.update(json.dumps(columns, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    digest.update(b"\n")
    for row in ordered.itertuples(index=False, name=None):
        canonical = [_canonical_value(column, value) for column, value in zip(columns, row, strict=True)]
        digest.update(json.dumps(canonical, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def logical_content_checksum(frame: pd.DataFrame) -> str:
    """Hash complete curated output values, never physical serialization.

    Rows are ordered by the natural key.  Dates are ISO calendar dates, nulls
    are JSON ``null``, booleans are 0/1, and floats are canonical IEEE-754
    hexadecimal strings.  The version and ordered schema are included in the
    byte stream so a later intentional format change cannot be silent.
    """
    if list(frame.columns) != LOGICAL_OUTPUT_COLUMNS:
        raise ValueError("Logical checksum requires the curated output schema")
    return _content_checksum(frame, LOGICAL_OUTPUT_COLUMNS, LOGICAL_CHECKSUM_VERSION)


def source_observation_checksum(frame: pd.DataFrame) -> str:
    """Checksum source-value fields for cross-platform verification manifests."""
    return _content_checksum(frame, SOURCE_OBSERVATION_COLUMNS, SOURCE_CHECKSUM_VERSION)
