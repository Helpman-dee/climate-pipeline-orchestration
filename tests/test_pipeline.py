from __future__ import annotations
import json
from datetime import date
import pandas as pd
import pytest
from climate_pipeline.parsing import parse_power_payload
from climate_pipeline.transform import create_features
from climate_pipeline.validation import validate_observations, hard_failures
from climate_pipeline.config import load_config
from climate_pipeline.models import PartitionRequest, PartitionResult
from climate_pipeline import pipeline
from climate_pipeline.orchestrators import common

def payload(year: int = 2001, days: int = 365) -> dict:
    dates = pd.date_range(f"{year}-01-01", periods=days, freq="D")
    values = {day.strftime("%Y%m%d"): 1.0 for day in dates}
    return {"header": {"title": "fixture"}, "properties": {"parameter": {"PRECTOTCORR": values, "T2M": {k: 25.0 for k in values}, "T2M_MIN": {k: 20.0 for k in values}, "T2M_MAX": {k: 30.0 for k in values}}}}

def test_parse_validate_and_features():
    frame = parse_power_payload(payload(), "abuja")
    issues = validate_observations(frame, "abuja", 2001, load_config())
    assert not hard_failures(issues)
    featured = create_features(frame)
    assert featured.loc[89, "precipitation_90d_mm"] == 90
    assert featured.loc[0, "daily_temperature_range_c"] == 10

def test_duplicate_is_hard_quality_failure():
    frame = parse_power_payload(payload(), "abuja")
    duplicate = pd.concat([frame, frame.iloc[[0]]])
    assert any(item.code == "duplicate_keys" for item in hard_failures(validate_observations(duplicate, "abuja", 2001, load_config())))


class _Response:
    def __init__(self, content: bytes, headers: dict[str, str]):
        self.content = content
        self.headers = headers

    def raise_for_status(self) -> None:
        return None


def test_transient_storage_fails_before_write_then_retry_succeeds(tmp_path, monkeypatch):
    responses = [_Response(json.dumps(payload()).encode(), {"X-Climate-Fault": "transient_storage"}), _Response(json.dumps(payload()).encode(), {})]
    monkeypatch.setattr(pipeline.requests, "get", lambda *args, **kwargs: responses.pop(0))
    request = PartitionRequest("abuja", 2001, "storage-retry", "http://replay", "transient_storage")
    first = pipeline.run_partition(request, tmp_path)
    assert first.status == "failed"
    assert not list((tmp_path / "curated").rglob("*.parquet"))
    second = pipeline.run_partition(request, tmp_path)
    assert second.status == "success"
    assert len(list((tmp_path / "curated").rglob("*.parquet"))) == 1


def test_hard_quality_failure_is_terminal_to_adapters(monkeypatch):
    result = PartitionResult("quality", "abuja", 2001, "quality_failed", error="Hard data-quality gate failed")
    monkeypatch.setattr(common, "run_partition", lambda *args, **kwargs: result)
    with pytest.raises(RuntimeError, match="Hard data-quality gate failed"):
        common.run_adapter_partition("abuja", 2001, "quality", "http://replay")
