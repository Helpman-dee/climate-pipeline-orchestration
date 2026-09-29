from __future__ import annotations
import json
import pandas as pd
from fastapi.testclient import TestClient
from climate_pipeline.acquisition import canonical_path
from climate_pipeline.config import load_config
from climate_pipeline.replay_api.app import create_app
def payload():
    dates = pd.date_range("2001-01-01", periods=365, freq="D")
    keys = [item.strftime("%Y%m%d") for item in dates]
    return {"properties": {"parameter": {"PRECTOTCORR": {k: 1 for k in keys}, "T2M": {k: 25 for k in keys}, "T2M_MIN": {k: 20 for k in keys}, "T2M_MAX": {k: 30 for k in keys}}}}

def test_replay_serves_canonical_and_transient_fault(tmp_path):
    config = load_config(); path = canonical_path(tmp_path, config, "abuja", 2001); path.parent.mkdir(parents=True); path.write_text(json.dumps(payload()))
    client = TestClient(create_app(tmp_path))
    query = {"location_id": "abuja", "start": "20010101", "end": "20011231", "parameters": "T2M"}
    assert client.get("/api/temporal/daily/point", params=query).status_code == 200


def test_replay_marks_only_first_transient_storage_attempt(tmp_path):
    config = load_config(); path = canonical_path(tmp_path, config, "abuja", 2001); path.parent.mkdir(parents=True); path.write_text(json.dumps(payload()))
    client = TestClient(create_app(tmp_path))
    query = {"location_id": "abuja", "start": "20010101", "end": "20011231", "parameters": "T2M", "fault_scenario": "transient_storage"}
    first = client.get("/api/temporal/daily/point", params=query)
    second = client.get("/api/temporal/daily/point", params=query)
    assert first.status_code == second.status_code == 200
    assert first.headers["X-Climate-Fault"] == "transient_storage"
    assert "X-Climate-Fault" not in second.headers
    assert [event["outcome"] for event in client.get("/attempts").json()["events"]] == ["storage_failure_injected", "success"]
    query["fault_scenario"] = "transient_http"
    assert client.get("/api/temporal/daily/point", params=query).status_code == 503
    assert client.get("/api/temporal/daily/point", params=query).status_code == 200
