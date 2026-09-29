from __future__ import annotations
import json
import hashlib
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response
from ..acquisition import canonical_path
from ..config import load_config

def create_app(data_root: str | Path = "data", config_path: str | Path = "configs/dataset.yaml") -> FastAPI:
    config = load_config(config_path)
    attempts: dict[tuple[str, int, str], int] = defaultdict(int)
    events: list[dict] = []
    app = FastAPI(title="Canonical NASA POWER replay", version="0.1.0")

    def event(location_id: str, year: int, fault_scenario: str, attempt: int, outcome: str) -> None:
        events.append({"location_id": location_id, "year": year, "fault_scenario": fault_scenario, "attempt": attempt, "outcome": outcome, "timestamp": datetime.now(timezone.utc).isoformat()})

    @app.get("/health")
    def health() -> dict: return {"status": "ok", "dataset_version": config["dataset_version"]}

    @app.get("/attempts")
    def attempt_summary() -> dict:
        return {"attempts": [{"location_id": location_id, "year": year, "fault_scenario": fault, "attempts": count} for (location_id, year, fault), count in sorted(attempts.items())], "events": events}

    @app.post("/attempts/reset")
    def reset_attempts() -> dict:
        attempts.clear(); events.clear()
        return {"status": "reset"}

    @app.get("/api/temporal/daily/point")
    def point(location_id: str, start: str, end: str, parameters: str, fault_scenario: str = "", seed: int = 0, fault_probability: float = 1.0):
        if len(start) != 8 or start[:4] != end[:4]: raise HTTPException(422, "Replay requests must cover exactly one canonical year")
        year = int(start[:4]); key = (location_id, year, f"{fault_scenario}:{seed}"); attempts[key] += 1; attempt = attempts[key]
        score = int(hashlib.sha256(f"{seed}:{location_id}:{year}:{fault_scenario}".encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        inject = bool(fault_scenario) and score < fault_probability
        if inject and fault_scenario == "transient_http" and attempt == 1:
            event(location_id, year, fault_scenario, attempt, "http_503")
            raise HTTPException(503, "Injected transient HTTP failure")
        if inject and fault_scenario == "timeout" and attempt == 1:
            event(location_id, year, fault_scenario, attempt, "timeout_injected")
            time.sleep(30)
        path = canonical_path(data_root, config, location_id, year)
        if not path.exists(): raise HTTPException(404, f"No canonical response for {location_id}/{year}")
        content = path.read_bytes()
        if inject and fault_scenario == "malformed_json":
            event(location_id, year, fault_scenario, attempt, "malformed_json")
            return Response(content=b"{not-json", media_type="application/json")
        payload = json.loads(content)
        parameter = payload.get("properties", {}).get("parameter", {})
        if inject and fault_scenario == "missing_field": parameter.pop("T2M", None)
        elif inject and fault_scenario == "duplicate_source":
            for values in parameter.values():
                if values:
                    date, value = next(iter(values.items())); values[f"{date[:-2]}99"] = value
        elif inject and fault_scenario == "invalid_observation":
            values = parameter.get("PRECTOTCORR", {});
            if values: values[next(iter(values))] = -1
        if inject and fault_scenario in {"missing_field", "duplicate_source", "invalid_observation"}:
            content = json.dumps(payload).encode()
        headers = {}
        if inject and fault_scenario == "transient_storage" and attempt == 1:
            # The shared pipeline raises only after it has transformed these
            # canonical bytes and immediately before its Parquet write.
            event(location_id, year, fault_scenario, attempt, "storage_failure_injected")
            headers["X-Climate-Fault"] = "transient_storage"
        else:
            event(location_id, year, fault_scenario, attempt, "success")
        return Response(content=content, media_type="application/json", headers=headers)
    return app

app = create_app()
