from __future__ import annotations

import pandas as pd

from climate_pipeline.checksum import logical_content_checksum, source_observation_checksum
from climate_pipeline.transform import create_features


def _frame() -> pd.DataFrame:
    base = pd.DataFrame({
        "location_id": ["abuja", "abuja", "abuja"],
        "observation_date": pd.to_datetime(["2001-01-03", "2001-01-01", "2001-01-02"]),
        "precipitation_corrected_mm_day": [0.0, 1.0, 2.0],
        "temperature_mean_c": [25.0, 26.0, 27.0],
        "temperature_min_c": [20.0, 21.0, 22.0],
        "temperature_max_c": [30.0, 31.0, 32.0],
    })
    return create_features(base)


def test_logical_checksum_ignores_row_order_and_date_representation():
    expected = _frame()
    equivalent = expected.sample(frac=1, random_state=796).reset_index(drop=True)
    equivalent["observation_date"] = equivalent["observation_date"].dt.strftime("%Y-%m-%d")

    assert logical_content_checksum(expected) == logical_content_checksum(equivalent)
    assert source_observation_checksum(expected) == source_observation_checksum(equivalent)
