from __future__ import annotations
import pandas as pd

def create_features(frame: pd.DataFrame, history: pd.DataFrame | None = None) -> pd.DataFrame:
    base = frame.copy()
    prefix = history.copy() if history is not None else None

    # pandas is deprecating dtype inference when concatenating empty or all-NA
    # frames.  Such a history cannot contribute to rolling features, so keep it
    # out of the concatenation instead of relying on its currently ignored dtype.
    has_history = prefix is not None and not prefix.empty and not prefix.isna().all().all()
    if base.empty:
        combined = prefix.copy() if has_history else base
    elif base.isna().all().all():
        # Preserve an all-NA input row-for-row without mixing it with history;
        # there are no values from which to derive features.
        combined = base
    elif has_history:
        combined = pd.concat([prefix, base], ignore_index=True)
    else:
        combined = base

    combined = combined.sort_values("observation_date")
    combined["daily_temperature_range_c"] = combined["temperature_max_c"] - combined["temperature_min_c"]
    combined["dry_day"] = combined["precipitation_corrected_mm_day"].eq(0)
    combined["precipitation_30d_mm"] = combined["precipitation_corrected_mm_day"].rolling(30, min_periods=30).sum()
    combined["precipitation_90d_mm"] = combined["precipitation_corrected_mm_day"].rolling(90, min_periods=90).sum()
    return combined.tail(len(base)).reset_index(drop=True)
