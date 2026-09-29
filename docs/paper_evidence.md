# Paper evidence ledger

## Canonical data and framework-neutral pipeline validation

This section records measured facts from the direct, framework-neutral processing
run `canonical-pipeline-nigeria-power-daily-2001-2024-v1`. It is not an
orchestration benchmark or a performance result.

- Canonical input: NASA POWER daily point data for five Nigerian locations,
  2001-01-01 through 2024-12-31 (UTC), stored as 120 immutable raw JSON files.
- Input and curated output rows: 43,830 each; rejected rows: 0.
- Curated layout: 120 Parquet partitions under
  `data/curated/observations/location_id=<id>/year=<year>/data.parquet`.
  Combined Parquet size: 2,674,935 bytes (per-file range: 21,228-23,350 bytes).
- Generated features: `daily_temperature_range_c`, `dry_day`,
  `precipitation_30d_mm`, and `precipitation_90d_mm`.
- Source observations had zero missing values, duplicate composite keys,
  invalid dates, invalid temperature values, invalid precipitation values, and
  temperature-order violations. The rolling 30-day and 90-day features have
  145 and 445 null warm-up values, respectively.
- Expected-output verification passed for all 120 partitions on both runs.
  The second run left canonical SHA-256 values and curated Parquet byte hashes
  unchanged; audit metadata contains 120 partition records, zero quality
  incidents, and one idempotent verification record for the run.
- The structured measured report is
  `data/reports/nigeria-power-daily-2001-2024-v1-quality-report.json`; audit
  metadata is stored in `data/audit.duckdb` (2,633,728 bytes at report time).

Framework-orchestrated experiments have since been completed and are summarized
in `docs/analysis_summary.md`: 27 normal runs (three frameworks, three
workloads, and three repetitions), nine fault/data-quality experiments, and
three framework-native reprocessing runs. Fault and reprocessing comparisons
have one validated run per framework, so their overheads are descriptive. These
experiments are distinct from the framework-neutral canonical validation above.
