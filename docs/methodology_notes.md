# Methodology notes

The unit of work is one configured location-year. Every engine receives identical replayed canonical bytes, invokes the same Python pipeline function, and writes the same Parquet/DuckDB semantics. Principal conditions are serial and isolated; warm-ups are excluded from configured measured repetitions. Fault selection, retries, seed, workload, environment, manifest checksum, Git revision, and verification result are recorded per run. Curated-output comparison uses the versioned, platform-independent method documented in `checksum_method.md`.
