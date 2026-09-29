# Implementation notes

`climate_pipeline.run_partition` owns business logic. Framework modules only declare native task/flow/asset behavior and delegate to that API. Canonical acquisition is a separate live-NASA command; replay and all benchmark execution operate solely on local snapshots.

