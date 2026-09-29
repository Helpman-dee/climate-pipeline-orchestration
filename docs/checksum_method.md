# Logical output checksum

All future benchmark records use `climate-logical-v1`, implemented in
`climate_pipeline.checksum.logical_content_checksum`. It hashes a version tag,
the ordered curated schema, and key-sorted rows encoded as canonical JSON
values: UTF-8 locations, ISO calendar dates, 0/1 booleans, null for missing
values, and normalized IEEE-754 hexadecimal floats. It therefore does not
depend on Parquet metadata, compression, row-group layout, physical file bytes,
host line endings, or dataframe row order.

Future Airflow, Prefect, and Dagster validation records all call this same
method through the shared pipeline and benchmark-output validation paths.
Source-value verification manifests use the companion `climate-source-v1`
encoding, with the same canonical value rules applied to the six source fields.

## Legacy records

The historical `19aa2570647b46cf5e32bd0e0cde367d8816a52d7307fb0ca298b8026835ce18`
value is legacy metadata and is not overwritten. It was SHA-256 of the sorted
ten-column curated dataframe emitted through `pandas.DataFrame.to_csv` with
`index=False`, `date_format="%Y-%m-%d"`, empty null representation, and the
platform-default CRLF terminator. That Windows serialization yields `19aa…`.
The identical dataframe serialized with the Linux-default LF terminator yields
`acefaa1929b668a505dc00681dd4d476b013f0201cb964cd24da6b2d4066d9d0`.

Those values are retained only to interpret existing historical benchmark
artifacts. They must not be compared with `climate-logical-v1` values.
