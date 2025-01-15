Duckdb has some interesting things:

[COPY TO](https://duckdb.org/docs/sql/statements/copy#copy--to-options) has

- `FORMAT`: parquet in our case
- `USE_TMP_FILE`: seems useful if an overwrite fails so we can decide to merge
- `OVERWRITE_OR_IGNORE`: false seems like sane default
- `OVERWRITE`: false seems good too
- `APPEND`: "When set, in the event a filename pattern is generated that already exists, the path will be regenerated", not sure what this regeneration is about

