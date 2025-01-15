Duckdb has some interesting things:

[COPY TO](https://duckdb.org/docs/sql/statements/copy#copy--to-options) has

- `FORMAT`: parquet in our case
- `USE_TMP_FILE`: seems useful if an overwrite fails so we can decide to merge
- `OVERWRITE_OR_IGNORE`: false seems like sane default
- `OVERWRITE`: false seems good too
- `APPEND`: "When set, in the event a filename pattern is generated that already exists, the path will be regenerated", not sure what this regeneration is about
- `FILE_SIZE_BYTES`: seems useful to limit files to useful sizes eg 2mb
- `PARTITION_BY` obviously useful
- `RETURN_FILES` also great
- `WRITE_PARTITION_COLUMNS` also useful

Can use https://github.com/auxten/postgresql-parser?tab=readme-ov-file to parse SQL since duckdb sql parser only parses select * for now and not all.