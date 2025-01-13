# Serverless DB based on duckdb + S3

## Inspired by icedb
  [icedb](https://github.com/danthegoodman1/icedb/) is fantastic.
  
  It cleverly uses timestamped json log files to keep track of parquet files, merge them, delete them.

  icebase makes following improvements:
  - Interface is entirely via an HTTP API using standard duckdb select, insert, create table statements
  - No S3 list operations, instead the plan is to use a single log file that keeps getting replaced(or appended to if using newer s3 features)
  - icedb requires separate configuration for every table, we handle this via `create table`
  - icedb def

## Partitioning

We will be able to specify partion algo via SQL as part of create table.

Eg here is an example of how to use 2 columns to create partition func:

```sql
WITH temp_data AS (
    SELECT 123 AS user_id, 1698765432000 AS ts
)
SELECT
    'u=' || user_id || '/d=' || strftime(
        to_timestamp(ts / 1000),
        '%Y-%m-%d'
    ) AS partition_path
FROM temp_data;
```

Example output:

| partition_path     |
|--------------------|
| u=123/d=2023-10-31 |
