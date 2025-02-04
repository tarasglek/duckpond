# Serverless DB based on duckdb + S3

## Edge functionality

We find the current idea of "edge" functions rather incomplete as most apps need some sort of database. Eg global cloudflare workers are expected to be backed by a single D1 region.

There are apparently global databases like https://cloud.google.com/spanner but they are eye-wateringly complex+expensive. One can also setup a traditional database like postgres with a global read replica, but that's still expensive and complicated.

duckpond is meant to to serve the typical infrequent write + cheap select() reads usecase. This should cover needs of 90% serverless functions.
- Data is stored as parquet on S3 with a single index file
- Users are expected to query tables via primary key only. (Can be loosened in future)
- Primary key is [uuiv7](https://uuid7.com/)
- At query time user can specify whether stale records are ok, and thus use local cdn copy of data
- If up-to-data data is needed can use react-style useQuery function that will:

  1) Look up cached data and return it (without closing the connection)
  2) Look up distant-up-to-date data to provide any corrections. This way an app can show query results right away and in unlikely event that data changed, replace it with corrected

duckdb is the SQL engine doing all the smart stuff. duckpond is basically an executable recipe for duckdb on how to organize data in S3. I suspect duckpond should become a duckdb extension.

Parquet is a legacy format that's not well-optimized for local lookups, but it's ubiquitously supported and can be replaced later.

Backends:
* Supposedly R2 public buckets https://developers.cloudflare.com/r2/buckets/public-buckets/ get one a free CDN
* Tigris is another interesting option https://www.tigrisdata.com/docs/objects/caching/
* Any cdn should work as duckpond is just a bunch of files.

## Protocol

### /query endpoint
Same protocol as [httpserver](https://github.com/quackscience/duckdb-extension-httpserver) duckdb extension

```bash
curl -X POST -d "SELECT now() AS current_time" http://localhost:8081/query
```

### cli -post flag
```bash
echo 'select now()' | ./duckpond -post /query
```

## Serverless
We don't like to manage infra. Initially plan to deploy on https://unikraft.cloud/ ala https://github.com/unikraft-cloud/examples/tree/main/duckdb-go

## S3 backend inspired by icedb
[icedb](https://github.com/danthegoodman1/icedb/) is fantastic.

It cleverly uses timestamped json log files to index parquet files, merge them, delete them and crutially to provide time-travel "snapshots"

duckpond diverges in following ways from icedb principles:
- Interface is entirely via an HTTP API using standard duckdb select, insert, create table statements. This is enabled by duckdb [json_serialize_sql](https://duckdb.org/docs/data/json/sql_to_and_from_json.html).
- No python/go/js exposed, everything via duckdb primitives
- No S3 list operations, instead the plan is to use a single log file that keeps getting replaced(or appended to if using newer s3 features)
- icedb requires separate configuration for every table, duckpond has a single global configuration and tracks `create table` params internally
- cheap writes: The most simple write is an upload of a parquet file + log update

### Transactional updates for log
- Single log file that gets overwritten using IfMatch: <prior-etag>

## Inspired by clickhouse
- deletes/updates: will use the clickhouse [approach](https://clickhouse.com/docs/en/sql-reference/statements/delete#how-lightweight-deletes-work-internally-in-clickhouse)
- cheapest delete will work by removing whole partitions (eg if a table is partitioned by user)


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


# TODO

## Log Schema
Switch to a single table of type

uuidv7, enum_of_event, struct_of_event

## Partitioning
Partitioning should not be too much work

## Transactionality for .parquet uploads
Atm .parquet gets uploaded before log

# See Also
* https://github.com/kylebarron/parquet-wasm
* https://www.tigrisdata.com/blog/tigris-vs-s3-cloudfront/
* https://duckdb.org/docs/guides/sql_features/query_and_query_table_functions.html