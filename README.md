# duckpond: Serverless DB based on duckdb + S3

duckpond is meant to be the database equivalent of a static blog. Your database state is all on S3-compatible storage, there are no backups, less read-scalability issues. For most apps latency of a db hosted as a bunch of files on S3 should be a reasonable tradeoff if one does not have to maintain/backup a db.

Data is written in a log-structured way which lends itself nicely to caching via a CDN. This means one can have a simple database while also being geo-distributed without the hassle of replication!

 This should cover needs of 90% serverless functions.
- Data is stored as parquet on S3 with a single index file
- Database is written in a log-structured style, eg all data is immutable, only the log is updated
- The database is compatible with Delta Lake, so the query backend can make use Delta Lake log `stat` field (it has min/max/count of every column right in log) to do query pushdown and thus read less parquet files from S3
- No expensive S3 list operations
- Log is transactional via S3 conditional writes

Duckdb is the SQL engine is handly most SQL smarts. duckpond is basically an executable recipe for duckdb on how to organize data in S3. I suspect duckpond could become a duckdb extension.

duckpond principles
- Interface is entirely via an HTTP API using standard duckdb SELECT, INSERT, CREATE, VACUUM table statements. This was gonna be enabled by duckdb [json_serialize_sql](https://duckdb.org/docs/data/json/sql_to_and_from_json.html) until I realized that API is incomplete. Instead it's enabled by a bunch of hacks (for now).
- No Python/Go/JS exposed to use, everything via normal SQL over HTTP


Backends:
* Supposedly R2 public buckets https://developers.cloudflare.com/r2/buckets/public-buckets/ get one a free CDN
* Tigris is another interesting option https://www.tigrisdata.com/docs/objects/caching/
* Any cdn should work as duckpond is just a bunch of files.
* Local



## Performance expectations

### Write

When operating with S3, writes are roughly 3 S3 roundtrips in performance. This could be optimized by using VMs with durable storage for sinking writes, in which case latencies will be |network latency| + |sub-ms nvme latency| with various tradeoffs that come with increased complexity.

### Reads

In my testing a basic read from S3 can be between 80-300ms.

### Cached Reads
However one can get that down to 0ms if reading past data that's cached locally. This would be the perfect database system to run on a serverless function provider right on the compute node.

It's easy to image an React style useQuery api (inspired by [useLiveQuery](https://dexie.org/docs/dexie-react-hooks/useLiveQuery())) where a web app could keep a cache right in the browser(The ultimate CDN experience), initially serve a cached data to query and then do a follow-up live query for the unlikely case that the data changed.


## Examples
See [CONTRIBUTING.md](CONTRIBUTING.md).


## Inspiration
* [icedb](https://github.com/danthegoodman1/icedb/) is fantastic
- While hacking on icedb I wanted it to avoid S3 List and to use S3 conditional writes and appends for log
* Turns out the [Delta Lake format](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) matched my IO-pattern desires perfectly.
- It cleverly uses timestamped json log files to index parquet files, merge them, delete them and crutially to provide time-travel "snapshots"
- deletes/updates: in future duckpond might follow lickhouse [approach](https://clickhouse.com/docs/en/sql-reference/statements/delete#how-lightweight-deletes-work-internally-in-clickhouse)


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

# See Also
* https://github.com/kylebarron/parquet-wasm
* https://www.tigrisdata.com/blog/tigris-vs-s3-cloudfront/
* https://duckdb.org/docs/guides/sql_features/query_and_query_table_functions.html