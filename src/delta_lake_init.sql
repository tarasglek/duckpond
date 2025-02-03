CREATE OR REPLACE MACRO delta_stats(table_name) AS (
  WITH base AS (
    SELECT 
      any_value(alias(COLUMNS(*))) AS "alias_\\0",
      min(COLUMNS(*))::VARCHAR          AS "min_\\0",
      max(COLUMNS(*))::VARCHAR          AS "max_\\0",
      SUM(CASE WHEN COLUMNS(*) IS NULL THEN 1 ELSE 0 END)
                                   ::VARCHAR AS "nulls_\\0"
    FROM query_table(table_name::VARCHAR)
  ),
  unnested AS (
    SELECT 
      unnest([*COLUMNS('alias_.*')]) AS col_name,
      unnest([*COLUMNS('min_.*')])     AS min_value,
      unnest([*COLUMNS('max_.*')])     AS max_value,
      unnest([*COLUMNS('nulls_.*')])   AS null_count
    FROM base
  )
  SELECT to_json(
           struct_pack(
             numRecords := (SELECT COUNT(*) 
                              FROM query_table(table_name::VARCHAR)),
             stats := array_agg(
                        struct_pack(
                          col_name  := col_name,
                          min       := min_value,
                          max       := max_value,
                          nullCount := null_count
                        )
                      )
           )
         )::VARCHAR
  FROM unnested
);

CREATE TABLE log_json(
    protocol JSON,
    metaData STRUCT(
        id UUID,
        format STRUCT(provider VARCHAR, "options" MAP(VARCHAR, JSON)),
        schemaString VARCHAR,
        partitionColumns JSON[],
        createdTime BIGINT,
        icebase STRUCT(createTable VARCHAR),
        "configuration" MAP(VARCHAR, JSON)
    ),
    "add" STRUCT(
        path VARCHAR,
        partitionValues MAP(VARCHAR, JSON),
        size BIGINT,
        modificationTime BIGINT,
        dataChange BOOLEAN
        -- ,
        -- stats VARCHAR
    ),
    remove STRUCT(
        path VARCHAR,
        dataChange BOOLEAN,
        deletionTimestamp BIGINT,
        extendedFileMetadata BOOLEAN,
        partitionValues MAP(VARCHAR, JSON),
        size BIGINT
    )
);

-- Initial protocol version entry
INSERT INTO log_json (protocol)
VALUES (struct_pack(
        minReaderVersion := 3,
        minWriterVersion := 7,
        readerFeatures := ['timestampNtz'],
        writerFeatures := ['timestampNtz']
        )::json);
