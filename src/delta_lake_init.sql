-- CREATE TABLE log_json (
--     protocol JSON, metaData JSON, add JSON
-- );
-- use duckdb type inference to generate these
-- first load delta lake json into a table then:
-- copy (select table_name,sql from duckdb_tables()) to '/tmp/table.json'
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
