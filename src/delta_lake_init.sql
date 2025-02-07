PRAGMA enable_object_cache; -- cache metadata in memory
SET threads TO 2;

CREATE TABLE log_json(
    protocol JSON,
    metaData STRUCT(
        id UUID,
        format STRUCT(provider VARCHAR, "options" MAP(VARCHAR, JSON)),
        schemaString VARCHAR,
        partitionColumns JSON[],
        createdTime BIGINT,
        duckpond STRUCT(createTable VARCHAR),
        "configuration" MAP(VARCHAR, JSON)
    ),
    "add" STRUCT(
        path VARCHAR,
        partitionValues MAP(VARCHAR, JSON),
        size BIGINT,
        modificationTime BIGINT,
        dataChange BOOLEAN,
        stats VARCHAR
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

SET VARIABLE log_json_etag = '';

-- Initial protocol version entry
INSERT INTO log_json (protocol)
VALUES (struct_pack(
        minReaderVersion := 3,
        minWriterVersion := 7,
        readerFeatures := ['timestampNtz'],
        writerFeatures := ['timestampNtz']
        )::json);
