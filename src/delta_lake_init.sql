PRAGMA enable_object_cache; -- cache metadata in memory
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

-- Initial protocol version entry
INSERT INTO log_json (protocol)
VALUES (struct_pack(
        minReaderVersion := 3,
        minWriterVersion := 7,
        readerFeatures := ['timestampNtz'],
        writerFeatures := ['timestampNtz']
        )::json);
