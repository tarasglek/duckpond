-- CREATE TABLE log_json (
--     protocol JSON, metaData JSON, add JSON
-- );
-- use duckdb type inference to generate these
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
    "add" JSON);

-- Initial protocol version entry
INSERT INTO log_json (protocol)
VALUES (struct_pack(
        minReaderVersion := 3,
        minWriterVersion := 7,
        readerFeatures := ['timestampNtz'],
        writerFeatures := ['timestampNtz']
        )::json);
