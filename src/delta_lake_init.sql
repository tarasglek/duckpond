CREATE TABLE delta_lake_log (
    event JSON
);

-- Initial protocol version entry
INSERT INTO delta_lake_log (event)
VALUES (struct_pack(
    protocol := struct_pack(
        minReaderVersion := 3,
        minWriterVersion := 7,
        readerFeatures := ['timestampNtz'],
        writerFeatures := ['timestampNtz']
    )
)::json);
