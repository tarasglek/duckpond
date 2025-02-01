INSERT INTO log_json (remove)
VALUES (struct_pack(
    path := $1,
    size := $2,
    modificationTime := epoch_ms(CURRENT_TIMESTAMP),
    dataChange := TRUE,
    deletionTimestamp := epoch_ms(CURRENT_TIMESTAMP),
    extendedFileMetadata := FALSE,
    partitionValues := MAP()
)::json);
