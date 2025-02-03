-- adds parquet files to delta lake log
INSERT INTO log_json (add)
VALUES (struct_pack(
    path:=$1,
    partitionValues:='{}'::json,
    size:=$2,
    modificationTime:=epoch_ms(CURRENT_TIMESTAMP),
    dataChange:=true,
    "stats":='{}'
  )::json);
