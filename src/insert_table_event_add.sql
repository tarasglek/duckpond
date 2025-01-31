INSERT INTO delta_lake_log (event)
VALUES (struct_pack(
  add:=struct_pack(
    path:=$1,
    partitionValues:='{}'::json,
    size:=$2,
    modificationTime:=epoch_ms(CURRENT_TIMESTAMP),
    dataChange:=true
  )
)::json);
