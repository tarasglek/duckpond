-- marks all existing 'add's that weren't marked for removal as as 'remove'd
-- and adds the merged 'add'
INSERT INTO log_json (add, remove) 
SELECT 
  NULL,
  struct_pack(
    path := add.path,
    dataChange := add.dataChange,
    deletionTimestamp := epoch_ms(CURRENT_TIMESTAMP),
    extendedFileMetadata := NULL,
    partitionValues := add.partitionValues,
    size := add.size
  ) 
FROM log_json 
WHERE add IS NOT NULL AND add.path NOT IN (SELECT remove.path FROM log_json WHERE remove IS NOT NULL)

UNION ALL

SELECT 
  struct_pack(
    path:=$1,
    partitionValues:='{}'::json,
    size:=$2,
    modificationTime:=epoch_ms(CURRENT_TIMESTAMP),
    dataChange:=true
  ),
  NULL;
