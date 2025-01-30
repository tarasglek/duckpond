WITH 
  -- Get the schema of the messages table
  messages_schema AS (
    SELECT 
      column_name,
      column_type,
      null
    FROM (DESCRIBE messages)
  ),
  
  -- Convert the schema to JSON format
  json_schema AS (
    SELECT 
      to_json(struct_pack(
        type:='struct',
        fields:=array_agg(struct_pack(
          name:=column_name,
          type:=
            CASE column_type
              WHEN 'BIGINT' THEN 'long'
              WHEN 'VARCHAR' THEN 'string'
              WHEN 'TIMESTAMP' THEN 'timestamp_ntz'
              ELSE column_type
            END,
          nullable:=CASE null WHEN 'YES' THEN true ELSE false END,
          metadata:= '{}'::json
        ))
      ))::varchar AS schema_string
    FROM messages_schema
  )

-- Create the final JSON output
SELECT 
  to_json(struct_pack(
    id:=uuid(),
    name:=NULL,
    description:=NULL,
    format:=struct_pack(
      provider:='parquet',
      options:='{}'::json
    ),
    schemaString:=schema_string,
    partitionColumns:=array[],
    createdTime:=epoch_ms(CURRENT_TIMESTAMP),
    configuration:='{}'::json
  )) AS final_json
FROM json_schema;
