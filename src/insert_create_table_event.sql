INSERT INTO delta_lake_events
WITH 
  -- Get the schema of the table
  books_schema AS (
    SELECT 
      column_name,
      data_type,
      is_nullable
    FROM duckdb_columns()
    WHERE table_name = $1
  ),
  
  -- Convert the schema to JSON format
  json_schema AS (
    SELECT 
      to_json(struct_pack(
        type:='struct',
        fields:=array_agg(struct_pack(
          name:=column_name,
          type:=
            CASE data_type
              WHEN 'BIGINT' THEN 'long'
              WHEN 'VARCHAR[]' THEN 'array<string>'
              ELSE lower(data_type)
            END,
          nullable:=CASE is_nullable WHEN true THEN true ELSE false END,
          metadata:= '{}'::json
        ))
      ))::varchar AS schema_string
    FROM books_schema
  )

-- Create the final JSON output
SELECT 
  struct_pack(
    metaData:=struct_pack(
      id:=uuid(),
      format:=struct_pack(
        provider:='parquet',
        options:='{}'::json
      ),
      schemaString:=schema_string,
      partitionColumns:=array[],
      createdTime:=epoch_ms(CURRENT_TIMESTAMP),
      configuration:='{}'::json
    )
  ) AS event
FROM json_schema;
