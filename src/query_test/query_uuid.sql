WITH gen_uuid AS (
  SELECT uuid() AS uuid_value
)
SELECT uuid_value AS col1
FROM gen_uuid
UNION ALL
SELECT uuid_value::VARCHAR AS col1
FROM gen_uuid;

