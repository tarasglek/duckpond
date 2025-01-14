WITH gen_json AS (
  SELECT json_object('key', 'value', 'uuid', uuid()) AS json_value
)
SELECT json_value AS col1
FROM gen_json
UNION ALL
SELECT json_value::VARCHAR AS col1
FROM gen_json;
