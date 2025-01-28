WITH gen_json AS (
  SELECT
    json_object('key', 'value', 'num', 1) AS json_value,
    'original' AS source
)
SELECT
  json_value AS json_data,
  typeof(json_value) AS json_type,
  json_value::VARCHAR AS varchar_data,
  typeof(json_value::VARCHAR) AS varchar_type
FROM gen_json;
