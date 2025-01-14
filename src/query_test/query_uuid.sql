WITH l33t_uuid AS (
  SELECT 'deadbeef-1337-4b1d-8008-0123456789ab'::UUID AS uuid_value
)
SELECT
  uuid_value::VARCHAR AS uuid_string,
  uuid_value AS uuid_uuid,
  typeof(uuid_value::VARCHAR) AS uuid_string_type,
  typeof(uuid_value) AS uuid_uuid_type
FROM l33t_uuid;
