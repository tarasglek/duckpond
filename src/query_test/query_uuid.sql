-- Manually pick a "l33t" UUID
WITH l33t_uuid AS (
  SELECT 'deadbeef-1337-4b1d-8008-0123456789ab'::UUID AS uuid_value
)
SELECT uuid_value AS col1
FROM l33t_uuid
UNION ALL
SELECT uuid_value::VARCHAR AS col1
FROM l33t_uuid;
