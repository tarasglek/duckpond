insert into schema_log values
(with schema_log as (
    select unnest(schema_log) as rows from 'dump.json'
) select rows.* from schema_log)