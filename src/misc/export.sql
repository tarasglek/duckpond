

INSERT INTO schema_log VALUES
    ('2023-01-15 09:30:00+00', 'CREATE TABLE users (id INT, name TEXT)'),
    ('2023-02-20 14:15:00+00', 'CREATE TABLE orders (order_id UUID, amount DECIMAL)');


INSERT INTO insert_log VALUES
    (uuid(), '2023', 0, 4096),
    (uuid(), '2023', 0, 2048);

create view table2json as
with json_data as (
    select 
        (SELECT ARRAY_AGG(struct_pack(timestamp, raw_query))
            FROM schema_log) as schema_log, 
        (SELECT ARRAY_AGG(struct_pack(id, partition, tombstoned_unix_time, size))
            FROM insert_log) as insert_log
)
select 
    to_json(struct_pack( schema_log, insert_log ))::varchar as json_result
from json_data;

select * from table2json;