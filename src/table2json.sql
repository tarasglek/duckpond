CREATE TABLE IF NOT EXISTS schema_log (
    timestamp TIMESTAMP PRIMARY KEY,
    raw_query TEXT NOT NULL
);

INSERT INTO schema_log VALUES
    ('2023-01-15 09:30:00+00', 'CREATE TABLE users (id INT, name TEXT)'),
    ('2023-02-20 14:15:00+00', 'CREATE TABLE orders (order_id UUID, amount DECIMAL)');

CREATE TABLE IF NOT EXISTS insert_log (
    id UUID PRIMARY KEY,
    partition TEXT NOT NULL DEFAULT '',
    tombstoned_unix_time BIGINT NOT NULL DEFAULT 0,
    size BIGINT NOT NULL DEFAULT 0
);

INSERT INTO insert_log VALUES
    (uuid(), '2023', 0, 4096),
    (uuid(), '2023', 0, 2048);

SELECT ARRAY_AGG(struct_pack(timestamp, raw_query))
FROM schema_log;

SELECT ARRAY_AGG(struct_pack(id, partition, tombstoned_unix_time, size))
FROM insert_log;