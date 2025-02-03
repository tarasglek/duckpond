CREATE TABLE messages (
    id UUID PRIMARY KEY,
    text VARCHAR NOT NULL,
    usage INTEGER,
    icebase_partition TEXT GENERATED ALWAYS AS (
        strftime(
            CAST(to_timestamp(uuid_v7_time(id) / 1000) AS TIMESTAMP),
            '%Y-%m-%d'
        )
    ) VIRTUAL
);
