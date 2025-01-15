CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    text VARCHAR NOT NULL,
    usage INTEGER,
    created_at VARCHAR GENERATED ALWAYS AS (TO_CHAR(TO_TIMESTAMP(uuid_v7_time(id)), 'YYYY-MM-DD')) VIRTUAL
);
