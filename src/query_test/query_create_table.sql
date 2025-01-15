CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    text VARCHAR NOT NULL,
    usage INTEGER,
    created_at TIMESTAMP GENERATED ALWAYS AS (uuid_v7_time(id)) VIRTUAL
);
