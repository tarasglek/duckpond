CREATE TABLE messages (
    uuid UUID PRIMARY KEY DEFAULT uuid(),
    text VARCHAR NOT NULL,
    usage INTEGER
);

