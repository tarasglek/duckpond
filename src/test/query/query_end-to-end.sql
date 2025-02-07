CREATE TABLE messages (
    id UUID PRIMARY KEY,
    text VARCHAR NOT NULL,
    usage INTEGER
)
;
INSERT INTO messages (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33f'::UUID, 'one', 1);
INSERT INTO messages (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33e'::UUID, 'more', 1);
select id, text from messages; 

-- DROP TABLE messages;
