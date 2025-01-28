CREATE TABLE merge_test (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    text VARCHAR NOT NULL,
    usage INTEGER
)
;
INSERT INTO merge_test (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33f'::UUID, 'one', 1);
INSERT INTO merge_test (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33e'::UUID, 'more', 1);
VACUUM merge_test;
select id, text from merge_test;