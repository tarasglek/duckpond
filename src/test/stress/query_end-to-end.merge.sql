CREATE TABLE stress_test (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    text VARCHAR NOT NULL,
    usage INTEGER
); -- table creation is logged, but no data is written at this point
-- ASSERT COUNT_PARQUET stress_test: 0
INSERT INTO stress_test (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33f'::UUID, 'one', 1);
INSERT INTO stress_test (id, text, usage) VALUES ('01947471-2ded-7812-cafe-34567000b33e'::UUID, 'more', 1);
-- ASSERT COUNT_PARQUET stress_test: 2
VACUUM stress_test; -- this will merge the parquet files, but leave the previous two and mark em tombstoned
-- ASSERT COUNT_PARQUET stress_test: 3
select id, text from stress_test;
VACUUM stress_test; -- this will delete the 2 tombstoned files
-- ASSERT COUNT_PARQUET stress_test: 1
VACUUM stress_test; -- this will do nothing
