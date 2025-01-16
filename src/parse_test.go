package main

/*
[
  'CREATE TABLE users',
  '  CREATE TEMPORARY TABLE temp_users',
  '\tCREATE OR REPLACE TABLE new_users',
  ' \t CREATE OR REPLACE TEMP TABLE tmp_users',
  'ALTER TABLE users ADD COLUMN name TEXT', // Should not match
  'SELECT * FROM users' // Should not match
];

[
  'INSERT INTO users',
  'INSERT OR REPLACE INTO app.users',
  'INSERT OR IGNORE INTO mydb.schema.users',
  '  INSERT INTO temp_users',
  'CREATE TABLE users', // Should not match
  'UPDATE users' // Should not match
];

[
  'SELECT * FROM users',
  'SELECT id, name FROM app.users',
  'SELECT count(*) FROM mydb.schema.users',
  '  SELECT col1,col2 FROM temp_users',
  'INSERT INTO users', // Should not match
  'UPDATE users' // Should not match
];
*/
