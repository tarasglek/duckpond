package main

import (
	"testing"
)

func TestParser(t *testing.T) {
	tests := []struct {
		query    string
		expected Operation
		table    string
	}{
		// Create table tests
		{"CREATE TABLE users", OpCreateTable, "users"},
		{"  CREATE TEMPORARY TABLE temp_users", OpCreateTable, "temp_users"},
		{"\tCREATE OR REPLACE TABLE new_users", OpCreateTable, "new_users"},
		{" \t CREATE OR REPLACE TEMP TABLE tmp_users", OpCreateTable, "tmp_users"},

		// Insert tests
		{"INSERT INTO users", OpInsert, "users"},
		{"INSERT OR REPLACE INTO app.users", OpInsert, "app.users"},
		{"INSERT OR IGNORE INTO mydb.schema.users", OpInsert, "mydb.schema.users"},
		{"  INSERT INTO temp_users", OpInsert, "temp_users"},

		// Alter table tests
		{"ALTER TABLE users ADD COLUMN email TEXT", OpAlterTable, "users"},
		{"ALTER TABLE app.users DROP COLUMN age", OpAlterTable, "app.users"},
		{"ALTER TABLE mydb.schema.users RENAME TO new_users", OpAlterTable, "mydb.schema.users"},
		{"  ALTER TABLE temp_users ADD PRIMARY KEY (id)", OpAlterTable, "temp_users"},

		// Select tests
		{"SELECT * FROM users", OpSelect, "users"},
		{"SELECT id, name FROM app.users", OpSelect, "app.users"},
		{"SELECT count(*) FROM mydb.schema.users", OpSelect, "mydb.schema.users"},
		{"  SELECT col1,col2 FROM temp_users", OpSelect, "temp_users"},
		{"SELECT 1 + 1", OpSelect, ""},
		{"SELECT NOW()", OpSelect, ""},

		// Negative tests
		{"UPDATE users", OpUnknown, ""},
	}

	parser := NewParser()
	for _, tt := range tests {
		op, table := parser.Parse(tt.query)
		if op != tt.expected || table != tt.table {
			t.Errorf("Parse(%q) = (%v, %q), want (%v, %q)",
				tt.query, op, table, tt.expected, tt.table)
		}
	}
}
