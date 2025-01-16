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
		{"ALTER TABLE users ADD COLUMN name TEXT", OpUnknown, ""},
		{"SELECT * FROM users", OpSelect, "users"},

		// Insert tests
		{"INSERT INTO users", OpInsert, "users"},
		{"INSERT OR REPLACE INTO app.users", OpInsert, "app.users"},
		{"INSERT OR IGNORE INTO mydb.schema.users", OpInsert, "mydb.schema.users"},
		{"  INSERT INTO temp_users", OpInsert, "temp_users"},
		{"CREATE TABLE users", OpCreateTable, "users"},
		{"UPDATE users", OpUnknown, ""},

		// Select tests
		{"SELECT * FROM users", OpSelect, "users"},
		{"SELECT id, name FROM app.users", OpSelect, "app.users"},
		{"SELECT count(*) FROM mydb.schema.users", OpSelect, "mydb.schema.users"},
		{"  SELECT col1,col2 FROM temp_users", OpSelect, "temp_users"},
		{"INSERT INTO users", OpInsert, "users"},
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
