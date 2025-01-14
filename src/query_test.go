package main

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEndpoint(t *testing.T) {
	// Setup test database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test cases
	tests := []struct {
		name        string
		inputQuery  string
		expectError bool
		checkOutput func(t *testing.T, output string)
	}{
		{
			name:       "simple select",
			inputQuery: "SELECT 1",
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, `"type":"SELECT_NODE"`)
				assert.Contains(t, output, `"select_list":`)
			},
		},
		{
			name:       "complex query",
			inputQuery: "SELECT * FROM (SELECT 1 AS a, 2 AS b) t WHERE a = 1",
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, `"type":"SELECT_NODE"`)
				assert.Contains(t, output, `"from_table":`)
				assert.Contains(t, output, `"where_clause":`)
			},
		},
		{
			name:        "invalid query",
			inputQuery:  "SELECT FROM", // Invalid SQL
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a reader from the input query
			body := strings.NewReader(tt.inputQuery)

			// Call the parse endpoint
			result, err := PostEndpoint(db, "/parse", body)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotEmpty(t, result)

			// Run additional checks if provided
			if tt.checkOutput != nil {
				tt.checkOutput(t, result)
			}
		})
	}
}
