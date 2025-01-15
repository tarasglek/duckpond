package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEndpoint(t *testing.T) {
	ib, err := NewIceBase()
	verbose := false // Set to true when debugging
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

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
				// For SELECT statements, we expect the serialized query
				assert.Contains(t, output, `"type":"SELECT_NODE"`)
				assert.Contains(t, output, `"select_list":`)
			},
		},
		{
			name:       "complex query",
			inputQuery: "SELECT * FROM (SELECT 1 AS a, 2 AS b) t WHERE a = 1",
			checkOutput: func(t *testing.T, output string) {
				// For SELECT statements, we expect the serialized query
				assert.Contains(t, output, `"type":"SELECT_NODE"`)
				assert.Contains(t, output, `"from_table":`)
				assert.Contains(t, output, `"where_clause":`)
			},
		},
		{
			name:        "invalid query",
			inputQuery:  "SELECT FROM", // Invalid SQL
			expectError: true,
			checkOutput: func(t *testing.T, output string) {
				// We can check the error message contains "syntax"
				assert.Contains(t, output, "syntax")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the parse endpoint with string body
			result, err := ib.PostEndpoint("/parse", tt.inputQuery)

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
