package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStressTest(t *testing.T) {
	testFiles, err := filepath.Glob("test/stress/query_*.sql")
	assert.NoError(t, err, "Failed to find test files")

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Create fresh IceBase for each test file
			prefix := "testdata/stress_test_tables"
			ib, err := NewIceBase(
				WithStorageDir(prefix),
				WithQuerySplittingEnabled(),
			)
			assert.NoError(t, err, "Failed to create IceBase")
			defer ib.Close()
			defer func() {
				assert.NoError(t, ib.Destroy(), "Failed to clean up after test")
			}()

			// Read test SQL file
			content, err := os.ReadFile(testFile)
			assert.NoError(t, err, "Failed to read test file")

			// Split into individual queries
			queries, err := ib.SplitNonEmptyQueries(string(content))
			assert.NoError(t, err, "Failed to split queries")

			// Execute each query against /query endpoint
			for _, query := range queries {
				fmt.Printf("Executing query: %s\n", query)
				_, err = ib.PostEndpoint("/query", query)
				assert.NoError(t, err, "Query failed: %s", query)
			}

			// Verify storage contents
			// assert.Equal(len(ib.logs), 1, "Expected one table")

			// Get first table's log (order isn't guaranteed but we just want to verify storage)
			for tableName, log := range ib.logs {
				files, err := log.storage.List("") // List all files under storage dir
				if err != nil {
					t.Fatalf("Failed to list storage for table %s: %v", tableName, err)
				}
				fmt.Printf("Files: %v\n", files)
				assert.NoError(t, err, "Failed to list storage for table %s", tableName)
				t.Logf("Storage files for table %s: %v", tableName, files)
				break // Just check first table
			}

		})
	}
}
