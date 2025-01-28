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
				_, err = ib.PostEndpoint("/query", query)
				assert.NoError(t, err, "Query failed: %s", query)
				fmt.Printf("list: %v", ib.<first value in logs>.storage.List(prefix))
			}
		})
	}
}
