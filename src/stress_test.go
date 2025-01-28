package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const assertPrefix = "-- ASSERT"

func handleAssertActionInComment(t *testing.T, ib *IceBase, comment string) {
    // Strip assertPrefix
    assertionParts := strings.SplitN(strings.TrimPrefix(comment, assertPrefix), ":", 2)
    fmt.Printf("------------------%v\n", assertionParts)
    if len(assertionParts) != 2 {
        t.Fatalf("Invalid assert format: %s", comment)
    }

    directive := strings.TrimSpace(assertionParts[0])
    expected := strings.TrimSpace(assertionParts[1])

    // Split into command and path
    directiveParts := strings.SplitN(directive, " ", 2)
    if len(directiveParts) != 2 {
        t.Fatalf("Invalid assert directive: %s", directive)
    }

    switch directiveParts[0] {
    case "COUNT_PARQUET":
        assertCountParquet(t, ib, directiveParts[1], expected)
    }
}

func assertCountParquet(t *testing.T, ib *IceBase, args string, expected string) {
    tableName := args // args is the table name for COUNT_PARQUET
    expectedCount, err := strconv.Atoi(expected)
    assert.NoError(t, err, "Invalid expected count format: %s", expected)
    
    log, exists := ib.logs[tableName]
    if !exists {
        t.Fatalf("Table %s not found for LIST assertion", tableName)
    }
    storagePath := tableName + "/data"

    files, err := log.storage.List(storagePath)
    assert.NoError(t, err, "Failed to list storage path %s", storagePath)
    assert.Equal(t, expectedCount, len(files), "File count mismatch for %s", tableName)
}

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
				cleanQuery := strings.TrimSpace(query)

				if strings.HasPrefix(cleanQuery, assertPrefix) {
					fmt.Println("--------------", cleanQuery)
					handleAssertActionInComment(t, ib, cleanQuery)
				} else {
					_, err = ib.PostEndpoint("/query", cleanQuery)
					assert.NoError(t, err, "Query failed: %s", cleanQuery)
				}
			}

		})
	}
}
