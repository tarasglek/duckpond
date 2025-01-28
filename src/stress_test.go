package main

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func StressTest(t *testing.T) {
	// Create IceBase with custom storage directory
	ib, err := NewIceBase(
		WithStorageDir("tmp/stress_test_tables"),
		WithQuerySplittingEnabled(),
	)
	assert.NoError(t, err, "Failed to create IceBase")
	defer ib.Close()

	// Run tests for all query files
	testFiles, err := filepath.Glob("test/stress/query_*.sql")
	assert.NoError(t, err, "Failed to find test files")
	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Destroy any existing state after each test
			defer func() {
				if err := ib.Destroy(); err != nil {
					t.Fatalf("Failed to destroy IceBase: %v", err)
				}
			}()
			testSQLs = ReadFile(testFile)
			for query in SplitNonEmptyQueries testSQLs {
				ib.PostEndpoint(query) and check for errors
			}
		})
	}
}
