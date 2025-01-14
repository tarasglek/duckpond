package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/assert"
)

func TestHTTPExtension(t *testing.T) {
	// Start DuckDB with HTTP server
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start HTTP server
	_, err = db.Exec("INSTALL httpfs; LOAD httpfs;")
	if err != nil {
		t.Fatalf("Failed to install httpfs: %v", err)
	}

	_, err = db.Exec("SELECT httpserve_start('0.0.0.0', 8882, '');")
	if err != nil {
		t.Fatalf("Failed to start HTTP server: %v", err)
	}
	defer db.Exec("SELECT httpserve_stop();")

	// Get list of test query files
	testFiles, err := filepath.Glob("query_test/query_*.sql")
	if err != nil {
		t.Fatalf("Failed to find test files: %v", err)
	}

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Read the query
			query, err := os.ReadFile(testFile)
			if err != nil {
				t.Fatalf("Failed to read query file: %v", err)
			}

			// Prepare the request
			req, err := http.NewRequest("POST", "http://localhost:8882/?default_format=JSONCompact", bytes.NewReader(query))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			// Send the request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer resp.Body.Close()

			// Read and pretty-print the response
			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			var prettyResponse bytes.Buffer
			err = json.Indent(&prettyResponse, responseBody, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print response: %v", err)
			}

			// Read and pretty-print the expected result
			resultFile := strings.Replace(testFile, "query_", "query_result_", 1)
			resultFile = strings.Replace(resultFile, ".sql", ".json", 1)

			expectedResult, err := os.ReadFile(resultFile)
			if err != nil {
				t.Fatalf("Failed to read expected result file: %v", err)
			}

			var prettyExpected bytes.Buffer
			err = json.Indent(&prettyExpected, expectedResult, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print expected result: %v", err)
			}

			// Compare the results
			assert.Equal(t, prettyExpected.String(), prettyResponse.String(), "Response does not match expected result")
		})
	}
}
