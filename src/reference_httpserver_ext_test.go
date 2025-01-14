package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	serverHost = "localhost"
	serverPort = "8882"
	pingPath   = "/ping"
)

func serverURL(path string) string {
	return fmt.Sprintf("http://%s:%s%s", serverHost, serverPort, path)
}

func waitForServerReady() error {
	client := &http.Client{
		Timeout: 1 * time.Millisecond, // 1ms timeout
	}

	url := serverURL(pingPath)
	attempt := 1
	startTime := time.Now()

	for {
		fmt.Printf("Attempt %d: Connecting to %s... ", attempt, url)
		
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Printf("Success! (after %v)\n", time.Since(startTime).Round(time.Millisecond))
				return nil
			}
			fmt.Printf("Got status %d\n", resp.StatusCode)
		} else {
			fmt.Printf("Failed: %v\n", err)
		}

		attempt++
		time.Sleep(100 * time.Millisecond)
	}
}

func filterResponseKeys(responseJSON, expectedJSON map[string]interface{}) map[string]interface{} {
	filtered := make(map[string]interface{})
	for key := range expectedJSON {
		if val, exists := responseJSON[key]; exists {
			filtered[key] = val
		}
	}
	return filtered
}

func TestHTTPExtension(t *testing.T) {
	// Start DuckDB with HTTP server
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start HTTP server
	_, err = db.Exec("INSTALL httpserver; LOAD httpserver;")
	if err != nil {
		t.Fatalf("Failed to install httpfs: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("SELECT httpserve_start('%s', %s, '');", serverHost, serverPort))
	if err != nil {
		t.Fatalf("Failed to start HTTP server: %v", err)
	}
	defer db.Exec("SELECT httpserve_stop();")

	// Wait for server to be ready
	if err := waitForServerReady(); err != nil {
		t.Fatalf("Server did not become ready: %v", err)
	}

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
			req, err := http.NewRequest("POST", serverURL("/?default_format=JSONCompact"), bytes.NewReader(query))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			// Send the request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer resp.Body.Close()

			// Read the response and expected result
			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			resultFile := testFile + ".result.json"
			expectedResult, err := os.ReadFile(resultFile)
			if err != nil {
				t.Fatalf("Failed to read expected result file: %v", err)
			}

			// Parse both JSON responses
			var responseJSON, expectedJSON map[string]interface{}
			err = json.Unmarshal(responseBody, &responseJSON)
			if err != nil {
				t.Fatalf("Failed to parse response JSON: %v", err)
			}

			err = json.Unmarshal(expectedResult, &expectedJSON)
			if err != nil {
				t.Fatalf("Failed to parse expected result JSON: %v", err)
			}

			// Filter response keys based on expected result
			filteredResponse := filterResponseKeys(responseJSON, expectedJSON)

			// Convert back to JSON for comparison
			filteredResponseBytes, err := json.Marshal(filteredResponse)
			if err != nil {
				t.Fatalf("Failed to marshal filtered response: %v", err)
			}

			expectedResultBytes, err := json.Marshal(expectedJSON)
			if err != nil {
				t.Fatalf("Failed to marshal expected result: %v", err)
			}

			// Pretty print both for comparison
			var prettyResponse, prettyExpected bytes.Buffer
			err = json.Indent(&prettyResponse, filteredResponseBytes, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print response: %v", err)
			}

			err = json.Indent(&prettyExpected, expectedResultBytes, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print expected result: %v", err)
			}

			// Compare the results
			assert.Equal(t, prettyExpected.String(), prettyResponse.String(), "Response does not match expected result")
		})
	}
}
