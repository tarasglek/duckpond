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

var (
	pingClient = &http.Client{
		Timeout: 1 * time.Millisecond,
	}
	requestClient = &http.Client{
		Timeout: 1 * time.Second,
	}
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
	url := serverURL(pingPath)
	timeout := 5 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}

		resp, err := pingClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(1 * time.Millisecond)
	}

	return fmt.Errorf("server did not become ready within %v", timeout)
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

	// Create IceBase instance
	ib, err := NewIceBase()
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

	// Start HTTP server
	_, err = db.Exec("INSTALL httpserver; LOAD httpserver;")
	if err != nil {
		t.Fatalf("Failed to install httpfs: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("SELECT httpserve_start('%s', %s, '');", serverHost, serverPort))
	if err != nil {
		t.Fatalf("Failed to start HTTP server: %v", err)
	}

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

			// Test against HTTP server
			req, err := http.NewRequest("POST", serverURL("/?default_format=JSONCompact"), bytes.NewReader(query))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			resp, err := requestClient.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer resp.Body.Close()

			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			// Test against IceBase
			icebaseResponse, err := ib.PostEndpoint("/query", string(query))
			if err != nil {
				t.Fatalf("IceBase request failed: %v", err)
			}

			// Read expected result
			resultFile := testFile + ".result.json"
			expectedResult, err := os.ReadFile(resultFile)
			if err != nil {
				t.Fatalf("Failed to read expected result file: %v", err)
			}

			// Parse all JSON responses
			var httpJSON, icebaseJSON, expectedJSON map[string]interface{}
			err = json.Unmarshal(responseBody, &httpJSON)
			if err != nil {
				t.Fatalf("Failed to parse HTTP response JSON: %v", err)
			}

			err = json.Unmarshal([]byte(icebaseResponse), &icebaseJSON)
			if err != nil {
				t.Fatalf("Failed to parse IceBase response JSON: %v", err)
			}

			err = json.Unmarshal(expectedResult, &expectedJSON)
			if err != nil {
				t.Fatalf("Failed to parse expected result JSON: %v", err)
			}

			// Filter response keys based on expected result
			filteredHTTP := filterResponseKeys(httpJSON, expectedJSON)
			filteredIcebase := filterResponseKeys(icebaseJSON, expectedJSON)

			// Convert back to JSON for comparison
			filteredHTTPBytes, err := json.Marshal(filteredHTTP)
			if err != nil {
				t.Fatalf("Failed to marshal filtered HTTP response: %v", err)
			}

			filteredIcebaseBytes, err := json.Marshal(filteredIcebase)
			if err != nil {
				t.Fatalf("Failed to marshal filtered IceBase response: %v", err)
			}

			expectedResultBytes, err := json.Marshal(expectedJSON)
			if err != nil {
				t.Fatalf("Failed to marshal expected result: %v", err)
			}

			// Pretty print all for comparison
			var HTTPExtResponse, IcebaseResponse, HTTPExtExpected bytes.Buffer
			err = json.Indent(&HTTPExtResponse, filteredHTTPBytes, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print HTTP response: %v", err)
			}

			err = json.Indent(&IcebaseResponse, filteredIcebaseBytes, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print IceBase response: %v", err)
			}

			err = json.Indent(&HTTPExtExpected, expectedResultBytes, "", "  ")
			if err != nil {
				t.Fatalf("Failed to pretty-print expected result: %v", err)
			}

			// Compare the results
			assert.Equal(t, HTTPExtExpected.String(), HTTPExtResponse.String(), "HTTP Response does not match expected result")
			assert.Equal(t, HTTPExtExpected.String(), IcebaseResponse.String(), "IceBase Response does not match expected result")
		})
	}
}
