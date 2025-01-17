package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	pingClient = &http.Client{Timeout: 1 * time.Millisecond}
	httpClient = &http.Client{Timeout: 1 * time.Second}
)

const serverURL = "http://localhost:8882"

// waitForServerReady checks if server is responding to ping requests
func waitForServerReady() error {
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if resp, err := pingClient.Get(serverURL + "/ping"); err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
	return fmt.Errorf("server timeout")
}

// processAndCompare handles JSON processing and comparison for a test case
func processAndCompare(t *testing.T, responseJSON, expectedJSON map[string]interface{}) string {
	// Ensure data is always an array
	if responseJSON["data"] == nil {
		responseJSON["data"] = []interface{}{}
	}

	// Remove type information from meta arrays
	if meta, ok := responseJSON["meta"].([]interface{}); ok {
		for _, m := range meta {
			if item, ok := m.(map[string]interface{}); ok {
				delete(item, "type")
			}
		}
	}

	// Filter response to only include expected keys
	filtered := make(map[string]interface{})
	for key := range expectedJSON {
		if val, exists := responseJSON[key]; exists {
			filtered[key] = val
		}
	}

	// Pretty print for comparison
	filteredBytes, _ := json.MarshalIndent(filtered, "", "  ")
	expectedBytes, _ := json.MarshalIndent(expectedJSON, "", "  ")

	return fmt.Sprintf("Expected:\n%s\n\nActual:\n%s",
		string(expectedBytes), string(filteredBytes))
}

// testQuery handles the core test logic for a single query file
func testQuery(t *testing.T, ib *IceBase, queryFile string) {
	// Read and execute query_bytes
	query_bytes, err := os.ReadFile(queryFile)
	assert.NoError(t, err, "Failed to read query file")

	// Test against HTTP server
	resp, err := httpClient.Post(serverURL+"/?default_format=JSONCompact",
		"text/plain", bytes.NewReader(query_bytes))
	assert.NoError(t, err, "HTTP request failed")
	defer resp.Body.Close()

	// Read the raw response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read response body: %v", err)
	}

	// Check if response is JSON
	var httpJSON map[string]interface{}
	err = json.Unmarshal(bodyBytes, &httpJSON)
	if err != nil {
		// If not JSON, treat as error response
		t.Errorf("Non-JSON response: %s", string(bodyBytes))
		return
	}

	// Reset the response body for JSON decoding
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// for icebase manually split query by ;, then run indiv queries and use last response as final response
	// split query by ;
	// Can't do this in PostEndpoint because we don't have a proper query parser :(
	var icebaseResp string
	all_queries := string(query_bytes)
	indiv_queries := strings.Split(all_queries, ";")
	for _, query := range indiv_queries {
		// continue if trimmed query is empty
		if strings.TrimSpace(query) == "" {
			continue
		}
		icebaseResp, err = ib.PostEndpoint("/query", query)
		assert.NoError(t, err, "IceBase request failed")
	}
	// Test against IceBase
	var icebaseJSON map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(icebaseResp), &icebaseJSON),
		"Failed to parse IceBase response")

	// Read expected result
	expectedPath := queryFile + ".result.json"
	expectedJSON, err := readJSON(t, expectedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// If expected result doesn't exist, write the actual result
			httpJSONBytes, _ := json.MarshalIndent(httpJSON, "", "  ")
			resultFile := writeExpectedResult(t, queryFile, string(httpJSONBytes))
			if resultFile != "" {
				t.Logf("Wrote expected result to: %s", resultFile)
				t.Logf("You can review and rename this file to use it as the expected result")
			}
			t.Fatalf("Expected result file not found: %s", expectedPath)
		} else {
			t.Fatalf("Failed to read expected result: %v", err)
		}
	}

	// Compare results
	assert.Equal(t,
		processAndCompare(t, expectedJSON, expectedJSON),
		processAndCompare(t, httpJSON, expectedJSON),
		"HTTP response mismatch")

	assert.Equal(t,
		processAndCompare(t, expectedJSON, expectedJSON),
		processAndCompare(t, icebaseJSON, expectedJSON),
		"IceBase response mismatch")
}

func writeExpectedResult(t *testing.T, queryFile string, httpJSON string) string {
	// Create the result file path
	resultFile := queryFile + ".result.json.let-me-help-you"

	// Write the JSON to file
	err := os.WriteFile(resultFile, []byte(httpJSON), 0644)
	if err != nil {
		t.Logf("Failed to write expected result file: %v", err)
		return ""
	}
	return resultFile
}

func readJSON(t *testing.T, path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return result, nil
}

func TestHttpQuery(t *testing.T) {
	// Create IceBase with custom storage directory
	ib, err := NewIceBase(WithStorageDir("http_query_test_tables"))
	assert.NoError(t, err, "Failed to create IceBase")
	defer ib.Close()

	// start HTTP server
	referenceDuckDB, err := InitializeDuckDB()
	defer referenceDuckDB.Close()
	_, err = referenceDuckDB.Exec(`
				INSTALL httpserver;
				LOAD httpserver;
				SELECT httpserve_start('localhost', '8882', '');`)
	assert.NoError(t, err, "Failed to setup HTTP server")
	assert.NoError(t, waitForServerReady(), "Server not ready")

	// Run tests for all query files
	testFiles, err := filepath.Glob("query_test/query_*.sql")
	assert.NoError(t, err, "Failed to find test files")

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Destroy any existing state after each test
			defer ib.Destroy()
			defer func() {
				ResetMemoryDB(referenceDuckDB)
			}()
			// Create temp schema for this test
			schemaName := fmt.Sprintf("test_%d", time.Now().UnixNano())
			if err != nil {
				t.Fatalf("Failed to create schema %s: %v", schemaName, err)
			}

			// Run the actual test
			testQuery(t, ib, testFile)
		})
	}
}
