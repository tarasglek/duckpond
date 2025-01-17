package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	// Read and execute query
	query, err := os.ReadFile(queryFile)
	assert.NoError(t, err, "Failed to read query file")

	// Test against HTTP server
	resp, err := httpClient.Post(serverURL+"/?default_format=JSONCompact",
		"text/plain", bytes.NewReader(query))
	assert.NoError(t, err, "HTTP request failed")
	defer resp.Body.Close()

	var httpJSON map[string]interface{}
	assert.NoError(t, json.NewDecoder(resp.Body).Decode(&httpJSON),
		"Failed to parse HTTP response")

	// Test against IceBase
	icebaseResp, err := ib.PostEndpoint("/query", string(query))
	assert.NoError(t, err, "IceBase request failed")
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
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
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
	_, err = ib.DB().Exec(`
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

			log.Printf("Running test: %s", testFile)
			// Create temp schema for this test
			schemaName := fmt.Sprintf("test_%d", time.Now().UnixNano())
			if err != nil {
				t.Fatalf("Failed to create schema %s: %v", schemaName, err)
			}

			// Run the actual test
			testQuery(t, ib, testFile)

			// Destroy any existing state before each test
			// don't use defer so we have state available for debugging if test fails
			if err := ib.Destroy(); err != nil {
				t.Logf("Warning: failed to destroy icebase state: %v", err)
			}

		})
	}
}
