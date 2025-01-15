package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	expectedJSON := readJSON(t, queryFile+".result.json")

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

// readJSON reads and parses a JSON file
func readJSON(t *testing.T, path string) map[string]interface{} {
	data, err := os.ReadFile(path)
	assert.NoError(t, err, "Failed to read JSON file")
	var result map[string]interface{}
	assert.NoError(t, json.Unmarshal(data, &result), "Failed to parse JSON")
	return result
}

func TestHttpQuery(t *testing.T) {
	// Setup IceBase and HTTP server
	ib, err := NewIceBase()
	assert.NoError(t, err, "Failed to create IceBase")
	defer ib.Close()

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
			// Create temp schema for this test
			schemaName := fmt.Sprintf("test_%d", time.Now().UnixNano())
			_, err := ib.DB().Exec(fmt.Sprintf(`
				CREATE SCHEMA %s;
				SET search_path TO %s;
			`, schemaName, schemaName))
			if err != nil {
				t.Fatalf("Failed to create temp schema: %v", err)
			}
			
			// Ensure schema is dropped after test
			defer func() {
				_, err := ib.DB().Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
				if err != nil {
					t.Logf("Warning: failed to drop temp schema: %v", err)
				}
			}()

			// Run the actual test
			testQuery(t, ib, testFile)
		})
	}
}
