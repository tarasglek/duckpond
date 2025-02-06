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
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Initialize logging for tests.
	InitLogger("info")
}

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
func testQuery(t *testing.T, ib *DuckpondDB, queryFile string) {
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

	duckpondResp, err := ib.PostEndpoint("/query", string(query_bytes))
	assert.NoError(t, err, "IceBase request failed")
	// Test against IceBase
	var duckpondJSON map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(duckpondResp), &duckpondJSON),
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

	log.Debug().Interface("httpJSON", httpJSON).Interface("expectedJSON", expectedJSON).Str("queryFile", queryFile).Msg("Comparing results")

	if queryFile == "test/query/query_end-to-end.sql" {
		// log that that http reference is broken for this test
		log.Info().Msgf("Skipping HTTP response comparison for %s because the reference is broken", expectedPath)
	} else {
		// Compare results
		assert.Equal(t,
			processAndCompare(t, expectedJSON, expectedJSON),
			processAndCompare(t, httpJSON, expectedJSON),
			fmt.Sprintf("HTTP response mismatch for %s", expectedPath))
	}
	assert.Equal(t,
		processAndCompare(t, expectedJSON, expectedJSON),
		processAndCompare(t, duckpondJSON, expectedJSON),
		fmt.Sprintf("IceBase response mismatch for %s", expectedPath))
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
	ib, err := NewIceBase(
		WithStorageDir("testdata/http_query_test_tables"),
		WithQuerySplittingEnabled(),
	)
	assert.NoError(t, err, "Failed to create IceBase")
	defer ib.Close()

	// start HTTP server
	referenceDuckDB, err := InitializeDuckDB()
	assert.NoError(t, err, "Failed to initialize DuckDB")
	defer referenceDuckDB.Close()
	_, err = referenceDuckDB.Exec(`
				INSTALL httpserver FROM community;
				LOAD httpserver;
				SELECT httpserve_start('localhost', '8882', '');`)
	assert.NoError(t, err, "Failed to setup HTTP server")
	assert.NoError(t, waitForServerReady(), "Server not ready")

	// Run tests for all query files
	testFiles, err := filepath.Glob("test/query/query_*.sql")
	assert.NoError(t, err, "Failed to find test files")

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Destroy any existing state after each test
			defer func() {
				if err := ib.Destroy(); err != nil {
					t.Fatalf("Failed to destroy IceBase: %v", err)
				}
				if err := ResetMemoryDB(referenceDuckDB); err != nil {
					t.Fatalf("Failed to reset memory DB: %v", err)
				}

			}()
			// Create temp schema for this test
			schemaName := fmt.Sprintf("test_%d", time.Now().UnixNano())
			if err != nil {
				t.Fatalf("Failed to create schema %s: %v", schemaName, err)
			}
			// fmt.Printf("Testing %s\n", testFile)
			// Run the actual test
			testQuery(t, ib, testFile)
		})
	}
}
