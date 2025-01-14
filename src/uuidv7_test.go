package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestUUIDv7(t *testing.T) {
	// Create IceBase instance
	ib, err := NewIceBase()
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

	// Generate two UUIDv7 values with minimal delay
	uuid1, err := ib.PostEndpoint("/query", "SELECT uuidv7()")
	if err != nil {
		t.Fatalf("Failed to generate first UUID: %v", err)
	}

	time.Sleep(1 * time.Millisecond) // Ensure we're in the same timestamp unit
	uuid2, err := ib.PostEndpoint("/query", "SELECT uuidv7()")
	if err != nil {
		t.Fatalf("Failed to generate second UUID: %v", err)
	}

	// Parse the JSON responses
	var resp1, resp2 QueryResponse
	err = json.Unmarshal([]byte(uuid1), &resp1)
	if err != nil {
		t.Fatalf("Failed to parse first UUID response: %v", err)
	}
	err = json.Unmarshal([]byte(uuid2), &resp2)
	if err != nil {
		t.Fatalf("Failed to parse second UUID response: %v", err)
	}

	// Extract UUID strings from responses
	uuidStr1 := resp1.Data[0][0].(string)
	uuidStr2 := resp2.Data[0][0].(string)

	// Validate UUID format
	_, err = uuid.Parse(uuidStr1)
	assert.NoError(t, err, "First UUID is invalid")
	_, err = uuid.Parse(uuidStr2)
	assert.NoError(t, err, "Second UUID is invalid")

	// Extract timestamp parts (first 8 characters of UUID)
	timestamp1 := uuidStr1[:8]
	timestamp2 := uuidStr2[:8]

	t.Logf("Generated UUIDs:\nUUID1: %s\nUUID2: %s", uuidStr1, uuidStr2)

	// Verify timestamps overlap (should be same or +1)
	assert.True(t, timestamp1 == timestamp2 || 
		timestamp2 == incrementHexTimestamp(timestamp1),
		"UUID timestamps should overlap or be sequential")

	// Verify uniqueness
	assert.NotEqual(t, uuidStr1, uuidStr2, "UUIDs should be unique")
}

// Helper function to increment hex timestamp by 1
func incrementHexTimestamp(hex string) string {
	val, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		panic("invalid hex timestamp")
	}
	return fmt.Sprintf("%08x", val+1)
}
