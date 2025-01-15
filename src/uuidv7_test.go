package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func generateUUIDWithTimestamp(t *testing.T, ib *IceBase, startTime time.Time) (string, int64) {
	// Generate UUID using the database function
	uuidResp, err := ib.PostEndpoint("/query", "SELECT uuidv7()")
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	// Parse the database response
	var resp QueryResponse
	err = json.Unmarshal([]byte(uuidResp), &resp)
	if err != nil {
		t.Fatalf("Failed to parse UUID response: %v", err)
	}

	// Get UUID string from response
	uuidStr := resp.Data[0][0].(string)

	// Validate UUID format
	uuidBytes, err := uuid.Parse(uuidStr)
	assert.NoError(t, err, "UUID is invalid")

	// Extract first 48 bits (6 bytes) as milliseconds since Unix epoch
	uuidTime := int64(uuidBytes[0])<<40 | int64(uuidBytes[1])<<32 | int64(uuidBytes[2])<<24 |
		int64(uuidBytes[3])<<16 | int64(uuidBytes[4])<<8 | int64(uuidBytes[5])

	// Convert startTime to milliseconds since Unix epoch for comparison
	startMillis := startTime.UnixMilli()

	// Verify UUID timestamp is >= test start time
	assert.True(t, uuidTime >= startMillis,
		fmt.Sprintf("UUID timestamp should be >= start time (uuid: %d, start: %d)",
			uuidTime, startMillis))

	return uuidStr, uuidTime
}

func TestUUIDv7Time(t *testing.T) {
	// Create IceBase instance
	ib, err := NewIceBase()
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

	// Generate a UUIDv7
	uuidResp, err := ib.PostEndpoint("/query", "SELECT uuidv7()")
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	// Parse the UUID from response
	var resp QueryResponse
	err = json.Unmarshal([]byte(uuidResp), &resp)
	if err != nil {
		t.Fatalf("Failed to parse UUID response: %v", err)
	}
	uuidStr := resp.Data[0][0].(string)

	// Extract timestamp using new UDF
	timeResp, err := ib.PostEndpoint("/query", 
		fmt.Sprintf("SELECT uuid_v7_time('%s')", uuidStr))
	if err != nil {
		t.Fatalf("Failed to extract timestamp: %v", err)
	}

	// Parse the timestamp
	var timeRespData QueryResponse
	err = json.Unmarshal([]byte(timeResp), &timeRespData)
	if err != nil {
		t.Fatalf("Failed to parse timestamp response: %v", err)
	}
	timestamp := timeRespData.Data[0][0].(float64)

	// Verify timestamp is within expected range
	now := time.Now().UnixMilli()
	assert.True(t, timestamp > 0, "Timestamp should be positive")
	assert.True(t, timestamp <= now, 
		"UUID timestamp should not be in the future")
}

func TestUUIDv7(t *testing.T) {
	// Create IceBase instance for database operations
	ib, err := NewIceBase()
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

	// Record start time before generating UUIDs
	startTime := time.Now()

	// Generate first UUID and extract timestamp
	uuidStr1, timestamp1 := generateUUIDWithTimestamp(t, ib, startTime)

	// Wait briefly to ensure timestamp progression
	time.Sleep(1 * time.Millisecond)

	// Generate second UUID and extract timestamp
	uuidStr2, timestamp2 := generateUUIDWithTimestamp(t, ib, startTime)

	// Log generated UUIDs for debugging
	t.Logf("Generated UUIDs:\nUUID1: %s\nUUID2: %s", uuidStr1, uuidStr2)

	// Verify timestamps are sequential (second UUID >= first UUID)
	assert.True(t, timestamp2 >= timestamp1,
		fmt.Sprintf("UUID timestamps should be sequential (timestamp2: %d should be >= timestamp1: %d)",
			timestamp2, timestamp1))

	// Verify UUIDs are unique
	assert.NotEqual(t, uuidStr1, uuidStr2, "UUIDs should be unique")
}
