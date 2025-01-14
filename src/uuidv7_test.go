package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func generateUUIDWithTimestamp(t *testing.T, ib *IceBase, startTime time.Time) (string, string) {
	uuidResp, err := ib.PostEndpoint("/query", "SELECT uuidv7()")
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	var resp QueryResponse
	err = json.Unmarshal([]byte(uuidResp), &resp)
	if err != nil {
		t.Fatalf("Failed to parse UUID response: %v", err)
	}

	uuidStr := resp.Data[0][0].(string)
	_, err = uuid.Parse(uuidStr)
	assert.NoError(t, err, "UUID is invalid")

	// Extract and verify timestamp
	// Parse UUID bytes
	uuidBytes, err := uuid.Parse(uuidStr)
	assert.NoError(t, err, "Failed to parse UUID bytes")

	// Extract first 48 bits (6 bytes) as milliseconds since Unix epoch
	uuidTime := int64(uuidBytes[0])<<40 | int64(uuidBytes[1])<<32 | int64(uuidBytes[2])<<24 |
		int64(uuidBytes[3])<<16 | int64(uuidBytes[4])<<8 | int64(uuidBytes[5])

	// Convert startTime to milliseconds since Unix epoch
	startMillis := startTime.UnixMilli()
	assert.True(t, uuidTime >= startMillis,
		fmt.Sprintf("UUID timestamp should be >= start time (uuid: %d, start: %d)",
			uuidTime, startMillis))

	// Convert back to hex string for comparison
	uuidTimestamp := fmt.Sprintf("%012x", uuidTime)

	return uuidStr, uuidTimestamp
}

func TestUUIDv7(t *testing.T) {
	// Create IceBase instance
	ib, err := NewIceBase()
	if err != nil {
		t.Fatalf("Failed to create IceBase: %v", err)
	}
	defer ib.Close()

	// Record start time before generating UUIDs
	startTime := time.Now()

	// Generate two UUIDv7 values with minimal delay
	uuidStr1, timestamp1 := generateUUIDWithTimestamp(t, ib, startTime)
	time.Sleep(1 * time.Millisecond) // Ensure we're in the same timestamp unit
	uuidStr2, timestamp2 := generateUUIDWithTimestamp(t, ib, startTime)

	t.Logf("Generated UUIDs:\nUUID1: %s\nUUID2: %s", uuidStr1, uuidStr2)

	// Verify timestamps are sequential
	assert.True(t, timestamp2 >= timestamp1,
		"UUID timestamps should be sequential (timestamp2 should be >= timestamp1)")

	// Verify uniqueness
	assert.NotEqual(t, uuidStr1, uuidStr2, "UUIDs should be unique")
}
