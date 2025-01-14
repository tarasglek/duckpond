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

func generateUUIDWithTimestamp(t *testing.T, ib *IceBase) (string, string) {
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

    return uuidStr, uuidStr[:8] // Return both full UUID and timestamp
}

func TestUUIDv7(t *testing.T) {
    // Create IceBase instance
    ib, err := NewIceBase()
    if err != nil {
        t.Fatalf("Failed to create IceBase: %v", err)
    }
    defer ib.Close()

    // Generate two UUIDv7 values with minimal delay
    uuidStr1, timestamp1 := generateUUIDWithTimestamp(t, ib)
    time.Sleep(1 * time.Millisecond) // Ensure we're in the same timestamp unit
    uuidStr2, timestamp2 := generateUUIDWithTimestamp(t, ib)

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
