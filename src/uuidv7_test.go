package main

import (
	"database/sql"
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
	var uuid1, uuid2 string
	err = ib.db.QueryRow("SELECT uuidv7()").Scan(&uuid1)
	if err != nil {
		t.Fatalf("Failed to generate first UUID: %v", err)
	}

	time.Sleep(1 * time.Millisecond) // Ensure we're in the same timestamp unit
	err = ib.db.QueryRow("SELECT uuidv7()").Scan(&uuid2)
	if err != nil {
		t.Fatalf("Failed to generate second UUID: %v", err)
	}

	// Validate UUID format
	_, err = uuid.Parse(uuid1)
	assert.NoError(t, err, "First UUID is invalid")
	_, err = uuid.Parse(uuid2)
	assert.NoError(t, err, "Second UUID is invalid")

	// Extract timestamp parts (first 8 characters of UUID)
	timestamp1 := uuid1[:8]
	timestamp2 := uuid2[:8]

	t.Logf("Generated UUIDs:\nUUID1: %s\nUUID2: %s", uuid1, uuid2)

	// Verify timestamps overlap (should be same or +1)
	assert.True(t, timestamp1 == timestamp2 || 
		timestamp2 == incrementHexTimestamp(timestamp1),
		"UUID timestamps should overlap or be sequential")

	// Verify uniqueness
	assert.NotEqual(t, uuid1, uuid2, "UUIDs should be unique")
}

// Helper function to increment hex timestamp by 1
func incrementHexTimestamp(hex string) string {
	val, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		panic("invalid hex timestamp")
	}
	return fmt.Sprintf("%08x", val+1)
}
