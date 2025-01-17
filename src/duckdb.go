package main

import (
	"database/sql"
	"fmt"
)

// InitializeDuckDB loads JSON extension and registers UUIDv7 UDFs
func InitializeDuckDB() (*sql.DB, error) {
	// Open database connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Load JSON extension
	if _, err := db.Exec("LOAD json;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load JSON extension: %w", err)
	}

	// Register UUIDv7 UDF
	if err := registerUUIDv7UDF(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to register UUIDv7 UDF: %w", err)
	}

	// Register UUIDv7 time extractor UDF
	if err := registerUUIDv7TimeUDF(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to register uuid_v7_time UDF: %w", err)
	}

	return db, nil
}

// ResetMemoryDB resets the in-memory database state
func ResetMemoryDB(db *sql.DB) error {
	_, err := db.Exec(`
		ATTACH ':memory:' AS tmp;
		DETACH icebase;
		ATTACH ':memory:' AS icebase;
		USE icebase;
		DETACH tmp;
	`)
	if err != nil {
		return fmt.Errorf("failed to reset memory database: %w", err)
	}
	return nil
}
