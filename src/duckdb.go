package main

import (
	"database/sql"
	"fmt"
	"log"
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
	// Get database name from character sets
	var dbName string
	if err := db.QueryRow(`
			SELECT default_collate_catalog
			FROM information_schema.character_sets
			LIMIT 1;
	`).Scan(&dbName); err != nil {
		return fmt.Errorf("failed to get database name: %w", err)
	}

	// Build and log the cleanup query
	cleanupQuery := fmt.Sprintf(`
		ATTACH ':memory:' AS tmp;
		DETACH %s;
		ATTACH ':memory:' AS %s;
		USE %s;
		DETACH tmp;
	`, dbName, dbName, dbName)
	log.Printf("Executing database cleanup:\n%s", cleanupQuery)

	// Execute the cleanup
	_, err := db.Exec(cleanupQuery)
	return err
}
