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
	// First attach temporary database
	_, err := db.Exec("ATTACH ':memory:' AS tmp;")
	if err != nil {
		return fmt.Errorf("failed to attach temporary database: %w", err)
	}

	// Query current database list
	rows, err := db.Query("PRAGMA database_list;")
	if err != nil {
		return fmt.Errorf("failed to query database list: %w", err)
	}
	defer rows.Close()

	// Detach all databases except 'tmp'
	for rows.Next() {
		var seq int64
		var name, file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return fmt.Errorf("failed to scan database list: %w", err)
		}
		
		// Skip detaching the temporary database
		if name != "tmp" {
			_, err := db.Exec(fmt.Sprintf("DETACH %s;", name))
			if err != nil {
				return fmt.Errorf("failed to detach database %s: %w", name, err)
			}
		}
	}

	return nil
}
