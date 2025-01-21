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
// by attaching a new memory database and detaching all others
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

	// now run ATTACH 'memory' as <dbName_tmp>; USE <dbName_tmp>; detach <dbName>; ATTACH 'memory' as <dbName>; USE <dbName>;
	_, err := db.Exec(fmt.Sprintf(
		`ATTACH ':memory:' AS %s_tmp; 
		 USE %s_tmp; 
		 DETACH %s; 
		 ATTACH ':memory:' AS %s; 
		 USE %s`,
		dbName, // %s_tmp
		dbName, // %s_tmp
		dbName, // %s
		dbName, // %s
		dbName, // %s
	))

	if err != nil {
		return fmt.Errorf("failed to reset memory database: %w", err)
	}

	rows, err := db.Query("SELECT name FROM pragma_database_list WHERE name != ?", dbName)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan database name: %w", err)
		}
		if _, err := db.Exec("DETACH " + name); err != nil {
			return fmt.Errorf("failed to detach %s: %w", name, err)
		}
	}

	return nil
}
