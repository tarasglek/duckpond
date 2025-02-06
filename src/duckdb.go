package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"syscall"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog/log"
)

//go:embed duckdb-uuidv7/uuidv7.sql
var uuid_v7_macro string

//go:embed delta_stats.sql
var delta_stats string

// loadMacros loads all required DuckDB macros
func loadMacros(db *sql.DB) error {
	// Load uuid_v7_macro
	if _, err := db.Exec(uuid_v7_macro); err != nil {
		return fmt.Errorf("failed to load UUIDv7 macro: %w", err)
	}
	// Load delta_stats
	if _, err := db.Exec(delta_stats); err != nil {
		return fmt.Errorf("failed to load delta_stats macro: %w", err)
	}
	return nil
}

// DownloadExtensions installs and loads required DuckDB extensions
func DownloadExtensions(db *sql.DB) error {
	// Retrieve platform info via DuckDB pragma
	var platform string
	if err := db.QueryRow("pragma platform;").Scan(&platform); err != nil {
		return fmt.Errorf("failed to get platform: %w", err)
	}

	// Retrieve DuckDB version info via pragma
	var version, sourceId string
	if err := db.QueryRow("pragma version;").Scan(&version, &sourceId); err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	// Build extension URLs for httpfs and delta
	urlHttpfs := fmt.Sprintf("http://extensions.duckdb.org/%s/%s/%s.duckdb_extension.gz", version, platform, "httpfs")
	urlDelta := fmt.Sprintf("http://extensions.duckdb.org/%s/%s/%s.duckdb_extension.gz", version, platform, "delta")

	log.Info().Msgf("Delta extension URL: %s", urlDelta)
	log.Info().Msgf("httpfs extension URL: %s", urlHttpfs)

	return nil
}

func GetFreeDiskSpace(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

// InitializeDuckDB loads JSON extension and registers UUIDv7 UDFs
func InitializeDuckDB() (*sql.DB, error) {
	// Ensure ~/.duckdb/extensions directory exists
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}

	freeDisk, err := GetFreeDiskSpace(homeDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk space for %s: %w", homeDir, err)
	}
	log.Info().Msgf("Available disk space in %s: %d bytes", homeDir, freeDisk)

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

	// Load all macros
	if err := loadMacros(db); err != nil {
		db.Close()
		return nil, err
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
		log.Debug().Msgf("DETACH %s", name)
		if _, err := db.Exec("DETACH " + name); err != nil {
			return fmt.Errorf("failed to detach %s: %w", name, err)
		}
	}

	// Load all macros
	if err := loadMacros(db); err != nil {
		return err
	}

	return nil
}
