package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type Log struct {
	db        *sql.DB
	tableName string
}

func NewLog(tableName string) *Log {
	return &Log{
		tableName: tableName,
	}
}

func (l *Log) getDB() (*sql.DB, error) {
	if l.db != nil {
		return l.db, nil
	}

	// Create storage directory structure
	logDir := filepath.Join("storage", l.tableName, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create database path
	dbPath := filepath.Join(logDir, "log.db")

	// Initialize main database connection
	db, err := InitializeDuckDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Attach log database
	_, err = db.Exec(fmt.Sprintf(`
		ATTACH DATABASE '%s' AS log_db;
		USE log_db;
	`, dbPath))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to attach log database: %w", err)
	}

	// Create schema if needed
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_log (
			timestamp TIMESTAMP PRIMARY KEY,
			raw_query TEXT NOT NULL
		);
		
		CREATE TABLE IF NOT EXISTS insert_log (
			id UUID PRIMARY KEY,
			partition TEXT NOT NULL DEFAULT '',
			tombstoned_unix_time BIGINT NOT NULL DEFAULT 0,
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create schema_log table: %w", err)
	}

	l.db = db
	return l.db, nil
}

func (l *Log) createTable(rawCreateTable string) (int, error) {
	db, err := l.getDB()
	if err != nil {
		return -1, err
	}

	// Insert the raw query
	_, err = db.Exec(`
		INSERT INTO schema_log (timestamp, raw_query)
		VALUES (CURRENT_TIMESTAMP, ?);
	`, rawCreateTable)
	if err != nil {
		return -1, fmt.Errorf("failed to log table creation: %w", err)
	}

	return 0, nil
}

func (l *Log) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

func (l *Log) Insert(tx *sql.Tx, table string, query string) (int, error) {
	// Generate UUIDv7 using Go library
	uuid, err := uuid.NewV7()
	if err != nil {
		return -1, fmt.Errorf("failed to generate UUID: %w", err)
	}

	// Create storage directory structure
	dataDir := filepath.Join("storage", table, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return -1, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create parquet file path using UUID
	parquetPath := filepath.Join(dataDir, uuid.String()+".parquet")

	// Execute COPY TO PARQUET using the transaction
	_, err = tx.Exec(fmt.Sprintf(`
		COPY (%s) TO '%s' (FORMAT PARQUET);
	`, query, parquetPath))
	if err != nil {
		return -1, fmt.Errorf("failed to copy to parquet: %w", err)
	}

	// Insert into insert_log table
	_, err = tx.Exec(`
		INSERT INTO insert_log (id, partition)
		VALUES (?, ?);
	`, uuid.String(), "")
	if err != nil {
		return -1, fmt.Errorf("failed to log insert: %w", err)
	}

	return 0, nil
}
