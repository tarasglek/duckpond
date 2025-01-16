package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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
			timestamp TIMESTAMP PRIMARY KEY,
			table_name TEXT NOT NULL,
			raw_query TEXT NOT NULL
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
