package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

type Log struct {
	tableName string
	db        *sql.DB
}

func NewLog(tableName string) (*Log, error) {
	// Create storage directory structure
	logDir := filepath.Join("storage", tableName, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create new database connection
	db, err := InitializeDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database for logging: %w", err)
	}

	// Attach log database
	dbPath := filepath.Join(logDir, "log.db")
	_, err = db.Exec(fmt.Sprintf(`
		ATTACH DATABASE '%s' AS db;
		USE db;
	`, dbPath))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to attach log database: %w", err)
	}

	return &Log{
		tableName: tableName,
		db:        db,
	}, nil
}

func (l *Log) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}
