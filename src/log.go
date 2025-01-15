package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Log struct {
	tableName string
	db        *sql.DB
}

func NewLog(tableName string) (*Log, error) {
	// Create new database connection
	db, err := InitializeDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database for logging: %w", err)
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

func (l *Log) addSchema(schemaName string) {
	// Get current timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	
	// Create log message
	msg := fmt.Sprintf("[%s] Schema created for table %s: %s", 
		timestamp, l.tableName, schemaName)
		
	// Log to standard logger
	log.Println(msg)

	// Optionally log to database
	_, err := l.db.Exec(`
		INSERT INTO schema_logs (timestamp, table_name, schema_name)
		VALUES (?, ?, ?)`,
		timestamp, l.tableName, schemaName)
	if err != nil {
		log.Printf("Warning: failed to log schema to database: %v", err)
	}
}
