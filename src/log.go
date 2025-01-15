package main

import (
	"fmt"
	"log"
	"time"
)

type Log struct {
	tableName string
	db        *sql.DB
}

func NewLog(tableName string, db *sql.DB) *Log {
	return &Log{
		tableName: tableName,
		db:        db,
	}
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
