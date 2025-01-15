package main

import (
	"fmt"
	"log"
	"time"
)

type Log struct {
	tableName string
}

func NewLog(tableName string) *Log {
	return &Log{tableName: tableName}
}

func (l *Log) addSchema(schema string) {
	// Get current timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Create log message
	msg := fmt.Sprintf("[%s] Schema created for table %s: %s",
		timestamp, l.tableName, schemaName)

	// Log to standard logger
	log.Println(msg)
}
