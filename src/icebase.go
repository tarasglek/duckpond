package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type QueryResponse struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data       [][]interface{} `json:"data"` // Always initialized as []
	Rows       int             `json:"rows"`
	Statistics struct {
		Elapsed float64 `json:"elapsed"` // in seconds
	} `json:"statistics"`
}

type IceBase struct {
	db     *sql.DB
	parser *Parser
	logs   map[string]*Log
}

func (ib *IceBase) ExecuteQuery(query string, tx *sql.Tx) (*QueryResponse, error) {
	start := time.Now()

	// Execute the query within transaction
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Initialize response with empty data slice
	response := QueryResponse{
		Data: make([][]interface{}, 0), // Ensure Data is never nil
	}
	var data [][]interface{} // Define data variable that will be used later

	// Populate meta information
	response.Meta = make([]struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}, len(columns))

	for i, col := range columns {
		response.Meta[i].Name = col
		response.Meta[i].Type = columnTypes[i].DatabaseTypeName()
	}

	for rows.Next() {
		// values will hold the actual data from the database row
		values := make([]interface{}, len(columns))

		// valuePtrs is an array of pointers to the values array elements
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			// Each pointer in valuePtrs points to the corresponding element in values
			valuePtrs[i] = &values[i]
		}

		// Scan the current row into our value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Process the scanned values...
		rowData := make([]interface{}, len(columns))
		for i := range values {
			if values[i] == nil {
				rowData[i] = "NULL"
				continue
			}

			// Handle UUID specifically
			if response.Meta[i].Type == "UUID" && values[i] != nil {
				if v, ok := values[i].([]byte); ok {
					rowData[i] = uuid.UUID(v).String()
					continue
				}
			}

			// Default case for all other values
			rowData[i] = fmt.Sprintf("%v", values[i])
		}
		data = append(data, rowData)
	}

	response.Data = data // Now data is properly defined
	response.Rows = len(data)
	elapsed := time.Since(start)
	response.Statistics.Elapsed = elapsed.Seconds()

	return &response, nil
}

// DB returns the underlying DuckDB instance
func (ib *IceBase) DB() *sql.DB {
	return ib.db
}

func NewIceBase() (*IceBase, error) {
	db, err := InitializeDuckDB()
	if err != nil {
		return nil, err
	}

	return &IceBase{
		db:     db,
		parser: NewParser(),
		logs:   make(map[string]*Log),
	}, nil
}

func (ib *IceBase) logByName(tableName string) (*Log, error) {
	if log, exists := ib.logs[tableName]; exists {
		return log, nil
	}

	// Create new log for table
	log := NewLog(tableName)
	ib.logs[tableName] = log
	return log, nil
}

func (ib *IceBase) Close() error {
	// Close all table logs
	for _, log := range ib.logs {
		if log.db != nil {
			if err := log.db.Close(); err != nil {
				return fmt.Errorf("failed to close log: %w", err)
			}
		}
	}
	return ib.db.Close()
}

func (ib *IceBase) SerializeQuery(query string) (string, error) {
	_, err := ib.db.Prepare(query)
	if err != nil {
		return "", fmt.Errorf("invalid query syntax: %w", err)
	}

	serializedQuery := fmt.Sprintf("SELECT json_serialize_sql('%s')", strings.ReplaceAll(query, "'", "''"))
	var serializedJSON string
	err = ib.db.QueryRow(serializedQuery).Scan(&serializedJSON)
	if err != nil {
		return "", fmt.Errorf("failed to serialize query: %w", err)
	}
	return serializedJSON, nil
}

func (ib *IceBase) handleQuery(body string) (string, error) {
	// Execute query, then discard results
	tx, err := ib.db.Begin()
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	op, table := ib.parser.Parse(body)

	// Get log if we have a valid table name
	var log *Log
	if table != "" {
		var err error
		log, err = ib.logByName(table)
		if err != nil {
			return "", fmt.Errorf("failed to get table log: %w", err)
		}
	}

	// Recreate schema before executing user query
	if log != nil {
		if err := log.RecreateSchema(tx); err != nil {
			return "", fmt.Errorf("failed to recreate schema: %w", err)
		}
	}

	response, err := ib.ExecuteQuery(body, tx)
	if err != nil {
		return "", fmt.Errorf("query execution failed: %w", err)
	}

	// Handle CREATE TABLE logging
	if op == OpCreateTable && log != nil {
		if _, err := log.createTable(body); err != nil {
			return "", fmt.Errorf("failed to log table creation: %w", err)
		}
	}

	// Handle INSERT logging
	if op == OpInsert && log != nil {
		if _, err := log.Insert(tx, table, body); err != nil {
			return "", fmt.Errorf("failed to log insert: %w", err)
		}
	}

	// return response as JSON
	jsonData, err := json.Marshal(response)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return string(jsonData), nil
}

func (ib *IceBase) handleParse(body string) (string, error) {
	op, table := ib.parser.Parse(body)

	response := struct {
		Operation string `json:"operation"`
		Table     string `json:"table"`
	}{
		Operation: op.String(),
		Table:     table,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return string(jsonData), nil
}

func (ib *IceBase) PostEndpoint(endpoint string, body string) (string, error) {
	switch endpoint {
	case "/query":
		return ib.handleQuery(body)
	case "/parse":
		return ib.handleParse(body)
	default:
		return "", fmt.Errorf("unknown endpoint: %s", endpoint)
	}
}

func (ib *IceBase) RequestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		jsonResponse, err := ib.PostEndpoint(r.URL.Path, string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(jsonResponse))
	}
}
