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

type IceBase struct {
	db *sql.DB
}

// DB returns the underlying DuckDB instance
func (ib *IceBase) DB() *sql.DB {
	return ib.db
}

func NewIceBase() (*IceBase, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Load JSON extension
	if _, err := db.Exec("LOAD json;"); err != nil {
		return nil, fmt.Errorf("failed to load JSON extension: %w", err)
	}

	// Register UUIDv7 UDF
	if err := registerUUIDv7UDF(db); err != nil {
		return nil, fmt.Errorf("failed to register UUIDv7 UDF: %w", err)
	}

	// Initialize sample data
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS person (id INTEGER, name VARCHAR)`); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &IceBase{db: db}, nil
}

func (ib *IceBase) Close() error {
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

func (ib *IceBase) ExecuteQuery(query string) (*QueryResponse, error) {
	start := time.Now()

	// Serialize and log the query
	// serializedJSON, err := ib.SerializeQuery(query)
	/*if err != nil {
		log.Printf("Failed to serialize query: %v\nQuery: %s", err, query)
	} else {
		log.Printf("Serialized query: %s", serializedJSON)
	}*/

	// Then execute the original query
	rows, err := ib.db.Query(query)
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

	var response QueryResponse
	response.Meta = make([]struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}, len(columns))

	// Create a map to track UUID columns
	uuidColumns := make(map[int]bool)

	for i, col := range columns {
		response.Meta[i].Name = col
		dbType := columnTypes[i].DatabaseTypeName()
		response.Meta[i].Type = dbType

		// Store whether this column is a UUID type
		if dbType == "UUID" {
			uuidColumns[i] = true
		}
	}

	var data [][]interface{}
	for rows.Next() {
		// values will hold the actual data from the database row
		values := make([]interface{}, len(columns))

		// valuePtrs is an array of pointers to the values array elements
		// This is necessary because rows.Scan() requires pointers to where it should
		// store the scanned values. We can't pass values directly since it contains
		// interface{} elements, not pointers.
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			// Each pointer in valuePtrs points to the corresponding element in values
			valuePtrs[i] = &values[i]
		}

		// Scan the current row into our value pointers
		// This will populate the values array through the pointers in valuePtrs
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

	response.Data = data
	response.Rows = len(data)
	elapsed := time.Since(start)
	response.Statistics.Elapsed = elapsed.Seconds()

	return &response, nil
}

func (ib *IceBase) PostEndpoint(endpoint string, body string) (string, error) {
	switch endpoint {
	case "/query":
		response, err := ib.ExecuteQuery(body)
		if err != nil {
			return "", fmt.Errorf("query execution failed: %w", err)
		}

		jsonData, err := json.Marshal(response)
		if err != nil {
			return "", fmt.Errorf("failed to marshal JSON: %w", err)
		}

		return string(jsonData), nil
	case "/parse":
		// Get structured table definition
		tableDef, err := LogWalkSQL(body, true)
		if err != nil {
			return "", fmt.Errorf("failed to parse SQL: %w", err)
		}

		// If it's a CREATE TABLE statement, return the structured definition
		if tableDef != nil {
			jsonData, err := json.Marshal(tableDef)
			if err != nil {
				return "", fmt.Errorf("failed to marshal table definition: %w", err)
			}
			return string(jsonData), nil
		}

		// For other queries, return the serialized version
		serializedJSON, err := ib.SerializeQuery(body)
		if err != nil {
			return "", fmt.Errorf("query serialization failed: %w", err)
		}
		return serializedJSON, nil
	default:
		return "", fmt.Errorf("unknown endpoint: %s", endpoint)
	}
}

func (ib *IceBase) QueryHandler() http.HandlerFunc {
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

func (ib *IceBase) ParseHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		query, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		serializedJSON, err := ib.SerializeQuery(string(query))
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to serialize query: %v", err), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(serializedJSON))
	}
}
