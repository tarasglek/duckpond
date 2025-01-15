package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

func SerializeQuery(db *sql.DB, query string) (string, error) {
	// First try to prepare the query to validate syntax
	_, err := db.Prepare(query)
	if err != nil {
		return "", fmt.Errorf("invalid query syntax: %w", err)
	}

	serializedQuery := fmt.Sprintf("SELECT json_serialize_sql('%s')", strings.ReplaceAll(query, "'", "''"))
	var serializedJSON string
	err = db.QueryRow(serializedQuery).Scan(&serializedJSON)
	if err != nil {
		return "", fmt.Errorf("failed to serialize query: %w", err)
	}
	return serializedJSON, nil
}

type QueryResponse struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data       [][]interface{} `json:"data"` // Will always be [] instead of null
	Rows       int             `json:"rows"`
	Statistics struct {
		Elapsed float64 `json:"elapsed"` // in seconds
	} `json:"statistics"`
}

func ExecuteQuery(db *sql.DB, query string) (*QueryResponse, error) {
	start := time.Now()

	// Serialize and log the query
	serializedJSON, err := SerializeQuery(db, query)
	if err != nil {
		log.Printf("Failed to serialize query: %v\nQuery: %s", err, query)
	} else {
		log.Printf("Serialized query: %s", serializedJSON)
	}

	// Then execute the original query
	rows, err := db.Query(query)
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
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowData := make([]interface{}, len(columns))
		for i := range values {
			if values[i] == nil {
				rowData[i] = "NULL"
				continue
			}

			// Convert all values to strings based on column type
			if response.Meta[i].Type == "UUID" {
				// Handle UUID specifically
				switch v := values[i].(type) {
				case []byte:
					rowData[i] = uuid.UUID(v).String()
				case string:
					rowData[i] = v
				default:
					rowData[i] = fmt.Sprintf("%v", v)
				}
			} else {
				rowData[i] = fmt.Sprintf("%v", values[i])
			}
		}
		data = append(data, rowData)
	}

	response.Data = data
	response.Rows = len(data)
	elapsed := time.Since(start)
	response.Statistics.Elapsed = elapsed.Seconds()

	return &response, nil
}

// Core function to handle POST requests
func PostEndpoint(db *sql.DB, endpoint string, body io.Reader) (string, error) {
	switch endpoint {
	case "/query":
		query, err := io.ReadAll(body)
		if err != nil {
			return "", fmt.Errorf("failed to read request body: %w", err)
		}

		response, err := ExecuteQuery(db, string(query))
		if err != nil {
			return "", fmt.Errorf("query execution failed: %w", err)
		}

		jsonData, err := json.Marshal(response)
		if err != nil {
			return "", fmt.Errorf("failed to marshal JSON: %w", err)
		}

		return string(jsonData), nil
	case "/parse":
		query, err := io.ReadAll(body)
		if err != nil {
			return "", fmt.Errorf("failed to read request body: %w", err)
		}

		serializedJSON, err := SerializeQuery(db, string(query))
		if err != nil {
			return "", fmt.Errorf("query serialization failed: %w", err)
		}

		return serializedJSON, nil
	default:
		return "", fmt.Errorf("unknown endpoint: %s", endpoint)
	}
}

func ParseHandler(db *sql.DB) http.HandlerFunc {
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

		serializedJSON, err := SerializeQuery(db, string(query))
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to serialize query: %v", err), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(serializedJSON))
	}
}

func QueryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		jsonResponse, err := PostEndpoint(db, r.URL.Path, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(jsonResponse))
	}
}
