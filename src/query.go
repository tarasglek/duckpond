package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type QueryResponse struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data [][]interface{} `json:"data"`
	Rows int             `json:"rows"`
	Statistics struct {
		Elapsed   float64 `json:"elapsed"`
		RowsRead  int64   `json:"rows_read"`
		BytesRead int64   `json:"bytes_read"`
	} `json:"statistics"`
}

func ExecuteQuery(db *sql.DB, query string) (*QueryResponse, error) {
	// First serialize and log the query
	serializedQuery := fmt.Sprintf("SELECT json_serialize_sql('%s')", strings.ReplaceAll(query, "'", "''"))
	var serializedJSON string
	err := db.QueryRow(serializedQuery).Scan(&serializedJSON)
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

	for i, col := range columns {
		response.Meta[i].Name = col
		response.Meta[i].Type = columnTypes[i].DatabaseTypeName()
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
			rowData[i] = values[i]
		}
		data = append(data, rowData)
	}

	response.Data = data
	response.Rows = len(data)
	// TODO: Add actual statistics collection
	response.Statistics.Elapsed = 0.01
	response.Statistics.RowsRead = int64(response.Rows)
	response.Statistics.BytesRead = 0

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
	default:
		return "", fmt.Errorf("unknown endpoint: %s", endpoint)
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
