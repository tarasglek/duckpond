package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func QueryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		query := string(body)
		response, err := ExecuteQuery(db, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	}
}
