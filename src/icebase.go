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

type IceBaseOptions struct {
	storageDir           string
	enableQuerySplitting bool
}

type IceBaseOption func(*IceBaseOptions)

func WithStorageDir(dir string) IceBaseOption {
	return func(o *IceBaseOptions) {
		o.storageDir = dir
	}
}

func WithQuerySplittingEnabled() IceBaseOption {
	return func(o *IceBaseOptions) {
		o.enableQuerySplitting = true
	}
}

type IceBase struct {
	_db        *sql.DB
	parser     *Parser
	logs       map[string]*Log
	options    IceBaseOptions
	storageDir string
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

// DB returns the underlying DuckDB instance, initializing it if needed
// This is an in-memory db
func (ib *IceBase) DB() *sql.DB {
	if ib._db == nil {
		var err error
		ib._db, err = InitializeDuckDB()
		if err != nil {
			panic(fmt.Sprintf("failed to initialize database: %v", err))
		}
	}
	return ib._db
}

func NewIceBase(opts ...IceBaseOption) (*IceBase, error) {
	// Set defaults
	options := IceBaseOptions{
		storageDir: "icebase_tables",
	}

	// Apply options
	for _, opt := range opts {
		opt(&options)
	}

	return &IceBase{
		parser:     NewParser(),
		logs:       make(map[string]*Log),
		options:    options,
		storageDir: options.storageDir,
	}, nil
}

func (ib *IceBase) logByName(tableName string) (*Log, error) {
	if log, exists := ib.logs[tableName]; exists {
		return log, nil
	}

	// Create new log for table with storageDir from IceBase
	log := NewLog(ib.storageDir, tableName)
	ib.logs[tableName] = log
	return log, nil
}

func (ib *IceBase) Close() error {
	// Close all table logs
	for _, log := range ib.logs {
		if log.db != nil {
			if err := log.Close(); err != nil {
				return fmt.Errorf("failed to close log: %w", err)
			}
		}
	}

	if ib._db != nil {
		return ib._db.Close()
	}
	return nil
}

// Destroy completely removes all logs and associated data
func (ib *IceBase) Destroy() error {
	// Destroy all table logs (keep existing logic)
	for tableName, log := range ib.logs {
		if err := log.Destroy(); err != nil {
			return fmt.Errorf("failed to destroy log for table %s: %w", tableName, err)
		}
		delete(ib.logs, tableName)
	}

	// // print that we check the storage directory
	// fmt.Println("Checking storage directory for remaining files:")
	// // Minimal recursive file print
	// filepath.WalkDir(ib.storageDir, func(path string, d os.DirEntry, err error) error {
	// 	if err == nil {
	// 		fmt.Println(path)
	// 	}
	// 	return nil
	// })

	// Reset memory database if connection exists
	if ib._db != nil {
		if err := ResetMemoryDB(ib._db); err != nil {
			return fmt.Errorf("failed to reset memory database: %w", err)
		}
	}

	return nil
}

func (ib *IceBase) SerializeQuery(query string) (string, error) {
	db := ib.DB()

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

func (ib *IceBase) handleQuery(body string) (string, error) {
	// Concise logging for query splitting and storage dir
	log.Printf("Query splitting: %v, storageDir: %q", ib.options.enableQuerySplitting, ib.storageDir)

	db := ib.DB()
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var response *QueryResponse
	queries := []string{body}
	if ib.options.enableQuerySplitting {
		queries = strings.Split(body, ";")
	}

	for i, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		op, table := ib.parser.Parse(query)
		log.Printf("%s(%d/%d): %s", op.String(), i+1, len(queries), query)
		var dblog *Log
		if table != "" {
			var err error
			dblog, err = ib.logByName(table)
			if err != nil {
				log.Printf("Failed to get table log for %q: %v", table, err)
				return "", fmt.Errorf("failed to get table log: %w", err)
			}
		}

		if dblog != nil {
			if op == OpSelect {
				if err := dblog.RecreateAsView(tx); err != nil {
					log.Printf("Failed to RecreateAsView for %q: %v", table, err)
					return "", fmt.Errorf("failed to RecreateAsView: %w", err)
				}
			} else {
				if err := dblog.RecreateSchema(tx); err != nil {
					log.Printf("Failed to recreate schema for %q: %v", table, err)
					return "", fmt.Errorf("failed to recreate schema: %w", err)
				}
			}
		}

		response, err = ib.ExecuteQuery(query, tx)
		if err != nil {
			log.Printf("Query execution failed: %v\nQuery: %q", err, query)
			return "", fmt.Errorf("query execution failed: %w", err)
		}

		if op == OpCreateTable && dblog != nil {
			if _, err := dblog.createTable(query); err != nil {
				log.Printf("Failed to log table creation for %q: %v", table, err)
				return "", fmt.Errorf("failed to log table creation: %w", err)
			}
		}

		if op == OpInsert && dblog != nil {
			if _, err := dblog.Insert(tx, table, query); err != nil {
				log.Printf("Failed to log insert for %q: %v", table, err)
				return "", fmt.Errorf("failed to log insert: %w", err)
			}
		}
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal JSON response: %v", err)
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
