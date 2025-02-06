package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (lrw *loggingResponseWriter) Write(p []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(p)
	lrw.bytesWritten += n
	return n, err
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

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

type DuckpondDB struct {
	dataDB     *sql.DB
	parser     *Parser
	logs       map[string]*Log
	options    IceBaseOptions
	storageDir string
	authToken  string
}

func (ib *DuckpondDB) ExecuteQuery(query string, dataTx *sql.Tx) (*QueryResponse, error) {
	start := time.Now()

	// Initialize response with empty data slice
	response := QueryResponse{
		Data: make([][]interface{}, 0), // Ensure Data is never nil
	}
	var data [][]interface{} // Define data variable that will be used later
	response.Meta = make([]struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}, 0)

	// Execute the query within transaction
	rows, err := dataTx.Query(query)
	if err == nil {
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get columns: %w", err)
		}

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, fmt.Errorf("failed to get column types: %w", err)
		}

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
	} else {
		if err.Error() != "empty query" {
			return nil, fmt.Errorf("query error: %w. %s", err, query)
		}
	}

	response.Data = data // Now data is properly defined
	response.Rows = len(data)
	elapsed := time.Since(start)
	response.Statistics.Elapsed = elapsed.Seconds()

	return &response, nil
}

// DataDB returns the underlying DuckDB instance, initializing it if needed
// This is an in-memory db
func (ib *DuckpondDB) DataDB() *sql.DB {
	if ib.dataDB == nil {
		var err error
		ib.dataDB, err = InitializeDuckDB()
		if err != nil {
			panic(fmt.Sprintf("failed to initialize database: %v", err))
		}
	}
	return ib.dataDB
}

func NewIceBase(opts ...IceBaseOption) (*DuckpondDB, error) {
	// Set defaults
	options := IceBaseOptions{
		storageDir: "duckpond_tables",
	}

	// Apply options
	for _, opt := range opts {
		opt(&options)
	}

	authToken := os.Getenv("BEARER_TOKEN")
	return &DuckpondDB{
		parser:     NewParser(),
		logs:       make(map[string]*Log),
		options:    options,
		storageDir: options.storageDir,
		authToken:  authToken,
	}, nil
}

func (ib *DuckpondDB) logByName(tableName string) (*Log, error) {
	if log, exists := ib.logs[tableName]; exists {
		return log, nil
	}

	// Create new log for table with storageDir from IceBase
	log := NewLog(ib.storageDir, tableName)
	ib.logs[tableName] = log
	return log, nil
}

func (ib *DuckpondDB) Close() error {
	// Close all table logs
	for _, log := range ib.logs {
		if log.logDB != nil {
			if err := log.Close(); err != nil {
				return fmt.Errorf("failed to close log: %w", err)
			}
		}
	}

	if ib.dataDB != nil {
		return ib.dataDB.Close()
	}
	return nil
}

// Destroy completely removes all logs and associated data
func (ib *DuckpondDB) Destroy() error {
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
	if ib.dataDB != nil {
		if err := ResetMemoryDB(ib.dataDB); err != nil {
			return fmt.Errorf("failed to reset memory database: %w", err)
		}
	}

	return nil
}

func (ib *DuckpondDB) SerializeQuery(query string) (string, error) {
	dataDB := ib.DataDB()

	_, err := dataDB.Prepare(query)
	if err != nil {
		return "", fmt.Errorf("invalid query syntax: %w", err)
	}

	serializedQuery := fmt.Sprintf("SELECT json_serialize_sql('%s')", strings.ReplaceAll(query, "'", "''"))
	var serializedJSON string
	err = dataDB.QueryRow(serializedQuery).Scan(&serializedJSON)
	if err != nil {
		return "", fmt.Errorf("failed to serialize query: %w", err)
	}
	return serializedJSON, nil
}

// SplitNonEmptyQueries splits a string of queries by semicolon
// also splits comments from the queries by newline
func SplitNonEmptyQueries(body string) []string {
	// fmt.Printf("SplitNonEmptyQueries: %q\n", body)
	queries := strings.Split(body, ";")

	filtered := make([]string, 0, len(queries))
	// Recursive func that checks if the split query is a comment
	var separateComments func(string)
	separateComments = func(s string) {
		// fmt.Printf("SeparateComments: `%q`\n", s)
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			return
		}
		if !strings.HasPrefix(trimmed, "--") {
			filtered = append(filtered, trimmed)
			return
		}
		// split once
		parts := strings.SplitN(trimmed, "\n", 2)
		filtered = append(filtered, parts[0])
		if len(parts) > 1 {
			separateComments(parts[1])
			return
		}
	}

	for _, q := range queries {
		separateComments(q)
	}

	return filtered
}

func (ib *DuckpondDB) handleQuery(body string) (string, error) {
	// Concise logging for query splitting and storage dir
	log.Info().
		Bool("query_splitting", ib.options.enableQuerySplitting).
		Str("storage_dir", ib.storageDir).
		Msg("Query handling")

	// Get connection to main DATA database (in-memory DuckDB)
	dataConn := ib.DataDB()

	var response *QueryResponse
	var filteredQueries []string
	var err error

	if ib.options.enableQuerySplitting {
		filteredQueries = SplitNonEmptyQueries(body)
	} else {
		// When query splitting is disabled, treat entire body as single query
		filteredQueries = []string{strings.TrimSpace(body)}
	}

	log.Debug().Strs("filteredQueries", filteredQueries).Int("len", len(filteredQueries)).Msg("handleQuery")
	for i, q := range filteredQueries {
		query := q // Already trimmed and filtered

		var handlerErr error
		func() {
			// Begin transaction on DATA database (main in-memory DuckDB)
			dataTx, err := dataConn.Begin()
			if err != nil {
				handlerErr = fmt.Errorf("failed to begin DATA transaction: %w", err)
				return
			}
			// Rollback DATA transaction if not committed
			defer func() {
				if err := dataTx.Rollback(); err != nil {
					log.Error().Err(err).Msg("Failed to rollback transaction")
				}
			}()

			op, table := ib.parser.Parse(query)
			log.Info().
				Str("operation", op.String()).
				Int("query_index", i+1).
				Int("total_queries", len(filteredQueries)).
				Str("query", query).
				Msg("Processing query")

			var dblog *Log
			if table != "" {
				dblog, handlerErr = ib.logByName(table)
				if handlerErr != nil {
					log.Error().Err(handlerErr).Str("table", table).Msg("Failed to get table log")
					return
				}
			}

			if dblog != nil {
				opExpectsTableToExist := op == OpSelect || op == OpVacuum
				tableIsEmpty := false
				if opExpectsTableToExist {
					// Recreate view using LOG database's file list in DATA transaction
					if handlerErr = dblog.CreateViewOfParquet(dataTx); handlerErr != nil {
						isErrNoParquetFilesInTable := errors.Is(handlerErr, ErrNoParquetFilesInTable)
						if isErrNoParquetFilesInTable {
							tableIsEmpty = true
							log.Debug().Msgf("CreateViewOfParquet indicated that tableIsEmpty")
						} else {
							log.Error().Err(handlerErr).Str("table", table).Msg("Failed to recreate view")
							return
						}
					}
				}

				if !opExpectsTableToExist || tableIsEmpty {
					// Recreate schema from LOG database in DATA transaction
					if handlerErr = dblog.CreateTempTable(dataTx); handlerErr != nil {
						log.Error().Err(handlerErr).Str("table", table).Msg("Failed to recreate schema")
						return
					}
				}
			}

			// Duckdb doesn't actually support vacuum yet, so fake it
			if op == OpVacuum {
				if table == "" {
					handlerErr = fmt.Errorf("VACUUM requires a table name")
					return
				}

				dblog, handlerErr = ib.logByName(table)
				if handlerErr != nil {
					handlerErr = fmt.Errorf("failed to get log for VACUUM: %w", handlerErr)
					return
				}

				// Call merge on the log with table name
				if handlerErr = dblog.Merge(table, dataTx); handlerErr != nil {
					handlerErr = fmt.Errorf("VACUUM failed: %w", handlerErr)
					return
				}

				// Return empty response since VACUUM doesn't produce data
				response = &QueryResponse{Data: make([][]interface{}, 0)}
			} else {
				// Execute query against DATA database
				response, handlerErr = ib.ExecuteQuery(query, dataTx)
				if handlerErr != nil {
					log.Error().Err(handlerErr).Str("query", query).Msg("Query execution failed")
					return
				}
			}
			if op == OpCreateTable && dblog != nil {
				// Log schema change to LOG database
				if handlerErr = dblog.logDDL(dataTx, query); handlerErr != nil {
					log.Error().Err(handlerErr).Str("table", table).Msg("Failed to log table creation")
					return
				}
			}

			if op == OpInsert && dblog != nil {
				// Log insert to LOG database while executing in DATA transaction
				if handlerErr = dblog.Insert(dataTx, table); handlerErr != nil {
					log.Error().Err(handlerErr).Str("table", table).Msg("Failed to log insert")
					return
				}
			}
			// No commit because log handles data persistence above
		}()

		if handlerErr != nil {
			return "", handlerErr
		}
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal JSON response")
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return string(jsonData), nil
}

func (ib *DuckpondDB) handleParse(body string) (string, error) {
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

func (ib *DuckpondDB) PostEndpoint(endpoint string, body string) (string, error) {
	switch endpoint {
	case "/query":
		return ib.handleQuery(body)
	case "/parse":
		return ib.handleParse(body)
	default:
		return "", fmt.Errorf("unknown endpoint: %s", endpoint)
	}
}

func (ib *DuckpondDB) RequestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Get client IP from headers or remote address
		clientIP := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			ips := strings.Split(forwarded, ",")
			clientIP = strings.TrimSpace(ips[0])
		} else if real := r.Header.Get("X-Real-IP"); real != "" {
			clientIP = real
		}

		defer func() {
			elapsed := time.Since(startTime)
			// Log in Apache/Nginx common format:
			// <remote_addr> - - [<date>] "<method> <uri> <proto>" <status> <bytes> "<referer>" "<user-agent>" <elapsed>
			log.Info().
				Str("client_ip", clientIP).
				Str("method", r.Method).
				Str("uri", r.RequestURI).
				Str("proto", r.Proto).
				Int("status", lrw.statusCode).
				Int("bytes", lrw.bytesWritten).
				Str("referer", r.Referer()).
				Str("user_agent", r.UserAgent()).
				Dur("elapsed", elapsed).
				Msg("Request completed")
		}()

		// Set CORS headers
		lrw.Header().Set("Access-Control-Allow-Origin", "*")
		lrw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		lrw.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// If BEARER_TOKEN is set, enforce auth checking
		if ib.authToken != "" {
			authHeader := r.Header.Get("Authorization")
			expectedHeader := "Bearer " + ib.authToken
			if authHeader != expectedHeader {
				http.Error(lrw, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			lrw.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(lrw, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(lrw, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		jsonResponse, err := ib.PostEndpoint(r.URL.Path, string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		lrw.Header().Set("Content-Type", "application/json")
		if _, err := lrw.Write([]byte(jsonResponse)); err != nil {
			log.Error().Err(err).Msg("Failed to write response")
			http.Error(lrw, "Internal server error", http.StatusInternalServerError)
		}
	}
}
