package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

type Log struct {
	logDB      *sql.DB
	tableName  string
	storageDir string
	storage    Storage
}

func NewLog(storageDir, tableName string) *Log {
	return &Log{
		tableName:  tableName,
		storageDir: storageDir,
		storage:    NewStorage(storageDir),
	}
}

func (l *Log) getLogDB() (*sql.DB, error) {
	if l.logDB != nil {
		return l.logDB, nil
	}

	// Initialize main database connection
	logDB, err := InitializeDuckDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Create S3 secret if configured
	secretSQL := l.storage.ToDuckDBSecret("icebase_log_s3_secret")
	if secretSQL != "" {
		if _, err := logDB.Exec(secretSQL); err != nil {
			logDB.Close()
			return nil, fmt.Errorf("failed to create S3 secret: %w", err)
		}
	}

	// Create schema if needed
	_, err = logDB.Exec(`
		CREATE TABLE IF NOT EXISTS schema_log (
			timestamp TIMESTAMP PRIMARY KEY,
			raw_query TEXT NOT NULL
		);
		
		CREATE TABLE IF NOT EXISTS insert_log (
			id UUID PRIMARY KEY,
			partition TEXT NOT NULL DEFAULT '',
			tombstoned_unix_time BIGINT NOT NULL DEFAULT 0,
			size BIGINT NOT NULL DEFAULT 0
		);
	`)
	if err != nil {
		logDB.Close()
		return nil, fmt.Errorf("failed to create schema_log table: %w", err)
	}

	l.logDB = logDB
	return l.logDB, nil
}

// Exports db state to a JSON file
func (l *Log) Export() ([]byte, error) {
	db, err := l.getLogDB()
	if err != nil {
		return nil, err
	}

	var jsonResult string
	err = db.QueryRow(`
        WITH json_data AS (
            SELECT
                (SELECT ARRAY_AGG(struct_pack(timestamp, raw_query))
                    FROM schema_log) as schema_log,
                (SELECT ARRAY_AGG(struct_pack(id, partition, tombstoned_unix_time, size))
                    FROM insert_log) as insert_log
        )
        SELECT to_json(struct_pack(schema_log, insert_log))::VARCHAR
        FROM json_data
    `).Scan(&jsonResult)

	return []byte(jsonResult), err
}

// Modified withPersistedLog
func (l *Log) withPersistedLog(op func(logDB *sql.DB) (int, error)) (int, error) {
	const jsonFileName = "log.json"

	logDB, err := l.getLogDB()
	if err != nil {
		return -1, fmt.Errorf("failed to open database: %w", err)
	}

	// Try to read and import existing data through temp file
	jsonPath := filepath.Join(l.tableName, jsonFileName)
	if data, err := l.storage.Read(jsonPath); err == nil {
		// Write to temp file
		tmpFile, err := os.CreateTemp("", "icebase-import-*.json")
		if err != nil {
			return -1, fmt.Errorf("failed to create temp file: %w", err)
		}
		// Deferred Close() for cleanup
		defer tmpFile.Close()           // Ensures file is closed even if errors occur
		defer os.Remove(tmpFile.Name()) // Ensures temp file is deleted

		if _, err := tmpFile.Write(data); err != nil {
			return -1, fmt.Errorf("failed to write temp file: %w", err)
		}
		// Close for writes, it's ready for reads
		tmpFile.Close()

		if importErr := l.Import(tmpFile.Name()); importErr != nil {
			return -1, fmt.Errorf("failed to import %s: %w", jsonPath, importErr)
		}
	}

	// Execute the operation
	result, err := op(logDB)
	if err != nil {
		return result, err
	}

	// Export and write new state
	if exported, exportErr := l.Export(); exportErr != nil {
		return -1, fmt.Errorf("export failed: %w", exportErr)
	} else {
		if writeErr := l.storage.Write(jsonPath, exported); writeErr != nil {
			return -1, fmt.Errorf("failed to write %s: %w", jsonPath, writeErr)
		}
	}

	return result, nil
}

func (l *Log) createTable(rawCreateTable string) (int, error) {
	return l.withPersistedLog(func(db *sql.DB) (int, error) {
		_, err := db.Exec(`
            INSERT INTO schema_log (timestamp, raw_query)
            VALUES (CURRENT_TIMESTAMP, ?);
        `, rawCreateTable)
		if err != nil {
			return -1, fmt.Errorf("failed to log table creation: %w", err)
		}
		return 0, nil
	})
}

func (l *Log) RecreateSchema(dataTx *sql.Tx) error {
	logDB, err := l.getLogDB()
	if err != nil {
		return fmt.Errorf("failed to get log database: %w", err)
	}

	// Query schema_log for all create table statements
	rows, err := logDB.Query(`
		SELECT raw_query 
		FROM schema_log
		ORDER BY timestamp ASC
	`)
	if err != nil {
		return fmt.Errorf("failed to query schema_log: %w", err)
	}
	defer rows.Close()

	// Execute each create table statement in the transaction
	for rows.Next() {
		var createQuery string
		if err := rows.Scan(&createQuery); err != nil {
			return fmt.Errorf("failed to scan schema_log row: %w", err)
		}

		// Execute the create table statement
		if _, err := dataTx.Exec(createQuery); err != nil {
			return fmt.Errorf("failed to execute schema_log query: %w", err)
		}
	}

	return nil
}

// First writes go into a memory-backed table that can be accessed via dataTx param
// They are then persisted to a parquet file and tracked in the insert_log table
// TODO:
// - persist log for parquet files we gonna upload first
// - Then modify reading code to detect missing parquet files and to tombstone them in log
// - This way we wont end up with orphaned parquet files
func (l *Log) Insert(dataTx *sql.Tx, table string, query string) (int, error) {
	return l.withPersistedLog(func(logDB *sql.DB) (int, error) {
		// Original insert logic wrapped in lambda
		var uuidBytes []byte
		err := logDB.QueryRow(`
            INSERT INTO insert_log (id, partition)
            VALUES (uuidv7(), '')
            RETURNING id;
        `).Scan(&uuidBytes)
		if err != nil {
			return -1, fmt.Errorf("failed to insert into insert_log: %w", err)
		}

		uuidStr := uuid.UUID(uuidBytes).String()
		dataDir := filepath.Join(table, "data")
		if err := l.storage.CreateDir(dataDir); err != nil {
			return -1, fmt.Errorf("failed to create data directory: %w", err)
		}

		secretName := "icebase_data_temp_secret"

		// Create secret in transaction
		secretSQL := l.storage.ToDuckDBSecret(secretName)
		if secretSQL != "" {
			if _, err := dataTx.Exec(secretSQL); err != nil {
				return -1, fmt.Errorf("failed to create secret: %w", err)
			}
			defer func() {
				// Clean up secret after operation completes
				_, _ = dataTx.Exec(fmt.Sprintf("DROP SECRET IF EXISTS %s", secretName))
			}()
		}

		// Modified copy command to use secret
		parquetPath := filepath.Join(dataDir, uuidStr+".parquet")
		copyQuery := fmt.Sprintf(`COPY %s TO '%s' (FORMAT PARQUET);`,
			table, l.storage.ToDuckDBPath(parquetPath))

		_, copyErr := dataTx.Exec(copyQuery)
		defer func() {
			log.Printf("%s err: %v", copyQuery, copyErr)
		}()
		if copyErr != nil {
			return -1, fmt.Errorf("failed to copy to parquet: %w", copyErr)
		}

		meta, copyErr := l.storage.Stat(parquetPath)
		if copyErr != nil {
			return -1, fmt.Errorf("failed to get file size: %w", copyErr)
		}

		if _, copyErr = logDB.Exec(`
            UPDATE insert_log 
            SET size = ?
            WHERE id = ?;
        `, meta.Size(), uuidStr); err != nil {
			return -1, fmt.Errorf("failed to update size: %w", err)
		}

		return 0, nil
	})
}

// Recreates the table described in the schema_log table as a view over partitioned parquet files
func (l *Log) listFiles(where string) ([]string, error) {
	db, err := l.getLogDB()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT id FROM insert_log" + where)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		files = append(files, filepath.Join(l.tableName, "data", uuid.UUID(id).String()+".parquet"))
	}
	return files, rows.Err()
}

func (l *Log) RecreateAsView(dataTx *sql.Tx) error {
	// Create temporary secret for the view operation
	secretName := "icebase_view_s3_secret"
	secretSQL := l.storage.ToDuckDBSecret(secretName)
	if secretSQL != "" {
		if _, err := dataTx.Exec(secretSQL); err != nil {
			return fmt.Errorf("failed to create view secret: %w", err)
		}
		// Note we don't drop secret here as the view lifetime persists past this function
	}

	files, err := l.listFiles(" WHERE tombstoned_unix_time = 0")
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no active files: %w", err)
	}

	// Map files to DuckDB paths
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = fmt.Sprintf("'%s'", l.storage.ToDuckDBPath(file))
	}

	_, err = dataTx.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])",
		l.tableName, strings.Join(paths, ", ")))
	return err
}

// Restores db state from a JSON file
// passing JSON to keep all logic in DB
// TODO: pass it in via arrow to reduce overhead
func (l *Log) Import(tmpFilename string) error {
	db, err := l.getLogDB()
	if err != nil {
		return err
	}

	// First transaction for deletes
	deleteTx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = deleteTx.Exec(`
        DELETE FROM schema_log;
        DELETE FROM insert_log;
    `)
	if err != nil {
		if rbErr := deleteTx.Rollback(); rbErr != nil {
			log.Printf("failed to rollback delete transaction: %v", rbErr)
		}
		return fmt.Errorf("failed to delete existing data: %w", err)
	}
	if err := deleteTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delete transaction: %w", err)
	}

	// Second transaction for import
	importTx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := importTx.Rollback(); err != nil {
			log.Printf("failed to rollback import transaction: %v", err)
		}
	}()

	// Create temp json_data table
	_, err = importTx.Exec(fmt.Sprintf(`
        CREATE TEMP TABLE json_data AS 
        SELECT * FROM read_json('%s', auto_detect=true);
    `, tmpFilename))
	if err != nil {
		return fmt.Errorf("failed to create json_data table: %w", err)
	}

	// Import schema_log using json_data
	_, err = importTx.Exec(l.generateRestoreSQL("schema_log"))
	if err != nil {
		return fmt.Errorf("schema_log import failed: %w", err)
	}

	// Check if there are any insert_log entries before importing
	var insertLogLength int
	err = importTx.QueryRow(`
        SELECT COALESCE(array_length(insert_log::json[]), 0)
        FROM json_data;
    `).Scan(&insertLogLength)
	if err != nil {
		return fmt.Errorf("failed to check insert_log length: %w", err)
	}

	// Only import insert_log if there are entries
	if insertLogLength > 0 {
		_, err = importTx.Exec(l.generateRestoreSQL("insert_log"))
		if err != nil {
			return fmt.Errorf("insert_log import failed: %w", err)
		}
	}

	// Drop the temp table before commit
	if _, err := importTx.Exec("DROP TABLE json_data;"); err != nil {
		return fmt.Errorf("failed to drop temp table: %w", err)
	}

	return importTx.Commit()
}

func (l *Log) Close() error {
	if l.logDB != nil {
		return l.logDB.Close()
	}
	return nil
}

// generateRestoreSQL creates SQL to restore a table from a JSON field
// with the same name in the json_data temporary table.
func (l *Log) generateRestoreSQL(tableName string) string {
	return fmt.Sprintf(`
        INSERT INTO %[1]s 
        SELECT rows.*
        FROM (
            SELECT unnest(%[1]s) AS rows 
            FROM json_data
        )`, tableName)
}

// Destroy completely removes the log and all associated data
// does not Close the database connection (useful for testing)
func (l *Log) Destroy() error {
	// Get all files (including tombstoned ones)
	files, err := l.listFiles("")
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Delete each parquet file
	for _, file := range files {
		if err := l.storage.Delete(file); err != nil {
			return fmt.Errorf("failed to delete file %s: %w", file, err)
		}
	}

	// Delete JSON log file
	jsonLogPath := filepath.Join(l.tableName, "log.json")
	if err := l.storage.Delete(jsonLogPath); err != nil {
		return fmt.Errorf("failed to delete log.json: %w", err)
	}

	// Close database connection if open
	if l.logDB != nil {
		if err := l.logDB.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		l.logDB = nil
	}

	// Remove entire storage directory using OpenDAL
	// but opendal go binding is missing recursive deletes
	// if err := l.op.Delete(l.tableName); err != nil {
	//     return fmt.Errorf("failed to remove storage directory: %w", err)
	// }

	return nil
}
