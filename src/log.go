package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

//go:embed delta_lake_init.sql
var deltaLakeInitSQL string

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

func (l *Log) WithDuckDBSecret(dataTx *sql.Tx, cb func() error) error {
	secretName := "icebase_temp_secret"
	if secretSQL := l.storage.ToDuckDBSecret(secretName); secretSQL != "" {
		if _, err := dataTx.Exec(secretSQL); err != nil {
			return fmt.Errorf("failed to create secret %s: %w", secretName, err)
		}
		defer func() {
			_, _ = dataTx.Exec(fmt.Sprintf("DROP SECRET IF EXISTS %s", secretName))
		}()
	}
	return cb()
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

	// Execute Delta Lake initialization SQL
	if _, err := logDB.Exec(deltaLakeInitSQL); err != nil {
		logDB.Close()
		return nil, fmt.Errorf("failed to initialize Delta Lake log: %w", err)
	}

	l.logDB = logDB
	return l.logDB, nil
}

// Exports log state to a JSON file and returns the current etag
func (l *Log) Export() ([]byte, []byte, string, error) {
	db, err := l.getLogDB()
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get database: %w", err)
	}

	// Get and log etag in one operation
	var etag string
	err = db.QueryRow("SELECT COALESCE(getvariable('log_json_etag'), '')").Scan(&etag)
	log.Printf("Export: etag=%v (err=%v)", etag, err)
	if err != nil {
		etag = ""
	}

	// Get the JSON data (existing query unchanged)
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

	// Get delta lake events as a single string
	var dl_events string
	err = db.QueryRow(`
         SELECT string_agg(event::TEXT, E'\n')
         FROM delta_lake_log
     `).Scan(&dl_events)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get delta lake events: %w", err)
	}
	return []byte(jsonResult), []byte(dl_events), etag, err
}

// Runs callback that does SQL while properly persisting it via log
func (l *Log) withPersistedLog(op func() error) error {
	const jsonFileName = "log.json"

	// Try to read and import existing data through temp file
	jsonPath := filepath.Join(l.tableName, jsonFileName)
	if data, fileInfo, err := l.storage.Read(jsonPath); err == nil {
		// Write to temp file
		tmpFile, err := os.CreateTemp("", "icebase-import-*.json")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		// Deferred Close() for cleanup
		defer tmpFile.Close()           // Ensures file is closed even if errors occur
		defer os.Remove(tmpFile.Name()) // Ensures temp file is deleted

		if _, err := tmpFile.Write(data); err != nil {
			return fmt.Errorf("failed to write temp file: %w", err)
		}
		// Close for writes, it's ready for reads
		tmpFile.Close()

		if importErr := l.Import(tmpFile.Name(), fileInfo.ETag()); importErr != nil {
			return fmt.Errorf("failed to import %s: %w", jsonPath, importErr)
		}
	}

	// Execute the operation
	if err := op(); err != nil {
		return err
	}

	// Export and write new state
	if exported, dl_events, etag, exportErr := l.Export(); exportErr != nil {
		return fmt.Errorf("export failed: %w", exportErr)
	} else {
		writeErr := l.storage.Write(jsonPath, exported, WithIfMatch(etag))
		if writeErr != nil {
			return fmt.Errorf("failed to write %s: %w", jsonPath, writeErr)
		}
		dlJsonPath := filepath.Join(l.tableName, "_delta_log/00000000000000000000.json")
		if writeErr = l.storage.Write(dlJsonPath, dl_events); writeErr != nil {
			return fmt.Errorf("failed to write %s: %w", jsonPath, writeErr)
		}
	}

	return nil
}

//go:embed json_from_create_table_event.sql
var query_json_from_create_table_event string

// Logs a DDL statement to the schema_log table
func (l *Log) logDDL(dataTx *sql.Tx, rawCreateTable string) error {
	return l.withPersistedLog(func() error {
		db, err := l.getLogDB()
		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}

		// Log the DDL statement in schema_log
		_, err = db.Exec(`
            INSERT INTO schema_log (timestamp, raw_query)
            VALUES (CURRENT_TIMESTAMP, ?);
        `, rawCreateTable)
		if err != nil {
			return fmt.Errorf("failed to log table creation: %w", err)
		}

		// Execute the create table event query and get the JSON result
		var stringOfJson string
		err = dataTx.QueryRow(query_json_from_create_table_event, l.tableName, rawCreateTable).Scan(&stringOfJson)
		if err != nil {
			return fmt.Errorf("failed to generate create table event JSON: %w", err)
		}

		// Insert the JSON into delta_lake_log
		_, err = db.Exec(`INSERT INTO delta_lake_log (event) VALUES ($1::json)`, stringOfJson)
		if err != nil {
			return fmt.Errorf("failed to insert create table event into delta lake events: %w", err)
		}

		return nil
	})
}

// Gets us to most recent state of schema by replaying schema_log from scratch
func (l *Log) PlaySchemaLogForward(dataTx *sql.Tx) error {
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

// Commits in-memory data table to log and parquet files
func (l *Log) Insert(dataTx *sql.Tx, table string) error {
	return l.withPersistedLog(func() error {
		_, err := l.CopyToLoggedPaquet(dataTx, table, table)
		return err
	})
}

//go:embed insert_table_event_add.sql
var query_insert_table_event_add string

// Commits writes from <table> (accessed via dataTx param) to log + parquet files
// They are then persisted to a parquet file and tracked in the insert_log table
// TODO:
// - persist log for parquet files we gonna upload first
// - Then modify reading code to detect missing parquet files and to tombstone them in log
// - This way we wont end up with orphaned parquet files
func (l *Log) CopyToLoggedPaquet(dataTx *sql.Tx, dstTable string, srcSQL string) (string, error) {
	logDB, err := l.getLogDB()
	if err != nil {
		return "", fmt.Errorf("failed to open database: %w", err)
	}

	var uuidBytes []byte
	err = logDB.QueryRow(`
            INSERT INTO insert_log (id, partition)
            VALUES (uuidv7(), '')
            RETURNING id;
        `).Scan(&uuidBytes)
	if err != nil {
		return "", fmt.Errorf("failed to insert into insert_log: %w", err)
	}

	uuidOfNewFile := uuid.UUID(uuidBytes).String()
	dataDir := filepath.Join(dstTable, "data")
	if err := l.storage.CreateDir(dataDir); err != nil {
		return uuidOfNewFile, fmt.Errorf("failed to create data directory: %w", err)
	}

	fname := uuidOfNewFile + ".parquet"
	parquetPath := filepath.Join(dataDir, fname)

	var copyErr error
	err = l.WithDuckDBSecret(dataTx, func() error {
		copyQuery := fmt.Sprintf(`COPY %s TO '%s' (FORMAT PARQUET);`,
			dstTable, l.storage.ToDuckDBWritePath(parquetPath))

		_, copyErr = dataTx.Exec(copyQuery)
		log.Printf("%s err: %v", copyQuery, copyErr)
		if copyErr != nil {
			return fmt.Errorf("failed to copy to parquet: %w", copyErr)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	meta, copyErr := l.storage.Stat(parquetPath)
	if copyErr != nil {
		return uuidOfNewFile, fmt.Errorf("failed to get file size: %w", copyErr)
	}

	if _, copyErr = logDB.Exec(`
            UPDATE insert_log 
            SET size = ?
            WHERE id = ?;
        `, meta.Size(), uuidOfNewFile); err != nil {
		return uuidOfNewFile, fmt.Errorf("failed to update size: %w", err)
	}
	_, err = logDB.Exec(query_insert_table_event_add, filepath.Join("data", fname), meta.Size())
	if err != nil {
		return "", fmt.Errorf("failed to insert table event: %w", err)
	}

	return uuidOfNewFile, nil
}

// Merge combines all active parquet files into a single file and tombstones the old ones
//
// dataTx is the transaction for the main data database operations
func (l *Log) Merge(table string, dataTx *sql.Tx) error {
	return l.withPersistedLog(func() error {
		// Phase 1: Delete tombstoned files
		files, err := l.listFiles(" WHERE tombstoned_unix_time != 0")
		if err != nil {
			return fmt.Errorf("failed to list tombstoned files: %w", err)
		}
		// we try to not be transactional here
		// so delete files before we remove them from the log
		if len(files) > 0 {
			deletedFiles := 0
			// delete tombstoned files
			for _, file := range files {
				if err := l.storage.Delete(file); err != nil {
					log.Printf("Failed to delete tombstoned file %s: %v. Maybe it was deleted on prior attempt?", file, err)
					continue
				}
				deletedFiles++
				log.Printf("Permanently deleting tombstoned file %s", file)
			}
			if deletedFiles > 0 {
				log.Printf("Deleted %d tombstoned files, issue VACUUM again to merge", deletedFiles)
				return nil
			}
		}
		// Phase 2: Merge active files
		// first list active files
		files, err = l.listFiles(" WHERE tombstoned_unix_time = 0")
		if err != nil {
			return fmt.Errorf("failed to list live files: %w", err)
		}
		if len(files) <= 1 {
			log.Printf("%d live files, nothing to merge", len(files))
			return nil
		}
		newUUID, err := l.CopyToLoggedPaquet(dataTx, table, table)
		if err != nil {
			return fmt.Errorf("failed to merge: %w", err)
		}

		logDB, err := l.getLogDB()
		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}

		// Tombstone old entries (excluding the new UUID we just created)
		result, err := logDB.Exec(`
			UPDATE insert_log 
			SET tombstoned_unix_time = EPOCH_MS(CURRENT_TIMESTAMP) 
			WHERE tombstoned_unix_time = 0
			AND id != ?;
		`, newUUID)
		if err != nil {
			return fmt.Errorf("failed to tombstone old entries: %w", err)
		}
		if count, err := result.RowsAffected(); err == nil {
			log.Printf("Tombstoned %d files during merge", count)
		}

		return nil
	})
}

// Lists parquet files managed by insert_log table
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

// Fake a table for reading by creating a view of live parquet files
func (l *Log) CreateViewOfParquet(dataTx *sql.Tx) error {
	// Create permanent secret for the view operation
	// TODO: would be better to wrap this around select-style operations :(
	secretSQL := l.storage.ToDuckDBSecret("icebase_view_s3_secret")
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
		paths[i] = fmt.Sprintf("'%s'", l.storage.ToDuckDBReadPath(file))
	}

	_, err = dataTx.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])",
		l.tableName, strings.Join(paths, ", ")))
	return err
}

// Restores db state from a JSON file
// passing JSON to keep all logic in DB
// TODO: pass it in via arrow to reduce overhead
func (l *Log) Import(tmpFilename string, etag string) error {
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
			_ = err // ignore rollback errors
		}
	}()

	// Set and log etag in one operation
	_, err = importTx.Exec(fmt.Sprintf("SET VARIABLE log_json_etag = '%s';", strings.ReplaceAll(etag, "'", "''")))
	log.Printf("Import: setting etag=%v (err=%v)", etag, err)
	if err != nil {
		return fmt.Errorf("failed to set log_json_etag: %w", err)
	}

	// Create temp json_data table
	_, err = importTx.Exec(fmt.Sprintf(`
        CREATE TEMP TABLE log_json AS 
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
        FROM log_json;
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
	if _, err := importTx.Exec("DROP TABLE log_json;"); err != nil {
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
            FROM log_json
        )`, tableName)
}

func (l *Log) Destroy() error {
	// Get all files (including tombstoned ones)
	files, err := l.listFiles("")
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Delete each parquet file
	for _, file := range files {
		if err := l.storage.Delete(file); err != nil {
			// it's ok if files are missing, they might've been deleted during VACUUM
			log.Printf("failed to delete file %s: %v. Maybe it was deleted during VACUUM?", file, err)
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

	return nil
}
