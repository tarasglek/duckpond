package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type CopyToLoggedPaquetResult struct {
	ParquetPath string
	Size        int64
	DeltaStats  string
}

type Log struct {
	logDB          *sql.DB
	tableName      string
	storageDir     string
	delta_log_json string
	storage        Storage
	ttl_seconds    int // TTL in seconds (default 0)
}

//go:embed delta_lake_init.sql
var deltaLakeInitSQL string

//go:embed export_delta_lake_log.sql
var exportDeltaLakeLogSQL string

func NewLog(storageDir, tableName string) *Log {
	ttlSeconds := 0
	if ttlStr := os.Getenv("TTL_SECONDS"); ttlStr != "" {
		if parsed, err := strconv.Atoi(ttlStr); err == nil {
			ttlSeconds = parsed
		}
	}

	return &Log{
		tableName:      tableName,
		storageDir:     storageDir,
		storage:        NewStorage(storageDir),
		delta_log_json: filepath.Join(tableName, "_delta_log/00000000000000000000.json"),
		ttl_seconds:    ttlSeconds,
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

	fmt.Println("=====deltaLakeInitSQL", deltaLakeInitSQL)
	// Execute Delta Lake initialization SQL
	if _, err := logDB.Exec(deltaLakeInitSQL); err != nil {
		logDB.Close()
		return nil, fmt.Errorf("failed to initialize Delta Lake log: %w", err)
	}

	l.logDB = logDB
	return l.logDB, nil
}

// Exports log state to a JSON file and returns the current etag
func (l *Log) Export() ([]byte, string, error) {
	db, err := l.getLogDB()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get database: %w", err)
	}

	// Get and log etag in one operation
	var etag string
	err = db.QueryRow("SELECT COALESCE(getvariable('log_json_etag'), '')").Scan(&etag)
	log.Printf("Export: etag=%v (err=%v)", etag, err)
	if err != nil {
		etag = ""
	}

	// Get delta lake events as a single string
	var dl_events string
	err = db.QueryRow(exportDeltaLakeLogSQL).Scan(&dl_events)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get delta lake events: %w", err)
	}
	return []byte(dl_events), etag, err
}

// Runs callback that does SQL while properly persisting it via log
func (l *Log) withPersistedLog(op func() error) error {
	// read file from s3/etc via storage iface(with etag)
	// then pass it (still as a file to duckdb so it can do type inference on it)
	data, fileInfo, err := l.storage.Read(l.delta_log_json)
	if err == nil {
		// Write to temp file
		tmpFile, err := os.CreateTemp("", "dl-log-import-*.jsonl")
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
			return fmt.Errorf("failed to import %s: %w", l.delta_log_json, importErr)
		}
	}

	// Execute the operation
	if err := op(); err != nil {
		return err
	}

	// Export and write new state
	if dl_events, etag, exportErr := l.Export(); exportErr != nil {
		return fmt.Errorf("export failed: %w", exportErr)
	} else {
		writeErr := l.storage.Write(l.delta_log_json, dl_events, WithIfMatch(etag))
		if writeErr != nil {
			return fmt.Errorf("failed to write %s: %w", l.delta_log_json, writeErr)
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

		// Execute the fancy query to create delta lake table metadata event
		// and get the JSON result.
		// This needs to be on data connection since it's needs access to data table metadata
		var stringOfJson string
		err = dataTx.QueryRow(query_json_from_create_table_event, l.tableName, rawCreateTable).Scan(&stringOfJson)
		if err != nil {
			return fmt.Errorf("failed to generate create table event JSON: %w", err)
		}

		// Insert the JSON into delta_lake_log
		_, err = db.Exec(`INSERT INTO log_json(metaData) VALUES ($1::json)`, stringOfJson)
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

	var createQuery string
	err = logDB.QueryRow(`
		select metaData.icebase.createTable::TEXT as text 
		from log_json 
		where metaData is not null;
	`).Scan(&createQuery)
	if err != nil {
		if err == sql.ErrNoRows {
			// table hasn't been initialized yet
			return nil
		}
		return fmt.Errorf("failed to query schema_log: %w", err)
	}

	// Decode the JSON string
	// var decodedQuery string
	// if err := json.Unmarshal([]byte(createQuery), &decodedQuery); err != nil {
	// return fmt.Errorf("failed to decode create table query: %w", err)
	// }

	// Execute the create table statement
	if _, err := dataTx.Exec(createQuery); err != nil {
		return fmt.Errorf("failed to execute schema_log query `%s`: %w", createQuery, err)
	}

	return nil
}

// Commits in-memory data table to log and parquet files
func (l *Log) Insert(dataTx *sql.Tx, table string) error {
	return l.withPersistedLog(func() error {
		logDB, err := l.getLogDB()
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		res, err := l.CopyToLoggedPaquet(dataTx, table, table)
		if err != nil {
			return fmt.Errorf("failed to copy to parquet: %w", err)
		}
		_, err = logDB.Exec(query_insert_table_event_add, res.ParquetPath, res.Size, table, res.DeltaStats)
		if err != nil {
			return fmt.Errorf("failed to record 'add' event: %w", err)
		}

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
func (l *Log) CopyToLoggedPaquet(dataTx *sql.Tx, dstTable string, srcSQL string) (*CopyToLoggedPaquetResult, error) {
	logDB, err := l.getLogDB()
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	var uuidOfNewFile string
	err = logDB.QueryRow(`select uuidv7()::text`).Scan(&uuidOfNewFile)
	if err != nil {
		return nil, fmt.Errorf("failed to call uuidv7(): %w", err)
	}

	fname := uuidOfNewFile + ".parquet"
	parquetPath := filepath.Join("data", fname)
	parquetPathWithTable := filepath.Join(dstTable, parquetPath)

	// create data directory for parquet files(when on localfs)
	dataDir := filepath.Join(dstTable, "data")
	if err := l.storage.CreateDir(dataDir); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Get delta stats
	var stats string
	err = dataTx.QueryRow("SELECT delta_stats($1)", dstTable).Scan(&stats)
	if err != nil {
		return nil, fmt.Errorf("delta_stats(%s) failed: %w", dstTable, err)
	}

	var copyErr error
	err = l.WithDuckDBSecret(dataTx, func() error {
		copyQuery := fmt.Sprintf(`COPY %s TO '%s' (FORMAT PARQUET);`,
			dstTable, l.storage.ToDuckDBWritePath(parquetPathWithTable))

		_, copyErr = dataTx.Exec(copyQuery)
		log.Printf("%s err: %v", copyQuery, copyErr)
		if copyErr != nil {
			return fmt.Errorf("failed to copy to parquet: %w", copyErr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	meta, copyErr := l.storage.Stat(parquetPathWithTable)
	if copyErr != nil {
		return nil, fmt.Errorf("failed to get file size: %w", copyErr)
	}

	return &CopyToLoggedPaquetResult{
		ParquetPath: parquetPath,
		Size:        meta.Size(),
		DeltaStats:  stats,
	}, nil
}

//go:embed merge.sql
var query_merge string

// Merge combines all active parquet files into a single file and tombstones the old ones
//
// dataTx is the transaction for the main data database operations
func (l *Log) Merge(table string, dataTx *sql.Tx) error {
	return l.withPersistedLog(func() error {
		// Get logDB connection once at the start
		logDB, err := l.getLogDB()
		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}

		// Phase 1: Delete tombstoned files
		files, err := l.listFiles(filesMarkedRemove)
		if err != nil {
			return fmt.Errorf("failed to list deleted files: %w", err)
		}
		// we try to not be transactional here
		// so delete files before we remove them from the log
		if len(files) > 0 {
			// delete tombstoned files
			for _, file := range files {
				// delete the file
				err := l.storage.Delete(filepath.Join(l.tableName, file))
				if err != nil {
					log.Printf("Failed to delete tombstoned file %s: %v. Maybe it was deleted on prior attempt?", file, err)
					continue
				}
			}
			log.Printf("Deleted %d files previously marked for deletion, issue VACUUM again to merge", len(files))
			return nil
		}

		// Phase 2: Merge active files

		result, err := l.CopyToLoggedPaquet(dataTx, table, table)
		if err != nil {
			return fmt.Errorf("failed to create merged parquet: %w", err)
		}
		_, err = logDB.Exec(query_merge, result.ParquetPath, result.Size, result.DeltaStats)
		if err != nil {
			return fmt.Errorf("merge: failed to record 'deleted', 'add': %w", err)
		}

		return nil
	})
}

type filesFilter int

const (
	filesAll filesFilter = iota
	filesLive
	filesMarkedRemove
)

//go:embed files_list_live.sql
var sqlFilesListLive string

//go:embed files_list_all.sql
var sqlFilesListAll string

// Lists parquet files managed by insert_log table
func (l *Log) listFiles(filter filesFilter) ([]string, error) {
	db, err := l.getLogDB()
	if err != nil {
		return nil, err
	}

	var query string
	switch filter {
	case filesLive:
		query = sqlFilesListLive
	case filesMarkedRemove:
		query = `SELECT remove.path AS path FROM log_json where remove IS NOT NULL`
	case filesAll:
		query = sqlFilesListAll
	}

	rows, err := db.Query("COPY (select * from duckdb_tables() where table_name = 'log_json') TO '/tmp/foo.json';" + query)
	if err != nil {
		log.Printf("listFiles(%d) failed `%s`: %v", filter, query, err)
		return nil, err
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var file string
		err := rows.Scan(&file)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	log.Printf("listFiles %d: %v\n", filter, files)
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

	files, err := l.listFiles(filesLive)
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no active files: %w", err)
	}

	// Map files to DuckDB paths
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = fmt.Sprintf("'%s'", l.storage.ToDuckDBReadPath(filepath.Join(l.tableName, file)))
	}
	createView := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])",
		l.tableName, strings.Join(paths, ", "))
	log.Printf("creatreView: %s", createView)
	_, err = dataTx.Exec(createView)
	return err
}

// Restores db state from a JSON file
// passing JSON to keep all logic in DB
// TODO: pass it in via arrow to reduce overhead
func (l *Log) Import(tmpFilename string, etag string) error {
	logdb, err := l.getLogDB()
	if err != nil {
		return err
	}

	// Set and log etag in one operation
	_, err = logdb.Exec("SET VARIABLE log_json_etag = $1", etag)
	log.Printf("Import: setting etag=%v (err=%v)", etag, err)
	if err != nil {
		return fmt.Errorf("failed to set log_json_etag: %w", err)
	}

	_, err = logdb.Exec(`
		DELETE FROM log_json;
		INSERT INTO log_json (SELECT * FROM read_json($1, auto_detect=true));
    `, tmpFilename)
	if err != nil {
		return fmt.Errorf("failed to create json_data table: %w", err)
	}
	return nil
}

func (l *Log) Close() error {
	if l.logDB != nil {
		return l.logDB.Close()
	}
	return nil
}

func (l *Log) Destroy() error {
	// Get all files (including tombstoned ones)
	files, err := l.listFiles(filesAll)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Delete each parquet file
	for _, file := range files {
		if err := l.storage.Delete(filepath.Join(l.tableName, file)); err != nil {
			// it's ok if files are missing, they might've been deleted during VACUUM
			log.Printf("failed to delete file %s: %v. Maybe it was deleted during VACUUM?", file, err)
		}
	}

	// Delete JSON log file
	if err := l.storage.Delete(l.delta_log_json); err != nil {
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
