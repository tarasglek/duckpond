package main

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

var ErrNoParquetFilesInTable = errors.New("no parquet files associated with table")

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
			ttlSeconds = max(parsed, 0)
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
	secretName := "duckpond_temp_secret"
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

// initLogDB should only be called from getLogDBAfterImport
// and from Import
func (l *Log) initLogDB() (*sql.DB, error) {
	if l.logDB != nil {
		return l.logDB, nil
	}

	log.Debug().Msgf("log.initLogDB()")

	logDB, err := InitializeDuckDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	l.logDB = logDB

	secretSQL := l.storage.ToDuckDBSecret("duckpond_log_s3_secret")
	if secretSQL != "" {
		if _, err := logDB.Exec(secretSQL); err != nil {
			logDB.Close()
			return nil, fmt.Errorf("failed to create S3 secret: %w", err)
		}
	}

	log.Debug().Msgf("deltaLakeInitSQL: %s", deltaLakeInitSQL)
	if _, err := logDB.Exec(deltaLakeInitSQL); err != nil {
		logDB.Close()
		return nil, fmt.Errorf("failed to initialize Delta Lake log: %w", err)
	}

	return l.logDB, nil
}

// initialized logDB and imports log during init
func (l *Log) getLogDBAfterImport() (*sql.DB, error) {
	if l.logDB != nil {
		return l.logDB, nil
	}

	db, err := l.initLogDB()
	if err != nil {
		return nil, err
	}

	log.Debug().Msgf("getLogDBAfterImport")

	if err := l.importPersistedLog(); err != nil {
		// ignore error
	}

	return db, nil
}

// Exports log state to a JSON file and returns the current etag
func (l *Log) Export() error {
	db, err := l.getLogDBAfterImport()
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	// Get and log etag in one operation
	var etag string
	err = db.QueryRow("SELECT COALESCE(getvariable('log_json_etag'), '')").Scan(&etag)
	log.Debug().Str("etag", etag).Err(err).Msgf("log.Export")

	if err != nil {
		etag = ""
	}

	// Get delta lake events as a single string
	var dl_events string
	err = db.QueryRow(exportDeltaLakeLogSQL).Scan(&dl_events)
	if err != nil {
		return fmt.Errorf("failed to get delta lake events: %w", err)
	}

	// Write the new state to storage
	writeErr := l.storage.Write(l.delta_log_json, []byte(dl_events), WithIfMatch(etag))

	if writeErr != nil {
		return fmt.Errorf("failed to write %s: %w", l.delta_log_json, writeErr)
	}

	return nil
}

// Runs callback that does SQL while properly persisting it via log
func (l *Log) withPersistedLog(op func() error) error {
	// Import any existing persisted log data
	if err := l.importPersistedLog(); err != nil {
		return err
	}

	// Execute the operation
	if err := op(); err != nil {
		return err
	}

	return l.Export()
}

//go:embed json_from_create_table_event.sql
var query_json_from_create_table_event string

// Logs a DDL statement to the schema_log table
func (l *Log) logDDL(dataTx *sql.Tx, rawCreateTable string) error {
	return l.withPersistedLog(func() error {
		db, err := l.getLogDBAfterImport()
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

// Commits in-memory data table to log and parquet files
func (l *Log) Insert(dataTx *sql.Tx, table string) error {
	return l.withPersistedLog(func() error {
		logDB, err := l.getLogDBAfterImport()
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
	logDB, err := l.getLogDBAfterImport()
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
		if copyErr != nil {
			log.Error().Msgf("%s err: %v", copyQuery, copyErr)
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
		logDB, err := l.getLogDBAfterImport()
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
					log.Warn().Msgf("Failed to delete tombstoned file %s: %v. Possibly already deleted?", file, err)
					continue
				}
			}
			log.Info().Msgf("VACUUM: Deleted %d files previously marked for deletion, issue VACUUM again to merge remaining files", len(files))
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
	db, err := l.getLogDBAfterImport()
	if err != nil {
		return nil, err
	}

	var query string
	switch filter {
	case filesLive:
		query = sqlFilesListLive
	case filesMarkedRemove:
		query = fmt.Sprintf(`SELECT remove.path AS path FROM log_json WHERE remove.deletionTimestamp <= (epoch_ms(CURRENT_TIMESTAMP) - %d * 1000)`, l.ttl_seconds)
	case filesAll:
		query = sqlFilesListAll
	}

	rows, err := db.Query("COPY (select * from duckdb_tables() where table_name = 'log_json') TO '/tmp/foo.json';" + query)
	if err != nil {
		log.Error().Msgf("listFiles(%d) failed `%s`: %v", filter, query, err)
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
	log.Debug().Msgf("listFiles %d: %v", filter, files)
	return files, rows.Err()
}

// Fake a table for reading by creating a view of live parquet files
func (l *Log) CreateViewOfParquet(dataTx *sql.Tx) error {
	// Create permanent secret for the view operation
	// TODO: would be better to wrap this around select-style operations :(
	secretSQL := l.storage.ToDuckDBSecret("duckpond_view_s3_secret")
	if secretSQL != "" {
		if _, err := dataTx.Exec(secretSQL); err != nil {
			return fmt.Errorf("failed to create view secret: %w", err)
		}
		// Note we don't drop secret here as the view lifetime persists past this function
	}

	parquetFiles, err := l.listFiles(filesLive)
	if err != nil {
		return fmt.Errorf("failed to list live parquet files: %w", err)
	}
	if len(parquetFiles) == 0 {
		log.Debug().Msgf("CreateViewOfParquet: ErrNoParquetFilesInTable")
		return ErrNoParquetFilesInTable
	}
	// delta extension requires _last_checkpoint which requires a parquet ver of log
	duckPath := l.storage.ToDuckDBWritePath(l.tableName)
	// load delta ext here in case it wasn't loaded yet
	// can do this read without delta lake using read_parquet(parquetFiles)
	// but then we wont benefit from deltalake/duckdb filter pushdowns
	createView := fmt.Sprintf("LOAD delta; CREATE VIEW %s AS SELECT * FROM delta_scan('%s');", l.tableName, duckPath)
	log.Debug().Str("duckPath", duckPath).Msgf("createView: %s", createView)
	_, err = dataTx.Exec(createView)
	return err
}

// This creates an inmemory table that we COPY (l.tableName) TO ...parquet
func (l *Log) CreateTempTable(dataTx *sql.Tx) error {

	logDB, err := l.getLogDBAfterImport()
	if err != nil {
		return fmt.Errorf("failed to get log database: %w", err)
	}

	var createQuery string
	err = logDB.QueryRow(`
		select metaData.duckpond.createTable::TEXT as text 
		from log_json 
		where metaData is not null;
	`).Scan(&createQuery)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Debug().Msgf("CreateTempTable:  table hasn't been initialized yet")

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

	log.Debug().Msgf("CreateTempTable: %s", createQuery)

	// Execute the create table statement
	if _, err := dataTx.Exec(createQuery); err != nil {
		return fmt.Errorf("failed to execute schema_log query `%s`: %w", createQuery, err)
	}

	return nil
}

// Restores db state from a JSON file
// passing JSON to keep all logic in DB
// TODO: pass it in via arrow to reduce overhead
func (l *Log) Import(tmpFilename string, etag string) error {

	logdb, err := l.initLogDB()
	if err != nil {
		return err
	}

	// Set and log etag in one operation
	log.Debug().Msgf("Import: setting etag=%v (err=%v)", etag, err)
	_, err = logdb.Exec("SET VARIABLE log_json_etag = $1", etag)
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

func (l *Log) tigrisStaleCacheWorkaround() bool {
	return strings.Contains(strings.ToLower(l.storage.GetEndpoint()), "tigris")
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
			log.Warn().Msgf("failed to delete file %s: %v. Maybe it was deleted during VACUUM?", file, err)
		}
	}

	log.Debug().Msgf("log.Destroy()ing")
	// deal with stale tigris cache by writing a blank log, so after when reading stale cache(while recreating table with same name), we don't have table metadata in it
	if l.logDB != nil && l.tigrisStaleCacheWorkaround() {
		log.Debug().Msgf("Writing a blank log to deal with tigris bug that likes to return stale read cache")
		l.storage.Write(l.delta_log_json, []byte(`{}`))
	}

	// Close database connection if open
	if l.logDB != nil {
		if err := l.logDB.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		l.logDB = nil
	}

	// Delete JSON log file
	if err := l.storage.Delete(l.delta_log_json); err != nil {
		return fmt.Errorf("failed to delete log.json: %w", err)
	}

	return nil
}

// importPersistedLog reads the delta log from storage, writes it to a temp file,
// and imports it into the log database
func (l *Log) importPersistedLog() (err error) {
	data, fileInfo, err := l.storage.Read(l.delta_log_json)
	if err != nil {
		// If the log file isn't present, skip import
		log.Debug().Msgf("importPersistedLog(%s) assuming empty table '%s': %v", l.delta_log_json, l.tableName, err)
		return nil
	}

	if fileInfo.Size() <= 2 && l.tigrisStaleCacheWorkaround() {
		log.Debug().Msgf("importPersistedLog(%s) empty or invalid json, assuming empty table on tigris", l.delta_log_json)
		return nil
	}

	defer func() {
		if err != nil {
			log.Error().Msgf("importPersistedLog(%s) failed: %v", l.delta_log_json, err)
		} else {
			log.Debug().Msgf("importPersistedLog succeeded(%s)", l.delta_log_json)
		}
	}()

	tmpFile, err := os.CreateTemp("", "dl-log-import-*.jsonl")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	// Close for writes, it's ready for reads
	tmpFile.Close()

	if importErr := l.Import(tmpFile.Name(), fileInfo.ETag()); importErr != nil {
		return fmt.Errorf("failed to import %s: %w", l.delta_log_json, importErr)
	}

	return nil
}
