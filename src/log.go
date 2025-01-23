package main

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
)

type Log struct {
	db         *sql.DB
	tableName  string
	storageDir string
	op         *opendal.Operator
}

func NewLog(storageDir, tableName string) *Log {
	op, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": storageDir,
	})
	if err != nil {
		panic(fmt.Errorf("failed to create OpenDAL operator: %w", err))
	}

	return &Log{
		tableName:  tableName,
		storageDir: storageDir,
		op:         op,
	}
}

func (l *Log) getDB() (*sql.DB, error) {
	if l.db != nil {
		return l.db, nil
	}

	// Create storage directory structure using OpenDAL
	logDir := filepath.Join(l.tableName, "log")
	if err := l.op.CreateDir(logDir + "/"); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Create database path
	dbPath := filepath.Join(logDir, "log.db")

	// Initialize main database connection
	db, err := InitializeDuckDB()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	attachSQL := fmt.Sprintf(`ATTACH DATABASE '%s' AS log_db;USE log_db`, l.toDuckDBPath(dbPath))

	// Attach log database
	_, err = db.Exec(attachSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to attach log database: %w", err)
	}

	// Create schema if needed
	_, err = db.Exec(`
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
		db.Close()
		return nil, fmt.Errorf("failed to create schema_log table: %w", err)
	}

	l.db = db
	return l.db, nil
}

func (l *Log) Export() ([]byte, error) {
    db, err := l.getDB()
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
func (l *Log) withPersistedLog(op func(*sql.DB) (int, error)) (int, error) {
    const jsonFileName = "log.json"
    
    db, err := l.getDB()
    if err != nil {
        return -1, fmt.Errorf("failed to open database: %w", err)
    }

    // Try to read and import existing data
    jsonPath := filepath.Join(l.tableName, jsonFileName)
    if data, err := l.op.Read(jsonPath); err == nil {
        if importErr := l.Import(data); importErr != nil {
            return -1, fmt.Errorf("failed to import %s: %w", jsonPath, importErr)
        }
    }

    // Execute the operation
    result, err := op(db)
    if err != nil {
        return result, err
    }

    // Export and write new state
    if exported, exportErr := l.Export(); exportErr != nil {
        return -1, fmt.Errorf("export failed: %w", exportErr)
    } else if writeErr := l.op.Write(jsonPath, exported); writeErr != nil {
        return -1, fmt.Errorf("failed to write %s: %w", jsonPath, writeErr)
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

func (l *Log) RecreateSchema(tx *sql.Tx) error {
	db, err := l.getDB()
	if err != nil {
		return fmt.Errorf("failed to get log database: %w", err)
	}

	// Query schema_log for all create table statements
	rows, err := db.Query(`
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
		if _, err := tx.Exec(createQuery); err != nil {
			return fmt.Errorf("failed to execute schema_log query: %w", err)
		}
	}

	return nil
}

func (l *Log) Insert(tx *sql.Tx, table string, query string) (int, error) {
    return l.withPersistedLog(func(db *sql.DB) (int, error) {
        // Original insert logic wrapped in lambda
        var uuidBytes []byte
        err := db.QueryRow(`
            INSERT INTO insert_log (id, partition)
            VALUES (uuidv7(), '')
            RETURNING id;
        `).Scan(&uuidBytes)
        if err != nil {
            return -1, fmt.Errorf("failed to insert into insert_log: %w", err)
        }

        uuidStr := uuid.UUID(uuidBytes).String()
        dataDir := filepath.Join(table, "data")
        if err := l.op.CreateDir(dataDir + "/"); err != nil {
            return -1, fmt.Errorf("failed to create data directory: %w", err)
        }

        parquetPath := filepath.Join(dataDir, uuidStr+".parquet")
        copyQuery := fmt.Sprintf(`COPY %s TO '%s' (FORMAT PARQUET);`, 
            table, l.toDuckDBPath(parquetPath))

        if _, err = tx.Exec(copyQuery); err != nil {
            return -1, fmt.Errorf("failed to copy to parquet: %w", err)
        }

        meta, err := l.op.Stat(parquetPath)
        if err != nil {
            return -1, fmt.Errorf("failed to get file size: %w", err)
        }

        if _, err = db.Exec(`
            UPDATE insert_log 
            SET size = ?
            WHERE id = ?;
        `, meta.ContentLength(), uuidStr); err != nil {
            return -1, fmt.Errorf("failed to update size: %w", err)
        }

        return 0, nil
    })
}

// Recreates the table described in the schema_log table as a view over partitioned parquet files
func (l *Log) listFiles(where string) ([]string, error) {
	db, err := l.getDB()
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

func (l *Log) RecreateAsView(tx *sql.Tx) error {
	files, err := l.listFiles(" WHERE tombstoned_unix_time = 0")
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no active files: %w", err)
	}

	// Map files to DuckDB paths
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = fmt.Sprintf("'%s'", l.toDuckDBPath(file))
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet([%s])",
		l.tableName, strings.Join(paths, ", ")))
	return err
}

func (l *Log) Import(data []byte) error {
    db, err := l.getDB()
    if err != nil {
        return err
    }

    // Use transaction for atomic import
    tx, err := db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    _, err = tx.Exec(
        `DELETE FROM schema_log;
        INSERT INTO schema_log SELECT rows.*
        FROM (SELECT unnest(schema_log) AS rows FROM (SELECT ?::JSON AS json_data))`,
        string(data))
    if err != nil {
        return fmt.Errorf("schema_log import failed: %w", err)
    }

    _, err = tx.Exec(
        `DELETE FROM insert_log;
        INSERT INTO insert_log SELECT rows.*
        FROM (SELECT unnest(insert_log) AS rows FROM (SELECT ?::JSON AS json_data))`,
        string(data))
    if err != nil {
        return fmt.Errorf("insert_log import failed: %w", err)
    }

    return tx.Commit()
}

func (l *Log) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

// toDuckDBPath converts a relative path to an absolute path for DuckDB
// by prepending the storage directory
func (l *Log) toDuckDBPath(path string) string {
	return filepath.Join(l.storageDir, path)
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
		if err := l.op.Delete(file); err != nil {
			return fmt.Errorf("failed to delete file %s: %w", file, err)
		}
	}

	// Delete log.db file
	logPath := filepath.Join(l.tableName, "log", "log.db")
	if err := l.op.Delete(logPath); err != nil {
		return fmt.Errorf("failed to delete log.db: %w", err)
	}

	// Delete JSON log file
	jsonLogPath := filepath.Join(l.tableName, "log.json")
	if err := l.op.Delete(jsonLogPath); err != nil {
		return fmt.Errorf("failed to delete log.json: %w", err)
	}

	// Close database connection if open
	if l.db != nil {
		if err := l.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		l.db = nil
	}

	// Remove entire storage directory using OpenDAL
	// but opendal go binding is missing recursive deletes
	// if err := l.op.Delete(l.tableName); err != nil {
	//     return fmt.Errorf("failed to remove storage directory: %w", err)
	// }

	return nil
}
