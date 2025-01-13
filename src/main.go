package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/coolaj86/uuidv7"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

type uuidv7Func struct{}

func uuidv7Fn(values []driver.Value) (any, error) {
    return uuidv7.New().String(), nil
}

func (*uuidv7Func) Config() duckdb.ScalarFuncConfig {
    uuidTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_UUID)
    if err != nil {
        return duckdb.ScalarFuncConfig{}
    }

    return duckdb.ScalarFuncConfig{
        ResultTypeInfo: uuidTypeInfo,
    }
}

func (*uuidv7Func) Executor() duckdb.ScalarFuncExecutor {
    return duckdb.ScalarFuncExecutor{RowExecutor: uuidv7Fn}
}

func registerUUIDv7UDF(db *sql.DB) error {
    c, err := db.Conn(context.Background())
    if err != nil {
        return fmt.Errorf("failed to get connection: %w", err)
    }

    var uuidv7UDF *uuidv7Func
    err = duckdb.RegisterScalarUDF(c, "uuidv7", uuidv7UDF)
    if err != nil {
        return fmt.Errorf("failed to register UUIDv7 UDF: %w", err)
    }

    return c.Close()
}

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	postEndpoint := flag.String("post", "", "send POST request to specified endpoint e.g.: echo 'select now()' | ./icebase -post /query")
	flag.Parse()

	// Print UUIDv7 on startup
	startupUUID := uuidv7.New()
	log.Printf("Starting icebase with UUID: %s", startupUUID.String())

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Load JSON extension
	if _, err := db.Exec("LOAD json;"); err != nil {
		log.Fatalf("Failed to load JSON extension: %v", err)
	}

	// Register UUIDv7 UDF
	if err := registerUUIDv7UDF(db); err != nil {
		log.Fatalf("Failed to register UUIDv7 UDF: %v", err)
	}

	// If -post flag is provided, act as CLI client
	if *postEndpoint != "" {
		// Read from stdin
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("Failed to read stdin: %v", err)
		}

		// Create a reader from the input
		body := strings.NewReader(string(input))

		// Use the same PostEndpoint function as the HTTP handler
		jsonResponse, err := PostEndpoint(db, *postEndpoint, body)
		if err != nil {
			log.Fatalf("POST request failed: %v", err)
		}

		fmt.Println(jsonResponse)
		return
	}

	// Initialize sample data
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS person (id INTEGER, name VARCHAR)`); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	http.HandleFunc("/query", QueryHandler(db))

	// Start server
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Error starting server: %v", err)
		flag.Usage()
		os.Exit(1)
	}
}
