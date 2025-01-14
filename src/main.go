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

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)


func main() {
	port := flag.Int("port", 8080, "port to listen on")
	postEndpoint := flag.String("post", "", "send POST request to specified endpoint e.g.: echo 'select now()' | ./icebase -post /query")
	flag.Parse()

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
