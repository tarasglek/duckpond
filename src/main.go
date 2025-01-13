package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	flag.Parse()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Initialize sample data
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS person (id INTEGER, name VARCHAR)`); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO person VALUES (42, 'John')`); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Register endpoints
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var (
			id   int
			name string
		)
		row := db.QueryRow(`SELECT id, name FROM person`)
		_ = row.Scan(&id, &name)
		fmt.Fprintf(w, "id: %d, name: %s\n", id, name)
	})

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
