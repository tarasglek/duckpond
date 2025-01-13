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
	// Add port flag
	port := flag.Int("port", 8080, "port to listen on")
	flag.Parse()

	db, _ := sql.Open("duckdb", "")

	db.Exec(`CREATE TABLE person (id INTEGER, name VARCHAR)`)
	db.Exec(`INSERT INTO person VALUES (42, 'John')`)

	var (
		id   int
		name string
	)
	row := db.QueryRow(`SELECT id, name FROM person`)
	_ = row.Scan(&id, &name)
	fmt.Println("id:", id, "name:", name)

	// Serve the print on port 8080 as a http server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "id: %d, name: %s\n", id, name)
	})

	// Update ListenAndServe with port flag and error handling
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Error starting server: %v", err)
		os.Exit(1)
	}
}
