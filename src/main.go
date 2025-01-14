package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	postEndpoint := flag.String("post", "", "send POST request to specified endpoint e.g.: echo 'select now()' | ./icebase -post /query")
	flag.Parse()

	ib, err := NewIceBase()
	if err != nil {
		log.Fatalf("Failed to initialize IceBase: %v", err)
	}
	defer ib.Close()

	// If -post flag is provided, act as CLI client
	if *postEndpoint != "" {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("Failed to read stdin: %v", err)
		}

		body := strings.NewReader(string(input))
		jsonResponse, err := ib.PostEndpoint(*postEndpoint, body)
		if err != nil {
			log.Fatalf("POST request failed: %v", err)
		}

		fmt.Println(jsonResponse)
		return
	}

	// Start server
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server on %s", addr)
	http.HandleFunc("/query", ib.QueryHandler())
	http.HandleFunc("/parse", ib.ParseHandler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Error starting server: %v", err)
		flag.Usage()
		os.Exit(1)
	}
}
