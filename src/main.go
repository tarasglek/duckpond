package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rs/zerolog/log"
)

func main() {
	port := flag.Int("port", 0, "port to listen on (if not provided, the HTTP server will not start)")
	postEndpoint := flag.String("post", "", "send POST request to specified endpoint e.g.: echo 'select now()' | ./duckpond -post /query")
	querySplitting := flag.Bool("query-splitting", false, "enable semicolon query splitting")
	logLevel := flag.String("log-level", "info", "set the logging level (debug, info, warn, error); can also be set via LOG_LEVEL env var")
	versionFlag := flag.Bool("version", false, "print the version and exit")
	printExtInfo := flag.Bool("print-duckdb-extension-info", false, "print DuckDB extension information as JSONL")
	loadExtFlag := flag.Bool("load-extensions", false, "load DuckDB extensions from extension paths")
	installExtFlag := flag.Bool("install-extensions", false, "install (INSTALL then LOAD) DuckDB extensions")
	flag.Parse()

	if *loadExtFlag || *installExtFlag {
		db, err := InitializeDuckDB()
		if err != nil {
			log.Fatal().Msgf("Failed to initialize DuckDB: %v", err)
		}
		defer db.Close()
		if err := ProcessExtensions(db, *installExtFlag); err != nil {
			log.Fatal().Msgf("Failed to process extensions: %v", err)
		}
	}

	if *printExtInfo {
		db, err := InitializeDuckDB()
		if err != nil {
			log.Fatal().Msgf("Failed to open DuckDB: %v", err)
		}
		defer db.Close()

		if err := PrintExtensionInfo(db); err != nil {
			log.Fatal().Msgf("Failed to print DuckDB extension info: %v", err)
		}
		fmt.Println("DuckDB extension information printed successfully")
		return
	}

	if *versionFlag {
		fmt.Println("Version:", Version)
		return
	}

	// Initialize logging
	InitLogger(*logLevel)

	opts := []IceBaseOption{}
	if *querySplitting {
		opts = append(opts, WithQuerySplittingEnabled())
	}

	ib, err := NewIceBase(opts...)
	if err != nil {
		log.Fatal().Msgf("Failed to initialize IceBase: %v", err)
	}
	defer ib.Close()

	// If -post flag is provided, act as CLI client
	if *postEndpoint != "" {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal().Msgf("Failed to read stdin: %v", err)
		}

		jsonResponse, err := ib.PostEndpoint(*postEndpoint, string(input))
		if err != nil {
			log.Fatal().Msgf("POST request failed: %v", err)
		}

		fmt.Println(jsonResponse)
		return
	}

	if *port != 0 {
		addr := fmt.Sprintf(":%d", *port)
		log.Info().Msgf("Starting server on %s", addr)
		handler := ib.RequestHandler()
		http.HandleFunc("/query", handler)
		http.HandleFunc("/parse", handler)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Error().Msgf("Error starting server: %v", err)
			flag.Usage()
			os.Exit(1)
		}
	} else {
		log.Info().Msg("No port provided; HTTP server is not started.")
	}
}
