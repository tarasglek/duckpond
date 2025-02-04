package main

import (
	"os"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogger sets up the global zerolog configuration.
// It uses the provided defaultLevel (e.g. "info") unless overridden by the LOG_LEVEL env var.
// If running interactively, it sets a ConsoleWriter for colored output.
func InitLogger(defaultLevel string) {
	level, err := zerolog.ParseLevel(defaultLevel)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid default log-level")
	}
	if envLog := os.Getenv("LOG_LEVEL"); envLog != "" {
		if l, err := zerolog.ParseLevel(envLog); err == nil {
			level = l
		}
	}
	zerolog.SetGlobalLevel(level)

	if isatty.IsTerminal(os.Stderr.Fd()) {
		cw := zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: "15:04:05",
		}
		log.Logger = log.Output(cw)
	}
}
