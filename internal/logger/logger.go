package logger

import (
	"log/slog"
	"os"
)

// New creates a structured JSON logger without setting it as default.
// Use Init if you need slog.SetDefault.
func New() *slog.Logger {
	return newLogger()
}

// Init creates a structured JSON logger and sets it as the default.
func Init() *slog.Logger {
	logger := newLogger()
	slog.SetDefault(logger)
	return logger
}

func newLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}