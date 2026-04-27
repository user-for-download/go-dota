package logger

import (
	"log/slog"
	"os"
)

// Init creates a structured JSON logger and sets it as the default.
func Init() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
