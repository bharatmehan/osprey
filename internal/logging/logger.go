package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

var (
	// Global logger instance
	logger *log.Logger
	// Log file handle
	logFile *os.File
)

// InitLogger initializes the logger with the given configuration
func InitLogger(logPath string, logLevel string) error {
	// If no log path specified, use stderr
	if logPath == "" {
		logger = log.New(os.Stderr, "", log.LstdFlags)
		return nil
	}

	// Create log directory if it doesn't exist
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open log file
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logFile = file

	// Create multi-writer to write to both file and stderr
	multiWriter := io.MultiWriter(os.Stderr, file)
	
	// Create logger
	logger = log.New(multiWriter, "", log.LstdFlags)
	
	// Set default logger to use our logger
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags)

	return nil
}

// CloseLogger closes the log file if open
func CloseLogger() {
	if logFile != nil {
		logFile.Close()
		logFile = nil
	}
}

// Printf logs a formatted message
func Printf(format string, v ...interface{}) {
	if logger != nil {
		logger.Printf(format, v...)
	} else {
		log.Printf(format, v...)
	}
}

// Println logs a message with newline
func Println(v ...interface{}) {
	if logger != nil {
		logger.Println(v...)
	} else {
		log.Println(v...)
	}
}

// Fatalf logs a formatted message and exits
func Fatalf(format string, v ...interface{}) {
	if logger != nil {
		logger.Fatalf(format, v...)
	} else {
		log.Fatalf(format, v...)
	}
}