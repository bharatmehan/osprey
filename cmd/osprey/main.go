package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/bharatmehan/osprey/internal/config"
	"github.com/bharatmehan/osprey/internal/logging"
	"github.com/bharatmehan/osprey/internal/server"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "osprey.toml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize logging
	logPath := cfg.LogFile
	if logPath == "" {
		// Default to data/logs/osprey.log if not specified
		logPath = filepath.Join(cfg.DataDir, "logs", "osprey.log")
	}
	
	if err := logging.InitLogger(logPath, cfg.LogLevel); err != nil {
		log.Fatalf("Failed to initialize logging: %v", err)
	}
	defer logging.CloseLogger()

	log.Printf("Starting Osprey server with config: %s", configPath)
	log.Printf("Log file: %s", logPath)

	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	fmt.Printf("Osprey server started on %s\n", cfg.ListenAddr)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	if err := srv.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
