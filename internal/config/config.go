package config

import (
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	// Network
	ListenAddr string `toml:"listen_addr"`
	MaxClients int    `toml:"max_clients"`

	// Limits
	MaxKeyBytes   int `toml:"max_key_bytes"`
	MaxValueBytes int `toml:"max_value_bytes"`

	// Persistence
	DataDir         string `toml:"data_dir"`
	WALMaxBytes     int64  `toml:"wal_max_bytes"`
	SyncPolicy      string `toml:"sync_policy"`
	BatchFsyncMs    int    `toml:"batch_fsync_ms"`
	BatchFsyncBytes int64  `toml:"batch_fsync_bytes"`

	// Snapshot
	EnableSnapshot     bool `toml:"enable_snapshot"`
	SnapshotPauseMaxMs int  `toml:"snapshot_pause_max_ms"`
	BusyWarnMs         int  `toml:"busy_warn_ms"`

	// Expiry
	SweepIntervalMs int `toml:"sweep_interval_ms"`
	SweepBatch      int `toml:"sweep_batch"`

	// Metrics
	MetricsEnable bool `toml:"metrics_enable"`

	// Logging
	LogLevel           string `toml:"log_level"`
	LogFile            string `toml:"log_file"`
	SlowlogThresholdMs int    `toml:"slowlog_threshold_ms"`
}

func DefaultConfig() *Config {
	return &Config{
		ListenAddr:         "0.0.0.0:7070",
		MaxClients:         10000,
		MaxKeyBytes:        256,
		MaxValueBytes:      16 * 1024 * 1024, // 16 MiB
		DataDir:            "./data",
		WALMaxBytes:        256 * 1024 * 1024, // 256 MiB
		SyncPolicy:         "batch",
		BatchFsyncMs:       100,
		BatchFsyncBytes:    1024 * 1024, // 1 MiB
		EnableSnapshot:     true,
		SnapshotPauseMaxMs: 500,
		BusyWarnMs:         50,
		SweepIntervalMs:    200,
		SweepBatch:         1000,
		MetricsEnable:      true,
		LogLevel:           "INFO",
		LogFile:            "",
		SlowlogThresholdMs: 50,
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Use defaults if config file doesn't exist
		return cfg, nil
	}

	if _, err := toml.DecodeFile(path, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) BatchFsyncDuration() time.Duration {
	return time.Duration(c.BatchFsyncMs) * time.Millisecond
}

func (c *Config) SweepInterval() time.Duration {
	return time.Duration(c.SweepIntervalMs) * time.Millisecond
}

func (c *Config) BusyWarnDuration() time.Duration {
	return time.Duration(c.BusyWarnMs) * time.Millisecond
}

func (c *Config) SlowlogThreshold() time.Duration {
	return time.Duration(c.SlowlogThresholdMs) * time.Millisecond
}
