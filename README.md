# Osprey 🦅

A high-performance, single-process key-value store with durable persistence and a simple text-based protocol.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://golang.org/dl/)

## Overview

Osprey is a lightweight, Redis-inspired key-value store designed for simplicity, performance, and reliability. It implements a single-threaded event loop architecture with write-ahead logging (WAL), snapshot-based compaction, and automatic expiry management.

### Key Features

- **Single-threaded event loop** - Redis-like architecture for maximum throughput
- **Text-based TCP protocol** - Simple, human-readable commands with binary payload support
- **Durable persistence** - Write-ahead logging with CRC32C checksums and configurable sync policies
- **Automatic compaction** - Snapshot-based compaction with manifest coordination
- **TTL expiration** - Lazy deletion with background sweeper for expired keys
- **Atomic operations** - Conditional SET operations with versioning (CAS)
- **Key validation** - Prevents invalid characters (ASCII spaces and control characters)
- **Rich command set** - GET, SET, DEL, EXISTS, EXPIRE, TTL, INCR/DECR, MGET/MSET, STATS
- **Built-in CLI client** - Full-featured command-line interface

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/bharatmehan/osprey.git
cd osprey

# Build the server and CLI
go build -o bin/osprey ./cmd/osprey
go build -o bin/osprey-cli ./cmd/osprey-cli
```

### Running the Server

```bash
# Start with default configuration
./bin/osprey

# Or specify a custom config file
./bin/osprey -config custom.toml
```

The server will start on `localhost:7070` by default.

### Using the CLI Client

```bash
# Basic operations
./bin/osprey-cli set mykey "Hello World"
./bin/osprey-cli get mykey
./bin/osprey-cli del mykey

# TTL operations
./bin/osprey-cli set temp "expires soon" EX 5000  # 5 second TTL
./bin/osprey-cli ttl temp

# Atomic operations
./bin/osprey-cli incr counter 10
./bin/osprey-cli decr counter 3

# Multiple keys
./bin/osprey-cli mget key1 key2 key3

# Server statistics
./bin/osprey-cli stats
```

## Protocol Reference

Osprey uses a simple text-based protocol over TCP. All commands are case-insensitive and responses use uppercase keywords.

### Basic Commands

| Command | Description | Example |
|---------|-------------|---------|
| `PING` | Health check | `PING` → `PONG` |
| `GET <key>` | Retrieve value | `GET user:1` → `VALUE 5 1 -1\r\nalice\r\n` |
| `SET <key> <len> [options]` | Store value | `SET user:1 5\r\nalice\r\n` → `OK 1` |
| `DEL <key>` | Delete key | `DEL user:1` → `DELETED 1` |
| `EXISTS <key>` | Check existence | `EXISTS user:1` → `EXISTS 1` |

### TTL Commands

| Command | Description | Example |
|---------|-------------|---------|
| `EXPIRE <key> <ms>` | Set TTL | `EXPIRE user:1 5000` → `OK` |
| `TTL <key>` | Get remaining TTL | `TTL user:1` → `4500` |

### Conditional SET Options

| Option | Description |
|--------|-------------|
| `EX <ms>` | Set relative TTL in milliseconds |
| `PXAT <ms>` | Set absolute expiry as epoch milliseconds |
| `NX` | Only set if key does not exist |
| `XX` | Only set if key exists |
| `VER <n>` | Only set if current version equals n (CAS) |

### Atomic Operations

| Command | Description | Example |
|---------|-------------|---------|
| `INCR <key> [delta]` | Increment numeric value | `INCR counter 5` → `15` |
| `DECR <key> [delta]` | Decrement numeric value | `DECR counter 3` → `12` |

### Batch Operations

| Command | Description |
|---------|-------------|
| `MGET <key1> <key2> ...` | Get multiple keys |
| `MSET <k1> <len1> <k2> <len2> ...` | Set multiple keys |

### Statistics

The `STATS` command returns server metrics:

```
STATS
uptime_ms=1234567
clients=5
keys=1042
expired_total=881
cmd_get=100231
cmd_set=55420
wal_current="wal-00000003.oswal"
wal_bytes=73400320
mem_rss_bytes=134217728
END
```

## Configuration

Create an `osprey.toml` configuration file:

```toml
# Network settings
listen_addr = "0.0.0.0:7070"
max_clients = 10000

# Data limits
max_key_bytes = 256
max_value_bytes = 16777216  # 16 MiB

# Persistence
data_dir = "./data"
wal_max_bytes = 268435456    # 256 MiB
sync_policy = "batch"        # os | batch | always
batch_fsync_ms = 100
batch_fsync_bytes = 1048576

# Snapshots
enable_snapshot = true
snapshot_pause_max_ms = 500
busy_warn_ms = 50

# Expiry management
sweep_interval_ms = 200
sweep_batch = 1000

# Observability
metrics_enable = true

# Logging
log_level = "INFO"
log_file = ""  # Empty means default: data/logs/osprey.log
slowlog_threshold_ms = 50
```

### Sync Policies

- **`os`** - No explicit fsync (fastest, data may be lost on OS crash)
- **`batch`** - Fsync every 100ms or 1MB, whichever comes first
- **`always`** - Fsync on every write (slowest, most durable)

## Architecture

### Storage Engine

- **In-memory hash map** - Primary data structure for O(1) key access
- **Expiry min-heap** - Efficient tracking of key expiration times
- **Write-ahead log (WAL)** - Durable record of all mutations with CRC32C checksums
- **Snapshot files** - Periodic compaction to reduce WAL replay time

### Concurrency Model

- **Single-threaded event loop** - All commands processed sequentially for maximum throughput
- **Background sweeper** - Separate thread for proactive expiry cleanup
- **Stop-the-world snapshots** - Brief pauses (< 500ms) during compaction

### File Layout

```
data/
├── MANIFEST.json           # Points to current snapshot and WAL
├── wal-00000001.oswal      # Write-ahead log files
├── wal-00000002.oswal
├── snap-00000001.osnap     # Snapshot files
└── logs/
    └── osprey.log          # Server logs
```

## Performance

Osprey is designed for high throughput on single-core workloads:

- **50,000+ ops/sec** for SET operations with 100-byte values (sync_policy=os)
- **Sub-millisecond p95 latency** for GET operations
- **Snapshot of 1M keys** completes in under 500ms

Actual performance depends on hardware, value sizes, and sync policy configuration.

## Error Handling

The protocol includes comprehensive error codes:

| Error Code | Description |
|------------|-------------|
| `ERR BADREQ` | Malformed command or arguments |
| `ERR TOOLARGE` | Value exceeds configured maximum size |
| `ERR EXISTS` | Conditional SET failed (key exists when NX specified) |
| `ERR NEXISTS` | Conditional SET failed (key missing when XX specified) |
| `ERR VER` | Version mismatch in CAS operation |
| `ERR TYPE` | INCR/DECR attempted on non-integer value |
| `ERR BUSY` | Server temporarily unavailable during snapshot |
| `ERR INTERNAL` | Unexpected server error |

## Development

### Prerequisites

- Go 1.21 or later
- Make (optional, for convenience)

### Building

```bash
# Build everything
go build ./...

# Run tests
go test ./...

# Run integration tests
go test ./internal/integration

# Run benchmarks
go test -bench=. ./internal/benchmark
```

### Testing

```bash
# Unit tests only
make test

# Integration tests only
make test-integration

# All tests (unit + integration)
make test-all

# Tests with coverage report
make test-coverage

# Performance benchmarks
cd cmd/bench && go run .
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`go test ./...`)
6. Commit your changes (`git commit -am 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

### Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Add tests for new features
- Update documentation for API changes
- Keep commits atomic and well-described

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Redis architecture and protocol design
- Uses CRC32C checksums for data integrity
- Built with Go's excellent networking and concurrency primitives
