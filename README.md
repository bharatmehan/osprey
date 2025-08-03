# Osprey - Key-Value Store

Osprey is a single-process, single-node key-value store that serves requests over a simple text protocol on TCP. It provides durability through write-ahead logging and supports TTL-based expiration.

## Quick Start

### Building

```bash
# Download dependencies
go mod download

# Build the server
go build -o osprey cmd/osprey/main.go

# Build the test client
go build -o test-client cmd/test-client/main.go
```

### Running

```bash
# Start the server with default configuration
./osprey

# Or with a custom config file
./osprey -config custom.toml
```

The server will start on port 7070 by default.

### Testing

```bash
# Run the test client
./test-client

# Or use telnet for manual testing
telnet localhost 7070
> PING
< PONG
> SET mykey 5
> hello
< OK 1
> GET mykey
< VALUE 5 1 -1
< hello
```

## Supported Commands

- `PING` - Health check
- `GET <key>` - Get a value
- `SET <key> <len> [options]` - Set a value
  - Options: `EX <ms>`, `PXAT <ms>`, `NX`, `XX`, `VER <n>`
- `DEL <key>` - Delete a key
- `EXISTS <key>` - Check if key exists
- `EXPIRE <key> <ttl_ms>` - Set expiration
- `TTL <key>` - Get time to live
- `INCR <key> [delta]` - Increment numeric value
- `DECR <key> [delta]` - Decrement numeric value
- `MGET <key1> <key2> ...` - Get multiple keys
- `MSET <k1> <len1> <k2> <len2> ...` - Set multiple keys
- `STATS` - Get server statistics

## Configuration

See `osprey.toml` for all configuration options:

- Network settings (port, max clients)
- Storage limits (key/value sizes)
- Persistence settings (WAL, snapshots)
- Performance tuning

## Architecture

- **Single-threaded event loop** for request processing
- **Write-ahead logging** for durability
- **In-memory hash map** for fast access
- **Lazy expiration** with optional background sweeper
- **CRC32 checksums** for data integrity

## Current Status

See [PROGRESS_STATUS.md](PROGRESS_STATUS.md) for detailed implementation status.

## Development

```bash
# Run all tests
make test

# Run integration tests
make test-integration

# Generate test coverage
make test-coverage

# Format code
make fmt

# Lint and check
make vet

# Clean build artifacts
make clean

# Build and run locally
make run
```

## License

MIT License - see [LICENSE](LICENSE) file for details.