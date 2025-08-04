.PHONY: all build clean test run

# Build variables
BIN_DIR=bin
BINARY_NAME=osprey
CLI_NAME=osprey-cli
TEST_CLIENT=test-client
BENCH_NAME=bench
GO=go
GOFLAGS=-v

all: build

build: build-server build-cli build-test-client build-bench

build-server:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(BINARY_NAME) cmd/osprey/main.go

build-cli:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(CLI_NAME) cmd/osprey-cli/main.go

build-test-client:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(TEST_CLIENT) cmd/test-client/main.go

build-bench:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(BENCH_NAME) cmd/bench/main.go

run: build-server
	./$(BIN_DIR)/$(BINARY_NAME)

test:
	$(GO) test $(GOFLAGS) ./internal/... ./pkg/...

test-integration:
	$(GO) test $(GOFLAGS) -v ./tests/integration/...

test-all:
	$(GO) test $(GOFLAGS) ./...

test-coverage:
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

clean:
	rm -rf $(BIN_DIR)
	rm -rf data/

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

deps:
	$(GO) mod download
	$(GO) mod tidy