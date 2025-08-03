.PHONY: all build clean test run

# Build variables
BINARY_NAME=osprey
CLI_NAME=osprey-cli
TEST_CLIENT=test-client
BENCH_NAME=bench
GO=go
GOFLAGS=-v

all: build

build: build-server build-cli build-test-client build-bench

build-server:
	$(GO) build $(GOFLAGS) -o $(BINARY_NAME) cmd/osprey/main.go

build-cli:
	$(GO) build $(GOFLAGS) -o $(CLI_NAME) cmd/osprey-cli/main.go

build-test-client:
	$(GO) build $(GOFLAGS) -o $(TEST_CLIENT) cmd/test-client/main.go

build-bench:
	$(GO) build $(GOFLAGS) -o $(BENCH_NAME) cmd/bench/main.go

run: build-server
	./$(BINARY_NAME)

test:
	$(GO) test $(GOFLAGS) ./...

test-integration:
	$(GO) test $(GOFLAGS) -v .

test-coverage:
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

clean:
	rm -f $(BINARY_NAME) $(CLI_NAME) $(TEST_CLIENT) $(BENCH_NAME)
	rm -rf data/

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

deps:
	$(GO) mod download
	$(GO) mod tidy