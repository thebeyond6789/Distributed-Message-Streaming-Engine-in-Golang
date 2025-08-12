# Makefile for Distributed Message Streaming Engine

.PHONY: build test clean proto benchmark docker

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
SERVER_BINARY=bin/streaming-server
CLIENT_BINARY=bin/streaming-client
BENCHMARK_BINARY=bin/streaming-benchmark

# Build targets
build: proto
	mkdir -p bin
	$(GOBUILD) -o $(SERVER_BINARY) ./cmd/server
	$(GOBUILD) -o $(CLIENT_BINARY) ./cmd/client
	$(GOBUILD) -o $(BENCHMARK_BINARY) ./cmd/benchmark

# Protocol buffer generation
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/*.proto

# Test targets
test:
	$(GOTEST) -v ./...

test-race:
	$(GOTEST) -race -v ./...

test-coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Benchmark targets
benchmark:
	$(GOTEST) -bench=. -benchmem ./...

benchmark-cpu:
	$(GOTEST) -bench=. -cpuprofile=cpu.prof ./...

benchmark-mem:
	$(GOTEST) -bench=. -memprofile=mem.prof ./...

# Development targets
deps:
	$(GOMOD) download
	$(GOMOD) tidy

lint:
	golangci-lint run

# Docker targets
docker:
	docker build -t streaming-engine -f docker/Dockerfile .

docker-compose:
	docker-compose -f docker/docker-compose.yml up -d

# Clean targets
clean:
	$(GOCLEAN)
	rm -rf bin/
	rm -f coverage.out coverage.html
	rm -f *.prof

# Installation targets
install-tools:
	$(GOGET) google.golang.org/protobuf/cmd/protoc-gen-go
	$(GOGET) google.golang.org/grpc/cmd/protoc-gen-go-grpc
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint

# Development server
dev-server:
	$(GOBUILD) -o $(SERVER_BINARY) ./cmd/server
	./$(SERVER_BINARY) --config=configs/dev.yml

# Multi-node cluster for testing
dev-cluster:
	./scripts/start-cluster.sh

# Performance testing
load-test:
	./scripts/load-test.sh

# Stop all processes
stop:
	pkill -f streaming-server || true
	pkill -f streaming-benchmark || true
