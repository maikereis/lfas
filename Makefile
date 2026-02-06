# Variables
CARGO = cargo
BINARY_NAME = lfas
TEST_FLAGS = --all-features
BENCH_NAME ?= index_benchmark

.PHONY: all build run test bench check clean doc help

# Default action: compile the project
all: build

## Build: Compile the project in debug mode
build:
	$(CARGO) build

## Run: Run the main application (expects CSV data via stdin as per your main.rs)
run:
	$(CARGO) run

## Test: Run all unit and integration tests
test:
	$(CARGO) test $(TEST_FLAGS)

bench:
	$(CARGO) bench --bench $(BENCH_NAME)

# Shortcut for index benchmarks
bench-index:
	$(CARGO) bench --bench index_benchmark

# Shortcut for search benchmarks
bench-search:
	$(CARGO) bench --bench search_benchmark

## Check: Run clippy for linting and static analysis (Essential for Rust learners!)
check:
	$(CARGO) clippy -- -D warnings
	$(CARGO) fmt --all -- --check

## Doc: Generate project documentation
doc:
	$(CARGO) doc --open --no-deps

## Clean: Remove build artifacts
clean:
	$(CARGO) clean

## Help: Display this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'