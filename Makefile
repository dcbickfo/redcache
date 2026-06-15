# redcache developer tasks. Run `make` (or `make help`) for the list.
# Tool versions (Go, golangci-lint) are pinned in .tool-versions; `make setup`
# installs them via asdf.

GO            ?= go
GOLANGCI_LINT ?= golangci-lint
PKG           ?= ./...

.DEFAULT_GOAL := help

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Install pinned tool versions via asdf (.tool-versions)
	asdf install

.PHONY: build
build: ## Compile all packages
	$(GO) build $(PKG)

.PHONY: test
test: ## Run all tests (needs Redis on localhost:6379 — see redis-up)
	$(GO) test $(PKG)

.PHONY: test-short
test-short: ## Run tests with -short
	$(GO) test -short $(PKG)

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test -race $(PKG)

.PHONY: bench
bench: ## Run benchmarks only (no tests)
	$(GO) test -run '^$$' -bench=. -benchmem $(PKG)

.PHONY: cover
cover: ## Run tests with coverage and print the total
	$(GO) test -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKG)

.PHONY: lint
lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run

.PHONY: fmt
fmt: ## Format code (golangci-lint v2 formatters)
	$(GOLANGCI_LINT) fmt

.PHONY: tidy
tidy: ## Tidy and verify go.mod / go.sum
	$(GO) mod tidy
	$(GO) mod verify

.PHONY: redis-up
redis-up: ## Start Redis for integration tests (docker compose)
	docker compose up -d

.PHONY: redis-down
redis-down: ## Stop Redis
	docker compose down

.PHONY: check
check: build vet lint test ## Full gate: build, vet, lint, test

.PHONY: clean
clean: ## Remove build and coverage artifacts
	$(GO) clean
	rm -f coverage.out
