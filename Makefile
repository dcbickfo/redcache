# redcache developer tasks. Run `make` (or `make help`) for the list.
# Tool versions (Go, golangci-lint) are pinned in .tool-versions; `make setup`
# installs them via asdf.

ASDF          ?= asdf
GO            ?= $(ASDF) exec go
GOLANGCI_LINT ?= $(ASDF) exec golangci-lint
PKG           ?= ./...
COVERAGE_PROFILE ?= coverage.out
MIN_COVERAGE ?= 83.0

.DEFAULT_GOAL := help

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Install pinned tool versions via asdf (.tool-versions)
	@command -v $(ASDF) >/dev/null || { echo "asdf is required; install it first: https://asdf-vm.com"; exit 1; }
	@awk 'NF && $$1 !~ /^#/ { print $$1 }' .tool-versions | while read -r plugin; do \
		$(ASDF) plugin list | grep -qx "$$plugin" || $(ASDF) plugin add "$$plugin"; \
	done
	$(ASDF) install

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
	$(GO) test -coverprofile=$(COVERAGE_PROFILE) $(PKG)
	$(GO) tool cover -func=$(COVERAGE_PROFILE) | tail -1

.PHONY: cover-check
cover-check: ## Run coverage and fail below MIN_COVERAGE
	$(GO) test -coverprofile=$(COVERAGE_PROFILE) $(PKG)
	@total="$$( $(GO) tool cover -func=$(COVERAGE_PROFILE) | awk '/^total:/ { sub(/%/, "", $$3); print $$3 }' )"; \
	awk -v total="$$total" -v min="$(MIN_COVERAGE)" 'BEGIN { \
		if (total+0 < min+0) { \
			printf "coverage %.1f%% is below %.1f%%\n", total, min; \
			exit 1; \
		} \
		printf "coverage %.1f%% >= %.1f%%\n", total, min; \
	}'

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
check: build lint cover-check ## Full gate: build, lint, coverage-backed tests

.PHONY: clean
clean: ## Remove build and coverage artifacts
	$(GO) clean
	rm -f $(COVERAGE_PROFILE)
