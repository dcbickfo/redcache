.PHONY: help test test-examples test-all lint lint-fix build clean vendor install-tools

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m
BOLD := \033[1m

# Default target
help:
	@echo "$(BOLD)Available targets:$(RESET)"
	@echo "  $(CYAN)make install-tools$(RESET) - Install required tools via asdf"
	@echo "  $(CYAN)make test$(RESET)          - Run main package tests"
	@echo "  $(CYAN)make test-examples$(RESET) - Run example tests with build tag"
	@echo "  $(CYAN)make test-all$(RESET)      - Run all tests including examples"
	@echo "  $(CYAN)make lint$(RESET)          - Run golangci-lint"
	@echo "  $(CYAN)make lint-fix$(RESET)      - Run golangci-lint with auto-fix"
	@echo "  $(CYAN)make build$(RESET)         - Build all packages"
	@echo "  $(CYAN)make clean$(RESET)         - Clean build artifacts"
	@echo "  $(CYAN)make vendor$(RESET)        - Download and vendor dependencies"

# Install required tools via asdf
install-tools:
	@echo "$(YELLOW)Installing tools from .tool-versions...$(RESET)"
	@command -v asdf >/dev/null 2>&1 || { echo "$(RED)Error: asdf is not installed. Please install asdf first: https://asdf-vm.com$(RESET)"; exit 1; }
	@echo "$(YELLOW)Adding asdf plugins if not already added...$(RESET)"
	@asdf plugin add golang || true
	@asdf plugin add golangci-lint || true
	@echo "$(YELLOW)Installing tools...$(RESET)"
	@-asdf install
	@echo "$(GREEN)✓ Tools installed successfully!$(RESET)"
	@echo ""
	@echo "$(BOLD)Installed versions:$(RESET)"
	@asdf current

# Run main package tests (without examples)
test:
	@echo "$(YELLOW)Running main package tests...$(RESET)"
	@go test -v ./... && echo "$(GREEN)✓ Tests passed!$(RESET)" || (echo "$(RED)✗ Tests failed!$(RESET)" && exit 1)

# Run example tests with build tag
test-examples:
	@echo "$(YELLOW)Running example tests...$(RESET)"
	@go test -tags=examples -v ./examples/... && echo "$(GREEN)✓ Example tests passed!$(RESET)" || (echo "$(RED)✗ Example tests failed!$(RESET)" && exit 1)

# Run all tests including examples
test-all:
	@echo "$(YELLOW)Running all tests (including examples)...$(RESET)"
	@go test -tags=examples -v ./... && echo "$(GREEN)✓ All tests passed!$(RESET)" || (echo "$(RED)✗ Tests failed!$(RESET)" && exit 1)

# Run linter (will automatically use build-tags from .golangci.yml)
lint:
	@echo "$(YELLOW)Running golangci-lint...$(RESET)"
	@golangci-lint run --timeout=5m && echo "$(GREEN)✓ Linting passed!$(RESET)" || (echo "$(RED)✗ Linting failed!$(RESET)" && exit 1)

# Run linter with auto-fix
lint-fix:
	@echo "$(YELLOW)Running golangci-lint with auto-fix...$(RESET)"
	@golangci-lint run --timeout=5m --fix && echo "$(GREEN)✓ Linting completed with fixes!$(RESET)" || (echo "$(RED)✗ Linting failed!$(RESET)" && exit 1)

# Build all packages
build:
	@echo "$(YELLOW)Building all packages...$(RESET)"
	@go build ./... && echo "$(GREEN)✓ Build successful!$(RESET)" || (echo "$(RED)✗ Build failed!$(RESET)" && exit 1)

# Build with examples
build-examples:
	@echo "$(YELLOW)Building all packages (including examples)...$(RESET)"
	@go build -tags=examples ./... && echo "$(GREEN)✓ Build successful!$(RESET)" || (echo "$(RED)✗ Build failed!$(RESET)" && exit 1)

# Clean build artifacts
clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(RESET)"
	@go clean -cache -testcache -modcache
	@echo "$(GREEN)✓ Clean complete!$(RESET)"

# Download and vendor dependencies
vendor:
	@echo "$(YELLOW)Downloading and vendoring dependencies...$(RESET)"
	@go mod download
	@go mod vendor
	@echo "$(GREEN)✓ Dependencies vendored!$(RESET)"

# CI target - runs linting and all tests
ci: lint test-all
	@echo "$(GREEN)$(BOLD)✓ CI checks complete!$(RESET)"
