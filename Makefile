.PHONY: help test test-fast test-unit test-unit-mocked test-integration test-distributed test-cluster test-examples test-coverage lint lint-fix build clean vendor install-tools mocks docker-up docker-down docker-cluster-up docker-cluster-down

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
	@echo ""
	@echo "$(BOLD)Tool Installation:$(RESET)"
	@echo "  $(CYAN)make install-tools$(RESET)     - Install required tools via asdf"
	@echo ""
	@echo "$(BOLD)Testing:$(RESET)"
	@echo "  $(CYAN)make test$(RESET)              - Run all tests including examples (default)"
	@echo "  $(CYAN)make test-fast$(RESET)         - Run all tests quickly (no examples)"
	@echo "  $(CYAN)make test-unit$(RESET)         - Run unit tests with mocks (no Redis required)"
	@echo "  $(CYAN)make test-integration$(RESET)  - Run integration tests (requires Redis)"
	@echo "  $(CYAN)make test-distributed$(RESET)  - Run distributed tests (multi-client coordination)"
	@echo "  $(CYAN)make test-cluster$(RESET)      - Run Redis cluster tests (requires cluster)"
	@echo "  $(CYAN)make test-examples$(RESET)     - Run example tests only"
	@echo "  $(CYAN)make test-coverage$(RESET)     - Run tests with coverage report"
	@echo ""
	@echo "$(BOLD)Mocking:$(RESET)"
	@echo "  $(CYAN)make mocks$(RESET)             - Generate all mocks using mockery v3"
	@echo ""
	@echo "$(BOLD)Docker/Redis:$(RESET)"
	@echo "  $(CYAN)make docker-up$(RESET)         - Start single Redis instance"
	@echo "  $(CYAN)make docker-down$(RESET)       - Stop single Redis instance"
	@echo "  $(CYAN)make docker-cluster-up$(RESET) - Start Redis cluster (6 nodes)"
	@echo "  $(CYAN)make docker-cluster-down$(RESET) - Stop Redis cluster"
	@echo ""
	@echo "$(BOLD)Code Quality:$(RESET)"
	@echo "  $(CYAN)make lint$(RESET)              - Run golangci-lint"
	@echo "  $(CYAN)make lint-fix$(RESET)          - Run golangci-lint with auto-fix"
	@echo "  $(CYAN)make build$(RESET)             - Build all packages"
	@echo ""
	@echo "$(BOLD)Maintenance:$(RESET)"
	@echo "  $(CYAN)make clean$(RESET)             - Clean build artifacts"
	@echo "  $(CYAN)make vendor$(RESET)            - Download and vendor dependencies"

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

# Run all tests: unit + integration + distributed + cluster + examples (default, most comprehensive)
test:
	@echo "$(YELLOW)Running all tests (unit + integration + distributed + cluster + examples)...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis on localhost:6379 AND Redis Cluster on localhost:17000-17005$(RESET)"
	@echo "$(YELLOW)      Start with: make docker-up && make docker-cluster-up$(RESET)"
	@echo ""
	@echo "$(CYAN)Step 1/2: Running unit tests (no Redis)...$(RESET)"
	@go test -v ./... && echo "$(GREEN)✓ Unit tests passed!$(RESET)" || (echo "$(RED)✗ Unit tests failed!$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)Step 2/2: Running integration/distributed/cluster/examples tests...$(RESET)"
	@go test -tags="integration,distributed,cluster,examples" -v ./... && echo "$(GREEN)✓ All integration tests passed!$(RESET)" || (echo "$(RED)✗ Integration tests failed!$(RESET)" && exit 1)
	@echo ""
	@echo "$(GREEN)$(BOLD)✓ All tests passed!$(RESET)"

# Run tests quickly without examples
test-fast:
	@echo "$(YELLOW)Running tests (no examples)...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis on localhost:6379 AND Redis Cluster on localhost:17000-17005$(RESET)"
	@echo "$(YELLOW)      Start with: make docker-up && make docker-cluster-up$(RESET)"
	@go test -tags="integration,distributed,cluster" -v ./... && echo "$(GREEN)✓ Tests passed!$(RESET)" || (echo "$(RED)✗ Tests failed!$(RESET)" && exit 1)

# Run only unit tests (no Redis required, no build tags)
test-unit:
	@echo "$(YELLOW)Running unit tests (no Redis required)...$(RESET)"
	@go test -v ./... && echo "$(GREEN)✓ Unit tests passed!$(RESET)" || (echo "$(RED)✗ Unit tests failed!$(RESET)" && exit 1)

# Run integration tests (requires Redis)
test-integration:
	@echo "$(YELLOW)Running integration tests (requires Redis)...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis on localhost:6379$(RESET)"
	@go test -tags=integration -v ./... && echo "$(GREEN)✓ Integration tests passed!$(RESET)" || (echo "$(RED)✗ Integration tests failed!$(RESET)" && exit 1)

# Run distributed tests (multi-client, single Redis instance)
test-distributed:
	@echo "$(YELLOW)Running distributed tests (multi-client coordination)...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis on localhost:6379 (start with: make docker-up)$(RESET)"
	@go test -tags=distributed -v ./... && echo "$(GREEN)✓ Distributed tests passed!$(RESET)" || (echo "$(RED)✗ Distributed tests failed!$(RESET)" && exit 1)

# Run Redis cluster tests (requires cluster setup)
test-cluster:
	@echo "$(YELLOW)Running Redis cluster tests...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis Cluster on localhost:17000-17005 (start with: make docker-cluster-up)$(RESET)"
	@go test -tags=cluster -v ./... && echo "$(GREEN)✓ Cluster tests passed!$(RESET)" || (echo "$(RED)✗ Cluster tests failed!$(RESET)" && exit 1)

# Run example tests with build tag
test-examples:
	@echo "$(YELLOW)Running example tests...$(RESET)"
	@go test -tags=examples -v ./examples/... && echo "$(GREEN)✓ Example tests passed!$(RESET)" || (echo "$(RED)✗ Example tests failed!$(RESET)" && exit 1)

# Run tests with coverage report
test-coverage:
	@echo "$(YELLOW)Running tests with coverage...$(RESET)"
	@echo "$(YELLOW)Note: Requires Redis on localhost:6379 AND Redis Cluster on localhost:17000-17005$(RESET)"
	@echo "$(YELLOW)      Start with: make docker-up && make docker-cluster-up$(RESET)"
	@go test -tags="integration,distributed,cluster" -v -coverprofile=coverage.out -covermode=atomic ./... && echo "$(GREEN)✓ Tests passed!$(RESET)" || (echo "$(RED)✗ Tests failed!$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)Coverage report:$(RESET)"
	@go tool cover -func=coverage.out | tail -1
	@echo ""
	@echo "$(CYAN)To view HTML coverage report: go tool cover -html=coverage.out$(RESET)"

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

# Generate mocks using mockery v3
mocks:
	@echo "$(YELLOW)Generating mocks with mockery v3...$(RESET)"
	@command -v mockery >/dev/null 2>&1 || { echo "$(RED)Error: mockery is not installed. Install with: go install github.com/vektra/mockery/v3@latest$(RESET)"; exit 1; }
	@mockery --config .mockery.yaml && echo "$(GREEN)✓ Mocks generated successfully!$(RESET)" || (echo "$(RED)✗ Mock generation failed!$(RESET)" && exit 1)

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

# Docker targets for Redis
docker-up:
	@echo "$(YELLOW)Starting single Redis instance...$(RESET)"
	@docker-compose up -d redis
	@echo "$(GREEN)✓ Redis started on localhost:6379$(RESET)"
	@echo "$(YELLOW)Waiting for Redis to be ready...$(RESET)"
	@sleep 2
	@docker-compose exec -T redis redis-cli ping || (echo "$(RED)✗ Redis not responding$(RESET)" && exit 1)
	@echo "$(GREEN)✓ Redis is ready!$(RESET)"

docker-down:
	@echo "$(YELLOW)Stopping single Redis instance...$(RESET)"
	@docker-compose down
	@echo "$(GREEN)✓ Redis stopped$(RESET)"

docker-cluster-up:
	@echo "$(YELLOW)Starting Redis Cluster (6 nodes)...$(RESET)"
	@docker-compose up -d redis-cluster
	@echo "$(GREEN)✓ Redis Cluster starting on localhost:17000-17005$(RESET)"
	@echo "$(YELLOW)Waiting for cluster to be ready (this may take 10-15 seconds)...$(RESET)"
	@sleep 15
	@docker exec redis-cluster redis-cli -p 17000 cluster nodes || (echo "$(RED)✗ Cluster not responding$(RESET)" && exit 1)
	@echo "$(GREEN)✓ Redis Cluster is ready!$(RESET)"

docker-cluster-down:
	@echo "$(YELLOW)Stopping Redis Cluster...$(RESET)"
	@docker-compose stop redis-cluster
	@docker-compose rm -f redis-cluster
	@echo "$(GREEN)✓ Redis Cluster stopped$(RESET)"

# CI target - runs linting and all tests
ci: lint test
	@echo "$(GREEN)$(BOLD)✓ CI checks complete!$(RESET)"
