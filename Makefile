# Makefile for tidb_diff

# 项目信息
BINARY_NAME=tidb_diff
MAIN_PACKAGE=main.go
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date +%Y-%m-%d\ %H:%M:%S)
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# 编译参数（如果需要版本信息，可以在 main.go 中定义 Version, BuildTime, Commit 变量）
# LDFLAGS=-ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.Commit=$(COMMIT)'"
GO_BUILD=go build

# 输出目录
BUILD_DIR=build
LINUX_DIR=$(BUILD_DIR)/linux
LINUX_AMD64_DIR=$(LINUX_DIR)/amd64
LINUX_ARM64_DIR=$(LINUX_DIR)/arm64

# 默认目标
.PHONY: all
all: linux

# 编译 Linux amd64 版本
.PHONY: linux
linux: linux-amd64 linux-arm64

.PHONY: linux-amd64
linux-amd64:
	@echo "Building Linux amd64 binary..."
	@mkdir -p $(LINUX_AMD64_DIR)
	GOOS=linux GOARCH=amd64 $(GO_BUILD) -o $(LINUX_AMD64_DIR)/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Binary built: $(LINUX_AMD64_DIR)/$(BINARY_NAME)"

.PHONY: linux-arm64
linux-arm64:
	@echo "Building Linux arm64 binary..."
	@mkdir -p $(LINUX_ARM64_DIR)
	GOOS=linux GOARCH=arm64 $(GO_BUILD) -o $(LINUX_ARM64_DIR)/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Binary built: $(LINUX_ARM64_DIR)/$(BINARY_NAME)"

# 编译当前平台版本
.PHONY: build
build:
	@echo "Building for current platform..."
	$(GO_BUILD) -o $(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Binary built: $(BINARY_NAME)"

# 编译所有平台（Linux amd64/arm64, macOS amd64/arm64, Windows amd64）
.PHONY: build-all
build-all: linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64

# macOS 版本
.PHONY: darwin
darwin: darwin-amd64 darwin-arm64

.PHONY: darwin-amd64
darwin-amd64:
	@echo "Building macOS amd64 binary..."
	@mkdir -p $(BUILD_DIR)/darwin/amd64
	GOOS=darwin GOARCH=amd64 $(GO_BUILD) -o $(BUILD_DIR)/darwin/amd64/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Binary built: $(BUILD_DIR)/darwin/amd64/$(BINARY_NAME)"

.PHONY: darwin-arm64
darwin-arm64:
	@echo "Building macOS arm64 binary..."
	@mkdir -p $(BUILD_DIR)/darwin/arm64
	GOOS=darwin GOARCH=arm64 $(GO_BUILD) -o $(BUILD_DIR)/darwin/arm64/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Binary built: $(BUILD_DIR)/darwin/arm64/$(BINARY_NAME)"

# Windows 版本
.PHONY: windows
windows: windows-amd64

.PHONY: windows-amd64
windows-amd64:
	@echo "Building Windows amd64 binary..."
	@mkdir -p $(BUILD_DIR)/windows/amd64
	GOOS=windows GOARCH=amd64 $(GO_BUILD) -o $(BUILD_DIR)/windows/amd64/$(BINARY_NAME).exe $(MAIN_PACKAGE)
	@echo "Binary built: $(BUILD_DIR)/windows/amd64/$(BINARY_NAME).exe"

# 清理编译产物
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@rm -f $(BINARY_NAME)
	@rm -f $(BINARY_NAME).exe
	@echo "Clean completed"

# 运行测试
.PHONY: test
test:
	@echo "Running tests..."
	go test -v ./...

# 格式化代码
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...

# 代码检查
.PHONY: vet
vet:
	@echo "Running go vet..."
	go vet ./...

# 下载依赖
.PHONY: deps
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# 安装依赖（用于 CI/CD）
.PHONY: install
install: deps

# 显示帮助信息
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make linux          - Build Linux amd64 and arm64 binaries"
	@echo "  make linux-amd64    - Build Linux amd64 binary"
	@echo "  make linux-arm64    - Build Linux arm64 binary"
	@echo "  make build          - Build for current platform"
	@echo "  make build-all      - Build for all platforms (Linux, macOS, Windows)"
	@echo "  make darwin         - Build macOS amd64 and arm64 binaries"
	@echo "  make windows        - Build Windows amd64 binary"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make test           - Run tests"
	@echo "  make fmt            - Format code"
	@echo "  make vet            - Run go vet"
	@echo "  make deps           - Download and tidy dependencies"
	@echo "  make help           - Show this help message"

