# TiDB Diff Tool - Go Implementation

This is a Go implementation of the database comparison tool that compares MySQL/TiDB databases.

## Features

- Compare table row counts between source and destination databases
- Compare schema object counts (tables, indexes, views) at database level
- Support concurrent checking for better performance
- Support TiDB snapshot timestamps
- Export results to CSV
- Configurable comparison items and thresholds

## Requirements

- Go 1.21 or later
- MySQL/TiDB database access

## Installation

```bash
go mod download
```

## Usage

```bash
go run main.go --config config.ini
```

Or build and run:

```bash
go build -o tidb_diff main.go
./tidb_diff --config config.ini
```

## Configuration

The tool uses the same `config.ini` file as the Python version:

```ini
[diff]
src.instance = mysql://root@127.0.0.1:4000
dst.instance = mysql://root@127.0.0.1:63844
dbs = test%
ignore_tables = tmp_log, sys_history
threshold = 0
output = diff_result.csv

# Comparison items: rows, tables, indexes, views
# Leave empty to enable all
compare = rows,tables,indexes,views

# Concurrency (only for rows comparison, default 1)
concurrency = 1

# Optional snapshot_ts (TiDB)
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667
```

## Differences from Python Version

1. **Performance**: Go's goroutines provide efficient concurrency
2. **Type Safety**: Strong typing helps catch errors at compile time
3. **Binary Distribution**: Single executable file, no Python runtime needed
4. **Memory Efficiency**: Generally lower memory footprint

## Building

```bash
# Build for current platform
go build -o tidb_diff main.go

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o tidb_diff_linux main.go

# Build for Windows
GOOS=windows GOARCH=amd64 go build -o tidb_diff.exe main.go
```

