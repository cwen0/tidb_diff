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

# Database-level concurrency (default 1)
concurrency = 1

# Use statistics for fast row count (default true, fast but may be inaccurate)
# Set to false to use exact COUNT(1) with table-level concurrency
use_stats = false

# Table-level concurrency (only effective when use_stats=false, default 10)
# Each table is counted concurrently with COUNT(1)
# Recommended range: 5-50, adjust based on database connections and server performance
table_concurrency = 10

# Optional snapshot_ts (TiDB)
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667
```

### Configuration Options

- `src.instance` / `dst.instance`: Connection strings for source and destination databases
- `dbs`: Database list to compare, supports LIKE patterns (e.g., `test%`), comma-separated
- `ignore_tables`: Tables to ignore during comparison, comma-separated
- `threshold`: Row count difference threshold (default 0, must be exactly equal)
- `output`: CSV output file path (optional)
- `compare`: Comparison items: `rows` (table row counts), `tables` (database-level table counts), `indexes` (database-level index counts), `views` (database-level view counts). Leave empty to enable all.
- `concurrency`: Database-level concurrency, number of databases processed simultaneously (default 1)
- `use_stats`: Whether to use statistics for fast row count (default `true`)
  - `true`: Use `INFORMATION_SCHEMA.TABLES.TABLE_ROWS`, fast but may be inaccurate
  - `false`: Use exact `COUNT(1)` with table-level concurrency, higher performance
- `table_concurrency`: Table-level concurrency (only effective when `use_stats=false`)
  - Each table is counted concurrently with independent `COUNT(1)` queries
  - Default 10, recommended range 5-50
  - For scenarios with many tables, increase this value for better performance
- `src.snapshot_ts` / `dst.snapshot_ts`: TiDB snapshot timestamps (optional, for comparing historical data)

## Performance Optimizations

The Go version implements several performance optimizations:

1. **Table-Level Concurrency** (when `use_stats=false`)
   - Each table is counted concurrently with independent `COUNT(1)` queries
   - Source and destination database table counts are executed in parallel
   - Controlled by `table_concurrency` parameter
   - Expected 5-10x performance improvement for scenarios with many tables

2. **Database Connection Pool Optimization**
   - Automatically configured connection pool parameters
   - Improved connection reuse efficiency

3. **Statistics Fast Mode** (when `use_stats=true`)
   - Uses `INFORMATION_SCHEMA.TABLES.TABLE_ROWS` for fast approximate row counts
   - Source and destination queries executed in parallel

### Performance Tuning Recommendations

- **Few tables (< 50)**: Use default configuration
- **Medium tables (50-200)**: Set `table_concurrency = 20-30`
- **Many tables (> 200)**: Set `table_concurrency = 30-50`, consider increasing database connection pool size
- **Need exact results**: Set `use_stats = false` to use exact COUNT
- **Quick check**: Set `use_stats = true` to use statistics mode

## Differences from Python Version

1. **Performance**: Go's goroutines provide efficient concurrency with table-level parallel counting
2. **Type Safety**: Strong typing helps catch errors at compile time
3. **Binary Distribution**: Single executable file, no Python runtime needed
4. **Memory Efficiency**: Generally lower memory footprint
5. **Advanced Concurrency**: Table-level concurrency for COUNT operations, significantly faster for large-scale comparisons

## Building

```bash
# Build for current platform
go build -o tidb_diff main.go

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o tidb_diff_linux main.go

# Build for Windows
GOOS=windows GOARCH=amd64 go build -o tidb_diff.exe main.go
```

