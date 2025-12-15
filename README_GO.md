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

The tool uses `config.ini` file:

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

# Database-level concurrency
# Default: 5 (optimized for multi-database scenarios)
# Recommended range: 3-20
# - Few databases (<10): 3-5
# - Medium databases (10-50): 5-10
# - Many databases (>50): 10-20
# Note: Each database needs 2 connection pools (source + destination)
concurrency = 5

# Use statistics for fast row count (default true, fast but may be inaccurate)
# Set to false to use exact COUNT(1) with table-level concurrency
use_stats = false

# Table-level concurrency (only effective when use_stats=false)
# Default: 30 (optimized for multi-table scenarios)
# Recommended range: 10-50
# - Few tables (<100): 10-20
# - Medium tables (100-500): 20-30
# - Many tables (500-1000): 30-40
# - Very many tables (>1000): 40-50
# Note: Source and destination execute in parallel, actual concurrency = table_concurrency * 2
table_concurrency = 30

# Connection pool configuration (optimized for multi-DB, multi-table, large table scenarios)
# max_open_conns: Maximum open connections
# If not configured (set to 0), automatically calculated based on concurrency and table_concurrency
#   Formula: concurrency * 2 * (table_concurrency + 10)
#   Range: 100-500 (auto-limited)
# Manual configuration recommendations:
# - Few DBs/tables: 100-150
# - Medium DBs/tables: 150-250
# - Many DBs/tables: 250-400
# - Very many DBs/tables: 400-500
max_open_conns = 0

# max_idle_conns: Maximum idle connections
# If not configured (set to 0), automatically calculated as 80% of max_open_conns (minimum 80)
max_idle_conns = 0

# conn_max_lifetime_minutes: Connection max lifetime (minutes)
# Default: 30 minutes (for large table scenarios)
# Set to 0 for unlimited (not recommended)
conn_max_lifetime_minutes = 30

# query_timeout_seconds: Query timeout (seconds), 0 means default (10 minutes)
# For large table COUNT(1) queries, adjust based on table size
# Examples: 10M rows: 600-1800s (10-30min), 100M rows: 1800-3600s (30-60min)
# Set to 0 to use default 10 minutes timeout (avoids program hanging due to long queries)
# 【Important】Recommend explicitly setting this value based on largest table size, avoid using default
query_timeout_seconds = 0

# read_timeout_seconds: Read timeout (seconds), 0 means default
# Recommended: query_timeout_seconds + 60
read_timeout_seconds = 0

# write_timeout_seconds: Write timeout (seconds), 0 means default
# Usually 30-60 seconds
write_timeout_seconds = 0

# max_retries: Query retry count (for large table query failures)
# Default: 2, range: 0-5
# For unstable networks, set to 3-5
max_retries = 2

# snapshot_ts: TiDB snapshot timestamp (optional, for comparing historical data)
#
# 【Important Prerequisite - Must Meet】
# The prerequisite for using src.snapshot_ts and dst.snapshot_ts is that TiCDC sync_point feature is enabled
# Need to enable in TiCDC configuration: enable-sync-point = true
# Without sync_point enabled, accurate sync point TSO pairs cannot be obtained, which may lead to inaccurate comparison results
#
# Use cases:
# 1. Compare data consistency at specific time points during data migration/synchronization
# 2. Compare data states at historical time points
# 3. Verify data consistency during CDC synchronization process
#
# How to get snapshot_ts (Must use CDC sync_point):
# 【Recommended Method】Get through TiCDC sync_point (Prerequisite: sync_point feature is enabled):
# Execute in downstream cluster:
# SELECT * FROM tidb_cdc.syncpoint_v1 ORDER BY created_at DESC LIMIT 1\G
#
# Example result:
# ***************************[ 1. row ]***************************
# ticdc_cluster_id | default
# changefeed       | default/test
# primary_ts       | 462798819164160000    # Source TSO, use for src.snapshot_ts
# secondary_ts     | 462798819559997443    # Downstream TSO, use for dst.snapshot_ts
# created_at       | 2025-12-11 15:16:31
#
# Field explanation:
# - primary_ts: Source (upstream) TSO, corresponds to src.snapshot_ts in config
# - secondary_ts: Downstream cluster TSO, corresponds to dst.snapshot_ts in config
# - These two TSOs represent snapshots at the same logical time point in different clusters, ensuring accuracy of data consistency comparison
#
# Configuration:
# - src.snapshot_ts: Snapshot timestamp for source database (19-digit integer)
#   - Use primary_ts value obtained from CDC sync_point
# - dst.snapshot_ts: Snapshot timestamp for destination database (19-digit integer)
#   - Use secondary_ts value obtained from CDC sync_point
# - Recommend configuring both src and dst to ensure comparing data at the same logical time point
# - If configured, tool will automatically set SET @@tidb_snapshot=? after connection
# - Note: When using snapshot_ts, queries return historical snapshot data, not real-time data
# - Important: Must use TSO pair from CDC sync_point to ensure comparing data at the same logical time point
#
# Example (using values from CDC sync_point):
# src.snapshot_ts = 462798819164160000  # primary_ts (source TSO)
# dst.snapshot_ts = 462798819559997443  # secondary_ts (downstream TSO)
```

### Configuration Options

#### Basic Configuration

- `src.instance` / `dst.instance`: Connection strings for source and destination databases
- `dbs`: Database list to compare, supports LIKE patterns (e.g., `test%`), comma-separated
- `ignore_tables`: Tables to ignore during comparison, comma-separated
- `threshold`: Row count difference threshold (default 0, must be exactly equal)
- `output`: CSV output file path (optional)
- `compare`: Comparison items: `rows` (table row counts), `tables` (database-level table counts), `indexes` (database-level index counts), `views` (database-level view counts). Leave empty to enable all.
- `src.snapshot_ts` / `dst.snapshot_ts`: TiDB snapshot timestamps (optional, for comparing historical data)
  - **【Important Prerequisite - Must Meet】**:
    - The **prerequisite for using `src.snapshot_ts` and `dst.snapshot_ts` is that TiCDC sync_point feature is enabled**
    - Need to enable in TiCDC configuration: `enable-sync-point = true`
    - Without sync_point enabled, accurate sync point TSO pairs cannot be obtained, which may lead to inaccurate comparison results
  - **Use cases**:
    - Compare data consistency at specific time points during data migration/synchronization
    - Compare data states at historical time points
    - Verify data consistency during CDC synchronization process
  - **How to get snapshot_ts (Must use CDC sync_point)**:
    - **【Recommended Method】Get through TiCDC sync_point** (Prerequisite: sync_point feature is enabled):
      Execute in downstream cluster:
      ```sql
      SELECT * FROM tidb_cdc.syncpoint_v1 ORDER BY created_at DESC LIMIT 1\G
      ```
      
      Example result:
      ```
      ***************************[ 1. row ]***************************
      ticdc_cluster_id | default
      changefeed       | default/test
      primary_ts       | 462798819164160000    # Source TSO, use for src.snapshot_ts
      secondary_ts     | 462798819559997443    # Downstream TSO, use for dst.snapshot_ts
      created_at       | 2025-12-11 15:16:31
      ```
      
      Field explanation:
      - `primary_ts`: Source (upstream) TSO, corresponds to `src.snapshot_ts` in config
      - `secondary_ts`: Downstream cluster TSO, corresponds to `dst.snapshot_ts` in config
      - These two TSOs represent snapshots at the same logical time point in different clusters, ensuring accuracy of data consistency comparison
  - **Configuration**:
    - `src.snapshot_ts`: Snapshot timestamp for source database (19-digit integer)
      - Use `primary_ts` value obtained from CDC sync_point
    - `dst.snapshot_ts`: Snapshot timestamp for destination database (19-digit integer)
      - Use `secondary_ts` value obtained from CDC sync_point
    - Recommend configuring both `src` and `dst` to ensure comparing data at the same logical time point
    - If configured, tool will automatically set `SET @@tidb_snapshot=?` after connection
    - **Note**: When using `snapshot_ts`, queries return historical snapshot data, not real-time data
    - **Important**: Must use TSO pair from CDC sync_point to ensure comparing data at the same logical time point
  - **Example (using values from CDC sync_point)**:
    ```ini
    src.snapshot_ts = 462798819164160000  # primary_ts (source TSO)
    dst.snapshot_ts = 462798819559997443  # secondary_ts (downstream TSO)
    ```

#### Concurrency Configuration

- `concurrency`: Database-level concurrency, number of databases processed simultaneously
  - Default: 5 (optimized for multi-database scenarios)
  - Recommended range: 3-20
  - Few databases (<10): 3-5
  - Medium databases (10-50): 5-10
  - Many databases (>50): 10-20

- `use_stats`: Whether to use statistics for fast row count (default `true`)
  - `true`: Use `INFORMATION_SCHEMA.TABLES.TABLE_ROWS`, fast but may be inaccurate
  - `false`: Use exact `COUNT(1)` with table-level concurrency, higher performance

- `table_concurrency`: Table-level concurrency (only effective when `use_stats=false`)
  - Default: 30 (optimized for multi-table scenarios)
  - Recommended range: 10-50
  - Few tables (<100): 10-20
  - Medium tables (100-500): 20-30
  - Many tables (500-1000): 30-40
  - Very many tables (>1000): 40-50
  - Note: Source and destination execute in parallel, actual concurrency = `table_concurrency * 2`

#### Connection Pool Configuration (optimized for multi-DB, multi-table, large table scenarios)

- `max_open_conns`: Maximum open connections
  - Default: 0 (auto-calculated)
  - Auto-calculation formula: `concurrency * 2 * (table_concurrency + 10)`
  - Auto-limited range: 100-500
  - Manual configuration recommendations:
    - Few DBs/tables: 100-150
    - Medium DBs/tables: 150-250
    - Many DBs/tables: 250-400
    - Very many DBs/tables: 400-500
  - Note: Each database needs 2 connection pools (source + destination)

- `max_idle_conns`: Maximum idle connections
  - Default: 0 (auto-calculated as 80% of `max_open_conns`, minimum 80)
  - Keeps connection pool warm, reduces connection establishment overhead

- `conn_max_lifetime_minutes`: Connection max lifetime (minutes)
  - Default: 30 minutes (for large table scenarios)
  - Set to 0 for unlimited (not recommended)

#### Timeout Configuration (optimized for large table queries)

- `query_timeout_seconds`: Query timeout (seconds)
  - Default: 0 (uses default 10 minutes, avoids program hanging due to long queries)
  - **【Important】**Recommend explicitly setting this value based on largest table size, avoid using default
  - Recommendations based on table size:
    - 10M rows: 600-1800 seconds (10-30 minutes)
    - 100M rows: 1800-3600 seconds (30-60 minutes)

- `read_timeout_seconds`: Read timeout (seconds)
  - Default: 0 (uses default)
  - Recommended: `query_timeout_seconds + 60`

- `write_timeout_seconds`: Write timeout (seconds)
  - Default: 0 (uses default)
  - Usually 30-60 seconds

#### Retry Configuration

- `max_retries`: Query retry count
  - Default: 2
  - Range: 0-5
  - For unstable networks, set to 3-5
  - Uses exponential backoff strategy

## Performance Optimizations

The tool implements several performance optimizations:

1. **Multi-Level Concurrency Architecture**
   - **Database-level concurrency**: Process multiple databases simultaneously (controlled by `concurrency`)
   - **Table-level concurrency**: Each table executes `COUNT(1)` queries concurrently (controlled by `table_concurrency`)
   - Source and destination table statistics execute in parallel
   - Expected 10-50x performance improvement for multi-DB, multi-table scenarios

2. **Intelligent Connection Pool Management**
   - **Auto-calculation**: Automatically calculates optimal connection pool size based on concurrency settings
   - **Connection reuse**: Keeps connection pool warm, reduces connection establishment overhead
   - **Connection lifetime management**: Prevents using expired connections, adapts to long-running queries

3. **Query Timeout and Retry Mechanism**
   - **Configurable query timeout**: Set reasonable timeout for large table queries
   - **Automatic retry**: Auto-retry on network instability with exponential backoff
   - **Context timeout control**: Precise query timeout control using Go context

4. **Statistics Fast Mode** (when `use_stats=true`)
   - Uses `INFORMATION_SCHEMA.TABLES.TABLE_ROWS` for fast approximate row counts
   - Source and destination queries executed in parallel
   - Suitable for quick check scenarios

5. **Progress Display and Performance Monitoring**
   - Real-time progress display at database and table levels
   - Detailed performance statistics
   - Error statistics and error rate analysis

### Performance Tuning Recommendations

#### Scenario 1: Few DBs/Tables (<10 DBs, <100 tables/DB)

```ini
concurrency = 3
table_concurrency = 10
max_open_conns = 0  # Auto-calculate
use_stats = false  # Exact count
```

#### Scenario 2: Medium DBs/Tables (10-50 DBs, 100-500 tables/DB)

```ini
concurrency = 5
table_concurrency = 20
max_open_conns = 0  # Auto-calculate (~200)
use_stats = false
query_timeout_seconds = 600  # 10 minutes
```

#### Scenario 3: Many DBs/Tables (>50 DBs, 500-1000 tables/DB)

```ini
concurrency = 10
table_concurrency = 30
max_open_conns = 0  # Auto-calculate (~400)
max_idle_conns = 0  # Auto-calculate
conn_max_lifetime_minutes = 30
use_stats = false
query_timeout_seconds = 1800  # 30 minutes
read_timeout_seconds = 1900
max_retries = 3
```

#### Scenario 4: Very Many DBs/Tables (>50 DBs, >1000 tables/DB, including large tables)

```ini
concurrency = 15
table_concurrency = 40
max_open_conns = 500  # Manual max value
max_idle_conns = 400
conn_max_lifetime_minutes = 60
use_stats = false
query_timeout_seconds = 3600  # 60 minutes (for 100M+ row tables)
read_timeout_seconds = 3700
write_timeout_seconds = 60
max_retries = 3
```

### General Tuning Tips

- **Need exact results**: Set `use_stats = false` to use exact COUNT
- **Quick check**: Set `use_stats = true` to use statistics mode
- **Connection pool**: Recommended to use auto-calculation (set to 0), tool will auto-optimize based on concurrency settings
- **Large table queries**: Set `query_timeout_seconds` based on largest table size to ensure sufficient time
  - **【Important】**Recommend explicitly setting this value, avoid using default (10 minutes), especially for very large tables
  - If tables are very large, recommend setting to 1800-3600 seconds (30-60 minutes)
- **Unstable network**: Increase `max_retries` to 3-5 for better success rate
- **Monitoring and debugging**: Observe connection pool configuration and performance statistics in console output, adjust based on actual situation
- **Program hanging issues**: If program hangs after completing all work, it may be waiting for database connections to close
  - Tool has added connection close timeout mechanism (5 seconds) to handle this automatically
  - If problem persists, check if query timeout is set too long, recommend explicitly setting `query_timeout_seconds`

## Features

1. **Performance**: Go's goroutines provide efficient concurrency with table-level parallel counting
2. **Type Safety**: Strong typing helps catch errors at compile time
3. **Binary Distribution**: Single executable file, no runtime dependencies needed
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

