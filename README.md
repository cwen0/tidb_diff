# 数据库表记录数一致性校验工具（扩展版）

支持：
- 逐表行数对比（可并发）
- 库级表数量对比
- 库级索引数量对比（TiDB 的 `INFORMATION_SCHEMA.TIDB_INDEXES`）
- 库级视图数量对比
- 可通过 `compare` 配置选择对比项

## 安装

```bash
git clone https://github.com/asddongmen/tidb_diff.git
cd tidb_diff

# 安装依赖
go mod download
# 或使用 Makefile
make deps

# 编译（可选）
go build -o tidb_diff main.go
# 或使用 Makefile
make build
```

依赖：
- `github.com/go-sql-driver/mysql` - MySQL/TiDB 驱动
- `gopkg.in/ini.v1` - INI 配置文件解析

#### 使用 Makefile 编译 Linux 版本

```bash
# 编译 Linux amd64 和 arm64 版本
make linux

# 仅编译 Linux amd64 版本
make linux-amd64

# 仅编译 Linux arm64 版本
make linux-arm64

# 编译所有平台（Linux, macOS, Windows）
make build-all

# 查看所有可用命令
make help
```

编译后的二进制文件位于 `build/linux/amd64/tidb_diff` 和 `build/linux/arm64/tidb_diff`

### 从 GitHub Releases 下载（推荐）

如果不想自己编译，可以直接从 GitHub Releases 下载预编译的二进制文件：

```bash
# 下载预编译的二进制文件包（请替换为实际的版本号和仓库地址）
wget https://github.com/cwen0/tidb_diff/releases/download/v0.0.3/build.tar.gz

# 解压
tar -zxvf build.tar.gz

# 下载配置文件模板
wget https://raw.githubusercontent.com/cwen0/tidb_diff/refs/heads/main/config.ini

# 编辑配置文件（根据实际情况修改数据库连接信息）
vi config.ini
# 或使用其他编辑器
# nano config.ini

# 运行（根据你的系统架构选择对应的二进制文件）
# Linux amd64
./build/linux/amd64/tidb_diff --config config.ini > diff.log 2>&1

# Linux arm64
# ./build/linux/arm64/tidb_diff --config config.ini > diff.log 2>&1
```

**注意**：
- 请将 `v0.0.3` 替换为最新的版本号
- 请将 `cwen0/tidb_diff` 替换为实际的 GitHub 仓库地址
- 根据你的系统架构（amd64 或 arm64）和操作系统（Linux、macOS、Windows）选择对应的二进制文件
- 首次运行前需要编辑 `config.ini` 配置文件，设置正确的数据库连接信息

## 配置

编辑 `config.ini`（示例）：

```ini
[diff]
src.instance = mysql://root@127.0.0.1:4000
dst.instance = mysql://root@127.0.0.1:63844
dbs = test%
ignore_tables = tmp_log, sys_history
threshold = 0
output = diff_result.csv

# 对比内容：rows(逐表行数), tables(库级表数), indexes(库级索引数), views(库级视图数)
# 留空或不填则默认全部启用
compare = rows,tables,indexes,views

# 数据库级别并发数（同时处理多个数据库）
# 多库场景默认值：5（针对多库场景优化）
# 建议根据数据库数量和服务器性能调整，范围 3-20
# - 少量库（<10）：3-5
# - 中等库（10-50）：5-10
# - 大量库（>50）：10-20
# 注意：每个数据库需要 2 个连接池（源+目标），请确保 max_open_conns 足够
concurrency = 5

# 是否使用统计信息快速获取行数（默认 true，速度快但可能不够精确）
# 设置为 false 时使用精确 COUNT(1)，支持表级别并发，性能更高
use_stats = false

# 表级别并发数（仅在使用精确 COUNT 时有效，每个表独立并发统计）
# 多库多表场景默认值：30（针对大量表优化）
# 建议根据数据库连接数和服务器性能调整，范围 10-50
# - 少量表（<100）：10-20
# - 中等表（100-500）：20-30
# - 大量表（500-1000）：30-40
# - 超大量表（>1000）：40-50
# 注意：每个数据库的源库和目标库会并行执行，实际并发 = table_concurrency * 2
table_concurrency = 30

# 连接池配置（针对多库多表大表场景优化）
# max_open_conns: 最大打开连接数
# 如果未配置（设置为 0），将根据 concurrency 和 table_concurrency 自动计算：
#   公式：concurrency * 2 * (table_concurrency + 10)
#   范围：100-500（自动限制）
# 手动配置建议：
# - 少量库表：100-150
# - 中等库表：150-250
# - 大量库表：250-400
# - 超大量库表：400-500
# 注意：每个数据库需要 2 个连接池（源+目标），请确保连接数足够
max_open_conns = 0

# max_idle_conns: 最大空闲连接数
# 如果未配置（设置为 0），将自动计算为 max_open_conns 的 80%（最小 80）
# 保持连接池热状态，减少连接建立开销
max_idle_conns = 0

# conn_max_lifetime_minutes: 连接最大生存时间（分钟），大表场景默认 30 分钟
# 设置为 0 表示不限制连接生存时间（不推荐，可能导致使用过期连接）
# 对于超大表查询，建议设置为 30-60 分钟，避免连接在长时间查询过程中过期
conn_max_lifetime_minutes = 30

# query_timeout_seconds: 单个查询超时时间（秒），0 表示使用默认值（10分钟）
# 对于超大表 COUNT(1) 查询，可能需要较长时间，建议根据表大小设置
# 例如：千万级表建议 600-1800 秒（10-30分钟），亿级表建议 1800-3600 秒（30-60分钟）
# 设置为 0 时，默认使用 10 分钟超时（避免查询时间过长导致程序卡住）
# 【重要】建议根据最大表的大小显式设置此值，避免使用默认值
query_timeout_seconds = 0

# read_timeout_seconds: 读取超时时间（秒），0 表示使用默认值
# 针对大表查询，建议设置为 query_timeout_seconds + 60，确保有足够时间读取数据
read_timeout_seconds = 0

# write_timeout_seconds: 写入超时时间（秒），0 表示使用默认值
# 通常设置为 30-60 秒即可
write_timeout_seconds = 0

# max_retries: 查询重试次数（针对大表查询失败场景）
# 默认 2 次，范围 0-5
# 对于网络不稳定的环境，可以设置为 3-5
max_retries = 2

# 可选 snapshot_ts（TiDB）
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667
```

### 配置项说明

#### 基础配置

- `src.instance` / `dst.instance`: 源库和目标库的连接串，格式：`mysql://用户名:密码@主机:端口`
- `dbs`: 要对比的数据库列表，支持 LIKE 模式（如 `test%`），多个用逗号分隔
- `ignore_tables`: 忽略校验的表名，多个用逗号分隔
- `threshold`: 行数差异阈值，超过此值会标记为不一致（默认 0，即必须完全一致）
- `output`: CSV 输出文件路径（可选）
- `compare`: 对比项，可选值：`rows`（逐表行数）、`tables`（库级表数）、`indexes`（库级索引数）、`views`（库级视图数），留空默认全部启用
- `src.snapshot_ts` / `dst.snapshot_ts`: TiDB 快照时间戳（可选，用于对比历史数据）
  - **【重要前提条件 - 必须满足】**：
    - 使用 `src.snapshot_ts` 和 `dst.snapshot_ts` 的**前提条件是 TiCDC 开启了 sync_point 功能**
    - 需要在 TiCDC 配置中启用：`enable-sync-point = true`
    - 如果不开启 sync_point，无法获取准确的同步点 TSO 对，可能导致对比结果不准确
  - **使用场景**：
    - 在数据迁移/同步过程中对比特定时间点的数据一致性
    - 对比历史时间点的数据状态
    - 验证 CDC 同步过程中数据的一致性
  - **获取 snapshot_ts 的方法（必须使用 CDC sync_point）**：
    - **【推荐方法】通过 TiCDC sync_point 获取**（前提：已开启 sync_point 功能）：
      在下游集群执行：
      ```sql
      SELECT * FROM tidb_cdc.syncpoint_v1 ORDER BY created_at DESC LIMIT 1\G
      ```
      
      返回结果示例：
      ```
      ***************************[ 1. row ]***************************
      ticdc_cluster_id | default
      changefeed       | default/test
      primary_ts       | 462798819164160000    # 源库的 TSO，用于 src.snapshot_ts
      secondary_ts     | 462798819559997443    # 下游的 TSO，用于 dst.snapshot_ts
      created_at       | 2025-12-11 15:16:31
      ```
      
      字段说明：
      - `primary_ts`: 源库（上游）的 TSO，对应配置中的 `src.snapshot_ts`
      - `secondary_ts`: 下游集群的 TSO，对应配置中的 `dst.snapshot_ts`
      - 这两个 TSO 代表同一逻辑时间点在不同集群的快照，确保数据一致性对比的准确性
  - **配置说明**：
    - `src.snapshot_ts`: 源库使用的快照时间戳（19位整数）
      - 使用 CDC sync_point 获取的 `primary_ts` 值
    - `dst.snapshot_ts`: 目标库使用的快照时间戳（19位整数）
      - 使用 CDC sync_point 获取的 `secondary_ts` 值
    - 建议同时配置 `src` 和 `dst`，确保对比同一逻辑时间点的数据
    - 如果配置了 `snapshot_ts`，工具会在连接后自动设置 `SET @@tidb_snapshot=?`
    - **注意**：使用 `snapshot_ts` 时，查询的是历史快照数据，不是实时数据
    - **重要**：必须使用 CDC sync_point 获取的 TSO 对，才能确保对比的是同一逻辑时间点的数据
  - **示例（使用 CDC sync_point 获取的值）**：
    ```ini
    src.snapshot_ts = 462798819164160000  # primary_ts（源库的 TSO）
    dst.snapshot_ts = 462798819559997443  # secondary_ts（下游的 TSO）
    ```

#### 并发配置

- `concurrency`: 数据库级别并发数，同时处理多个数据库
  - 默认值：5（多库场景优化）
  - 建议范围：3-20
  - 少量库（<10）：3-5
  - 中等库（10-50）：5-10
  - 大量库（>50）：10-20

- `use_stats`: 是否使用统计信息快速获取行数（默认 `true`）
  - `true`: 使用 `INFORMATION_SCHEMA.TABLES.TABLE_ROWS`，速度快但可能不够精确
  - `false`: 使用精确 `COUNT(1)`，支持表级别并发，性能更高

- `table_concurrency`: 表级别并发数（仅当 `use_stats=false` 时有效）
  - 默认值：30（多库多表场景优化）
  - 建议范围：10-50
  - 少量表（<100）：10-20
  - 中等表（100-500）：20-30
  - 大量表（500-1000）：30-40
  - 超大量表（>1000）：40-50
  - 注意：每个数据库的源库和目标库会并行执行，实际并发 = `table_concurrency * 2`

#### 连接池配置（针对多库多表大表场景优化）

- `max_open_conns`: 最大打开连接数
  - 默认值：0（自动计算）
  - 自动计算公式：`concurrency * 2 * (table_concurrency + 10)`
  - 自动限制范围：100-500
  - 手动配置建议：
    - 少量库表：100-150
    - 中等库表：150-250
    - 大量库表：250-400
    - 超大量库表：400-500
  - 注意：每个数据库需要 2 个连接池（源+目标），请确保连接数足够

- `max_idle_conns`: 最大空闲连接数
  - 默认值：0（自动计算为 `max_open_conns` 的 80%，最小 80）
  - 保持连接池热状态，减少连接建立开销

- `conn_max_lifetime_minutes`: 连接最大生存时间（分钟）
  - 默认值：30 分钟（大表场景）
  - 设置为 0 表示不限制（不推荐，可能导致使用过期连接）
  - 对于超大表查询，建议设置为 30-60 分钟

#### 超时配置（针对大表查询优化）

- `query_timeout_seconds`: 单个查询超时时间（秒）
  - 默认值：0（使用默认 10 分钟，避免查询时间过长导致程序卡住）
  - **【重要】**建议根据最大表的大小显式设置此值，避免使用默认值
  - 建议根据表大小设置：
    - 千万级表：600-1800 秒（10-30分钟）
    - 亿级表：1800-3600 秒（30-60分钟）

- `read_timeout_seconds`: 读取超时时间（秒）
  - 默认值：0（使用默认值）
  - 建议设置为 `query_timeout_seconds + 60`

- `write_timeout_seconds`: 写入超时时间（秒）
  - 默认值：0（使用默认值）
  - 通常设置为 30-60 秒即可

#### 重试配置

- `max_retries`: 查询重试次数
  - 默认值：2
  - 范围：0-5
  - 对于网络不稳定的环境，可以设置为 3-5
  - 使用指数退避策略，避免频繁重试

## 使用

```bash
# 直接运行
go run main.go                  # 使用默认 config.ini
go run main.go --config config.ini

# 或使用编译后的二进制文件
./tidb_diff                     # 使用默认 config.ini
./tidb_diff --config config.ini

# 输出到日志
./tidb_diff --config config.ini > diff.log 2>&1
```

## 输出

### 控制台日志

- **连接池配置信息**：显示实际使用的连接池参数（自动计算或手动配置）
- **并发配置信息**：显示数据库级别并发、表级别并发和查询重试次数
- **数据库列表**：显示找到的需要校验的数据库数量
- **库级对象数量对比结果**：按 schema 展示表/索引/视图数量差异
- **进度显示**：
  - 数据库级别：`[进度 X/Y] 开始/完成校验数据库: 数据库名`
  - 表级别：`[数据库名] 表统计进度: X/Y (Z%)`（每 10% 或每 10 张表显示一次）
- **性能统计**：
  - 总耗时、平均每张表耗时
  - 错误统计和错误率
  - 并发效率分析

### CSV 输出

若设置 `output`，生成 CSV 文件：
- 列：`数据库, 表名, 源库条数, 目标库条数, 差额(绝对值), 结果`
- 结果列可能的值：`一致`、`不一致`、`目的表不存在`

### 最终汇总

在控制台打印逐表行数对比的汇总：
- 每个数据库的校验结果
- 如果关闭 `rows` 对比，汇总会提示已跳过逐表行数对比

## 对比项说明

- `rows`：逐表行数对比（支持并发）
  - 使用统计信息模式（`use_stats=true`）：快速但可能不够精确，适合快速检查
  - 精确 COUNT 模式（`use_stats=false`）：使用表级别并发，每个表独立并发执行 `COUNT(1)`，性能更高且精确
- `tables`：库级表数量对比
- `indexes`：库级索引数量对比（TiDB）
- `views`：库级视图数量对比
- 使用 `compare` 指定需要的子集，逗号分隔；留空默认全选。

## 性能优化说明

### 性能特性

本工具实现了以下性能优化：

1. **多级并发架构**
   - **数据库级别并发**：同时处理多个数据库（通过 `concurrency` 控制）
   - **表级别并发**：每个表独立并发执行 `COUNT(1)` 查询（通过 `table_concurrency` 控制）
   - 源库和目标库的表统计并行执行
   - 对于多库多表场景，预计性能提升 10-50 倍

2. **智能连接池管理**
   - **自动计算**：根据并发配置自动计算最优连接池大小
   - **连接复用**：保持连接池热状态，减少连接建立开销
   - **连接生存时间管理**：避免使用过期连接，适应长时间查询

3. **查询超时和重试机制**
   - **可配置查询超时**：针对大表查询设置合理的超时时间
   - **自动重试**：网络不稳定时自动重试，使用指数退避策略
   - **Context 超时控制**：使用 Go context 精确控制查询超时

4. **统计信息快速模式**（`use_stats=true` 时）
   - 使用 `INFORMATION_SCHEMA.TABLES.TABLE_ROWS` 快速获取近似行数
   - 源库和目标库查询并行执行
   - 适合快速检查场景

5. **进度显示和性能监控**
   - 数据库级别和表级别进度实时显示
   - 详细的性能统计信息
   - 错误统计和错误率分析

### 性能调优建议

#### 场景一：少量库表（<10 库，<100 表/库）

```ini
concurrency = 3
table_concurrency = 10
max_open_conns = 0  # 自动计算
use_stats = false  # 精确计数
```

#### 场景二：中等库表（10-50 库，100-500 表/库）

```ini
concurrency = 5
table_concurrency = 20
max_open_conns = 0  # 自动计算（约 200）
use_stats = false
query_timeout_seconds = 600  # 10 分钟
```

#### 场景三：大量库表（>50 库，500-1000 表/库）

```ini
concurrency = 10
table_concurrency = 30
max_open_conns = 0  # 自动计算（约 400）
max_idle_conns = 0  # 自动计算
conn_max_lifetime_minutes = 30
use_stats = false
query_timeout_seconds = 1800  # 30 分钟
read_timeout_seconds = 1900
max_retries = 3
```

#### 场景四：超大量库表（>50 库，>1000 表/库，包含大表）

```ini
concurrency = 15
table_concurrency = 40
max_open_conns = 500  # 手动设置最大值
max_idle_conns = 400
conn_max_lifetime_minutes = 60
use_stats = false
query_timeout_seconds = 3600  # 60 分钟（针对亿级大表）
read_timeout_seconds = 3700
write_timeout_seconds = 60
max_retries = 3
```

### 通用调优建议

- **需要精确结果**：设置 `use_stats = false` 使用精确 COUNT
- **快速检查**：设置 `use_stats = true` 使用统计信息模式
- **连接池配置**：推荐使用自动计算（设置为 0），工具会根据并发配置自动优化
- **大表查询**：根据最大表的大小设置 `query_timeout_seconds`，确保有足够时间完成查询
  - **【重要】**建议显式设置此值，避免使用默认值（10分钟），特别是对于超大表场景
  - 如果表很大，建议设置为 1800-3600 秒（30-60分钟）
- **网络不稳定**：增加 `max_retries` 到 3-5，提高成功率
- **监控和调试**：观察控制台输出的连接池配置和性能统计，根据实际情况调整

### 故障排除

#### 程序卡住问题

如果程序在完成所有工作后（已输出汇总结果）卡住不退出，可能的原因和解决方案：

1. **数据库连接关闭等待**
   - **原因**：`sql.DB.Close()` 会等待所有正在进行的查询完成，如果某些查询卡住，会导致程序无法退出
   - **解决方案**：工具已添加连接关闭超时机制（5秒），会自动处理此问题
   - **预防措施**：
     - 显式设置 `query_timeout_seconds`，避免查询时间过长
     - 检查数据库慢查询日志，确认是否有查询执行时间过长
     - 监控数据库连接数，确认是否连接池耗尽

2. **查询超时设置过长**
   - **原因**：如果 `query_timeout_seconds = 0`（使用默认 10 分钟），对于超大表可能不够
   - **解决方案**：根据最大表的大小显式设置 `query_timeout_seconds`
     ```ini
     # 对于亿级大表，建议设置 30-60 分钟
     query_timeout_seconds = 1800  # 30 分钟
     read_timeout_seconds = 1900  # 比 query_timeout_seconds 多 60 秒
     ```

3. **数据库锁等待**
   - **原因**：如果数据库表被锁，查询会等待锁释放
   - **解决方案**：检查数据库是否有长时间运行的事务或锁
     ```sql
     -- 检查长时间运行的事务
     SELECT * FROM information_schema.innodb_trx WHERE trx_started < DATE_SUB(NOW(), INTERVAL 1 HOUR);
     
     -- 检查锁等待
     SELECT * FROM information_schema.innodb_lock_waits;
     ```

4. **连接池耗尽**
   - **原因**：如果并发数设置过高，可能导致连接池耗尽
   - **解决方案**：检查配置中的 `max_open_conns` 是否足够
     ```ini
     # 根据实际并发需求调整
     max_open_conns = 500  # 手动设置最大值
     ```

如果问题仍然存在，可以：
- 使用 `pprof` 查看 goroutine 堆栈，找出阻塞的 goroutine
- 检查数据库慢查询日志
- 监控连接池使用情况