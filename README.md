# 数据库表记录数一致性校验工具（扩展版）

支持：
- 逐表行数对比（可并发）
- 库级表数量对比
- 库级索引数量对比（TiDB 的 `INFORMATION_SCHEMA.TIDB_INDEXES`）
- 库级视图数量对比
- 可通过 `compare` 配置选择对比项

## 安装

### Python 版本

```bash
git clone https://github.com/asddongmen/tidb_diff.git
cd tidb_diff

# 可选：创建并激活虚拟环境
python3 -m venv devenv
source devenv/bin/activate   # Windows: devenv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

依赖：`pymysql`

### Go 版本

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

# 数据库级别并发数（同时处理多个数据库，默认 1）
concurrency = 1

# 是否使用统计信息快速获取行数（默认 true，速度快但可能不够精确）
# 设置为 false 时使用精确 COUNT(1)，支持表级别并发，性能更高
use_stats = false

# 表级别并发数（仅在使用精确 COUNT 时有效，每个表独立并发统计，默认 10）
# 建议根据数据库连接数和服务器性能调整，范围 5-50
# 对于大量表的场景，可以适当增加以提高性能
table_concurrency = 10

# 可选 snapshot_ts（TiDB）
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667
```

### 配置项说明

- `src.instance` / `dst.instance`: 源库和目标库的连接串，格式：`mysql://用户名:密码@主机:端口`
- `dbs`: 要对比的数据库列表，支持 LIKE 模式（如 `test%`），多个用逗号分隔
- `ignore_tables`: 忽略校验的表名，多个用逗号分隔
- `threshold`: 行数差异阈值，超过此值会标记为不一致（默认 0，即必须完全一致）
- `output`: CSV 输出文件路径（可选）
- `compare`: 对比项，可选值：`rows`（逐表行数）、`tables`（库级表数）、`indexes`（库级索引数）、`views`（库级视图数），留空默认全部启用
- `concurrency`: 数据库级别并发数，同时处理多个数据库（默认 1，串行处理）
- `use_stats`: 是否使用统计信息快速获取行数（默认 `true`）
  - `true`: 使用 `INFORMATION_SCHEMA.TABLES.TABLE_ROWS`，速度快但可能不够精确
  - `false`: 使用精确 `COUNT(1)`，支持表级别并发，性能更高
- `table_concurrency`: 表级别并发数（仅当 `use_stats=false` 时有效）
  - 每个表独立并发执行 `COUNT(1)` 查询
  - 默认 10，建议根据数据库连接数和服务器性能调整（范围 5-50）
  - 对于大量表的场景，可以适当增加以提高性能
- `src.snapshot_ts` / `dst.snapshot_ts`: TiDB 快照时间戳（可选，用于对比历史数据）

## 使用

### Python 版本

```bash
python diff.py                  # 使用默认 config.ini
# 或指定配置并输出到日志
python diff.py --config config.ini > diff.log 2>&1
```

### Go 版本

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

- 控制台日志：
  - 库级表/索引/视图数量对比结果（按 schema 展示差异）
  - 逐表行数对比的过程信息
- 若设置 `output`，生成 CSV：
  - 列：`数据库, 表名, 源库条数, 目标库条数, 差额(绝对值), 结果`
- 最终在控制台打印逐表行数对比的汇总。如果关闭 `rows` 对比，汇总会提示已跳过逐表行数对比。

## 对比项说明

- `rows`：逐表行数对比（支持并发）
  - 使用统计信息模式（`use_stats=true`）：快速但可能不够精确，适合快速检查
  - 精确 COUNT 模式（`use_stats=false`）：使用表级别并发，每个表独立并发执行 `COUNT(1)`，性能更高且精确
- `tables`：库级表数量对比
- `indexes`：库级索引数量对比（TiDB）
- `views`：库级视图数量对比
- 使用 `compare` 指定需要的子集，逗号分隔；留空默认全选。

## 性能优化说明

### Go 版本性能特性

Go 版本实现了以下性能优化：

1. **表级别并发统计**（`use_stats=false` 时）
   - 每个表独立并发执行 `COUNT(1)` 查询
   - 源库和目标库的表统计并行执行
   - 通过 `table_concurrency` 控制并发数
   - 对于大量表的场景，预计性能提升 5-10 倍

2. **数据库连接池优化**
   - 自动配置连接池参数
   - 提高连接复用效率

3. **统计信息快速模式**（`use_stats=true` 时）
   - 使用 `INFORMATION_SCHEMA.TABLES.TABLE_ROWS` 快速获取近似行数
   - 源库和目标库查询并行执行

### 性能调优建议

- **少量表（< 50）**：使用默认配置即可
- **中等数量表（50-200）**：设置 `table_concurrency = 20-30`
- **大量表（> 200）**：设置 `table_concurrency = 30-50`，并考虑增加数据库连接池大小
- **需要精确结果**：设置 `use_stats = false` 使用精确 COUNT
- **快速检查**：设置 `use_stats = true` 使用统计信息模式