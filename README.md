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

# 并发线程数（仅 rows 对比使用，默认 1）
concurrency = 1

# 可选 snapshot_ts（TiDB）
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667
```

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
- `tables`：库级表数量对比
- `indexes`：库级索引数量对比（TiDB）
- `views`：库级视图数量对比
- 使用 `compare` 指定需要的子集，逗号分隔；留空默认全选。