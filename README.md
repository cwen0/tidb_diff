# 数据库表记录数一致性校验工具

## 环境配置

### 1. 创建 Python 虚拟环境

```bash
python3 -m venv devenv
```

### 2. 激活虚拟环境

**macOS/Linux:**
```bash
source devenv/bin/activate
```

**Windows:**
```bash
devenv\Scripts\activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

## 功能说明

用于比对源数据库和目标数据库中同名表的记录数，支持批量校验多个数据库。

## 使用方法

编辑 `config.ini` 文件，配置所有参数：

```ini
[diff]

# 必填参数，指定源数据库连接串
src.instance = mysql://user:pass@host:port
dst.instance = mysql://user:pass@host:port

# 可选参数，可指定源集群和目标集群的快照 TSO 进行对比
# src.snapshot_ts = 462796050923520000
# dst.snapshot_ts = 462796051305201667

# 必填参数，指定要对比的数据库列表， test% 表示对比 test 开头的所有数据库
dbs = test%,db1,db2

# 可选参数，指定忽略校验的表名，逗号分隔
ignore_tables = tmp_log,sys_history
# 
# 可选参数，指定允许的记录数差异阈值（默认 0）
threshold = 0

# 可选参数，指定校验结果 CSV 文件路径（可选）
output = diff_result.csv
```

然后运行：
```bash
python diff.py
```

或指定配置文件路径：
```bash
python3 diff.py --config config.ini > diff.log 2>&1
```

## 配置说明

- `src.instance`: 源数据库连接串（格式：`mysql://user:pass@host:port`）
- `dst.instance`: 目标数据库连接串（格式同上）
- `dbs`: 数据库列表，支持通配符 `%`（如 `test%`）
- `ignore_tables`: 忽略校验的表名，逗号分隔
- `threshold`: 允许的记录数差异阈值（默认 0）
- `output`: 校验结果 CSV 文件路径（可选）
- `src.snapshot_ts` / `dst.snapshot_ts`: TiDB 快照时间戳（可选）

## 输出结果

校验结果会输出到控制台，如配置了 `output` 参数，会同时导出为 CSV 文件。

