import sys
import csv
from math import ceil
import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- 基础配置（按需调整）----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def info(msg):
    logger.info(msg)

def error(msg):
    logger.error(msg)

class DBDataDiff:
    def _get_connection(self, instance):
        import pymysql
        from pymysql.cursors import DictCursor
        from urllib.parse import urlparse

        if not instance:
            raise ValueError("数据库连接串不能为空")

        parsed = urlparse(instance.strip())
        if parsed.scheme.lower() != "mysql":
            raise ValueError(f"不支持的数据库类型，当前仅支持 MySQL/TiDB：{instance}")

        if parsed.username is None:
            raise ValueError(f"数据库连接串缺少用户名：{instance}")

        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        user = parsed.username
        password = parsed.password or ""

        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            cursorclass=DictCursor,
            charset="utf8mb4"
        )
        return conn

    def get_db_list(self, cursor, db_pattern):
        sql = "SELECT SCHEMA_NAME AS db_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE %s"
        cursor.execute(sql, (db_pattern.replace("%", "%%"),))
        db_list = [item["db_name"] for item in cursor.fetchall()]
        cursor.close()
        return db_list

    # 库级对象数量统计（表/索引/视图）
    def _get_schema_object_counts(self, conn):
        result = {"tables": {}, "indexes": {}, "views": {}}
        cur = conn.cursor()

        table_sql = """
            SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
            FROM INFORMATION_SCHEMA.`TABLES` t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY t.TABLE_SCHEMA
        """
        cur.execute(table_sql)
        for row in cur.fetchall():
            result["tables"][row["TABLE_SCHEMA"]] = row["sum"]

        index_sql = """
            SELECT TABLE_SCHEMA, COUNT(*) AS sum
            FROM INFORMATION_SCHEMA.TIDB_INDEXES
            GROUP BY TABLE_SCHEMA
        """
        try:
            cur.execute(index_sql)
            for row in cur.fetchall():
                result["indexes"][row["TABLE_SCHEMA"]] = row["sum"]
        except Exception as e:
            info(f"查询 INFORMATION_SCHEMA.TIDB_INDEXES 失败，可能不是 TiDB 集群：{str(e)}")

        view_sql = """
            SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
            FROM INFORMATION_SCHEMA.`TABLES` t
            WHERE t.TABLE_TYPE = 'VIEW'
            GROUP BY t.TABLE_SCHEMA
        """
        cur.execute(view_sql)
        for row in cur.fetchall():
            result["views"][row["TABLE_SCHEMA"]] = row["sum"]

        cur.close()
        return result

    def _compare_schema_counts(self, src_counts, dst_counts, threshold):
        all_schemas = set(src_counts.get("tables", {}).keys()) | \
                      set(dst_counts.get("tables", {}).keys()) | \
                      set(src_counts.get("indexes", {}).keys()) | \
                      set(dst_counts.get("indexes", {}).keys()) | \
                      set(src_counts.get("views", {}).keys()) | \
                      set(dst_counts.get("views", {}).keys())

        compare_result = {
            "tables": {},
            "indexes": {},
            "views": {}
        }

        def _one_type(key):
            for schema in all_schemas:
                s_val = src_counts.get(key, {}).get(schema, 0)
                d_val = dst_counts.get(key, {}).get(schema, 0)
                diff = abs(d_val - s_val)
                compare_result[key][schema] = {
                    "src": s_val,
                    "dst": d_val,
                    "diff": diff,
                    "ok": diff <= threshold
                }

        _one_type("tables")
        _one_type("indexes")
        _one_type("views")
        return compare_result

    # 单库行数校验（可并发）
    def _check_single_db(self, db, src, dst, ignore_tables, threshold, batch_size,
                         src_snapshot_ts=None, dst_snapshot_ts=None):
        err_list = []
        rows_for_csv = []

        try:
            src_ret, dst_ret = {}, {}
            with self._get_connection(src) as sc, self._get_connection(dst) as dc:
                if src_snapshot_ts:
                    # Validate snapshot_ts is numeric to prevent SQL injection
                    try:
                        snapshot_val = int(src_snapshot_ts) if isinstance(src_snapshot_ts, str) else src_snapshot_ts
                        cur = sc.cursor()
                        cur.execute("SET @@tidb_snapshot=%s", (snapshot_val,))
                        cur.close()
                    except (ValueError, TypeError) as e:
                        error(f"无效的 snapshot_ts 值: {src_snapshot_ts}, 错误: {str(e)}")
                        raise
                if dst_snapshot_ts:
                    # Validate snapshot_ts is numeric to prevent SQL injection
                    try:
                        snapshot_val = int(dst_snapshot_ts) if isinstance(dst_snapshot_ts, str) else dst_snapshot_ts
                        cur = dc.cursor()
                        cur.execute("SET @@tidb_snapshot=%s", (snapshot_val,))
                        cur.close()
                    except (ValueError, TypeError) as e:
                        error(f"无效的 snapshot_ts 值: {dst_snapshot_ts}, 错误: {str(e)}")
                        raise

                sc_tables_cursor = sc.cursor()
                sc_tables_cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                    (db,)
                )
                st = sorted([row["table_name"] for row in sc_tables_cursor.fetchall()])
                sc_tables_cursor.close()

                dc_tables_cursor = dc.cursor()
                dc_tables_cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                    (db,)
                )
                dt = sorted([row["table_name"] for row in dc_tables_cursor.fetchall()])
                dc_tables_cursor.close()

                for t in ignore_tables:
                    if t in st:
                        st.remove(t)
                    if t in dt:
                        dt.remove(t)

                if len(st) != len(dt):
                    msg = f"【{db}】源库和目标库表个数不一致，校验异常退出！"
                    error(msg)
                    err_list.append(msg)
                    return db, err_list, rows_for_csv
                if len(st) == 0:
                    msg = f"【{db}】源库和目标库都是空的，不做校验退出"
                    error(msg)
                    err_list.append(msg)
                    return db, err_list, rows_for_csv

                info("DB【{}】共{}张表，开始数据行数校验...".format(db, len(st)))

                batch = 0
                sc.autocommit(False)
                dc.autocommit(False)
                try:
                    for _ in range(ceil(len(st) / batch_size)):
                        start = batch * batch_size
                        end = (batch + 1) * batch_size if (batch + 1) * batch_size <= len(st) else len(st)
                        current_st = st[start:end]
                        current_dt = dt[start:end]

                        src_sql = " union all ".join([
                            f"select count(1) as cnt, '{t}' as table_name from `{db}`.`{t}`"
                            for t in current_st
                        ])
                        dst_sql = " union all ".join([
                            f"select count(1) as cnt, '{t}' as table_name from `{db}`.`{t}`"
                            for t in current_dt
                        ])

                        sc_cursor = sc.cursor()
                        sc_cursor.execute(src_sql)
                        for cnts in sc_cursor.fetchall():
                            src_ret[cnts["table_name"]] = cnts["cnt"]
                        sc_cursor.close()

                        dc_cursor = dc.cursor()
                        dc_cursor.execute(dst_sql)
                        for cntd in dc_cursor.fetchall():
                            dst_ret[cntd["table_name"]] = cntd["cnt"]
                        dc_cursor.close()

                        batch += 1

                    sc.commit()
                    dc.commit()
                except Exception as e:
                    sc.rollback()
                    dc.rollback()
                    raise

                for r in src_ret.keys():
                    if r not in dst_ret.keys():
                        msg = f"DB【{db}】的源表: {r}在目标库中不存在同名的表！该表count数置为-1"
                        error(msg)
                        err_list.append(r)
                        rows_for_csv.append([db, r, src_ret[r], -1, "N/A", "目的表不存在"])
                    else:
                        diff_val = abs(dst_ret[r] - src_ret[r])
                        if diff_val <= threshold:
                            rows_for_csv.append([db, r, src_ret[r], dst_ret[r], diff_val, "一致"])
                        else:
                            msg = f"DB【{db}】的源表:{r}({src_ret[r]})和目标库同名表记录数({dst_ret[r]})相差较大，请检查！！！"
                            error(msg)
                            rows_for_csv.append([db, r, src_ret[r], dst_ret[r], diff_val, "不一致"])
                            err_list.append(r)

            info(f"DB【{db}】校验正常结束")
        except Exception as e:
            msg = f"DB【{db}】校验失败：{str(e)}"
            error(msg)
            err_list.append(f"校验异常：{str(e)}")

        return db, err_list, rows_for_csv

    def diff(self, conf):
        """
        支持配置化选择对比内容：
          - rows: 表行数逐表对比（默认启用，可并发）
          - tables: 库级表数量对比
          - indexes: 库级索引数量对比
          - views: 库级视图数量对比
        配置项：
          compare = rows,tables,indexes,views
          concurrency = N  （仅 rows 对比使用，默认 1）
        """
        threshold = 0
        err_tls = {}
        dbs, ignore_tables = [], []
        src, dst = '', ''
        wr = None
        action = sys._getframe().f_code.co_name
        batch_size = 5
        concurrency = 1
        compare_items = {"rows", "tables", "indexes", "views"}

        if "threshold" in conf[action].keys():
            threshold = conf[action].getint("threshold")

        if "concurrency" in conf[action].keys():
            try:
                concurrency = max(1, conf[action].getint("concurrency"))
            except ValueError:
                concurrency = 1

        if "compare" in conf[action].keys():
            raw_compare = conf[action].get("compare", "")
            items = {x.strip().lower() for x in raw_compare.split(",") if x.strip()}
            if items:
                compare_items = items

        output = None
        if "output" in conf[action].keys():
            output = conf[action].get("output", "").strip()

        wr = None

        if "src.instance" in conf[action].keys() and "dst.instance" in conf[action].keys():
            src = conf[action].get("src.instance", "").strip()
            dst = conf[action].get("dst.instance", "").strip()
        else:
            error("未指定原实例和目标实例的连接方式，退出")
            return

        if "dbs" in conf[action].keys():
            exprs = conf[action].get("dbs", "").strip().split(",")
            for exp in exprs:
                exp = exp.strip()
                if not exp:
                    continue
                with self._get_connection(src) as c:
                    cursor = c.cursor()
                    db_list = self.get_db_list(cursor, exp)
                    dbs.extend(db_list)
            dbs = list(set(dbs))
        else:
            error("未指定对应数据库清单，退出")
            return

        if "ignore_tables" in conf[action].keys():
            ignore_tables = [t.strip() for t in conf[action].get("ignore_tables", "").strip().split(",") if t.strip()]
            info("忽略校验的表: {}".format(ignore_tables))

        src_snapshot_ts = None
        dst_snapshot_ts = None
        if "src.snapshot_ts" in conf[action].keys():
            src_snapshot_ts = conf[action].get("src.snapshot_ts", "").strip() or None
            if src_snapshot_ts:
                info("源库将使用 snapshot_ts: {}".format(src_snapshot_ts))
        if "dst.snapshot_ts" in conf[action].keys():
            dst_snapshot_ts = conf[action].get("dst.snapshot_ts", "").strip() or None
            if dst_snapshot_ts:
                info("目标库将使用 snapshot_ts: {}".format(dst_snapshot_ts))

        # 库级对象数量对比（表 / 索引 / 视图）
        if {"tables", "indexes", "views"} & compare_items:
            try:
                with self._get_connection(src) as sc, self._get_connection(dst) as dc:
                    if src_snapshot_ts:
                        # Validate snapshot_ts is numeric to prevent SQL injection
                        try:
                            snapshot_val = int(src_snapshot_ts) if isinstance(src_snapshot_ts, str) else src_snapshot_ts
                            cur = sc.cursor()
                            cur.execute("SET @@tidb_snapshot=%s", (snapshot_val,))
                            cur.close()
                        except (ValueError, TypeError) as e:
                            error(f"无效的 snapshot_ts 值: {src_snapshot_ts}, 错误: {str(e)}")
                            raise
                    if dst_snapshot_ts:
                        # Validate snapshot_ts is numeric to prevent SQL injection
                        try:
                            snapshot_val = int(dst_snapshot_ts) if isinstance(dst_snapshot_ts, str) else dst_snapshot_ts
                            cur = dc.cursor()
                            cur.execute("SET @@tidb_snapshot=%s", (snapshot_val,))
                            cur.close()
                        except (ValueError, TypeError) as e:
                            error(f"无效的 snapshot_ts 值: {dst_snapshot_ts}, 错误: {str(e)}")
                            raise

                    src_counts = self._get_schema_object_counts(sc)
                    dst_counts = self._get_schema_object_counts(dc)
                    schema_compare = self._compare_schema_counts(src_counts, dst_counts, threshold)

                    info("库级对象数量对比结果：")
                    for kind in ("tables", "indexes", "views"):
                        if kind not in compare_items:
                            continue
                        info(f"== {kind} ==")
                        for schema, val in sorted(schema_compare[kind].items()):
                            status = "一致" if val["ok"] else "不一致"
                            info(
                                f"schema={schema}, src={val['src']}, dst={val['dst']}, "
                                f"diff={val['diff']} -> {status}"
                            )
            except Exception as e:
                error(f"统计库级对象数量（表/索引/视图）失败：{str(e)}")

        # 逐库表行数对比（可并发）
        all_rows = []
        if "rows" in compare_items:
            for db in dbs:
                err_tls[db] = []
            if concurrency <= 1:
                for db in dbs:
                    db_name, errs, rows = self._check_single_db(
                        db=db,
                        src=src,
                        dst=dst,
                        ignore_tables=ignore_tables,
                        threshold=threshold,
                        batch_size=batch_size,
                        src_snapshot_ts=src_snapshot_ts,
                        dst_snapshot_ts=dst_snapshot_ts,
                    )
                    err_tls[db_name].extend(errs)
                    all_rows.extend(rows)
            else:
                info(f"使用并发校验，线程数：{concurrency}")
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    future_to_db = {
                        executor.submit(
                            self._check_single_db,
                            db=db,
                            src=src,
                            dst=dst,
                            ignore_tables=ignore_tables,
                            threshold=threshold,
                            batch_size=batch_size,
                            src_snapshot_ts=src_snapshot_ts,
                            dst_snapshot_ts=dst_snapshot_ts,
                        ): db
                        for db in dbs
                    }
                    for future in as_completed(future_to_db):
                        db_name, errs, rows = future.result()
                        err_tls[db_name].extend(errs)
                        all_rows.extend(rows)

        # 写 CSV
        if output:
            try:
                with open(output, 'w', newline='', encoding='utf-8') as out:
                    wr = csv.writer(out)
                    wr.writerow(["数据库", "表名", "源库条数", "目标库条数", "差额(绝对值)", "结果"])
                    for row in all_rows:
                        wr.writerow(row)
                info(f"校验结果已导出到：{output}")
            except Exception as e:
                error(f"写入CSV文件失败：{str(e)}")

        # 汇总（仅对 rows 对比做汇总，其余结果已在日志输出）
        result_lines = []
        if "rows" in compare_items:
            for d in dbs:
                if err_tls.get(d):
                    result_lines.append(f"DB:【{d}】相差较大或目的端不存在的表清单如下：{err_tls[d]}")
                else:
                    result_lines.append(f"DB:【{d}】所有表记录数一致，无异常")
        else:
            result_lines.append("已按配置跳过逐表行数对比（rows），仅输出库级对象数量对比日志。")
        return "\n".join(result_lines)

def main():
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="数据库表记录数一致性校验工具（含库级表/索引/视图数量统计，并支持并发与对比项选择）")
    parser.add_argument("--config", help="配置文件路径（默认：config.ini）", default="config.ini")
    args = parser.parse_args()
    
    config_path = args.config
    if not os.path.exists(config_path):
        error(f"配置文件不存在: {config_path}")
        sys.exit(1)
    
    conf = configparser.ConfigParser(interpolation=None)
    conf.read(config_path, encoding="utf-8")
    
    if "diff" not in conf:
        error(f"配置文件中缺少 [diff] 配置节: {config_path}")
        sys.exit(1)
    
    diff_tool = DBDataDiff()
    try:
        info(f"使用配置文件: {config_path}")
        info("开始数据库表记录数一致性校验...")
        result = diff_tool.diff(conf)
        info("\n" + "="*50)
        info("校验汇总结果：")
        info("="*50)
        print(result)
    except Exception as e:
        error(f"校验过程异常终止：{str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()