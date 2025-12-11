import sys
import csv
import json
from math import ceil
import configparser
import logging

# ---------------------- 基础配置（按需调整）----------------------
# 初始化日志（默认输出到控制台，可改为文件）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 自定义日志辅助函数（脚本原有 info/error 函数，适配日志配置）
def info(msg):
    logger.info(msg)

def error(msg):
    logger.error(msg)

# ---------------------- 原脚本类（保留核心逻辑）----------------------
class DBDataDiff:
    def _get_connection(self, instance):
        """
        数据库连接获取方法（需根据实际数据库类型实现）
        示例：支持 MySQL（需安装 pymysql），其他数据库需适配驱动
        """
        import pymysql
        from pymysql.cursors import DictCursor
        from urllib.parse import urlparse

        if not instance:
            raise ValueError("数据库连接串不能为空")

        parsed = urlparse(instance.strip())
        if parsed.scheme.lower() != "mysql":
            raise ValueError(f"不支持的数据库类型，当前仅支持 MySQL：{instance}")

        if parsed.username is None:
            raise ValueError(f"数据库连接串缺少用户名：{instance}")

        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        user = parsed.username
        password = parsed.password or ""

        # 创建连接（返回 DictCursor，方便按字段名取值）
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
        """根据模式获取数据库列表（适配通配符%）"""
        # 查询实例中所有匹配的数据库
        sql = "SELECT SCHEMA_NAME AS db_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE %s"
        cursor.execute(sql, (db_pattern.replace("%", "%%"),))  # 转义通配符%
        db_list = [item["db_name"] for item in cursor.fetchall()]
        cursor.close()
        return db_list

    def diff(self, conf):
        """
        比对源和目的端的数据库表数据记录条数，
        连接参数需要在配置文件中配置，需要包含[diff]配置；
        注意，比对两端的库名和表名必须同名。
        """
        threshold = 0
        err_tls = {}
        dbs, ignore_tables = [], []
        src, dst = '', ''
        wr = None
        action = sys._getframe().f_code.co_name
        batch_size = 5

        # 读取阈值配置
        if "threshold" in conf[action].keys():
            threshold = conf[action].getint("threshold")

        # 读取输出文件配置
        output = None
        if "output" in conf[action].keys():
            output = conf[action].get("output", "").strip()
        
        # 初始化输出文件
        if output:
            out = open(output, 'w', newline='', encoding='utf-8')
            wr = csv.writer(out)
            wr.writerow(["数据库", "表名", "源库条数", "目标库条数", "差额(绝对值)", "结果"])

        # 确定源实例和目标实例（从配置文件读取）
        if "src.instance" in conf[action].keys() and "dst.instance" in conf[action].keys():
            src = conf[action].get("src.instance", "").strip()
            dst = conf[action].get("dst.instance", "").strip()
        else:
            logger.error("未指定原实例和目标实例的连接方式，退出")
            return

        # 获取待校验数据库列表
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
            # 去重（避免通配符匹配重复数据库）
            dbs = list(set(dbs))
        else:
            logger.error("未指定对应数据库清单，退出")
            return

        # 获取忽略的表列表
        if "ignore_tables" in conf[action].keys():
            ignore_tables = [t.strip() for t in conf[action].get("ignore_tables", "").strip().split(",") if t.strip()]
            logger.info("忽略校验的表: {}".format(ignore_tables))
            info("忽略校验的表: {}".format(ignore_tables))

        # 获取 snapshot_ts 配置（可选）
        src_snapshot_ts = None
        dst_snapshot_ts = None
        if "src.snapshot_ts" in conf[action].keys():
            src_snapshot_ts = conf[action].get("src.snapshot_ts", "").strip()
            if src_snapshot_ts:
                logger.info("源库将使用 snapshot_ts: {}".format(src_snapshot_ts))
                info("源库将使用 snapshot_ts: {}".format(src_snapshot_ts))
        if "dst.snapshot_ts" in conf[action].keys():
            dst_snapshot_ts = conf[action].get("dst.snapshot_ts", "").strip()
            if dst_snapshot_ts:
                logger.info("目标库将使用 snapshot_ts: {}".format(dst_snapshot_ts))
                info("目标库将使用 snapshot_ts: {}".format(dst_snapshot_ts))

        # 逐库校验
        for db in dbs:
            err_tls[db] = []
            try:
                src_ret, dst_ret, ret = {}, {}, {}
                with self._get_connection(src) as sc, self._get_connection(dst) as dc:
                    # 设置 snapshot_ts（如果配置了）
                    if src_snapshot_ts:
                        sc_snapshot_cursor = sc.cursor()
                        # TiDB 支持 SET @@tidb_snapshot=TSO 语法，TSO 可以是数字字符串
                        sc_snapshot_cursor.execute(f"SET @@tidb_snapshot={src_snapshot_ts}")
                        sc_snapshot_cursor.close()
                        info(f"源库已设置 snapshot_ts: {src_snapshot_ts}")
                    if dst_snapshot_ts:
                        dc_snapshot_cursor = dc.cursor()
                        # TiDB 支持 SET @@tidb_snapshot=TSO 语法，TSO 可以是数字字符串
                        dc_snapshot_cursor.execute(f"SET @@tidb_snapshot={dst_snapshot_ts}")
                        dc_snapshot_cursor.close()
                        info(f"目标库已设置 snapshot_ts: {dst_snapshot_ts}")
                    
                    # 获取源库和目标库的表清单（按表名排序）
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

                    # 过滤忽略的表
                    for t in ignore_tables:
                        if t in st:
                            st.remove(t)
                        if t in dt:
                            dt.remove(t)

                    # 校验表数量是否一致
                    if len(st) != len(dt):
                        logger.error("【{}】源库和目标库表个数不一致，校验异常退出！".format(db))
                        error("【{}】源库和目标库表个数不一致，校验异常退出！".format(db))
                        return
                    else:
                        if len(st) == 0:
                            logger.error("【{}】源库和目标库都是空的，不做校验退出".format(db))
                            error("【{}】源库和目标库都是空的，不做校验退出".format(db))
                            continue
                        info("DB【{}】共{}张表，开始数据行数校验...".format(db, len(st)))

                    # 批量执行count查询（提高效率）
                    batch = 0
                    sc.autocommit(False)
                    dc.autocommit(False)
                    for _ in range(ceil(len(st) / batch_size)):
                        # 截取当前批次的表
                        start = batch * batch_size
                        end = (batch + 1) * batch_size if (batch + 1) * batch_size <= len(st) else len(st)
                        current_st = st[start:end]
                        current_dt = dt[start:end]

                        # 拼接批量count SQL
                        src_sql = " union all ".join([
                            f"select count(1) as cnt, '{t}' as table_name from `{db}`.`{t}`" 
                            for t in current_st
                        ])
                        dst_sql = " union all ".join([
                            f"select count(1) as cnt, '{t}' as table_name from `{db}`.`{t}`" 
                            for t in current_dt
                        ])

                        # 执行查询并存储结果
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

                    # 处理校验结果
                    for r in src_ret.keys():
                        if r not in dst_ret.keys():
                            error(f"DB【{db}】的源表: {r}在目标库中不存在同名的表！该表count数置为-1")
                            ret[r] = json.dumps({"src": src_ret[r], "dst": -1})
                            err_tls[db].append(r)
                            if wr:
                                wr.writerow([db, r, src_ret[r], -1, "N/A", "目的表不存在"])
                        else:
                            ret[r] = json.dumps({"src": src_ret[r], "dst": dst_ret[r]})
                            diff_val = abs(dst_ret[r] - src_ret[r])
                            if diff_val <= threshold:
                                info(f"DB【{db}】的源表:{r}({src_ret[r]})和目标库同名表记录数({dst_ret[r]})相等或相差在{threshold}条以内，记录一致")
                                if wr:
                                    wr.writerow([db, r, src_ret[r], dst_ret[r], diff_val, "一致"])
                            else:
                                error(f"DB【{db}】的源表:{r}({src_ret[r]})和目标库同名表记录数({dst_ret[r]})相差较大，请检查！！！")
                                if wr:
                                    wr.writerow([db, r, src_ret[r], dst_ret[r], diff_val, "不一致"])
                                err_tls[db].append(r)

                info(f"DB【{db}】校验正常结束")
            except Exception as e:
                error(f"DB【{db}】校验失败：{str(e)}")
                err_tls[db].append(f"校验异常：{str(e)}")
                continue

        # 关闭输出文件
        if output:
            out.close()
            info(f"校验结果已导出到：{output}")

        # 生成最终汇总结果
        result_lines = []
        for d in dbs:
            if err_tls[d]:
                result_lines.append(f"DB:【{d}】相差较大或目的端不存在的表清单如下：{err_tls[d]}")
            else:
                result_lines.append(f"DB:【{d}】所有表记录数一致，无异常")
        return "\n".join(result_lines)

# ---------------------- main 函数（命令行入口）----------------------
def main():
    import argparse
    import os
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="数据库表记录数一致性校验工具")
    parser.add_argument("--config", help="配置文件路径（默认：config.ini）", default="config.ini")
    args = parser.parse_args()
    
    # 读取配置文件
    config_path = args.config
    if not os.path.exists(config_path):
        error(f"配置文件不存在: {config_path}")
        sys.exit(1)
    
    conf = configparser.ConfigParser(interpolation=None)
    conf.read(config_path, encoding="utf-8")
    
    # 检查是否包含 [diff] 配置节
    if "diff" not in conf:
        error(f"配置文件中缺少 [diff] 配置节: {config_path}")
        sys.exit(1)
    
    # 初始化校验实例并执行
    diff_tool = DBDataDiff()
    try:
        info(f"使用配置文件: {config_path}")
        info("开始数据库表记录数一致性校验...")
        result = diff_tool.diff(conf)
        # 输出最终汇总结果
        info("\n" + "="*50)
        info("校验汇总结果：")
        info("="*50)
        print(result)
    except Exception as e:
        error(f"校验过程异常终止：{str(e)}")
        sys.exit(1)

# ---------------------- 运行入口 ----------------------
if __name__ == "__main__":
    main()