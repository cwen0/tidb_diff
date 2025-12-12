package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

var logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

func info(msg string) {
	logger.Printf("[INFO] %s\n", msg)
}

func errorLog(msg string) {
	logger.Printf("[ERROR] %s\n", msg)
}

type DBDataDiff struct{}

func (d *DBDataDiff) getConnection(instance string) (*sql.DB, error) {
	if instance == "" {
		return nil, fmt.Errorf("数据库连接串不能为空")
	}

	parsed, err := url.Parse(strings.TrimSpace(instance))
	if err != nil {
		return nil, fmt.Errorf("解析连接串失败: %v", err)
	}

	if parsed.Scheme != "mysql" {
		return nil, fmt.Errorf("不支持的数据库类型，当前仅支持 MySQL/TiDB: %s", instance)
	}

	if parsed.User == nil || parsed.User.Username() == "" {
		return nil, fmt.Errorf("数据库连接串缺少用户名: %s", instance)
	}

	host := parsed.Hostname()
	if host == "" {
		host = "localhost"
	}

	port := parsed.Port()
	if port == "" {
		port = "3306"
	}

	password, _ := parsed.User.Password()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=True&loc=Local",
		parsed.User.Username(), password, host, port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	return db, nil
}

func (d *DBDataDiff) getDBList(db *sql.DB, dbPattern string) ([]string, error) {
	// Escape % for SQL LIKE pattern (same as Python: db_pattern.replace("%", "%%"))
	escapedPattern := strings.ReplaceAll(dbPattern, "%", "%%")
	query := "SELECT SCHEMA_NAME AS db_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE ?"
	rows, err := db.Query(query, escapedPattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbList []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		dbList = append(dbList, dbName)
	}
	return dbList, rows.Err()
}

type SchemaObjectCounts struct {
	Tables  map[string]int
	Indexes map[string]int
	Views   map[string]int
}

func (d *DBDataDiff) getSchemaObjectCounts(db *sql.DB) (*SchemaObjectCounts, error) {
	result := &SchemaObjectCounts{
		Tables:  make(map[string]int),
		Indexes: make(map[string]int),
		Views:   make(map[string]int),
	}

	// Count tables
	tableSQL := `
		SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		GROUP BY t.TABLE_SCHEMA
	`
	rows, err := db.Query(tableSQL)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var schema string
		var count int
		if err := rows.Scan(&schema, &count); err != nil {
			rows.Close()
			return nil, err
		}
		result.Tables[schema] = count
	}
	rows.Close()

	// Count indexes (TiDB specific)
	indexSQL := `
		SELECT TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TIDB_INDEXES
		GROUP BY TABLE_SCHEMA
	`
	rows, err = db.Query(indexSQL)
	if err != nil {
		info(fmt.Sprintf("查询 INFORMATION_SCHEMA.TIDB_INDEXES 失败，可能不是 TiDB 集群：%v", err))
	} else {
		for rows.Next() {
			var schema string
			var count int
			if err := rows.Scan(&schema, &count); err != nil {
				rows.Close()
				return nil, err
			}
			result.Indexes[schema] = count
		}
		rows.Close()
	}

	// Count views
	viewSQL := `
		SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'VIEW'
		GROUP BY t.TABLE_SCHEMA
	`
	rows, err = db.Query(viewSQL)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var schema string
		var count int
		if err := rows.Scan(&schema, &count); err != nil {
			rows.Close()
			return nil, err
		}
		result.Views[schema] = count
	}
	rows.Close()

	return result, nil
}

type CompareResult struct {
	Src  int
	Dst  int
	Diff int
	OK   bool
}

func (d *DBDataDiff) compareSchemaCounts(srcCounts, dstCounts *SchemaObjectCounts, threshold int) map[string]map[string]*CompareResult {
	allSchemas := make(map[string]bool)
	for k := range srcCounts.Tables {
		allSchemas[k] = true
	}
	for k := range dstCounts.Tables {
		allSchemas[k] = true
	}
	for k := range srcCounts.Indexes {
		allSchemas[k] = true
	}
	for k := range dstCounts.Indexes {
		allSchemas[k] = true
	}
	for k := range srcCounts.Views {
		allSchemas[k] = true
	}
	for k := range dstCounts.Views {
		allSchemas[k] = true
	}

	result := map[string]map[string]*CompareResult{
		"tables":  make(map[string]*CompareResult),
		"indexes": make(map[string]*CompareResult),
		"views":   make(map[string]*CompareResult),
	}

	compareOneType := func(key string, srcMap, dstMap map[string]int) {
		for schema := range allSchemas {
			srcVal := srcMap[schema]
			dstVal := dstMap[schema]
			diff := int(math.Abs(float64(dstVal - srcVal)))
			result[key][schema] = &CompareResult{
				Src:  srcVal,
				Dst:  dstVal,
				Diff: diff,
				OK:   diff <= threshold,
			}
		}
	}

	compareOneType("tables", srcCounts.Tables, dstCounts.Tables)
	compareOneType("indexes", srcCounts.Indexes, dstCounts.Indexes)
	compareOneType("views", srcCounts.Views, dstCounts.Views)

	return result
}

type CheckResult struct {
	DBName     string
	ErrList    []string
	RowsForCSV [][]string
}

func (d *DBDataDiff) checkSingleDB(db, src, dst string, ignoreTables []string, threshold, batchSize int, srcSnapshotTS, dstSnapshotTS *string) CheckResult {
	errList := []string{}
	rowsForCSV := [][]string{}

	srcDB, err := d.getConnection(src)
	if err != nil {
		errList = append(errList, fmt.Sprintf("连接源库失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}
	defer srcDB.Close()

	dstDB, err := d.getConnection(dst)
	if err != nil {
		errList = append(errList, fmt.Sprintf("连接目标库失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}
	defer dstDB.Close()

	// Set snapshot timestamps if provided
	if srcSnapshotTS != nil && *srcSnapshotTS != "" {
		snapshotVal, err := strconv.ParseInt(*srcSnapshotTS, 10, 64)
		if err != nil {
			errList = append(errList, fmt.Sprintf("无效的 snapshot_ts 值: %s, 错误: %v", *srcSnapshotTS, err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}
		_, err = srcDB.Exec("SET @@tidb_snapshot=?", snapshotVal)
		if err != nil {
			errList = append(errList, fmt.Sprintf("设置源库 snapshot_ts 失败：%v", err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}
	}

	if dstSnapshotTS != nil && *dstSnapshotTS != "" {
		snapshotVal, err := strconv.ParseInt(*dstSnapshotTS, 10, 64)
		if err != nil {
			errList = append(errList, fmt.Sprintf("无效的 snapshot_ts 值: %s, 错误: %v", *dstSnapshotTS, err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}
		_, err = dstDB.Exec("SET @@tidb_snapshot=?", snapshotVal)
		if err != nil {
			errList = append(errList, fmt.Sprintf("设置目标库 snapshot_ts 失败：%v", err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}
	}

	// Get table lists
	srcTables, err := d.getTableList(srcDB, db)
	if err != nil {
		errList = append(errList, fmt.Sprintf("获取源库表列表失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	dstTables, err := d.getTableList(dstDB, db)
	if err != nil {
		errList = append(errList, fmt.Sprintf("获取目标库表列表失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	// Remove ignored tables
	srcTables = d.removeIgnoredTables(srcTables, ignoreTables)
	dstTables = d.removeIgnoredTables(dstTables, ignoreTables)

	if len(srcTables) != len(dstTables) {
		msg := fmt.Sprintf("【%s】源库和目标库表个数不一致，校验异常退出！", db)
		errorLog(msg)
		errList = append(errList, msg)
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	if len(srcTables) == 0 {
		msg := fmt.Sprintf("【%s】源库和目标库都是空的，不做校验退出", db)
		errorLog(msg)
		errList = append(errList, msg)
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	info(fmt.Sprintf("DB【%s】共%d张表，开始数据行数校验...", db, len(srcTables)))

	srcRet := make(map[string]int64)
	dstRet := make(map[string]int64)

	// Process in batches
	for i := 0; i < len(srcTables); i += batchSize {
		end := i + batchSize
		if end > len(srcTables) {
			end = len(srcTables)
		}
		currentSrcTables := srcTables[i:end]
		currentDstTables := dstTables[i:end]

		// Build UNION ALL queries
		srcSQL := d.buildCountQuery(db, currentSrcTables)
		dstSQL := d.buildCountQuery(db, currentDstTables)

		// Execute source query
		rows, err := srcDB.Query(srcSQL)
		if err != nil {
			errList = append(errList, fmt.Sprintf("查询源库失败：%v", err))
			continue
		}
		for rows.Next() {
			var tableName string
			var count int64
			if err := rows.Scan(&count, &tableName); err != nil {
				rows.Close()
				errList = append(errList, fmt.Sprintf("扫描源库结果失败：%v", err))
				continue
			}
			srcRet[tableName] = count
		}
		rows.Close()

		// Execute destination query
		rows, err = dstDB.Query(dstSQL)
		if err != nil {
			errList = append(errList, fmt.Sprintf("查询目标库失败：%v", err))
			continue
		}
		for rows.Next() {
			var tableName string
			var count int64
			if err := rows.Scan(&count, &tableName); err != nil {
				rows.Close()
				errList = append(errList, fmt.Sprintf("扫描目标库结果失败：%v", err))
				continue
			}
			dstRet[tableName] = count
		}
		rows.Close()
	}

	// Compare results
	for tableName, srcCount := range srcRet {
		dstCount, exists := dstRet[tableName]
		if !exists {
			msg := fmt.Sprintf("DB【%s】的源表: %s在目标库中不存在同名的表！该表count数置为-1", db, tableName)
			errorLog(msg)
			errList = append(errList, tableName)
			rowsForCSV = append(rowsForCSV, []string{db, tableName, fmt.Sprintf("%d", srcCount), "-1", "N/A", "目的表不存在"})
		} else {
			diffVal := int64(math.Abs(float64(dstCount - srcCount)))
			if diffVal <= int64(threshold) {
				rowsForCSV = append(rowsForCSV, []string{db, tableName, fmt.Sprintf("%d", srcCount), fmt.Sprintf("%d", dstCount), fmt.Sprintf("%d", diffVal), "一致"})
			} else {
				msg := fmt.Sprintf("DB【%s】的源表:%s(%d)和目标库同名表记录数(%d)相差较大，请检查！！！", db, tableName, srcCount, dstCount)
				errorLog(msg)
				rowsForCSV = append(rowsForCSV, []string{db, tableName, fmt.Sprintf("%d", srcCount), fmt.Sprintf("%d", dstCount), fmt.Sprintf("%d", diffVal), "不一致"})
				errList = append(errList, tableName)
			}
		}
	}

	info(fmt.Sprintf("DB【%s】校验正常结束", db))
	return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
}

func (d *DBDataDiff) getTableList(db *sql.DB, schema string) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	rows, err := db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	sort.Strings(tables)
	return tables, rows.Err()
}

func (d *DBDataDiff) removeIgnoredTables(tables []string, ignoreTables []string) []string {
	ignoreMap := make(map[string]bool)
	for _, t := range ignoreTables {
		ignoreMap[t] = true
	}

	result := []string{}
	for _, t := range tables {
		if !ignoreMap[t] {
			result = append(result, t)
		}
	}
	return result
}

func (d *DBDataDiff) buildCountQuery(db string, tables []string) string {
	queries := []string{}
	for _, table := range tables {
		queries = append(queries, fmt.Sprintf("SELECT COUNT(1) AS cnt, '%s' AS table_name FROM `%s`.`%s`", table, db, table))
	}
	return strings.Join(queries, " UNION ALL ")
}

func (d *DBDataDiff) diff(conf *ini.File) string {
	section := conf.Section("diff")

	threshold := section.Key("threshold").MustInt(0)
	concurrency := section.Key("concurrency").MustInt(1)
	if concurrency < 1 {
		concurrency = 1
	}

	compareStr := section.Key("compare").String()
	compareItems := make(map[string]bool)
	if compareStr == "" {
		compareItems["rows"] = true
		compareItems["tables"] = true
		compareItems["indexes"] = true
		compareItems["views"] = true
	} else {
		items := strings.Split(compareStr, ",")
		for _, item := range items {
			item = strings.TrimSpace(strings.ToLower(item))
			if item != "" {
				compareItems[item] = true
			}
		}
	}

	output := section.Key("output").String()

	src := section.Key("src.instance").String()
	dst := section.Key("dst.instance").String()
	if src == "" || dst == "" {
		errorLog("未指定原实例和目标实例的连接方式，退出")
		return ""
	}

	dbPatterns := section.Key("dbs").Strings(",")
	if len(dbPatterns) == 0 {
		errorLog("未指定对应数据库清单，退出")
		return ""
	}

	ignoreTables := section.Key("ignore_tables").Strings(",")
	if len(ignoreTables) > 0 {
		info(fmt.Sprintf("忽略校验的表: %v", ignoreTables))
	}

	srcSnapshotTS := section.Key("src.snapshot_ts").String()
	if srcSnapshotTS != "" {
		info(fmt.Sprintf("源库将使用 snapshot_ts: %s", srcSnapshotTS))
	}

	dstSnapshotTS := section.Key("dst.snapshot_ts").String()
	if dstSnapshotTS != "" {
		info(fmt.Sprintf("目标库将使用 snapshot_ts: %s", dstSnapshotTS))
	}

	// Get database list
	srcDB, err := d.getConnection(src)
	if err != nil {
		errorLog(fmt.Sprintf("连接源库失败：%v", err))
		return ""
	}
	defer srcDB.Close()

	var dbs []string
	dbSet := make(map[string]bool)
	for _, pattern := range dbPatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		dbList, err := d.getDBList(srcDB, pattern)
		if err != nil {
			errorLog(fmt.Sprintf("获取数据库列表失败：%v", err))
			continue
		}
		for _, db := range dbList {
			if !dbSet[db] {
				dbs = append(dbs, db)
				dbSet[db] = true
			}
		}
	}

	if len(dbs) == 0 {
		errorLog("未找到匹配的数据库")
		return ""
	}

	// Schema object counts comparison
	if compareItems["tables"] || compareItems["indexes"] || compareItems["views"] {
		srcDB2, err := d.getConnection(src)
		if err != nil {
			errorLog(fmt.Sprintf("连接源库失败：%v", err))
		} else {
			defer srcDB2.Close()

			dstDB2, err := d.getConnection(dst)
			if err != nil {
				errorLog(fmt.Sprintf("连接目标库失败：%v", err))
			} else {
				defer dstDB2.Close()

				if srcSnapshotTS != "" {
					snapshotVal, err := strconv.ParseInt(srcSnapshotTS, 10, 64)
					if err != nil {
						errorLog(fmt.Sprintf("无效的 snapshot_ts 值: %s, 错误: %v", srcSnapshotTS, err))
					} else {
						_, err = srcDB2.Exec("SET @@tidb_snapshot=?", snapshotVal)
						if err != nil {
							errorLog(fmt.Sprintf("设置源库 snapshot_ts 失败：%v", err))
						}
					}
				}

				if dstSnapshotTS != "" {
					snapshotVal, err := strconv.ParseInt(dstSnapshotTS, 10, 64)
					if err != nil {
						errorLog(fmt.Sprintf("无效的 snapshot_ts 值: %s, 错误: %v", dstSnapshotTS, err))
					} else {
						_, err = dstDB2.Exec("SET @@tidb_snapshot=?", snapshotVal)
						if err != nil {
							errorLog(fmt.Sprintf("设置目标库 snapshot_ts 失败：%v", err))
						}
					}
				}

				srcCounts, err := d.getSchemaObjectCounts(srcDB2)
				if err != nil {
					errorLog(fmt.Sprintf("统计源库对象数量失败：%v", err))
				} else {
					dstCounts, err := d.getSchemaObjectCounts(dstDB2)
					if err != nil {
						errorLog(fmt.Sprintf("统计目标库对象数量失败：%v", err))
					} else {
						schemaCompare := d.compareSchemaCounts(srcCounts, dstCounts, threshold)

						info("库级对象数量对比结果：")
						types := []string{"tables", "indexes", "views"}
						for _, kind := range types {
							if !compareItems[kind] {
								continue
							}
							info(fmt.Sprintf("== %s ==", kind))
							// Sort schemas for consistent output
							schemas := []string{}
							for schema := range schemaCompare[kind] {
								schemas = append(schemas, schema)
							}
							sort.Strings(schemas)
							for _, schema := range schemas {
								val := schemaCompare[kind][schema]
								status := "一致"
								if !val.OK {
									status = "不一致"
								}
								info(fmt.Sprintf("schema=%s, src=%d, dst=%d, diff=%d -> %s",
									schema, val.Src, val.Dst, val.Diff, status))
							}
						}
					}
				}
			}
		}
	}

	// Row count comparison
	allRows := [][]string{}
	errTls := make(map[string][]string)

	if compareItems["rows"] {
		for _, db := range dbs {
			errTls[db] = []string{}
		}

		if concurrency <= 1 {
			// Sequential processing
			for _, db := range dbs {
				var srcTS, dstTS *string
				if srcSnapshotTS != "" {
					srcTS = &srcSnapshotTS
				}
				if dstSnapshotTS != "" {
					dstTS = &dstSnapshotTS
				}
				result := d.checkSingleDB(db, src, dst, ignoreTables, threshold, 5, srcTS, dstTS)
				errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
				allRows = append(allRows, result.RowsForCSV...)
			}
		} else {
			// Concurrent processing
			info(fmt.Sprintf("使用并发校验，线程数：%d", concurrency))
			var wg sync.WaitGroup
			var mu sync.Mutex
			semaphore := make(chan struct{}, concurrency)

			for _, db := range dbs {
				wg.Add(1)
				go func(dbName string) {
					defer wg.Done()
					semaphore <- struct{}{}        // Acquire
					defer func() { <-semaphore }() // Release

					var srcTS, dstTS *string
					if srcSnapshotTS != "" {
						srcTS = &srcSnapshotTS
					}
					if dstSnapshotTS != "" {
						dstTS = &dstSnapshotTS
					}
					result := d.checkSingleDB(dbName, src, dst, ignoreTables, threshold, 5, srcTS, dstTS)

					mu.Lock()
					errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
					allRows = append(allRows, result.RowsForCSV...)
					mu.Unlock()
				}(db)
			}
			wg.Wait()
		}
	}

	// Write CSV
	if output != "" {
		file, err := os.Create(output)
		if err != nil {
			errorLog(fmt.Sprintf("创建CSV文件失败：%v", err))
		} else {
			defer file.Close()
			writer := csv.NewWriter(file)
			writer.Write([]string{"数据库", "表名", "源库条数", "目标库条数", "差额(绝对值)", "结果"})
			for _, row := range allRows {
				writer.Write(row)
			}
			writer.Flush()
			if err := writer.Error(); err != nil {
				errorLog(fmt.Sprintf("写入CSV文件失败：%v", err))
			} else {
				info(fmt.Sprintf("校验结果已导出到：%s", output))
			}
		}
	}

	// Summary
	resultLines := []string{}
	if compareItems["rows"] {
		for _, db := range dbs {
			if len(errTls[db]) > 0 {
				resultLines = append(resultLines, fmt.Sprintf("DB:【%s】相差较大或目的端不存在的表清单如下：%v", db, errTls[db]))
			} else {
				resultLines = append(resultLines, fmt.Sprintf("DB:【%s】所有表记录数一致，无异常", db))
			}
		}
	} else {
		resultLines = append(resultLines, "已按配置跳过逐表行数对比（rows），仅输出库级对象数量对比日志。")
	}

	return strings.Join(resultLines, "\n")
}

func main() {
	configPath := flag.String("config", "config.ini", "配置文件路径（默认：config.ini）")
	flag.Parse()

	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		errorLog(fmt.Sprintf("配置文件不存在: %s", *configPath))
		os.Exit(1)
	}

	conf, err := ini.Load(*configPath)
	if err != nil {
		errorLog(fmt.Sprintf("读取配置文件失败: %v", err))
		os.Exit(1)
	}

	if !conf.HasSection("diff") {
		errorLog(fmt.Sprintf("配置文件中缺少 [diff] 配置节: %s", *configPath))
		os.Exit(1)
	}

	diffTool := &DBDataDiff{}
	info(fmt.Sprintf("使用配置文件: %s", *configPath))
	info("开始数据库表记录数一致性校验...")
	result := diffTool.diff(conf)
	info("\n" + strings.Repeat("=", 50))
	info("校验汇总结果：")
	info(strings.Repeat("=", 50))
	fmt.Println(result)
}
