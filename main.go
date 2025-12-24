package main

import (
	"context"
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
	"time"

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

const defaultDBCloseTimeout = 5 * time.Second

// closeDBWithTimeout 关闭 *sql.DB，避免 driver/网络异常导致 Close() 阻塞不退出。
func closeDBWithTimeout(db *sql.DB, label string) {
	if db == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_ = db.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(defaultDBCloseTimeout):
		if label == "" {
			label = "数据库"
		}
		errorLog(fmt.Sprintf("关闭%s连接超时，强制退出", label))
	}
}

type DBDataDiff struct {
	maxOpenConns        int
	maxIdleConns        int
	connMaxLifetime     time.Duration
	queryTimeoutSeconds int
	readTimeoutSeconds  int
	writeTimeoutSeconds int
	maxRetries          int
}

func (d *DBDataDiff) setConnectionPoolConfig(maxOpenConns, maxIdleConns int, connMaxLifetimeMinutes int, queryTimeoutSeconds, readTimeoutSeconds, writeTimeoutSeconds int) {
	d.maxOpenConns = maxOpenConns
	d.maxIdleConns = maxIdleConns
	if connMaxLifetimeMinutes > 0 {
		d.connMaxLifetime = time.Duration(connMaxLifetimeMinutes) * time.Minute
	} else {
		d.connMaxLifetime = 0
	}
	d.queryTimeoutSeconds = queryTimeoutSeconds
	d.readTimeoutSeconds = readTimeoutSeconds
	d.writeTimeoutSeconds = writeTimeoutSeconds
}

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

	dsnParams := []string{
		"charset=utf8mb4",
		"parseTime=True",
		"loc=Local",
	}

	if d.readTimeoutSeconds > 0 {
		dsnParams = append(dsnParams, fmt.Sprintf("readTimeout=%ds", d.readTimeoutSeconds))
	}
	if d.writeTimeoutSeconds > 0 {
		dsnParams = append(dsnParams, fmt.Sprintf("writeTimeout=%ds", d.writeTimeoutSeconds))
	}
	dsnParams = append(dsnParams, "timeout=30s")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?%s",
		parsed.User.Username(), password, host, port, strings.Join(dsnParams, "&"))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	if d.maxOpenConns > 0 {
		db.SetMaxOpenConns(d.maxOpenConns)
	} else {
		db.SetMaxOpenConns(1)
	}

	if d.maxIdleConns > 0 {
		db.SetMaxIdleConns(d.maxIdleConns)
	} else {
		db.SetMaxIdleConns(1)
	}

	if d.connMaxLifetime > 0 {
		db.SetConnMaxLifetime(d.connMaxLifetime)
	}

	return db, nil
}

func (d *DBDataDiff) setSnapshotOnConn(ctx context.Context, conn *sql.Conn, snapshotTS *string) error {
	if snapshotTS != nil && *snapshotTS != "" {
		snapshotVal, parseErr := strconv.ParseInt(*snapshotTS, 10, 64)
		if parseErr != nil {
			return fmt.Errorf("无效的 snapshot_ts 值: %s, 错误: %v", *snapshotTS, parseErr)
		}
		_, setErr := conn.ExecContext(ctx, "SET @@tidb_snapshot=?", snapshotVal)
		if setErr != nil {
			return fmt.Errorf("设置 snapshot_ts 失败: %v", setErr)
		}
	}
	return nil
}

func diffSortedStrings(a, b []string) (onlyA, onlyB []string) {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			i++
			j++
			continue
		}
		if a[i] < b[j] {
			onlyA = append(onlyA, a[i])
			i++
			continue
		}
		onlyB = append(onlyB, b[j])
		j++
	}
	for ; i < len(a); i++ {
		onlyA = append(onlyA, a[i])
	}
	for ; j < len(b); j++ {
		onlyB = append(onlyB, b[j])
	}
	return onlyA, onlyB
}

// parseTables 解析 tables 参数，格式：db1.tb1, db2.tb2
// 返回按数据库分组的表列表 map[db][]table
func parseTables(tablesStr string) (map[string][]string, error) {
	result := make(map[string][]string)
	if tablesStr == "" {
		return result, nil
	}

	items := strings.Split(tablesStr, ",")
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		// 解析 db.table 格式
		parts := strings.Split(item, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("无效的表格式: %s，应为 db.table 格式", item)
		}

		dbName := strings.TrimSpace(parts[0])
		tableName := strings.TrimSpace(parts[1])

		if dbName == "" || tableName == "" {
			return nil, fmt.Errorf("无效的表格式: %s，数据库名和表名不能为空", item)
		}

		if result[dbName] == nil {
			result[dbName] = []string{}
		}
		result[dbName] = append(result[dbName], tableName)
	}

	return result, nil
}

func (d *DBDataDiff) getDBList(db *sql.DB, dbPattern string, snapshotTS *string) ([]string, error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := d.setSnapshotOnConn(ctx, conn, snapshotTS); err != nil {
		return nil, err
	}

	pattern := strings.TrimSpace(dbPattern)
	if pattern == "" {
		return []string{}, nil
	}
	// LIKE pattern: 直接按用户输入传入（例如 test%），不要把 % 替换成 %%（那是 fmt.Sprintf 场景）。
	query := "SELECT SCHEMA_NAME AS db_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME"
	rows, err := conn.QueryContext(ctx, query, pattern)
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

func (d *DBDataDiff) getSchemaObjectCounts(db *sql.DB, snapshotTS *string) (*SchemaObjectCounts, error) {
	result := &SchemaObjectCounts{
		Tables:  make(map[string]int),
		Indexes: make(map[string]int),
		Views:   make(map[string]int),
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := d.setSnapshotOnConn(ctx, conn, snapshotTS); err != nil {
		return nil, err
	}

	tableSQL := `
		SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		GROUP BY t.TABLE_SCHEMA
	`
	rows, err := conn.QueryContext(ctx, tableSQL)
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

	indexSQL := `
		SELECT TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TIDB_INDEXES
		GROUP BY TABLE_SCHEMA
	`
	rows, err = conn.QueryContext(ctx, indexSQL)
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

	viewSQL := `
		SELECT t.TABLE_SCHEMA, COUNT(*) AS sum
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'VIEW'
		GROUP BY t.TABLE_SCHEMA
	`
	rows, err = conn.QueryContext(ctx, viewSQL)
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

func (d *DBDataDiff) checkSingleDB(db string, srcDB, dstDB *sql.DB, ignoreTables []string, threshold int, useStats bool, tableConcurrency int, srcSnapshotTS, dstSnapshotTS *string, specifiedTables []string) CheckResult {
	errList := []string{}
	rowsForCSV := [][]string{}
	var errListMu sync.Mutex

	var srcTables, dstTables []string
	var err error

	// 如果指定了表列表，直接使用指定的表；否则获取数据库的所有表
	if len(specifiedTables) > 0 {
		srcTables = specifiedTables
		dstTables = specifiedTables
	} else {
		srcTables, err = d.getTableList(srcDB, db, srcSnapshotTS)
		if err != nil {
			errList = append(errList, fmt.Sprintf("获取源库表列表失败：%v", err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}

		dstTables, err = d.getTableList(dstDB, db, dstSnapshotTS)
		if err != nil {
			errList = append(errList, fmt.Sprintf("获取目标库表列表失败：%v", err))
			return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
		}
	}

	srcTables = d.removeIgnoredTables(srcTables, ignoreTables)
	dstTables = d.removeIgnoredTables(dstTables, ignoreTables)

	onlySrc, onlyDst := diffSortedStrings(srcTables, dstTables)
	if len(onlySrc) > 0 || len(onlyDst) > 0 {
		msg := fmt.Sprintf("【%s】源库和目标库表清单不一致，校验异常退出！src_only=%v, dst_only=%v", db, onlySrc, onlyDst)
		errorLog(msg)
		errList = append(errList, msg)
		for _, t := range onlySrc {
			errList = append(errList, t)
			rowsForCSV = append(rowsForCSV, []string{db, t, "-1", "-1", "N/A", "目的表不存在"})
		}
		for _, t := range onlyDst {
			errList = append(errList, t)
			rowsForCSV = append(rowsForCSV, []string{db, t, "-1", "-1", "N/A", "源表不存在"})
		}
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	if len(srcTables) == 0 {
		msg := fmt.Sprintf("【%s】源库和目标库都是空的，不做校验退出", db)
		errorLog(msg)
		errList = append(errList, msg)
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}

	method := "精确COUNT"
	if useStats {
		method = "统计信息"
	}
	info(fmt.Sprintf("DB【%s】共%d张表，使用%s方式开始数据行数校验...", db, len(srcTables), method))

	srcRet := make(map[string]int64)
	dstRet := make(map[string]int64)

	if useStats {
		var statsWg sync.WaitGroup
		var srcData map[string]int64
		var dstData map[string]int64
		var srcErr, dstErr error
		statsWg.Add(2)

		go func() {
			defer statsWg.Done()
			srcData, srcErr = d.getTableRowCountsFromStats(srcDB, db, srcTables, srcSnapshotTS)
			if srcErr != nil {
				errListMu.Lock()
				errList = append(errList, fmt.Sprintf("从统计信息获取源库行数失败：%v", srcErr))
				errListMu.Unlock()
			}
		}()

		go func() {
			defer statsWg.Done()
			dstData, dstErr = d.getTableRowCountsFromStats(dstDB, db, dstTables, dstSnapshotTS)
			if dstErr != nil {
				errListMu.Lock()
				errList = append(errList, fmt.Sprintf("从统计信息获取目标库行数失败：%v", dstErr))
				errListMu.Unlock()
			}
		}()

		statsWg.Wait()
		if srcData != nil {
			srcRet = srcData
		}
		if dstData != nil {
			dstRet = dstData
		}
	} else {
		var countWg sync.WaitGroup
		var srcData, dstData map[string]int64
		var srcErrList, dstErrList []error
		countWg.Add(2)

		go func() {
			defer countWg.Done()
			srcData, srcErrList = d.countTableRowsConcurrent(srcDB, db, srcTables, tableConcurrency, srcSnapshotTS)
			for _, err := range srcErrList {
				errListMu.Lock()
				errList = append(errList, err.Error())
				errListMu.Unlock()
			}
		}()

		go func() {
			defer countWg.Done()
			dstData, dstErrList = d.countTableRowsConcurrent(dstDB, db, dstTables, tableConcurrency, dstSnapshotTS)
			for _, err := range dstErrList {
				errListMu.Lock()
				errList = append(errList, err.Error())
				errListMu.Unlock()
			}
		}()

		countWg.Wait()
		if srcData != nil {
			srcRet = srcData
		}
		if dstData != nil {
			dstRet = dstData
		}
	}

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

	for tableName, dstCount := range dstRet {
		if _, exists := srcRet[tableName]; !exists {
			msg := fmt.Sprintf("DB【%s】的目标表: %s在源库中不存在同名的表！该表count数置为-1", db, tableName)
			errorLog(msg)
			errList = append(errList, tableName)
			rowsForCSV = append(rowsForCSV, []string{db, tableName, "-1", fmt.Sprintf("%d", dstCount), "N/A", "源表不存在"})
		}
	}

	info(fmt.Sprintf("DB【%s】校验正常结束", db))
	return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
}

func (d *DBDataDiff) getTableList(db *sql.DB, schema string, snapshotTS *string) ([]string, error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := d.setSnapshotOnConn(ctx, conn, snapshotTS); err != nil {
		return nil, err
	}

	// 只返回 BASE TABLE，避免把 VIEW 也纳入逐表 COUNT 导致报错/结果不准。
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name"
	rows, err := conn.QueryContext(ctx, query, schema)
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

func (d *DBDataDiff) countTableRowsConcurrent(db *sql.DB, dbName string, tables []string, concurrency int, snapshotTS *string) (map[string]int64, []error) {
	result := make(map[string]int64)
	var errList []error
	var mu sync.Mutex

	if len(tables) == 0 {
		return result, errList
	}
	if concurrency < 1 {
		concurrency = 1
	}

	totalTables := len(tables)
	processedTables := 0

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const connAcquireTimeout = 30 * time.Second
			var conn *sql.Conn
			defer func() {
				if conn != nil {
					_ = conn.Close()
				}
			}()

			ensureConn := func() error {
				if conn != nil {
					return nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), connAcquireTimeout)
				defer cancel()
				c, err := db.Conn(ctx)
				if err != nil {
					return err
				}
				if err := d.setSnapshotOnConn(ctx, c, snapshotTS); err != nil {
					_ = c.Close()
					return err
				}
				conn = c
				return nil
			}

			for tblName := range jobs {
				query := fmt.Sprintf("SELECT COUNT(1) AS cnt FROM `%s`.`%s`", dbName, tblName)
				var count int64
				var err error

				for retry := 0; retry <= d.maxRetries; retry++ {
					if retry > 0 {
						waitTime := time.Duration(retry) * time.Second
						time.Sleep(waitTime)
					}

					if connErr := ensureConn(); connErr != nil {
						err = connErr
						if retry == d.maxRetries {
							break
						}
						continue
					}

					var ctx context.Context
					var cancel context.CancelFunc
					if d.queryTimeoutSeconds > 0 {
						ctx, cancel = context.WithTimeout(context.Background(), time.Duration(d.queryTimeoutSeconds)*time.Second)
					} else {
						ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
					}

					err = conn.QueryRowContext(ctx, query).Scan(&count)
					cancel()

					if err == nil {
						break
					}

					// 出错后主动丢弃连接，避免 session 状态/超时导致后续查询受影响
					_ = conn.Close()
					conn = nil
					if retry == d.maxRetries {
						break
					}
				}

				mu.Lock()
				processedTables++
				if err != nil {
					errList = append(errList, fmt.Errorf("表 %s 统计失败: %v", tblName, err))
				} else {
					result[tblName] = count
				}
				shouldLog := false
				if totalTables > 0 {
					progress := processedTables * 100 / totalTables
					prevProgress := (processedTables - 1) * 100 / totalTables
					shouldLog = processedTables%10 == 0 || processedTables == totalTables || progress != prevProgress
				} else {
					shouldLog = processedTables%10 == 0 || processedTables == totalTables
				}
				if shouldLog && totalTables > 0 {
					progress := processedTables * 100 / totalTables
					if progress > 100 {
						progress = 100
					}
					info(fmt.Sprintf("  [%s] 表统计进度: %d/%d (%d%%)", dbName, processedTables, totalTables, progress))
				}
				mu.Unlock()
			}
		}()
	}

	for _, table := range tables {
		jobs <- table
	}
	close(jobs)

	wg.Wait()
	return result, errList
}

func (d *DBDataDiff) getTableRowCountsFromStats(db *sql.DB, schema string, tables []string, snapshotTS *string) (map[string]int64, error) {
	result := make(map[string]int64)

	if len(tables) == 0 {
		return result, nil
	}

	for _, table := range tables {
		result[table] = 0
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := d.setSnapshotOnConn(ctx, conn, snapshotTS); err != nil {
		return nil, err
	}

	// 分批构造 IN 子句，避免表数量过多导致 SQL 太长/占位符超限。
	const maxInClauseItems = 500
	for start := 0; start < len(tables); start += maxInClauseItems {
		end := start + maxInClauseItems
		if end > len(tables) {
			end = len(tables)
		}
		batch := tables[start:end]
		if len(batch) == 0 {
			continue
		}

		placeholders := make([]string, len(batch))
		args := make([]interface{}, 0, len(batch)+1)
		args = append(args, schema)
		for i, table := range batch {
			placeholders[i] = "?"
			args = append(args, table)
		}

		query := fmt.Sprintf(
			"SELECT TABLE_NAME, TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME IN (%s)",
			strings.Join(placeholders, ","),
		)

		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tableName string
			var rowCount sql.NullInt64
			if err := rows.Scan(&tableName, &rowCount); err != nil {
				rows.Close()
				return nil, err
			}
			if rowCount.Valid {
				result[tableName] = rowCount.Int64
			} else {
				result[tableName] = 0
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	return result, nil
}

func (d *DBDataDiff) diff(conf *ini.File) string {
	section := conf.Section("diff")

	threshold := section.Key("threshold").MustInt(0)
	concurrency := section.Key("concurrency").MustInt(5)
	if concurrency < 1 {
		concurrency = 5
	}

	useStats := section.Key("use_stats").MustBool(true)

	tableConcurrency := section.Key("table_concurrency").MustInt(30)
	if tableConcurrency < 1 {
		tableConcurrency = 30
	}

	var maxOpenConns, maxIdleConns int
	if section.HasKey("max_open_conns") {
		maxOpenConns = section.Key("max_open_conns").MustInt(0)
	} else {
		maxOpenConns = concurrency * 2 * (tableConcurrency + 10)
		if maxOpenConns < 1 {
			maxOpenConns = 1
		}
		if maxOpenConns > 500 {
			maxOpenConns = 500
		}
	}

	if section.HasKey("max_idle_conns") {
		maxIdleConns = section.Key("max_idle_conns").MustInt(0)
	} else {
		maxIdleConns = int(float64(maxOpenConns) * 0.8)
		if maxIdleConns < 1 {
			maxIdleConns = 1
		}
	}

	var connMaxLifetimeMinutes int
	if section.HasKey("conn_max_lifetime_minutes") {
		connMaxLifetimeMinutes = section.Key("conn_max_lifetime_minutes").MustInt(0)
	} else {
		connMaxLifetimeMinutes = 30
	}

	queryTimeoutSeconds := section.Key("query_timeout_seconds").MustInt(0)

	readTimeoutSeconds := section.Key("read_timeout_seconds").MustInt(0)
	writeTimeoutSeconds := section.Key("write_timeout_seconds").MustInt(0)

	maxRetries := section.Key("max_retries").MustInt(2)
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetries > 5 {
		maxRetries = 5
	}

	if maxOpenConns < 1 {
		maxOpenConns = concurrency * 2 * (tableConcurrency + 10)
		if maxOpenConns < 1 {
			maxOpenConns = 1
		}
		if maxOpenConns > 500 {
			maxOpenConns = 500
		}
	}
	if maxIdleConns < 1 {
		maxIdleConns = int(float64(maxOpenConns) * 0.8)
		if maxIdleConns < 1 {
			maxIdleConns = 1
		}
	}
	d.setConnectionPoolConfig(maxOpenConns, maxIdleConns, connMaxLifetimeMinutes, queryTimeoutSeconds, readTimeoutSeconds, writeTimeoutSeconds)
	d.maxRetries = maxRetries

	info(fmt.Sprintf("连接池配置：max_open_conns=%d, max_idle_conns=%d, conn_max_lifetime=%d分钟",
		maxOpenConns, maxIdleConns, connMaxLifetimeMinutes))
	info(fmt.Sprintf("并发配置：数据库级别=%d, 表级别=%d, 查询重试次数=%d", concurrency, tableConcurrency, maxRetries))

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
	tablesStr := section.Key("tables").String()

	// 验证 dbs 和 tables 必须有一个为空
	dbPatternsEmpty := len(dbPatterns) == 0 || (len(dbPatterns) == 1 && strings.TrimSpace(dbPatterns[0]) == "")
	tablesEmpty := strings.TrimSpace(tablesStr) == ""

	if dbPatternsEmpty && tablesEmpty {
		errorLog("dbs 和 tables 参数必须指定一个，退出")
		return ""
	}
	if !dbPatternsEmpty && !tablesEmpty {
		errorLog("dbs 和 tables 参数不能同时指定，必须有一个为空，退出")
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

	srcDB, err := d.getConnection(src)
	if err != nil {
		errorLog(fmt.Sprintf("连接源库失败：%v", err))
		return ""
	}
	defer closeDBWithTimeout(srcDB, "源库")

	dstDB, err := d.getConnection(dst)
	if err != nil {
		errorLog(fmt.Sprintf("连接目标库失败：%v", err))
		return ""
	}
	defer closeDBWithTimeout(dstDB, "目标库")

	var dbs []string
	dbTablesMap := make(map[string][]string) // 数据库到表列表的映射
	var srcSnapshotTSPtr *string
	if srcSnapshotTS != "" {
		srcSnapshotTSPtr = &srcSnapshotTS
	}

	// 如果使用 tables 参数
	if !tablesEmpty {
		parsedTables, err := parseTables(tablesStr)
		if err != nil {
			errorLog(fmt.Sprintf("解析 tables 参数失败：%v", err))
			return ""
		}

		if len(parsedTables) == 0 {
			errorLog("tables 参数解析后为空，退出")
			return ""
		}

		// 从 tables 参数中提取数据库列表
		for dbName, tables := range parsedTables {
			dbs = append(dbs, dbName)
			dbTablesMap[dbName] = tables
		}

		info(fmt.Sprintf("使用 tables 参数，找到 %d 个数据库需要校验", len(dbs)))
		for dbName, tables := range dbTablesMap {
			info(fmt.Sprintf("  数据库 %s: %d 张表", dbName, len(tables)))
		}
	} else {
		// 使用 dbs 参数
		dbSet := make(map[string]bool)
		for _, pattern := range dbPatterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			dbList, err := d.getDBList(srcDB, pattern, srcSnapshotTSPtr)
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

		info(fmt.Sprintf("找到 %d 个数据库需要校验", len(dbs)))
	}

	if compareItems["tables"] || compareItems["indexes"] || compareItems["views"] {
		var srcSnapshotTSPtr, dstSnapshotTSPtr *string
		if srcSnapshotTS != "" {
			srcSnapshotTSPtr = &srcSnapshotTS
		}
		if dstSnapshotTS != "" {
			dstSnapshotTSPtr = &dstSnapshotTS
		}

		srcCounts, err := d.getSchemaObjectCounts(srcDB, srcSnapshotTSPtr)
		if err != nil {
			errorLog(fmt.Sprintf("统计源库对象数量失败：%v", err))
		} else {
			dstCounts, err := d.getSchemaObjectCounts(dstDB, dstSnapshotTSPtr)
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

	allRows := [][]string{}
	errTls := make(map[string][]string)

	if compareItems["rows"] {
		for _, db := range dbs {
			errTls[db] = []string{}
		}

		if useStats {
			info("使用统计信息模式（快速但可能不够精确），如需精确计数请设置 use_stats=false")
		} else {
			info(fmt.Sprintf("使用精确 COUNT 模式，表级别并发数：%d", tableConcurrency))
		}

		startTime := time.Now()
		processedDBs := 0
		totalDBs := len(dbs)

		if concurrency <= 1 {
			for _, db := range dbs {
				processedDBs++
				info(fmt.Sprintf("[进度 %d/%d] 开始校验数据库: %s", processedDBs, totalDBs, db))
				var srcTS, dstTS *string
				if srcSnapshotTS != "" {
					srcTS = &srcSnapshotTS
				}
				if dstSnapshotTS != "" {
					dstTS = &dstSnapshotTS
				}
				// 如果指定了表列表，使用指定的表；否则传入 nil 表示使用所有表
				var specifiedTables []string
				if tables, exists := dbTablesMap[db]; exists {
					specifiedTables = tables
				}
				result := d.checkSingleDB(db, srcDB, dstDB, ignoreTables, threshold, useStats, tableConcurrency, srcTS, dstTS, specifiedTables)
				errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
				allRows = append(allRows, result.RowsForCSV...)
				info(fmt.Sprintf("[进度 %d/%d] 完成校验数据库: %s", processedDBs, totalDBs, db))
			}
		} else {
			info(fmt.Sprintf("使用并发校验，数据库级别并发数：%d", concurrency))
			var wg sync.WaitGroup
			var mu sync.Mutex
			semaphore := make(chan struct{}, concurrency)

			for _, db := range dbs {
				// 在 goroutine 外部获取表列表，避免在 goroutine 内加锁
				var specifiedTables []string
				if tables, exists := dbTablesMap[db]; exists {
					specifiedTables = tables
				}

				wg.Add(1)
				go func(dbName string, tables []string) {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					mu.Lock()
					processedDBs++
					currentProgress := processedDBs
					mu.Unlock()

					info(fmt.Sprintf("[进度 %d/%d] 开始校验数据库: %s", currentProgress, totalDBs, dbName))

					var srcTS, dstTS *string
					if srcSnapshotTS != "" {
						srcTS = &srcSnapshotTS
					}
					if dstSnapshotTS != "" {
						dstTS = &dstSnapshotTS
					}
					result := d.checkSingleDB(dbName, srcDB, dstDB, ignoreTables, threshold, useStats, tableConcurrency, srcTS, dstTS, tables)

					mu.Lock()
					errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
					allRows = append(allRows, result.RowsForCSV...)
					info(fmt.Sprintf("[进度 %d/%d] 完成校验数据库: %s", currentProgress, totalDBs, dbName))
					mu.Unlock()
				}(db, specifiedTables)
			}
			wg.Wait()
		}

		elapsed := time.Since(startTime)
		totalTables := len(allRows)
		totalErrors := 0
		for _, errs := range errTls {
			totalErrors += len(errs)
		}
		info(fmt.Sprintf("校验完成！共处理 %d 个数据库，%d 张表，耗时: %v", totalDBs, totalTables, elapsed))
		if totalTables > 0 {
			avgTimePerTable := elapsed / time.Duration(totalTables)
			info(fmt.Sprintf("平均每张表耗时: %v", avgTimePerTable))
			if totalErrors > 0 {
				errorRate := float64(totalErrors) * 100.0 / float64(totalTables)
				info(fmt.Sprintf("错误统计: %d 张表校验失败或异常 (错误率: %.2f%%)", totalErrors, errorRate))
			}
		}
		if concurrency > 1 {
			info(fmt.Sprintf("并发效率: 使用 %d 个并发处理数据库，理论加速比: %.2fx", concurrency, float64(concurrency)))
		}
	}

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
