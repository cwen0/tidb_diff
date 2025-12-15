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

type DBDataDiff struct {
	maxOpenConns        int
	maxIdleConns        int
	connMaxLifetime     time.Duration
	queryTimeoutSeconds int
	readTimeoutSeconds  int
	writeTimeoutSeconds int
	maxRetries          int // 查询重试次数
}

// setConnectionPoolConfig 设置连接池配置（从配置文件读取）
func (d *DBDataDiff) setConnectionPoolConfig(maxOpenConns, maxIdleConns int, connMaxLifetimeMinutes int, queryTimeoutSeconds, readTimeoutSeconds, writeTimeoutSeconds int) {
	d.maxOpenConns = maxOpenConns
	d.maxIdleConns = maxIdleConns
	if connMaxLifetimeMinutes > 0 {
		d.connMaxLifetime = time.Duration(connMaxLifetimeMinutes) * time.Minute
	} else {
		d.connMaxLifetime = 0 // 0 表示不限制
	}
	d.queryTimeoutSeconds = queryTimeoutSeconds
	d.readTimeoutSeconds = readTimeoutSeconds
	d.writeTimeoutSeconds = writeTimeoutSeconds
	// maxRetries 在 diff 函数中单独设置
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

	// 构建 DSN，针对大表场景优化参数
	dsnParams := []string{
		"charset=utf8mb4",
		"parseTime=True",
		"loc=Local",
	}

	// 添加超时参数（针对大表查询优化）
	if d.readTimeoutSeconds > 0 {
		dsnParams = append(dsnParams, fmt.Sprintf("readTimeout=%ds", d.readTimeoutSeconds))
	}
	if d.writeTimeoutSeconds > 0 {
		dsnParams = append(dsnParams, fmt.Sprintf("writeTimeout=%ds", d.writeTimeoutSeconds))
	}
	// 对于大表，增加连接超时时间
	dsnParams = append(dsnParams, "timeout=30s")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?%s",
		parsed.User.Username(), password, host, port, strings.Join(dsnParams, "&"))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 应用连接池配置（如果已设置，否则使用针对大表的默认值）
	if d.maxOpenConns > 0 {
		db.SetMaxOpenConns(d.maxOpenConns)
	} else {
		db.SetMaxOpenConns(100) // 大表场景默认值：100
	}

	if d.maxIdleConns > 0 {
		db.SetMaxIdleConns(d.maxIdleConns)
	} else {
		db.SetMaxIdleConns(80) // 大表场景默认值：80
	}

	// 连接最大生存时间：如果配置为 0 表示不限制，否则使用配置值
	// 注意：如果 connMaxLifetime 为 0，表示配置为不限制，不调用 SetConnMaxLifetime
	if d.connMaxLifetime > 0 {
		db.SetConnMaxLifetime(d.connMaxLifetime)
	}
	// 如果 connMaxLifetime 被显式设置为 0（通过配置），则不设置（表示不限制）

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

func (d *DBDataDiff) checkSingleDB(db, src, dst string, ignoreTables []string, threshold int, useStats bool, tableConcurrency int, srcSnapshotTS, dstSnapshotTS *string) CheckResult {
	errList := []string{}
	rowsForCSV := [][]string{}

	srcDB, err := d.getConnection(src)
	if err != nil {
		errList = append(errList, fmt.Sprintf("连接源库失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}
	defer func() {
		// 使用 goroutine 和超时来关闭连接，避免阻塞
		done := make(chan struct{})
		go func() {
			srcDB.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			// 5秒超时后强制退出，避免无限等待
		}
	}()

	dstDB, err := d.getConnection(dst)
	if err != nil {
		errList = append(errList, fmt.Sprintf("连接目标库失败：%v", err))
		return CheckResult{DBName: db, ErrList: errList, RowsForCSV: rowsForCSV}
	}
	defer func() {
		// 使用 goroutine 和超时来关闭连接，避免阻塞
		done := make(chan struct{})
		go func() {
			dstDB.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			// 5秒超时后强制退出，避免无限等待
		}
	}()

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

	method := "精确COUNT"
	if useStats {
		method = "统计信息"
	}
	info(fmt.Sprintf("DB【%s】共%d张表，使用%s方式开始数据行数校验...", db, len(srcTables), method))

	srcRet := make(map[string]int64)
	dstRet := make(map[string]int64)

	if useStats {
		// 使用统计信息快速获取（一次性获取所有表，并行执行）
		var statsWg sync.WaitGroup
		var srcData map[string]int64
		var dstData map[string]int64
		var srcErr, dstErr error
		statsWg.Add(2)

		go func() {
			defer statsWg.Done()
			srcData, srcErr = d.getTableRowCountsFromStats(srcDB, db, srcTables)
			if srcErr != nil {
				errList = append(errList, fmt.Sprintf("从统计信息获取源库行数失败：%v", srcErr))
			}
		}()

		go func() {
			defer statsWg.Done()
			dstData, dstErr = d.getTableRowCountsFromStats(dstDB, db, dstTables)
			if dstErr != nil {
				errList = append(errList, fmt.Sprintf("从统计信息获取目标库行数失败：%v", dstErr))
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
		// 使用精确 COUNT（表级别并发：每个表独立并发统计）
		// 并行执行源库和目标库的表级别统计
		var countWg sync.WaitGroup
		var srcData, dstData map[string]int64
		var srcErrList, dstErrList []error
		countWg.Add(2)

		// 并发统计源库所有表的行数
		go func() {
			defer countWg.Done()
			srcData, srcErrList = d.countTableRowsConcurrent(srcDB, db, srcTables, tableConcurrency)
			for _, err := range srcErrList {
				errList = append(errList, err.Error())
			}
		}()

		// 并发统计目标库所有表的行数
		go func() {
			defer countWg.Done()
			dstData, dstErrList = d.countTableRowsConcurrent(dstDB, db, dstTables, tableConcurrency)
			for _, err := range dstErrList {
				errList = append(errList, err.Error())
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

// countTableRowsConcurrent 表级别并发统计：对每个表并发执行 COUNT(1)
func (d *DBDataDiff) countTableRowsConcurrent(db *sql.DB, dbName string, tables []string, concurrency int) (map[string]int64, []error) {
	result := make(map[string]int64)
	var errList []error
	var mu sync.Mutex

	if len(tables) == 0 {
		return result, errList
	}

	totalTables := len(tables)
	processedTables := 0

	// 使用 channel 控制并发数
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, table := range tables {
		wg.Add(1)
		go func(tblName string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			// 对单个表执行 COUNT(1)，使用 context 控制超时（针对大表）
			query := fmt.Sprintf("SELECT COUNT(1) AS cnt FROM `%s`.`%s`", dbName, tblName)
			var count int64
			var err error

			// 重试机制（针对大表查询失败场景）
			for retry := 0; retry <= d.maxRetries; retry++ {
				if retry > 0 {
					// 重试前等待，指数退避
					waitTime := time.Duration(retry) * time.Second
					time.Sleep(waitTime)
				}

				// 如果配置了查询超时，使用 context 控制
				var ctx context.Context
				var cancel context.CancelFunc
				if d.queryTimeoutSeconds > 0 {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(d.queryTimeoutSeconds)*time.Second)
				} else {
					// 默认超时：大表可能需要较长时间，但设置一个合理的上限（10分钟）
					// 避免查询时间过长导致程序卡住
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
				}
				defer cancel() // 确保 context 被取消，释放资源

				err = db.QueryRowContext(ctx, query).Scan(&count)

				if err == nil {
					break // 成功，退出重试循环
				}

				// 如果是最后一次重试，记录错误
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
			// 每处理 10% 的表或每 10 张表输出一次进度（针对大量表场景）
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
		}(table)
	}

	wg.Wait()
	return result, errList
}

// getTableRowCountsFromStats 使用统计信息快速获取表行数（近似值，但非常快）
func (d *DBDataDiff) getTableRowCountsFromStats(db *sql.DB, schema string, tables []string) (map[string]int64, error) {
	result := make(map[string]int64)

	if len(tables) == 0 {
		return result, nil
	}

	// 初始化所有表为 0，确保即使统计信息中没有也能返回
	for _, table := range tables {
		result[table] = 0
	}

	// 构建 IN 子句的占位符
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables))
	for i, table := range tables {
		placeholders[i] = "?"
		args[i] = table
	}

	query := fmt.Sprintf(`
		SELECT TABLE_NAME, TABLE_ROWS 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (%s)
	`, strings.Join(placeholders, ","))

	args = append([]interface{}{schema}, args...)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var rowCount sql.NullInt64
		if err := rows.Scan(&tableName, &rowCount); err != nil {
			return nil, err
		}
		if rowCount.Valid {
			result[tableName] = rowCount.Int64
		} else {
			result[tableName] = 0
		}
	}

	return result, rows.Err()
}

func (d *DBDataDiff) diff(conf *ini.File) string {
	section := conf.Section("diff")

	threshold := section.Key("threshold").MustInt(0)
	// 数据库级别并发数（同时处理多个数据库）
	// 多库场景默认值：5（针对多库场景优化，可根据实际情况调整）
	concurrency := section.Key("concurrency").MustInt(5)
	if concurrency < 1 {
		concurrency = 5
	}

	// 是否使用统计信息快速获取行数（默认 true，速度快但可能不够精确）
	useStats := section.Key("use_stats").MustBool(true)

	// 批量大小（仅在使用精确 COUNT 时有效，默认 30 以提高性能）
	batchSize := section.Key("batch_size").MustInt(30)
	if batchSize < 1 {
		batchSize = 30
	}

	// 表级别并发数（每个表独立并发统计时的并发数）
	// 多库多表场景默认值：30（针对大量表优化）
	tableConcurrency := section.Key("table_concurrency").MustInt(30)
	if tableConcurrency < 1 {
		tableConcurrency = 30
	}

	// 连接池配置（针对多库多表大表场景优化）
	// 动态计算：每个数据库需要 2 个连接池（源+目标），每个连接池需要支持 table_concurrency 并发
	// 公式：max_open_conns = concurrency * 2 * (table_concurrency + 缓冲)
	// 默认值：如果未配置，根据并发数自动计算
	var maxOpenConns, maxIdleConns int
	if section.HasKey("max_open_conns") {
		maxOpenConns = section.Key("max_open_conns").MustInt(0)
	} else {
		// 自动计算：concurrency * 2（源+目标） * (table_concurrency + 10缓冲)
		maxOpenConns = concurrency * 2 * (tableConcurrency + 10)
		if maxOpenConns < 100 {
			maxOpenConns = 100 // 最小 100
		}
		if maxOpenConns > 500 {
			maxOpenConns = 500 // 最大 500，避免过多连接
		}
	}

	if section.HasKey("max_idle_conns") {
		maxIdleConns = section.Key("max_idle_conns").MustInt(0)
	} else {
		// 自动计算：max_open_conns 的 80%
		maxIdleConns = int(float64(maxOpenConns) * 0.8)
		if maxIdleConns < 80 {
			maxIdleConns = 80 // 最小 80
		}
	}

	// 处理 conn_max_lifetime_minutes：如果未配置则使用默认值 30（大表查询可能需要更长时间），如果配置为 0 则表示不限制
	var connMaxLifetimeMinutes int
	if section.HasKey("conn_max_lifetime_minutes") {
		connMaxLifetimeMinutes = section.Key("conn_max_lifetime_minutes").MustInt(0)
	} else {
		connMaxLifetimeMinutes = 30 // 大表场景默认 30 分钟
	}

	// 查询超时配置（秒），0 表示使用默认值（30分钟）
	queryTimeoutSeconds := section.Key("query_timeout_seconds").MustInt(0)

	// 读写超时配置（秒），0 表示使用默认值
	readTimeoutSeconds := section.Key("read_timeout_seconds").MustInt(0)
	writeTimeoutSeconds := section.Key("write_timeout_seconds").MustInt(0)

	// 查询重试次数（针对大表查询失败场景）
	maxRetries := section.Key("max_retries").MustInt(2)
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetries > 5 {
		maxRetries = 5 // 最多重试 5 次
	}

	if maxOpenConns < 1 {
		// 如果配置为 0 或负数，使用自动计算值
		maxOpenConns = concurrency * 2 * (tableConcurrency + 10)
		if maxOpenConns < 100 {
			maxOpenConns = 100
		}
		if maxOpenConns > 500 {
			maxOpenConns = 500
		}
	}
	if maxIdleConns < 1 {
		maxIdleConns = int(float64(maxOpenConns) * 0.8)
		if maxIdleConns < 80 {
			maxIdleConns = 80
		}
	}
	// 设置连接池配置
	d.setConnectionPoolConfig(maxOpenConns, maxIdleConns, connMaxLifetimeMinutes, queryTimeoutSeconds, readTimeoutSeconds, writeTimeoutSeconds)
	d.maxRetries = maxRetries

	// 输出连接池配置信息（帮助用户了解实际配置）
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
	defer func() {
		// 使用 goroutine 和超时来关闭连接，避免阻塞
		done := make(chan struct{})
		go func() {
			srcDB.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			// 5秒超时后强制退出，避免无限等待
		}
	}()

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

	info(fmt.Sprintf("找到 %d 个数据库需要校验", len(dbs)))

	// Schema object counts comparison
	if compareItems["tables"] || compareItems["indexes"] || compareItems["views"] {
		srcDB2, err := d.getConnection(src)
		if err != nil {
			errorLog(fmt.Sprintf("连接源库失败：%v", err))
		} else {
			defer func() {
				// 使用 goroutine 和超时来关闭连接，避免阻塞
				done := make(chan struct{})
				go func() {
					srcDB2.Close()
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					// 5秒超时后强制退出，避免无限等待
				}
			}()

			dstDB2, err := d.getConnection(dst)
			if err != nil {
				errorLog(fmt.Sprintf("连接目标库失败：%v", err))
			} else {
				defer func() {
					// 使用 goroutine 和超时来关闭连接，避免阻塞
					done := make(chan struct{})
					go func() {
						dstDB2.Close()
						close(done)
					}()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						// 5秒超时后强制退出，避免无限等待
					}
				}()

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

		if useStats {
			info("使用统计信息模式（快速但可能不够精确），如需精确计数请设置 use_stats=false")
		} else {
			info(fmt.Sprintf("使用精确 COUNT 模式，表级别并发数：%d", tableConcurrency))
		}

		startTime := time.Now()
		processedDBs := 0
		totalDBs := len(dbs)

		if concurrency <= 1 {
			// Sequential processing
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
				result := d.checkSingleDB(db, src, dst, ignoreTables, threshold, useStats, tableConcurrency, srcTS, dstTS)
				errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
				allRows = append(allRows, result.RowsForCSV...)
				info(fmt.Sprintf("[进度 %d/%d] 完成校验数据库: %s", processedDBs, totalDBs, db))
			}
		} else {
			// Concurrent processing
			info(fmt.Sprintf("使用并发校验，数据库级别并发数：%d", concurrency))
			var wg sync.WaitGroup
			var mu sync.Mutex
			semaphore := make(chan struct{}, concurrency)

			for _, db := range dbs {
				wg.Add(1)
				go func(dbName string) {
					defer wg.Done()
					semaphore <- struct{}{}        // Acquire
					defer func() { <-semaphore }() // Release

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
					result := d.checkSingleDB(dbName, src, dst, ignoreTables, threshold, useStats, tableConcurrency, srcTS, dstTS)

					mu.Lock()
					errTls[result.DBName] = append(errTls[result.DBName], result.ErrList...)
					allRows = append(allRows, result.RowsForCSV...)
					info(fmt.Sprintf("[进度 %d/%d] 完成校验数据库: %s", currentProgress, totalDBs, dbName))
					mu.Unlock()
				}(db)
			}
			wg.Wait()
		}

		// 输出性能统计
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
