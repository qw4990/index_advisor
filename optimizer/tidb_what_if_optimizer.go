package optimizer

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/qw4990/index_advisor/utils"
	"github.com/qw4990/index_advisor/workload"
)

// TiDBWhatIfOptimizer is the what-if optimizer implementation fot TiDB.
type TiDBWhatIfOptimizer struct {
	db        *sql.DB
	stats     WhatIfOptimizerStats
	debugFlag bool
}

// NewTiDBWhatIfOptimizer creates a new TiDB what-if optimizer with the specified DSN.
func NewTiDBWhatIfOptimizer(DSN string) (WhatIfOptimizer, error) {
	utils.Debugf("connecting to %v", DSN)
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &TiDBWhatIfOptimizer{db, WhatIfOptimizerStats{}, false}, nil
}

// ResetStats resets the statistics.
func (o *TiDBWhatIfOptimizer) ResetStats() {
	o.stats = WhatIfOptimizerStats{}
}

// Stats returns the statistics.
func (o *TiDBWhatIfOptimizer) Stats() WhatIfOptimizerStats {
	return o.stats
}

func (o *TiDBWhatIfOptimizer) recordStats(startTime time.Time, dur *time.Duration, counter *int) {
	*dur = *dur + time.Since(startTime)
	*counter = *counter + 1
}

func (o *TiDBWhatIfOptimizer) Query(sql string) (*sql.Rows, error) {
	if o.debugFlag {
		fmt.Println(sql)
	}
	return o.db.Query(sql)
}

// Execute executes the specified SQL statement.
func (o *TiDBWhatIfOptimizer) Execute(sql string) error {
	defer o.recordStats(time.Now(), &o.stats.ExecuteTime, &o.stats.ExecuteCount)
	if o.debugFlag {
		fmt.Println(sql)
	}
	_, err := o.db.Exec(sql)
	return err
}

// Close releases the underlying database connection.
func (o *TiDBWhatIfOptimizer) Close() error {
	return o.db.Close()
}

// CreateHypoIndex creates a hypothetical index.
func (o *TiDBWhatIfOptimizer) CreateHypoIndex(index workload.Index) error {
	defer o.recordStats(time.Now(), &o.stats.CreateOrDropHypoIdxTime, &o.stats.CreateOrDropHypoIdxCount)
	createStmt := fmt.Sprintf(`create index %v type hypo on %v.%v (%v)`, index.IndexName, index.SchemaName, index.TableName, strings.Join(index.ColumnNames(), ", "))
	err := o.Execute(createStmt)
	if err != nil {
		utils.Errorf("failed to create hypo index '%v': %v", createStmt, err)
	}
	return err
}

// DropHypoIndex drops a hypothetical index.
func (o *TiDBWhatIfOptimizer) DropHypoIndex(index workload.Index) error {
	defer o.recordStats(time.Now(), &o.stats.CreateOrDropHypoIdxTime, &o.stats.CreateOrDropHypoIdxCount)
	return o.Execute(fmt.Sprintf("drop index %v on %v.%v", index.IndexName, index.SchemaName, index.TableName))
}

// Explain returns the execution plan of the specified query.
func (o *TiDBWhatIfOptimizer) Explain(query string) (plan workload.Plan, err error) {
	result, err := o.query("explain format = 'verbose' " + query)
	if err != nil {
		return workload.Plan{}, err
	}
	defer result.Close()
	var p [][]string
	for result.Next() {
		// | id | estRows | estCost | task | access object | operator info |
		var id, estRows, estCost, task, obj, opInfo string
		if err = result.Scan(&id, &estRows, &estCost, &task, &obj, &opInfo); err != nil {
			return
		}
		p = append(p, []string{id, estRows, estCost, task, obj, opInfo})
	}
	return p, nil
}

// ExplainAnalyze returns the execution plan of the specified query.
func (o *TiDBWhatIfOptimizer) ExplainAnalyze(query string) (plan workload.Plan, err error) {
	result, err := o.query("explain analyze format = 'verbose' " + query)
	utils.Must(err)
	defer result.Close()
	var p [][]string
	for result.Next() {
		// | id | estRows  | estCost | actRows | task | access object | execution info | operator info | memory | disk |
		var id, estRows, estCost, actRows, task, obj, execInfo, opInfo, mem, disk string
		if err = result.Scan(&id, &estRows, &estCost, &actRows, &task, &obj, &execInfo, &opInfo, &mem, &disk); err != nil {
			return
		}
		p = append(p, []string{id, estRows, estCost, actRows, task, obj, execInfo, opInfo, mem, disk})
	}
	return p, nil
}

// SetDebug sets the debug flag.
func (o *TiDBWhatIfOptimizer) SetDebug(flag bool) {
	o.debugFlag = flag
}

func (o *TiDBWhatIfOptimizer) query(query string) (*sql.Rows, error) {
	if o.debugFlag {
		fmt.Println(query)
	}
	return o.db.Query(query)
}
