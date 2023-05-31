package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type WhatIfOptimizerStats struct {
	ExecuteCount             int
	ExecuteTime              time.Duration
	CreateOrDropHypoIdxCount int
	CreateOrDropHypoIdxTime  time.Duration
	GetCostCount             int
	GetCostTime              time.Duration
}

func (s WhatIfOptimizerStats) Format() string {
	return fmt.Sprintf(`Execute(count/time): (%v/%v), CreateOrDropHypoIndex: (%v/%v), GetCost: (%v/%v)`,
		s.ExecuteCount, s.ExecuteTime, s.CreateOrDropHypoIdxCount, s.CreateOrDropHypoIdxTime, s.GetCostCount, s.GetCostTime)
}

type WhatIfOptimizer interface {
	Execute(sql string) error
	Close() error // release the underlying database connection

	CreateHypoIndex(index Index) error
	DropHypoIndex(index Index) error

	GetPlanCost(query string) (planCost float64, err error)

	ResetStats()
	Stats() WhatIfOptimizerStats
}

type TiDBWhatIfOptimizer struct {
	db    *sql.DB
	stats WhatIfOptimizerStats
}

func NewTiDBWhatIfOptimizer(DSN string) (WhatIfOptimizer, error) {
	Debugf("connecting to %v", DSN)
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &TiDBWhatIfOptimizer{db, WhatIfOptimizerStats{}}, nil
}

func (o *TiDBWhatIfOptimizer) ResetStats() {
	o.stats = WhatIfOptimizerStats{}
}

func (o *TiDBWhatIfOptimizer) Stats() WhatIfOptimizerStats {
	return o.stats
}

func (o *TiDBWhatIfOptimizer) recordStats(startTime time.Time, dur *time.Duration, counter *int) {
	*dur = *dur + time.Since(startTime)
	*counter = *counter + 1
}

func (o *TiDBWhatIfOptimizer) Execute(sql string) error {
	defer o.recordStats(time.Now(), &o.stats.ExecuteTime, &o.stats.ExecuteCount)
	_, err := o.db.Exec(sql)
	return err
}

func (o *TiDBWhatIfOptimizer) Close() error {
	return o.db.Close()
}

func (o *TiDBWhatIfOptimizer) CreateHypoIndex(index Index) error {
	defer o.recordStats(time.Now(), &o.stats.CreateOrDropHypoIdxTime, &o.stats.CreateOrDropHypoIdxCount)
	createStmt := fmt.Sprintf(`create index %v type hypo on %v.%v (%v)`, index.IndexName, index.SchemaName, index.TableName, strings.Join(index.columnNames(), ", "))
	err := o.Execute(createStmt)
	if err != nil {
		Errorf("failed to create hypo index '%v': %v", createStmt, err)
	}
	return err
}

func (o *TiDBWhatIfOptimizer) DropHypoIndex(index Index) error {
	defer o.recordStats(time.Now(), &o.stats.CreateOrDropHypoIdxTime, &o.stats.CreateOrDropHypoIdxCount)
	return o.Execute(fmt.Sprintf("drop index %v on %v.%v", index.IndexName, index.SchemaName, index.TableName))
}

func (o *TiDBWhatIfOptimizer) getPlan(query string) (plan [][]string, err error) {
	//	mysql> explain format='verbose' select * from t;
	//	+-----------------------+----------+------------+-----------+---------------+--------------------------------+
	//	| id                    | estRows  | estCost    | task      | access object | operator info                  |
	//	+-----------------------+----------+------------+-----------+---------------+--------------------------------+
	//	| TableReader_5         | 10000.00 | 177906.67  | root      |               | data:TableFullScan_4           |
	//	| └─TableFullScan_4     | 10000.00 | 2035000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo |
	//	+-----------------------+----------+------------+-----------+---------------+--------------------------------+
	result, err := o.db.Query("explain format = 'verbose' " + query)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	for result.Next() {
		var id, estRows, estCost, task, obj, opInfo string
		if err = result.Scan(&id, &estRows, &estCost, &task, &obj, &opInfo); err != nil {
			return
		}
		plan = append(plan, []string{id, estRows, estCost, task, obj, opInfo})
	}
	return
}

func (o *TiDBWhatIfOptimizer) GetPlanCost(query string) (planCost float64, err error) {
	defer o.recordStats(time.Now(), &o.stats.GetCostTime, &o.stats.GetCostCount)
	plan, err := o.getPlan(query)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(plan[0][2], 64)
}
