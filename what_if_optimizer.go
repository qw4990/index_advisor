package main

import (
	"database/sql"
	"fmt"
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

	Explain(query string) (plan Plan, err error)
	ExplainAnalyze(query string) (plan Plan, err error)

	ResetStats()
	Stats() WhatIfOptimizerStats

	SetDebug(flag bool) // print each query if set to true
}

type TiDBWhatIfOptimizer struct {
	db        *sql.DB
	stats     WhatIfOptimizerStats
	debugFlag bool
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

	return &TiDBWhatIfOptimizer{db, WhatIfOptimizerStats{}, false}, nil
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
	if o.debugFlag {
		fmt.Println(sql)
	}
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

func (o *TiDBWhatIfOptimizer) Explain(query string) (plan Plan, err error) {
	result, err := o.query("explain format = 'verbose' " + query)
	if err != nil {
		return Plan{}, err
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
	return Plan{p}, nil
}

func (o *TiDBWhatIfOptimizer) ExplainAnalyze(query string) (plan Plan, err error) {
	result, err := o.query("explain analyze format = 'verbose' " + query)
	must(err)
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
	return Plan{p}, nil
}

func (o *TiDBWhatIfOptimizer) SetDebug(flag bool) {
	o.debugFlag = flag
}

func (o *TiDBWhatIfOptimizer) query(query string) (*sql.Rows, error) {
	if o.debugFlag {
		fmt.Println(query)
	}
	return o.db.Query(query)
}
