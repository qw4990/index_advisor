package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type WhatIfOptimizer interface {
	Execute(sql string) error
	Close() error // release the underlying database connection

	CreateHypoIndex(index Index) error
	DropHypoIndex(index Index) error

	GetPlanCost(query string) (planCost float64, err error)
}

type TiDBWhatIfOptimizer struct {
	db *sql.DB
}

func NewTiDBWhatIfOptimizer(DSN string) (WhatIfOptimizer, error) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &TiDBWhatIfOptimizer{db}, nil
}

func (o *TiDBWhatIfOptimizer) Execute(sql string) error {
	_, err := o.db.Exec(sql)
	return err
}

func (o *TiDBWhatIfOptimizer) Close() error {
	return o.db.Close()
}

func (o *TiDBWhatIfOptimizer) CreateHypoIndex(index Index) error {
	return o.Execute(fmt.Sprintf(`create index %v type hypo on %v.%v (%v)`, index.IndexName, index.SchemaName, index.TableName, index.columnNames()))
}
func (o *TiDBWhatIfOptimizer) DropHypoIndex(index Index) error {
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
	plan, err := o.getPlan(query)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(plan[0][2], 64)
}
