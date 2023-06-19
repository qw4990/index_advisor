package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
)

func prepareTable(db optimizer.WhatIfOptimizer, schema, createStmt string, nRows int) {
	t, err := wk.ParseCreateTableStmt(schema, createStmt)
	utils.Must(err)

	utils.Must(db.Execute(createStmt))
	for i := 0; i < nRows; i++ {
		var values []string
		for j := 0; j < len(t.Columns); j++ {
			values = append(values, fmt.Sprintf("%v", i*100+rand.Intn(100)))
		}
		utils.Must(db.Execute(fmt.Sprintf("insert into %v values (%v)", t.TableName, strings.Join(values, ","))))
	}
	db.Execute(`analyze table ` + t.TableName)
}

func prepareTestIndexSelectionAAEnd2End(db optimizer.WhatIfOptimizer, schema string, createStmts []string, rows int) {
	utils.Must(db.Execute(`drop database if exists ` + schema))
	utils.Must(db.Execute(`create database ` + schema))
	utils.Must(db.Execute(`use ` + schema))

	for _, createStmt := range createStmts {
		prepareTable(db, schema, createStmt, rows)
	}
}

func TestIndexSelectionEnd2End(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/"
	db, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
	utils.Must(err)

	prepareData := false
	schema := "test_aa"
	createTableStmts := []string{
		`create table t1 (a int)`,
		`create table t2 (a int, b int)`,
		`create table t3 (a int, b int, c int)`,
	}
	if prepareData {
		prepareTestIndexSelectionAAEnd2End(db, schema, createTableStmts, 100)
	}

	type aaCase struct {
		queries []string
		result  []string
		param   advisor.Parameter
	}
	cases := []aaCase{
		{[]string{`select * from t1 where a=1`}, nil, advisor.Parameter{0, 0}},
	}

	for _, c := range cases {
		workload := wk.CreateWorkloadFromRawStmt(schema, createTableStmts, c.queries)
		result, err := advisor.IndexAdvise(db, workload, c.param)
		utils.Must(err)
		for _, r := range result.ToList() {
			fmt.Println(">> ", r.DDL())
		}
	}
}
