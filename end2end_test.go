package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

func prepareTable(db optimizer.WhatIfOptimizer, schema, createStmt string, nRows int) {
	t, err := utils.ParseCreateTableStmt(schema, createStmt)
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
		prepareTestIndexSelectionAAEnd2End(db, schema, createTableStmts, 3000)
	}

	type aaCase struct {
		queries []string
		param   advisor.Parameter
		result  []string
	}
	cases := []aaCase{
		// single-table cases
		// zero-predicate cases
		{[]string{`select * from t1`}, advisor.Parameter{1, 3},
			[]string{}}, // no index can help
		// TODO: cannot pass this case now since `a` is not considered as an indexable column.
		//{[]string{`select a from t1`}, advisor.Parameter{1, 3},
		//	[]string{"test_aa.t1(a)"}}, // idx(a) can help decrease the scan cost.
		{[]string{`select a from t1 order by a`}, advisor.Parameter{1, 3},
			[]string{"test_aa.t1(a)"}}, // idx(a) can help decrease the scan cost.
		{[]string{`select a from t1 group by a`}, advisor.Parameter{1, 3},
			[]string{"test_aa.t1(a)"}}, // idx(a) can help decrease the scan cost.

		// 	single-predicate cases
		{[]string{`select * from t1 where a=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t1(a)"}},
		{[]string{`select * from t1 where a=1`}, advisor.Parameter{5, 3},
			[]string{"test_aa.t1(a)"}}, // only 1 index should be generated even if it asks for 5.
		{[]string{`select * from t1 where a<50`}, advisor.Parameter{1, 3}, []string{"test_aa.t1(a)"}},
		{[]string{`select * from t1 where a in (1, 2, 3, 4, 5)`}, advisor.Parameter{1, 3}, []string{"test_aa.t1(a)"}},
		{[]string{`select * from t1 where a=1 order by a`}, advisor.Parameter{1, 3}, []string{"test_aa.t1(a)"}},
		{[]string{`select * from t2 where a=1 order by b`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a in (1, 2, 3) order by b`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a < 20 order by b`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},
		// TODO: should be t(b, a)
		{[]string{`select * from t2 where a > 20 order by b`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},

		// multi-predicate cases
		{[]string{`select * from t2 where a=1 and b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a=1 and b=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a=1 and b=1`}, advisor.Parameter{3, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a<1 and b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(b,a)"}},
		{[]string{`select * from t2 where a<1 and b=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t2(b,a)"}},
		{[]string{`select * from t2 where a<1 and b=1`}, advisor.Parameter{1, 1}, []string{"test_aa.t2(b)"}},
		{[]string{`select * from t2 where a=1 or b=1`}, advisor.Parameter{1, 1}, []string{"test_aa.t2(a)"}},
		{[]string{`select * from t2 where a=1 or b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},

		// multi-queries cases
		{[]string{`select * from t1 where a=1`, `select * from t2 where a=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t1(a)"}},
		{[]string{`select * from t1 where a>1`, `select * from t2 where a=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a)"}},
		{[]string{`select * from t1 where a=1`, `select * from t2 where a=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t1(a)", "test_aa.t2(a)"}},
		{[]string{`select * from t3 where a=1`, `select * from t3 where a=2`, `select * from t3 where b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t3(a)"}},
		{[]string{`select * from t3 where a=1`, `select * from t3 where a=2`, `select * from t3 where b=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t3(a)", "test_aa.t3(b)"}},
		{[]string{`select * from t3 where a=1`, `select * from t3 where a=2`, `select * from t3 where b=1 and a=3`}, advisor.Parameter{1, 3}, []string{"test_aa.t3(a,b)"}},
		{[]string{`select * from t3 where a=1`, `select * from t3 where a=2`, `select * from t3 where b=1 and a=3`}, advisor.Parameter{2, 3}, []string{"test_aa.t3(a,b)"}},
		{[]string{`select * from t2 where a=1 and b=1`, `select * from t3 where a=1 and b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(a,b)"}},
		{[]string{`select * from t2 where a=1 and b=1`, `select * from t3 where a=1 and b=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t2(a,b)", "test_aa.t3(a,b)"}},
		{[]string{`select * from t2 where a>1 and b=1`, `select * from t3 where a>1 and b=1`}, advisor.Parameter{1, 3}, []string{"test_aa.t2(b,a)"}},
		{[]string{`select * from t2 where a>1 and b=1`, `select * from t3 where a>1 and b=1`}, advisor.Parameter{2, 3}, []string{"test_aa.t2(b,a)", "test_aa.t3(b,a)"}},
	}

	for i, c := range cases {
		workload := utils.CreateWorkloadFromRawStmt(schema, createTableStmts, c.queries)
		result, err := advisor.IndexAdvise(db, workload, c.param)
		utils.Must(err)

		var resultKeys []string
		for _, r := range result.ToList() {
			resultKeys = append(resultKeys, r.Key())
		}
		sort.Strings(resultKeys)
		sort.Strings(c.result)

		expected := strings.Join(c.result, ",")
		actual := strings.Join(resultKeys, ",")
		if expected != actual {
			t.Errorf("case: %v, expected: %v, actual: %v, query: %v", i, expected, actual, c.queries)
		}
	}
}
