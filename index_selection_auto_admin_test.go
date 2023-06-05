package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func prepareTestWorkload(dsn, schemaName string, createTableStmts, rawSQLs []string) (WorkloadInfo, WhatIfOptimizer) {
	w := NewWorkloadFromStmt(schemaName, createTableStmts, rawSQLs)
	must(IndexableColumnsSelectionSimple(&w))
	if dsn == "" {
		dsn = "root:@tcp(127.0.0.1:4000)/"
	}
	opt, err := NewTiDBWhatIfOptimizer("root:@tcp(127.0.0.1:4000)/")
	must(err)
	opt.SetDebug(true)

	for _, schemaName := range w.AllSchemaNames() {
		must(opt.Execute("drop database if exists " + schemaName))
		must(opt.Execute("create database " + schemaName))
	}
	for _, t := range w.TableSchemas.ToList() {
		must(opt.Execute("use " + t.SchemaName))
		must(opt.Execute(t.CreateStmtText))
	}
	return w, opt
}

type indexSelectionCase struct {
	numIndexes       int
	schemaName       string
	createTableStmts []string
	rawSQLs          []string
	expectedIndexes  []Index
}

func workloadCostForTest(opt WhatIfOptimizer, w WorkloadInfo, idxs []Index) float64 {
	for _, idx := range idxs {
		must(opt.CreateHypoIndex(idx))
	}
	cost, err := workloadQueryCost(w, opt)
	must(err)
	for _, idx := range idxs {
		must(opt.DropHypoIndex(idx))
	}
	return cost
}

func testIndexSelection(dsn string, cases []indexSelectionCase) {
	for i, c := range cases {
		fmt.Printf("======================= case %v =======================\n", i)
		w, opt := prepareTestWorkload(dsn, c.schemaName, c.createTableStmts, c.rawSQLs)
		res, err := SelectIndexAAAlgo(w, w, Parameter{MaximumIndexesToRecommend: c.numIndexes}, opt)
		must(err)
		PrintAdvisorResult(res)

		notEqual := false
		if len(c.expectedIndexes) != len(res.RecommendedIndexes) {
			notEqual = true
		} else {
			sort.Slice(res.RecommendedIndexes, func(i, j int) bool { return res.RecommendedIndexes[i].Key() < res.RecommendedIndexes[j].Key() })
			sort.Slice(c.expectedIndexes, func(i, j int) bool { return c.expectedIndexes[i].Key() < c.expectedIndexes[j].Key() })
			for i := range c.expectedIndexes {
				if c.expectedIndexes[i].Key() != res.RecommendedIndexes[i].Key() {
					notEqual = true
				}
			}
		}

		if notEqual {
			originalCost := workloadCostForTest(opt, w, nil)
			expectedCost := workloadCostForTest(opt, w, c.expectedIndexes)
			actualCost := workloadCostForTest(opt, w, res.RecommendedIndexes)
			fmt.Printf("original cost: %.2E, expected cost: %.2E, actual cost: %.2E\n", originalCost, expectedCost, actualCost)
			fmt.Printf("expected: %v\n", c.expectedIndexes)
			fmt.Printf("actual: %v\n", res.RecommendedIndexes)
			panic("")
		}
	}
}

func TestSimulateAndCost(t *testing.T) {
	_, opt := prepareTestWorkload("", "test",
		[]string{"create table t (a int, b int, c int, d int , e int)"},
		[]string{
			"select * from t where a = 1 and c = 1",
			"select * from t where b = 1 and e = 1",
		})

	opt.CreateHypoIndex(NewIndex("test", "t", "a", "a"))
	plan1, _ := opt.GetPlan("select * from t where a = 1 and c < 1")
	opt.DropHypoIndex(NewIndex("test", "t", "a", "a"))

	for _, p := range plan1 {
		fmt.Println(">> ", p)
	}

	opt.CreateHypoIndex(NewIndex("test", "t", "ac", "a", "c"))
	plan2, _ := opt.GetPlan("select * from t where a = 1 and c < 1")
	opt.DropHypoIndex(NewIndex("test", "t", "ac", "a", "c"))
	for _, p := range plan2 {
		fmt.Println(">> ", p)
	}
}

func TestIndexSelectionAACase(t *testing.T) {
	cases := []indexSelectionCase{
		{
			1, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
			}, []Index{
				newIndex4Test("test.t(a)"),
			},
		},
		{
			2, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
			}, []Index{
				newIndex4Test("test.t(a)"), // only 1 index even if we ask for 2
			},
		},
		{
			1, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 2",
				"select * from t where b = 1",
			}, []Index{
				newIndex4Test("test.t(a)"),
			},
		},
		{
			1, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 2",
				"select * from t where b = 1 and a = 1",
			}, []Index{
				newIndex4Test("test.t(a,b)"),
			},
		},
		{
			2, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 2",
				"select * from t where b = 1 and a = 1",
			}, []Index{
				newIndex4Test("test.t(a,b)"), // only ab is recommended even if we ask for 2
			},
		},
		{
			1, "test", []string{
				"create table t (a int, b int, c int, key(a))",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 2",
				"select * from t where b = 1",
			}, []Index{
				newIndex4Test("test.t(b)"),
			},
		},
		{
			10, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 2",
				"select * from t where b = 1",
			}, []Index{
				newIndex4Test("test.t(a)"),
				newIndex4Test("test.t(b)"),
			},
		},
		{
			2, "test", []string{
				"create table t (a int, b int, c int, d int , e int)",
			}, []string{
				"select * from t where a = 1 and c = 1",
				"select * from t where b = 1 and e = 1",
			}, []Index{
				newIndex4Test("test.t(a,c)"),
				newIndex4Test("test.t(b,e)"),
			},
		},
	}
	testIndexSelection("", cases)
}

func newIndex4Test(key string) Index {
	// test.t(b)
	tmp := strings.Split(key, ".")
	schemaName := tmp[0]
	tmp = strings.Split(tmp[1], "(")
	tableName := tmp[0]
	cols := tmp[1][:len(tmp[1])-1]
	colNames := strings.Split(cols, ",")
	return NewIndex(schemaName, tableName, fmt.Sprintf("%v_%v_%v", schemaName, tableName, strings.Join(colNames, "_")), colNames...)
}
