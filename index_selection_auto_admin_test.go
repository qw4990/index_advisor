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
	numIndexes        int
	schemaName        string
	createTableStmts  []string
	rawSQLs           []string
	expectedIndexKeys []string
}

func testIndexSelection(dsn string, cases []indexSelectionCase) {
	for i, c := range cases {
		fmt.Printf("======================= case %v =======================\n", i)
		w, opt := prepareTestWorkload(dsn, c.schemaName, c.createTableStmts, c.rawSQLs)
		res, err := SelectIndexAAAlgo(w, w, Parameter{MaximumIndexesToRecommend: c.numIndexes}, opt)
		must(err)

		var actualIndexKeys []string
		for _, idx := range res.RecommendedIndexes {
			actualIndexKeys = append(actualIndexKeys, idx.Key())
		}

		sort.Strings(actualIndexKeys)
		sort.Strings(c.expectedIndexKeys)
		if len(actualIndexKeys) != len(c.expectedIndexKeys) {
			panic(fmt.Sprintf("unexpected %v, %v", c.expectedIndexKeys, actualIndexKeys))
		}
		if strings.Join(actualIndexKeys, "|") != strings.Join(c.expectedIndexKeys, "|") {
			panic(fmt.Sprintf("unexpected %v, %v", c.expectedIndexKeys, actualIndexKeys))
		}

		PrintAdvisorResult(res)
	}
}

func TestIndexSelectionAACase1(t *testing.T) {
	cases := []indexSelectionCase{
		{
			1, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
			}, []string{
				"test.t(a)",
			},
		},
		{
			2, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
			}, []string{
				"test.t(a)", // only 1 index even if we ask for 2
			},
		},
		{
			1, "test", []string{
				"create table t (a int, b int, c int)",
			}, []string{
				"select * from t where a = 1",
				"select * from t where a = 1",
				"select * from t where b = 1",
			}, []string{
				"test.t(a)", // a should be selected since it has a higher frequency
			},
		},
	}
	testIndexSelection("", cases)
}
