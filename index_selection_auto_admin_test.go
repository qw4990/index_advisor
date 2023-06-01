package main

import "testing"

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

func TestIndexSelectionAACase1(t *testing.T) {
	w, opt := prepareTestWorkload("", "test", []string{
		"create table t (a int, b int, c int)",
	}, []string{
		"select * from t where a = 1",
	})

	res, err := SelectIndexAAAlgo(w, w, Parameter{MaximumIndexesToRecommend: 1}, opt)
	must(err)
	PrintAdvisorResult(res)
}
