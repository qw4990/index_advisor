package advisor

import (
	"fmt"
	"testing"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
)

func prepareTestWorkload(dsn, schemaName string, createTableStmts, rawSQLs []string) (wk.WorkloadInfo, optimizer.WhatIfOptimizer) {
	w := wk.CreateWorkloadFromRawStmt(schemaName, createTableStmts, rawSQLs)
	utils.Must(IndexableColumnsSelectionSimple(&w))
	if dsn == "" {
		dsn = "root:@tcp(127.0.0.1:4000)/"
	}
	opt, err := optimizer.NewTiDBWhatIfOptimizer("root:@tcp(127.0.0.1:4000)/")
	utils.Must(err)
	for _, t := range w.TableSchemas.ToList() {
		utils.Must(opt.Execute("use " + t.SchemaName))
		utils.Must(opt.Execute(t.CreateStmtText))
	}
	return w, opt
}

func TestSimulateAndCost(t *testing.T) {
	_, opt := prepareTestWorkload("", "test",
		[]string{"create table t (a int, b int, c int, d int , e int)"},
		[]string{
			"select * from t where a = 1 and c = 1",
			"select * from t where b = 1 and e = 1",
		})

	opt.CreateHypoIndex(wk.NewIndex("test", "t", "a", "a"))
	plan1, _ := opt.Explain("select * from t where a = 1 and c < 1")
	opt.DropHypoIndex(wk.NewIndex("test", "t", "a", "a"))

	for _, p := range plan1 {
		fmt.Println(">> ", p)
	}

	opt.CreateHypoIndex(wk.NewIndex("test", "t", "ac", "a", "c"))
	plan2, _ := opt.Explain("select * from t where a = 1 and c < 1")
	opt.DropHypoIndex(wk.NewIndex("test", "t", "ac", "a", "c"))
	for _, p := range plan2 {
		fmt.Println(">> ", p)
	}
}
