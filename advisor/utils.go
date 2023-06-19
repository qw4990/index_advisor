package advisor

import (
	"fmt"
	"strings"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
)

// evaluateIndexConfCost evaluates the workload cost under the given indexes.
func evaluateIndexConfCost(info wk.WorkloadInfo, optimizer optimizer.WhatIfOptimizer, indexes utils.Set[wk.Index]) wk.IndexConfCost {
	for _, index := range indexes.ToList() {
		utils.Must(optimizer.CreateHypoIndex(index))
	}
	var workloadCost float64
	for _, sql := range info.SQLs.ToList() { // TODO: run them concurrently to save time
		if sql.Type() != wk.SQLTypeSelect {
			continue
		}
		utils.Must(optimizer.Execute(`use ` + sql.SchemaName))
		p, err := optimizer.Explain(sql.Text)
		utils.Must(err, sql.Text)
		workloadCost += p.PlanCost() * float64(sql.Frequency)
	}
	for _, index := range indexes.ToList() {
		utils.Must(optimizer.DropHypoIndex(index))
	}
	var totCols int
	for _, index := range indexes.ToList() {
		totCols += len(index.Columns)
	}
	return wk.IndexConfCost{workloadCost, totCols}
}

// tempIndexName returns a temp index name for the given columns.
func tempIndexName(cols ...wk.Column) string {
	var names []string
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	return fmt.Sprintf("idx_%v", strings.Join(names, "_"))
}

func checkWorkloadInfo(w wk.WorkloadInfo) {
	for _, col := range w.IndexableColumns.ToList() {
		if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
			panic(fmt.Sprintf("invalid indexable column: %v", col))
		}
	}
	for _, sql := range w.SQLs.ToList() {
		if sql.SchemaName == "" || sql.Text == "" {
			panic(fmt.Sprintf("invalid sql: %v", sql))
		}
		for _, col := range sql.IndexableColumns.ToList() {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
	}
	for _, tbl := range w.TableSchemas.ToList() {
		if tbl.SchemaName == "" || tbl.TableName == "" {
			panic(fmt.Sprintf("invalid table schema: %v", tbl))
		}
		for _, col := range tbl.Columns {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" || col.ColumnType == nil {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
		for _, idx := range tbl.Indexes {
			if idx.SchemaName == "" || idx.TableName == "" || idx.IndexName == "" {
				panic(fmt.Sprintf("invalid index: %v", idx))
			}
			for _, col := range idx.Columns {
				if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
					panic(fmt.Sprintf("invalid indexable column: %v", col))
				}
			}
		}
	}
}