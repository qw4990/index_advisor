package advisor

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

// evaluateIndexConfCost evaluates the workload cost under the given indexes.
func evaluateIndexConfCost(info utils.WorkloadInfo, optimizer optimizer.WhatIfOptimizer, indexes utils.Set[utils.Index]) (utils.IndexConfCost, error) {
	for _, index := range indexes.ToList() {
		if err := optimizer.CreateHypoIndex(index); err != nil {
			return utils.IndexConfCost{}, err
		}
	}
	var workloadCost float64
	for _, sql := range info.Queries.ToList() { // TODO: run them concurrently to save time
		if err := optimizer.Execute(`use ` + sql.SchemaName); err != nil {
			return utils.IndexConfCost{}, err
		}
		p, err := optimizer.Explain(sql.Text)
		if err != nil {
			return utils.IndexConfCost{}, err
		}
		workloadCost += p.PlanCost() * float64(sql.Frequency)
	}
	for _, index := range indexes.ToList() {
		if err := optimizer.DropHypoIndex(index); err != nil {
			return utils.IndexConfCost{}, err
		}
	}
	var totCols int
	var keys []string
	for _, index := range indexes.ToList() {
		totCols += len(index.Columns)
		keys = append(keys, index.Key())
	}
	sort.Strings(keys)

	return utils.IndexConfCost{workloadCost, totCols, strings.Join(keys, ",")}, nil
}

var indexID atomic.Int64

// tempIndexName returns a temp index name for the given columns.
func tempIndexName(cols ...utils.Column) string {
	var names []string
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	idxName := fmt.Sprintf("idx_%v", strings.Join(names, "_"))
	if len(idxName) <= 64 {
		return idxName
	}
	return fmt.Sprintf("idx_%v", indexID.Add(1))
}

func checkWorkloadInfo(w utils.WorkloadInfo) {
	for _, col := range w.IndexableColumns.ToList() {
		if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
			panic(fmt.Sprintf("invalid indexable column: %v", col))
		}
	}
	for _, sql := range w.Queries.ToList() {
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
