package advisor

import (
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

// EvaluateIndexConfCost evaluates the workload cost under the given indexes.
func EvaluateIndexConfCost(info utils.WorkloadInfo, optimizer optimizer.WhatIfOptimizer, indexes utils.Set[utils.Index]) utils.IndexConfCost {
	for _, index := range indexes.ToList() {
		utils.Must(optimizer.CreateHypoIndex(index))
	}
	var workloadCost float64
	for _, sql := range info.SQLs.ToList() { // TODO: run them concurrently to save time
		if sql.Type() != utils.SQLTypeSelect {
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
	return utils.IndexConfCost{workloadCost, totCols}
}
