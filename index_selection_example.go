package main

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(workloadInfo WorkloadInfo, parameter Parameter,
	columns []IndexableColumn, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	var originalWorkloadCost float64
	for _, sql := range workloadInfo.SQLs {
		if sql.SQLType != SQLTypeSelect {
			continue
		}
		cost, err := optimizer.GetPlanCost(sql.Text)
		if err != nil {
			return AdvisorResult{}, err
		}
		originalWorkloadCost += cost
	}

	var optimizedWorkloadCost float64
	for _, sql := range workloadInfo.SQLs {
		if sql.SQLType != SQLTypeSelect {
			continue
		}
		cost, err := optimizer.GetPlanCost(sql.Text)
		if err != nil {
			return AdvisorResult{}, err
		}
		optimizedWorkloadCost += cost
	}

	return AdvisorResult{
		RecommendedIndexes:    []TableIndex{},
		OriginalWorkloadCost:  originalWorkloadCost,
		OptimizedWorkloadCost: optimizedWorkloadCost,
	}, nil
}
