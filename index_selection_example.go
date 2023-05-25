package main

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(workloadInfo WorkloadInfo, parameter Parameter,
	columns []IndexableColumn, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	originalCost, err := workloadQueryCost(workloadInfo, optimizer)
	if err != nil {
		return AdvisorResult{}, err
	}

	optimizedCost, err := workloadQueryCost(workloadInfo, optimizer)
	return AdvisorResult{
		RecommendedIndexes:    []TableIndex{},
		OriginalWorkloadCost:  originalCost,
		OptimizedWorkloadCost: optimizedCost,
	}, err
}
