package main

type IndexSelectionAlgo func(
	workloadInfo WorkloadInfo, // the target workload
	parameter Parameter, // the input parameters
	columns []IndexableColumn, // indexable column candidates
	optimizer WhatIfOptimizer, // the what-if optimizer
) (AdvisorResult, error)

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(workloadInfo WorkloadInfo, parameter Parameter,
	columns []IndexableColumn, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	// TODO
	return AdvisorResult{}, nil
}
