package main

type IndexSelectionAlgo func(
	originalWorkloadInfo WorkloadInfo, // the target workload
	compressedWorkloadInfo WorkloadInfo, // the compressed workload
	parameter Parameter, // the input parameters
	columns []Column, // indexable column candidates
	optimizer WhatIfOptimizer, // the what-if optimizer
) (AdvisorResult, error)
