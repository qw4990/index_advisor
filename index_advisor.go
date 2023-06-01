package main

import "fmt"

// IndexSelectionAlgo is the interface for index selection algorithms.
type IndexSelectionAlgo func(
	originalWorkloadInfo WorkloadInfo, // the target workload
	compressedWorkloadInfo WorkloadInfo, // the compressed workload
	parameter Parameter, // the input parameters
	optimizer WhatIfOptimizer, // the what-if optimizer
) (AdvisorResult, error)

// IndexableColumnsSelectionAlgo is the interface for indexable columns selection algorithms.
type IndexableColumnsSelectionAlgo func(workloadInfo *WorkloadInfo) error

// WorkloadInfoCompressionAlgo is the interface for workload info compression algorithms.
type WorkloadInfoCompressionAlgo func(workloadInfo WorkloadInfo) WorkloadInfo

var (
	compressAlgorithms = map[string]WorkloadInfoCompressionAlgo{
		"none":       NoneWorkloadInfoCompress,
		"naive":      NaiveWorkloadInfoCompress,
		"clustering": ClusteringWorkloadInfoCompress,
	}

	findIndexableColsAlgorithms = map[string]IndexableColumnsSelectionAlgo{
		"simple": IndexableColumnsSelectionSimple,
	}

	selectIndexAlgorithms = map[string]IndexSelectionAlgo{
		"auto_admin": SelectIndexAAAlgo,
		"example":    SelectIndexExample,
		"genetic":    nil,
	}
)

type Parameter struct {
	MaximumIndexesToRecommend int
	//StorageBudgetInBytes          int
	//ConsiderTiFlashReplica        bool // whether to consider recommending TiFlash replica
	//ConsiderRemoveExistingIndexes bool // whether to consider removing existing indexes
}

type AdvisorResult struct {
	RecommendedIndexes    []Index
	OriginalWorkloadCost  float64 // the total workload cost without these recommended indexes
	OptimizedWorkloadCost float64 // the total workload cost with these recommended indexes
}

func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn string, originalWorkloadInfo WorkloadInfo, param Parameter) (AdvisorResult, error) {
	Debugf("starting index advise with compress algorithm %s, indexable algorithm %s, index selection algorithm %s", compressAlgo, indexableAlgo, selectionAlgo)

	compress, ok := compressAlgorithms[compressAlgo]
	if !ok {
		return AdvisorResult{}, fmt.Errorf("compress algorithm %s not found", compressAlgo)
	}

	indexable, ok := findIndexableColsAlgorithms[indexableAlgo]
	if !ok {
		return AdvisorResult{}, fmt.Errorf("indexable algorithm %s not found", indexableAlgo)
	}

	selection, ok := selectIndexAlgorithms[selectionAlgo]
	if !ok {
		return AdvisorResult{}, fmt.Errorf("selection algorithm %s not found", selectionAlgo)
	}

	optimizer, err := NewTiDBWhatIfOptimizer(dsn)
	if err != nil {
		return AdvisorResult{}, err
	}

	compressedWorkloadInfo := compress(originalWorkloadInfo)
	Debugf("compressing workload info from %v SQLs to %v SQLs", originalWorkloadInfo.SQLs.Size(), compress(originalWorkloadInfo).SQLs.Size())

	must(indexable(&compressedWorkloadInfo))
	must(indexable(&originalWorkloadInfo))
	Debugf("finding %v indexable columns", compressedWorkloadInfo.IndexableColumns.Size())

	checkWorkloadInfo(compressedWorkloadInfo)
	checkWorkloadInfo(originalWorkloadInfo)
	result, err := selection(originalWorkloadInfo, compressedWorkloadInfo, param, optimizer)
	must(err)

	PrintAdvisorResult(result)
	return result, err
}

func PrintAdvisorResult(result AdvisorResult) {
	for _, index := range result.RecommendedIndexes {
		fmt.Println(index.DDL())
	}
	fmt.Printf("original workload cost: %.2E\n", result.OriginalWorkloadCost)
	fmt.Printf("optimized workload cost: %.2E\n", result.OptimizedWorkloadCost)
}
