package main

import "fmt"

var (
	compressAlgorithms = map[string]WorkloadInfoCompressionAlgo{
		"none":       NoneWorkloadInfoCompress,
		"naive":      NaiveWorkloadInfoCompress,
		"clustering": ClusteringWorkloadInfoCompress,
	}

	findIndexableColsAlgorithms = map[string]IndexableColumnsFindingAlgo{
		"simple": FindIndexableColumnsSimple,
	}

	selectIndexAlgorithms = map[string]IndexSelectionAlgo{
		"auto_admin": SelectIndexAAAlgo,
		"example":    SelectIndexExample,
		"genetic":    nil,
	}
)

type Parameter struct {
	MaximumIndexesToRecommend     int
	StorageBudgetInBytes          int
	ConsiderTiFlashReplica        bool // whether to consider recommending TiFlash replica
	ConsiderRemoveExistingIndexes bool // whether to consider removing existing indexes
}

type AdvisorResult struct {
	RecommendedIndexes    []TableIndex
	OriginalWorkloadCost  float64 // the total workload cost without these recommended indexes
	OptimizedWorkloadCost float64 // the total workload cost with these recommended indexes
}

func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn string, originalWorkloadInfo WorkloadInfo, param Parameter) (AdvisorResult, error) {
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
	indexableCols, err := indexable(originalWorkloadInfo)
	if err != nil {
		return AdvisorResult{}, err
	}

	fmt.Println("========================== indexable columns ==========================")
	for _, col := range indexableCols {
		fmt.Println(col.String())
	}

	result, err := selection(originalWorkloadInfo, compressedWorkloadInfo, param, indexableCols, optimizer)
	if err != nil {
		return AdvisorResult{}, err
	}
	fmt.Println("========================== advise result ==========================")
	for _, index := range result.RecommendedIndexes {
		fmt.Println(index.DDL())
	}
	fmt.Println("original workload cost: ", result.OriginalWorkloadCost)
	fmt.Println("optimized workload cost: ", result.OptimizedWorkloadCost)

	return result, err
}
