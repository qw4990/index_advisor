package main

import "fmt"

var (
	compressAlgorithms = map[string]WorkloadInfoCompressionAlgo{
		"none": NoneWorkloadInfoCompress,
	}

	findIndexableColsAlgorithms = map[string]IndexableColumnsFindingAlgo{
		"simple": FindIndexableColumnsSimple,
	}

	selectIndexAlgorithms = map[string]IndexSelectionAlgo{
		"auto_admin": SelectIndexAAAlgo,
	}
)

type Parameter struct {
	NumIndexesToRecommend  int
	StorageBudgetInBytes   int
	ConsiderTiFlashReplica bool
}

type AdvisorResult struct {
}

func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn string, info WorkloadInfo, param Parameter) (AdvisorResult, error) {
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

	info = compress(info)
	indexableCols := indexable(info)
	return selection(info, param, indexableCols, optimizer), nil
}
