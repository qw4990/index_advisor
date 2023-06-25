package advisor

import (
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

// IndexSelectionAlgo is the interface for index selection algorithms.
type IndexSelectionAlgo func(
	workloadInfo utils.WorkloadInfo, // the target workload
	parameter Parameter, // the input parameters
	optimizer optimizer.WhatIfOptimizer, // the what-if optimizer
) (utils.Set[utils.Index], error)

// IndexableColumnsSelectionAlgo is the interface for indexable columns selection algorithms.
type IndexableColumnsSelectionAlgo func(workloadInfo *utils.WorkloadInfo) error

// WorkloadInfoCompressionAlgo is the interface for workload info compression algorithms.
type WorkloadInfoCompressionAlgo func(workloadInfo utils.WorkloadInfo) utils.WorkloadInfo

var (
	compressAlgorithms = map[string]WorkloadInfoCompressionAlgo{
		"none":   NoneWorkloadInfoCompress,
		"digest": DigestWorkloadInfoCompress,
	}

	findIndexableColsAlgorithms = map[string]IndexableColumnsSelectionAlgo{
		"simple": IndexableColumnsSelectionSimple,
	}

	selectIndexAlgorithms = map[string]IndexSelectionAlgo{
		"auto_admin": SelectIndexAAAlgo,
	}
)

// Parameter is the input parameters of index advisor.
type Parameter struct {
	MaxNumberIndexes int // the max number of indexes to recommend
	MaxIndexWidth    int // the max number of columns in recommended indexes
}

func validateParameter(p Parameter) Parameter {
	if p.MaxNumberIndexes < 1 {
		utils.Warningf("max number of indexes should be at least 1, set from %v to 1", p.MaxNumberIndexes)
		p.MaxNumberIndexes = 1
	}
	if p.MaxNumberIndexes > 10 {
		utils.Warningf("max number of indexes should be at most 10, set from %v to 10", p.MaxNumberIndexes)
		p.MaxNumberIndexes = 10
	}
	if p.MaxIndexWidth < 1 {
		utils.Warningf("max index width should be at least 1, set from %v to 1", p.MaxIndexWidth)
		p.MaxIndexWidth = 1
	}
	if p.MaxIndexWidth > 5 {
		utils.Warningf("max index width should be at most 5, set from %v to 5", p.MaxIndexWidth)
		p.MaxIndexWidth = 5
	}
	return p
}

// IndexAdvise is the entry point of index advisor.
func IndexAdvise(db optimizer.WhatIfOptimizer, originalWorkloadInfo utils.WorkloadInfo, param Parameter) (utils.Set[utils.Index], error) {
	utils.Debugf("starting index advise")
	param = validateParameter(param)

	compress := compressAlgorithms["none"]
	indexable := findIndexableColsAlgorithms["simple"]
	selection := selectIndexAlgorithms["auto_admin"]

	compressedWorkloadInfo := compress(originalWorkloadInfo)

	if err := indexable(&compressedWorkloadInfo); err != nil {
		return nil, err
	}
	utils.Debugf("finding %v indexable columns", compressedWorkloadInfo.IndexableColumns.Size())

	checkWorkloadInfo(compressedWorkloadInfo)
	recommendedIndexes, err := selection(compressedWorkloadInfo, param, db)
	return recommendedIndexes, err
}
