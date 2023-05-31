package main

import (
	"fmt"
	"math/rand"
)

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(originalWorkloadInfo WorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	originalCost, err := workloadQueryCost(originalWorkloadInfo, optimizer)
	if err != nil {
		return AdvisorResult{}, err
	}

	var indexes []Index
	for _, column := range compressedWorkloadInfo.IndexableColumns.ToList() {
		if rand.Intn(compressedWorkloadInfo.IndexableColumns.Size()) < parameter.MaximumIndexesToRecommend {
			idx := Index{
				SchemaName: column.SchemaName,
				TableName:  column.TableName,
				IndexName:  fmt.Sprintf("key_%v", column.ColumnName),
				Columns:    []Column{column},
			}
			indexes = append(indexes, idx)
			optimizer.CreateHypoIndex(idx)
		}
	}

	optimizedCost, err := workloadQueryCost(originalWorkloadInfo, optimizer)
	return AdvisorResult{
		RecommendedIndexes:    indexes,
		OriginalWorkloadCost:  originalCost,
		OptimizedWorkloadCost: optimizedCost,
	}, err
}
